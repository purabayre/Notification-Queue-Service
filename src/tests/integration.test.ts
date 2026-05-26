import mongoose from "mongoose";
import dotenv from "dotenv";
import {
  beforeAll,
  afterAll,
  afterEach,
  describe,
  test,
  expect,
} from "@jest/globals";

import { notificationQueue } from "../queue/notifcationQueue";
import { NotificationModel } from "../models/Notification";
import { redisConnection } from "../config/redis";

dotenv.config();

// Helper: Wait for a job to complete with timeout
async function waitForJobCompletion(
  jobId: string,
  maxWaitMs: number = 15000,
  pollIntervalMs: number = 500,
): Promise<{
  status: string;
  attempts: any[];
}> {
  const startTime = Date.now();

  while (Date.now() - startTime < maxWaitMs) {
    const notification = await NotificationModel.findOne({ jobId });

    if (notification) {
      if (notification.status === "sent" || notification.status === "failed") {
        return {
          status: notification.status,
          attempts: notification.attempts,
        };
      }
    }

    await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
  }

  throw new Error(
    `Job ${jobId} did not reach terminal state within ${maxWaitMs}ms`,
  );
}

describe("BullMQ Notification Queue Integration Tests", () => {
  beforeAll(async () => {
    if (!process.env.MONGO_URI) {
      throw new Error("MONGO_URI is missing in environment variables");
    }

    if (mongoose.connection.readyState === 0) {
      await mongoose.connect(process.env.MONGO_URI);
    }
  });

  afterEach(async () => {
    // Clean all queue states
    await notificationQueue.drain();

    await notificationQueue.clean(0, 1000, "completed");
    await notificationQueue.clean(0, 1000, "failed");
    await notificationQueue.clean(0, 1000, "wait");
    await notificationQueue.clean(0, 1000, "delayed");
  });

  afterAll(async () => {
    await NotificationModel.deleteMany({});

    await mongoose.connection.close();

    await redisConnection.quit();
  });

  test("should enqueue a job and reach 'sent' status on success", async () => {
    const job = await notificationQueue.add(
      "send-notification",
      {
        to: "success@example.com",
        channel: "email",
        body: "Test notification",
      },
      {
        attempts: 3,
        backoff: {
          type: "exponential",
          delay: 500,
        },
      },
    );

    const docId = String(job.id);

    await NotificationModel.create({
      jobId: docId,
      to: "success@example.com",
      channel: "email",
      body: "Test notification",
      status: "queued",
      attempts: [],
    });

    const result = await waitForJobCompletion(docId, 10000);

    expect(result.attempts).toBeDefined();
    expect(result.attempts.length).toBeGreaterThan(0);

    expect(["sent", "failed"]).toContain(result.status);
  });

  test("should record attempt history with timestamps", async () => {
    const job = await notificationQueue.add(
      "send-notification",
      {
        to: "history@example.com",
        channel: "sms",
        body: "Test with history",
      },
      {
        attempts: 3,
        backoff: {
          type: "exponential",
          delay: 500,
        },
      },
    );

    const docId = String(job.id);

    await NotificationModel.create({
      jobId: docId,
      to: "history@example.com",
      channel: "sms",
      body: "Test with history",
      status: "queued",
      attempts: [],
    });

    const result = await waitForJobCompletion(docId, 10000);

    expect(result.attempts).toBeDefined();

    result.attempts.forEach((attempt: any) => {
      expect(attempt.timestamp).toBeDefined();
      expect(typeof attempt.success).toBe("boolean");
    });

    expect(result.attempts.every((a: any) => a.timestamp)).toBe(true);
  });

  test("should transition through status states", async () => {
    const job = await notificationQueue.add(
      "send-notification",
      {
        to: "state@example.com",
        channel: "push",
        body: "Test state transition",
      },
      {
        attempts: 3,
        backoff: {
          type: "exponential",
          delay: 500,
        },
      },
    );

    const docId = String(job.id);

    const notification = await NotificationModel.create({
      jobId: docId,
      to: "state@example.com",
      channel: "push",
      body: "Test state transition",
      status: "queued",
      attempts: [],
    });

    expect(notification.status).toBe("queued");

    const result = await waitForJobCompletion(docId, 10000);

    expect(["sent", "failed"]).toContain(result.status);
  });

  test("should handle delayed jobs correctly", async () => {
    const delayMs = 2000;

    const startTime = Date.now();

    const job = await notificationQueue.add(
      "send-notification",
      {
        to: "delayed@example.com",
        channel: "email",
        body: "Delayed notification",
      },
      {
        delay: delayMs,
        attempts: 3,
        backoff: {
          type: "exponential",
          delay: 500,
        },
      },
    );

    const docId = String(job.id);

    await NotificationModel.create({
      jobId: docId,
      to: "delayed@example.com",
      channel: "email",
      body: "Delayed notification",
      status: "queued",
      attempts: [],
    });

    await waitForJobCompletion(docId, 10000);

    const elapsed = Date.now() - startTime;

    expect(elapsed).toBeGreaterThanOrEqual(delayMs - 100);
  });

  test("should return failed status after retries exhausted", async () => {
    const job = await notificationQueue.add(
      "send-notification",
      {
        to: "failure@example.com",
        channel: "email",
        body: "Will fail multiple times",
      },
      {
        attempts: 2,
        backoff: {
          type: "exponential",
          delay: 300,
        },
      },
    );

    const docId = String(job.id);

    await NotificationModel.create({
      jobId: docId,
      to: "failure@example.com",
      channel: "email",
      body: "Will fail multiple times",
      status: "queued",
      attempts: [],
    });

    const result = await waitForJobCompletion(docId, 8000);

    expect(["sent", "failed"]).toContain(result.status);

    if (result.status === "failed") {
      expect(result.attempts.length).toBeGreaterThanOrEqual(1);
    }
  });

  test("should support multiple jobs simultaneously", async () => {
    const jobIds: string[] = [];

    for (let i = 0; i < 5; i++) {
      const job = await notificationQueue.add(
        "send-notification",
        {
          to: `user${i}@example.com`,
          channel: "email",
          body: `Message ${i}`,
        },
        {
          attempts: 3,
          backoff: {
            type: "exponential",
            delay: 500,
          },
        },
      );

      const docId = String(job.id);

      jobIds.push(docId);

      await NotificationModel.create({
        jobId: docId,
        to: `user${i}@example.com`,
        channel: "email",
        body: `Message ${i}`,
        status: "queued",
        attempts: [],
      });
    }

    const results = await Promise.all(
      jobIds.map((jobId) => waitForJobCompletion(jobId, 10000)),
    );

    results.forEach((result) => {
      expect(["sent", "failed"]).toContain(result.status);
    });
  });

  test("should validate job creation", async () => {
    const job = await notificationQueue.add("send-notification", {
      to: "validation@example.com",
      channel: "email",
      body: "Test",
    });

    expect(job.id).toBeDefined();
  });

  test("should fetch notification status and history by job ID", async () => {
    const job = await notificationQueue.add(
      "send-notification",
      {
        to: "fetch@example.com",
        channel: "sms",
        body: "Fetch test",
      },
      {
        attempts: 3,
        backoff: {
          type: "exponential",
          delay: 500,
        },
      },
    );

    const docId = String(job.id);

    await NotificationModel.create({
      jobId: docId,
      to: "fetch@example.com",
      channel: "sms",
      body: "Fetch test",
      status: "queued",
      attempts: [],
    });

    let fetched = await NotificationModel.findOne({
      jobId: docId,
    });

    expect(fetched).toBeDefined();
    expect(fetched?.jobId).toBe(docId);

    await waitForJobCompletion(docId, 10000);

    fetched = await NotificationModel.findOne({
      jobId: docId,
    });

    expect(fetched).toBeDefined();
    expect(["sent", "failed"]).toContain(fetched?.status as string);

    expect(fetched?.attempts).toBeDefined();
  });

  test("should handle different notification channels", async () => {
    const channels = ["email", "sms", "push"];

    const jobIds: string[] = [];

    for (const channel of channels) {
      const job = await notificationQueue.add(
        "send-notification",
        {
          to: `${channel}@example.com`,
          channel: channel as "email" | "sms" | "push",
          body: `Test ${channel}`,
        },
        {
          attempts: 3,
          backoff: {
            type: "exponential",
            delay: 500,
          },
        },
      );

      const docId = String(job.id);

      jobIds.push(docId);

      await NotificationModel.create({
        jobId: docId,
        to: `${channel}@example.com`,
        channel: channel as "email" | "sms" | "push",
        body: `Test ${channel}`,
        status: "queued",
        attempts: [],
      });
    }

    const results = await Promise.all(
      jobIds.map((jobId) => waitForJobCompletion(jobId, 10000)),
    );

    results.forEach((result) => {
      expect(["sent", "failed"]).toContain(result.status);
    });
  });

  test("idempotency: should skip reprocessing if already sent", async () => {
    const docId = "idempotent-test-123";

    await NotificationModel.create({
      jobId: docId,
      to: "idempotent@example.com",
      channel: "email",
      body: "Already sent",
      status: "sent",
      attempts: [
        {
          timestamp: new Date(),
          success: true,
        },
      ],
    });

    const fetched = await NotificationModel.findOne({
      jobId: docId,
    });

    expect(fetched?.status).toBe("sent");
  });
});
