import { Worker, Job } from "bullmq";
import { redisConnection } from "../config/redis";
import { connectMongo } from "../config/mongo";
import { NotificationModel } from "../models/Notification";
import { fakeSender } from "../services/fakeSender";
import { analyticsQueue } from "../queue/analyticsQueue";
import { Worker as BullWorker } from "bullmq";

export async function createTestWorker(): Promise<BullWorker> {
  await connectMongo();

  const worker = new Worker(
    "notifications",
    async (job: Job) => {
      if (job.name === "send-digest") {
        // keep same behavior as src/worker/worker.ts, but tests don’t rely on this
        return;
      }

      if (job.name === "send-recurring") {
        const notificationJobId = `recurring-${String(job.id)}`;
        const notification = await NotificationModel.create({
          jobId: notificationJobId,
          to: job.data.to,
          channel: job.data.channel,
          body: job.data.body,
          status: "processing",
          attempts: [],
        });

        try {
          await fakeSender(job.data.to, job.data.channel, job.data.body);
          notification.attempts.push({ timestamp: new Date(), success: true });
          notification.status = "sent";
          await notification.save();
          return;
        } catch (error) {
          const err = error as Error;
          notification.attempts.push({
            timestamp: new Date(),
            success: false,
            errorMessage: err.message,
          });

          const maxAttempts = job.opts.attempts ?? 1;
          const nextAttemptNumber = job.attemptsMade + 1;
          notification.status =
            nextAttemptNumber >= maxAttempts ? "failed" : "processing";
          await notification.save();
          throw error;
        }
      }

      const notification = await NotificationModel.findOne({
        jobId: String(job.id),
      });
      if (!notification) throw new Error("Notification record not found");
      if (notification.status === "sent") return;

      // Keep updates idempotent and resilient to repeated execution.
      notification.status = "processing";
      await notification.save();

      try {
        await fakeSender(job.data.to, job.data.channel, job.data.body);
        notification.attempts.push({ timestamp: new Date(), success: true });
        notification.status = "sent";
        await notification.save();
      } catch (error) {
        const err = error as Error;
        notification.attempts.push({
          timestamp: new Date(),
          success: false,
          errorMessage: err.message,
        });

        const maxAttempts = job.opts.attempts ?? 1;
        const nextAttemptNumber = job.attemptsMade + 1;
        notification.status =
          nextAttemptNumber >= maxAttempts ? "failed" : "processing";
        await notification.save();

        throw error;
      }
    },
    { connection: redisConnection },
  );

  worker.on("failed", async (job) => {
    if (!job) return;
    const maxAttempts = job.opts.attempts || 1;
    const isFinalFailure = job.attemptsMade >= maxAttempts;

    if (isFinalFailure) {
      await NotificationModel.findOneAndUpdate(
        { jobId: String(job.id), status: { $ne: "failed" } },
        { $set: { status: "failed" } },
      );

      await analyticsQueue.add(
        "log-failure",
        { notificationJobId: String(job.id), status: "failed" },
        { jobId: `analytics-${job.id}-failure` },
      );
    }
  });

  return worker;
}
