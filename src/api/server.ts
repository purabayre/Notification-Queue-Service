import express from "express";
import dotenv from "dotenv";
import crypto from "crypto";
dotenv.config();

import { connectMongo } from "../config/mongo";
import { flowProducer, notificationQueue } from "../queue/notificationQueue";
import { analyticsQueue } from "../queue/analyticsQueue";
import { scheduleDailyDigest } from "../queue/digestScheduler";
import { NotificationModel } from "../models/Notification";
import { ExpressAdapter } from "@bull-board/express";
import { createBullBoard } from "@bull-board/api";
import { BullMQAdapter } from "@bull-board/api/bullMQAdapter";

const app = express();
app.use(express.json());
const PORT = process.env.PORT || 3000;

const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath("/admin/queues");

createBullBoard({
  queues: [
    new BullMQAdapter(notificationQueue),
    new BullMQAdapter(analyticsQueue),
  ],
  serverAdapter,
});

app.use("/admin/queues", serverAdapter.getRouter());

app.get("/health", (req, res) => {
  res.send("server is healthy");
});

app.post("/notifications", async (req, res) => {
  try {
    const { to, channel, body, delayMs } = req.body;

    if (!to || !channel || !body) {
      return res.status(400).json({
        error: "to, channel and body are required",
      });
    }

    // Task §4.1: Validate inputs
    if (typeof body !== "string" || body.trim().length === 0) {
      return res.status(400).json({
        error: "body must be a non-empty string",
      });
    }

    const MAX_BODY_CHARS = 10_000;

    if (body.length > MAX_BODY_CHARS) {
      return res.status(400).json({
        error: `body exceeds max length of ${MAX_BODY_CHARS} characters`,
      });
    }

    const toStr = String(to).trim();

    if (toStr.length === 0) {
      return res.status(400).json({
        error: "to must be a non-empty string",
      });
    }

    const allowedChannels = ["email", "sms", "push"];

    if (!allowedChannels.includes(channel)) {
      return res.status(400).json({
        error: "Invalid channel",
      });
    }

    // Validate `to` shape based on `channel`.
    const MAX_TO_CHARS = 2000;

    if (toStr.length > MAX_TO_CHARS) {
      return res.status(400).json({
        error: `to exceeds max length of ${MAX_TO_CHARS} characters`,
      });
    }

    if (channel === "email") {
      // Simple, pragmatic email validation.
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

      if (!emailRegex.test(toStr)) {
        return res.status(400).json({
          error: "to must be a valid email address when channel=email",
        });
      }
    } else if (channel === "sms") {
      // Require a phone-ish identifier: digits with optional leading '+'
      // and a sensible length range.
      const phoneRegex = /^\+?[0-9]{7,15}$/;

      if (!phoneRegex.test(toStr)) {
        return res.status(400).json({
          error: "to must be a valid phone number when channel=sms",
        });
      }
    } else if (channel === "push") {
      // For push we just require an identifier-like string.
      const pushRegex = /^[A-Za-z0-9_.:-]{1,2000}$/;

      if (!pushRegex.test(toStr)) {
        return res.status(400).json({
          error: "to must be a valid push identifier",
        });
      }
    }

    if (delayMs !== undefined) {
      const parsedDelay =
        typeof delayMs === "number" ? delayMs : Number(delayMs);

      const MAX_DELAY_MS = 60 * 60 * 1000; // 1 hour safety cap for the demo

      if (
        !Number.isFinite(parsedDelay) ||
        !Number.isInteger(parsedDelay) ||
        parsedDelay < 0 ||
        parsedDelay > MAX_DELAY_MS
      ) {
        return res.status(400).json({
          error: `delayMs must be an integer >= 0 and <= ${MAX_DELAY_MS}`,
        });
      }
    }

    const notificationJobId = `notification-${crypto.randomUUID()}`;

    // Fix C1: create the Mongo document BEFORE enqueuing the job/flow.
    // Also make the jobId unique for the *child job* so the worker can find it.

    const existingNotification = await NotificationModel.findOne({
      jobId: notificationJobId,
    });

    if (existingNotification) {
      return res.status(409).json({
        error: "Duplicate notification job",
      });
    }

    await NotificationModel.create({
      jobId: notificationJobId,
      to,
      channel,
      body,
      status: "queued",
      attempts: [],
    });

    await flowProducer.add({
      name: "log-success",
      queueName: "analytics",
      data: {
        notificationJobId,
        status: "sent",
      },
      opts: {
        removeOnComplete: false,
        removeOnFail: false,
      },
      children: [
        {
          name: "send-notification",
          queueName: "notifications",
          data: {
            to,
            channel,
            body,
          },
          opts: {
            // Critical: worker looks up by job.id, which is the child jobId.
            jobId: notificationJobId,
            delay: delayMs === undefined ? 0 : delayMs,
            attempts: 3,
            backoff: {
              type: "exponential",
              delay: 2000,
            },
            removeOnComplete: false,
            removeOnFail: false,
          },
        },
      ],
    });

    return res.status(202).json({
      message: "Notification queued",
      jobId: notificationJobId,
    });
  } catch (error) {
    console.error(error);

    return res.status(500).json({
      error: "Internal server error",
    });
  }
});

app.get("/notifications/:jobId", async (req, res) => {
  try {
    const notification = await NotificationModel.findOne({
      jobId: req.params.jobId,
    });

    if (!notification) {
      return res.status(404).json({
        error: "Notification not found",
      });
    }

    return res.json(notification);
  } catch (error) {
    console.error(error);

    return res.status(500).json({
      error: "Internal server error",
    });
  }
});

app.post("/notifications/repeat", async (req, res) => {
  try {
    const { to, channel, body, pattern } = req.body;

    if (!to || !channel || !body || !pattern) {
      return res.status(400).json({
        error: "to, channel, body, and pattern (cron) are required",
      });
    }

    const allowedChannels = ["email", "sms", "push"];

    if (!allowedChannels.includes(channel)) {
      return res.status(400).json({
        error: "Invalid channel",
      });
    }

    // Fix C2: use a distinct job name for repeating executions.
    // Worker will handle `send-recurring` separately.

    const repeatableJob = await notificationQueue.add(
      "send-recurring",
      {
        to,
        channel,
        body,
      },
      {
        repeat: {
          pattern,
        },
        jobId: `repeatable-${Date.now()}`,
      },
    );

    return res.status(202).json({
      message: "Repeatable job created",
      jobId: repeatableJob.id,
      pattern,
    });
  } catch (error) {
    console.error(error);

    return res.status(500).json({
      error: "Internal server error",
    });
  }
});

app.get("/admin/repeatable", async (req, res) => {
  try {
    const repeatableJobs = await notificationQueue.getRepeatableJobs();

    return res.json(repeatableJobs);
  } catch (error) {
    console.error(error);

    return res.status(500).json({
      error: "Internal server error",
    });
  }
});

app.delete("/admin/repeatable/:jobKey", async (req, res) => {
  try {
    await notificationQueue.removeRepeatableByKey(req.params.jobKey);

    return res.json({
      message: "Repeatable job removed",
    });
  } catch (error) {
    console.error(error);

    return res.status(500).json({
      error: "Internal server error",
    });
  }
});

async function startServer() {
  try {
    await connectMongo();

    // M1: schedule digest controlled by env
    // Enable by setting ENABLE_DIGEST=true (default: true)

    const enableDigest =
      (process.env.ENABLE_DIGEST || "true").toLowerCase() === "true";

    if (enableDigest) {
      await scheduleDailyDigest();
    }

    const server = app.listen(PORT, () => {
      console.log(
        `server running on ${PORT},check health on http://localhost:${PORT}/health`,
      );
    });
  } catch (error) {
    console.error("Failed to start server", error);
  }
}
startServer();
