import express from "express";
import dotenv from "dotenv";
dotenv.config();

import { connectMongo } from "../config/mongo";
import { flowProducer, notificationQueue } from "../queue/notifcationQueue";
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

    const allowedChannels = ["email", "sms", "push"];
    if (!allowedChannels.includes(channel)) {
      return res.status(400).json({
        error: "Invalid channel",
      });
    }

    const notificationJobId = `notification-${Date.now()}-${Math.random()
      .toString(36)
      .slice(2, 8)}`;

    const flow = await flowProducer.add({
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
            jobId: notificationJobId,
            delay: delayMs || 0,
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

    await NotificationModel.create({
      jobId: notificationJobId,
      to,
      channel,
      body,
      status: "queued",
      attempts: [],
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
    // Add repeatable job
    const repeatableJob = await notificationQueue.add(
      "send-notification",
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
    // Schedule daily digest repeatable job
    await scheduleDailyDigest();
    app.listen(PORT, () => {
      console.log(
        `server running on ${PORT},check health on http://localhost:${PORT}/health`,
      );
    });
  } catch (error) {
    console.error("Failed to start server", error);
  }
}
startServer();
