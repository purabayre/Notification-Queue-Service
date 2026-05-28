import { Worker, Job } from "bullmq";
import dotenv from "dotenv";
import { redisConnection } from "../config/redis";
import { connectMongo } from "../config/mongo";
import { NotificationModel } from "../models/Notification";
import { fakeSender } from "../services/fakeSender";
import { analyticsQueue } from "../queue/analyticsQueue";
dotenv.config();
async function startWorker() {
  await connectMongo();

  const worker = new Worker(
    "notifications",
    async (job: Job) => {
      // Handle repeatable daily digest job separately
      if (job.name === "send-digest") {
        console.log(`Processing digest job ${job.id}`);
        // M3 + M7: digest recipient/channel come from env (with defaults)
        const to =
          process.env.DIGEST_TO || process.env.ADMIN || "admin@admin.com";
        const channel = (process.env.DIGEST_CHANNEL || "email") as any;

        const since = new Date(Date.now() - 24 * 60 * 60 * 1000);
        const items = await NotificationModel.find({
          createdAt: { $gte: since },
        }).lean();
        const lines = items.map(
          (n: any) => `${n.to} [${n.channel}] ${n.status}: ${n.body}`,
        );
        const body =
          `Daily digest: ${items.length} notifications\n\n` + lines.join("\n");
        await fakeSender(to, channel, body);
        await analyticsQueue.add(
          "log-digest",
          { count: items.length },
          { jobId: `analytics-digest-${job.id}` },
        );
        console.log(`Digest job ${job.id} completed`);
        return;
      }

      console.log(`Processing job ${job.id} (${job.name})`);

      // Fix C2: recurring jobs are enqueued without a Mongo record beforehand.
      if (job.name === "send-recurring") {
        // Create a notification record per execution to keep behavior consistent.
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
          notification.attempts.push({
            timestamp: new Date(),
            success: true,
          });
          notification.status = "sent";
          await notification.save();
          console.log(`Recurring job ${job.id} completed`);
          return;
        } catch (error) {
          const err = error as Error;
          notification.attempts.push({
            timestamp: new Date(),
            success: false,
            errorMessage: err.message,
          });

          // Fix C3: set status=failed on final attempt inside the processor.
          const maxAttempts = job.opts.attempts ?? 1;
          const nextAttemptNumber = job.attemptsMade + 1;
          if (nextAttemptNumber >= maxAttempts) {
            notification.status = "failed";
          } else {
            notification.status = "processing";
          }
          await notification.save();
          console.log(
            `Recurring job ${job.id} failed on attempt ${nextAttemptNumber}`,
          );
          throw error;
        }
      }

      // Default path for one-off jobs created by the API.
      const notification = await NotificationModel.findOne({
        jobId: String(job.id),
      });
      if (!notification) {
        throw new Error("Notification record not found");
      }
      if (notification.status === "sent") {
        console.log(`Job ${job.id} already processed`);
        return;
      }

      notification.status = "processing";
      await notification.save();

      try {
        await fakeSender(job.data.to, job.data.channel, job.data.body);
        notification.attempts.push({
          timestamp: new Date(),
          success: true,
        });
        notification.status = "sent";
        await notification.save();

        console.log(`Job ${job.id} completed`);
      } catch (error) {
        const err = error as Error;
        notification.attempts.push({
          timestamp: new Date(),
          success: false,
          errorMessage: err.message,
        });

        // Fix C3: mark failed on final attempt before throwing.
        const maxAttempts = job.opts.attempts ?? 1;
        const nextAttemptNumber = job.attemptsMade + 1;
        if (nextAttemptNumber >= maxAttempts) {
          notification.status = "failed";
        } else {
          notification.status = "processing";
        }

        await notification.save();
        console.log(`Job ${job.id} failed on attempt ${nextAttemptNumber}`);
        throw error;
      }
    },
    {
      connection: redisConnection,
    },
  );
  worker.on("failed", async (job, error) => {
    if (!job) return;

    const maxAttempts = job.opts.attempts || 1;
    const isFinalFailure = job.attemptsMade >= maxAttempts;

    // Fix C3 (race): do not update Mongo status in the `failed` listener.
    // The processor sets `status: failed` on the final attempt before throwing.
    if (isFinalFailure) {
      console.log(
        `Job ${job.id} ultimately failed after ${job.attemptsMade} attempts`,
      );

      // Create child analytics job for failure
      await analyticsQueue.add(
        "log-failure",
        {
          notificationJobId: String(job.id),
          status: "failed",
        },
        {
          jobId: `analytics-${job.id}-failure`,
        },
      );
    } else {
      console.log(
        `Job ${job.id} failed on attempt ${job.attemptsMade}, retrying...`,
      );
    }
  });

  worker.on("completed", (job) => {
    console.log(`Job ${job.id} completed successfully`);
  });
  const shutdown = async () => {
    console.log("Shutting down worker...");
    await worker.close();
    console.log("Worker closed gracefully");
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}
startWorker().catch((error) => {
  console.error("Worker failed to start", error);
});
