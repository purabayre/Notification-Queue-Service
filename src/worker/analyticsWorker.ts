import { Worker, Job } from "bullmq";
import dotenv from "dotenv";
import { redisConnection } from "../config/redis";
import { analyticsQueue } from "../queue/analyticsQueue";

dotenv.config();

async function startAnalyticsWorker() {
  const worker = new Worker(
    "analytics",
    async (job: Job) => {
      console.log(`Processing analytics job ${job.id}`);
      const { notificationJobId, status } = job.data;

      // Simulate analytics processing
      console.log(
        `Recording analytics: notification ${notificationJobId} → ${status}`,
      );

      await new Promise((resolve) => setTimeout(resolve, 1000));

      console.log(`Analytics job ${job.id} completed`);
    },
    {
      connection: redisConnection,
    },
  );

  worker.on("failed", (job, error) => {
    if (!job) return;
    console.log(`Analytics job ${job.id} failed: ${error?.message}`);
  });

  worker.on("completed", (job) => {
    console.log(`Analytics job ${job.id} completed successfully`);
  });

  const shutdown = async () => {
    console.log("Shutting down analytics worker...");
    await worker.close();
    console.log("Analytics worker closed gracefully");
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

startAnalyticsWorker().catch((error) => {
  console.error("Analytics worker failed to start", error);
});
