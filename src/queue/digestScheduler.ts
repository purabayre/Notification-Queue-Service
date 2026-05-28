import { notificationQueue } from "./notificationQueue";

export async function scheduleDailyDigest() {
  // M2: configurable cron (default daily at midnight)
  const pattern = process.env.DIGEST_CRON || "0 0 * * *";

  try {
    // Remove old repeatable jobs
    const repeatableJobs = await notificationQueue.getRepeatableJobs();

    for (const job of repeatableJobs) {
      if (job.name === "send-digest") {
        await notificationQueue.removeRepeatableByKey(job.key);
        console.log(`Removed old repeat job: ${job.key}`);
      }
    }

    // Add new repeatable job
    await notificationQueue.add(
      "send-digest",
      {},
      {
        repeat: {
          pattern,
        },
        jobId: "daily-digest",
      },
    );

    console.log(`Scheduled digest job every 2 hours with cron '${pattern}'`);
  } catch (err) {
    console.log("Failed to schedule daily digest", err);
  }
}
