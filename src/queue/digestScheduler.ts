import { notificationQueue } from "../queue/notifcationQueue";

export async function scheduleDailyDigest() {
  const pattern = process.env.DIGEST_CRON || "0 0 * * *";

  try {
    await notificationQueue.add(
      "send-digest",
      {},
      {
        repeat: {
          pattern: pattern,
        },
        jobId: "daily-digest",
      },
    );

    console.log(`Scheduled digest job every 2 hour with cron '${pattern}'`);
  } catch (err) {
    console.error("Failed to schedule daily digest", err);
  }
}
