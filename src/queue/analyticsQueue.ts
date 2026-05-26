import { Queue } from "bullmq";
import { redisConnection } from "../config/redis";

export const analyticsQueue = new Queue("analytics", {
  connection: redisConnection,

  defaultJobOptions: {
    attempts: 1,
    removeOnComplete: false,
    removeOnFail: false,
  },
});
