import Redis from "ioredis";
import dotenv from "dotenv";

dotenv.config();

export const redisConnection = new Redis({
  host: process.env.REDIS_HOST,
  port: Number(process.env.REDIS_PORT),

  maxRetriesPerRequest: null,
});
// console.log(
//   `Connected to redis at ${process.env.REDIS_HOST}:${process.env.REDIS_PORT}`,
// );
