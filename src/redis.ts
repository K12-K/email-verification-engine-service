import { Redis as IORedis } from "ioredis";
import { config } from "./config.ts";

export const redis = new IORedis(config.REDIS_URL, {
  maxRetriesPerRequest: null,
  enableReadyCheck: true,
  lazyConnect: false
});

export async function closeRedis() {
  await redis.quit();
}
