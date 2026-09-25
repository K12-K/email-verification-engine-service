import { Redis as IORedis } from "ioredis";
import { config } from "./config.js";

export const redis = new IORedis(config.REDIS_URL, {
  maxRetriesPerRequest: null,
  enableReadyCheck: true
});
