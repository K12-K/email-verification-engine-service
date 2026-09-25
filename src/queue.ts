import { Queue } from "bullmq";
import { redis } from "./redis.js";
import { config } from "./config.js";
import type { VerificationJobPayload } from "./types.js";

export const verificationQueue =
  new Queue<VerificationJobPayload>(
    config.QUEUE_NAME,
    {
      connection: redis
    }
  );
