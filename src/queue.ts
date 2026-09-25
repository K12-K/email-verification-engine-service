import { Queue } from "bullmq";
import { redis } from "./redis.ts";
import { config } from "./config.ts";
import type { VerificationJobPayload } from "./types.ts";

export const verificationQueue =
  new Queue<VerificationJobPayload>(
    config.QUEUE_NAME,
    {
      connection: redis
    }
  );
