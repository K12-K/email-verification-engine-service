import "dotenv/config";
import { z } from "zod";

const envSchema = z.object({
  NODE_ENV: z.string().default("development"),

  PORT: z.coerce.number().default(8080),

  DATABASE_URL: z.string().min(1),
  REDIS_URL: z.string().min(1),

  QUEUE_NAME: z.string().default("email-verification"),

  MAX_EMAILS_PER_JOB: z.coerce.number().default(1000),

  DNS_CONCURRENCY: z.coerce.number().default(30),
  SMTP_CONCURRENCY: z.coerce.number().default(5),

  DNS_TIMEOUT_MS: z.coerce.number().default(5000),
  SMTP_TIMEOUT_MS: z.coerce.number().default(10000),

  SMTP_FROM: z.string().email(),
  SMTP_EHLO: z.string().min(1),

  CATCH_ALL_ENABLED: z
    .string()
    .default("true")
    .transform(v => v === "true"),

  API_KEY: z.string().min(16)
});

export const config = envSchema.parse(process.env);
