import "dotenv/config";
import { z } from "zod";

const envSchema = z.object({
  NODE_ENV: z.string().default("development"),
  PORT: z.coerce.number().int().positive().default(8080),

  DATABASE_URL: z.string().min(1),
  REDIS_URL: z.string().min(1),

  QUEUE_NAME: z.string().default("email-verification"),
  REDIS_KEY_PREFIX: z.string().default("verify-engine"),

  MAX_EMAILS_PER_JOB: z.coerce.number().int().positive().default(1000),

  DNS_CONCURRENCY: z.coerce.number().int().positive().default(30),
  SMTP_CONCURRENCY: z.coerce.number().int().positive().default(5),

  DNS_TIMEOUT_MS: z.coerce.number().int().positive().default(5000),
  SMTP_TIMEOUT_MS: z.coerce.number().int().positive().default(10000),
  DNS_SERVER: z.string().optional(),

  DNS_CACHE_MIN_TTL_SECONDS: z.coerce.number().int().positive().default(30),
  DNS_CACHE_MAX_TTL_SECONDS: z.coerce.number().int().positive().default(86400),
  DNS_NEGATIVE_CACHE_MAX_TTL_SECONDS: z.coerce.number().int().positive().default(300),

  SMTP_FROM: z.string().email(),
  SMTP_EHLO: z.string().min(1),
  SMTP_USE_STARTTLS: z
    .string()
    .default("true")
    .transform(v => v.toLowerCase() === "true"),
  SMTP_REQUIRE_STARTTLS: z
    .string()
    .default("false")
    .transform(v => v.toLowerCase() === "true"),

  SMTP_MAX_ATTEMPTS_PER_HOST: z.coerce.number().int().positive().default(2),
  SMTP_MAX_MX_HOSTS: z.coerce.number().int().positive().default(3),
  SMTP_RETRY_BASE_DELAY_MS: z.coerce.number().int().positive().default(500),
  SMTP_DOMAIN_RATE_LIMIT: z.coerce.number().int().positive().default(3),
  SMTP_DOMAIN_RATE_WINDOW_MS: z.coerce.number().int().positive().default(1000),

  CATCH_ALL_ENABLED: z
    .string()
    .default("true")
    .transform(v => v.toLowerCase() === "true"),
  CATCH_ALL_CACHE_SECONDS: z.coerce.number().int().positive().default(3600),

  RESULT_BATCH_SIZE: z.coerce.number().int().positive().default(100),
  PROGRESS_EVERY: z.coerce.number().int().positive().default(25),

  API_KEY: z.string().min(16),
  API_RATE_LIMIT: z.coerce.number().int().positive().default(60),
  API_RATE_WINDOW_MS: z.coerce.number().int().positive().default(60000)
});

export const config = envSchema.parse(process.env);
