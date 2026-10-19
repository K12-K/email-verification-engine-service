import crypto from "node:crypto";
import { config } from "./config.ts";
import { db } from "./db.ts";
import { redis } from "./redis.ts";
import { withRedisLock } from "./throttle.ts";
import { parseEmail, verifyEmail } from "./verifier.ts";
import type { VerificationResult, VerificationStatus } from "./types.ts";

export interface EmailResultCacheEntry {
  emailHash: string;
  normalizedEmail: string;
  domain: string;
  result: VerificationResult;
  checkedAt: string;
  expiresAt: string;
  retentionUntil: string;
}

function normalizedCacheInput(email: string): string {
  return String(email).trim().toLowerCase();
}

export function emailCacheHash(email: string): string {
  const secret = config.EMAIL_CACHE_HMAC_SECRET || config.API_KEY;
  return crypto
    .createHmac("sha256", secret)
    .update(normalizedCacheInput(email))
    .digest("hex");
}

function resultRedisKey(hash: string): string {
  return `${config.REDIS_KEY_PREFIX}:email-result:${hash}`;
}

function resultLockKey(hash: string): string {
  return `${config.REDIS_KEY_PREFIX}:email-result-lock:${hash}`;
}

function ttlForStatus(status: VerificationStatus): number {
  switch (status) {
    case "invalid":
    case "undeliverable":
    case "likely_deliverable":
    case "deliverable":
      return config.EMAIL_RESULT_TTL_SECONDS;
    case "catch_all":
      return config.EMAIL_CATCH_ALL_TTL_SECONDS;
    case "temporary_error":
      return config.EMAIL_TEMPORARY_COOLDOWN_SECONDS;
    case "no_mx":
    case "unknown":
    default:
      return config.EMAIL_INCONCLUSIVE_TTL_SECONDS;
  }
}

function redisTtlFor(entry: EmailResultCacheEntry): number {
  const remaining = Math.ceil((Date.parse(entry.expiresAt) - Date.now()) / 1000);
  return Math.max(0, Math.min(config.EMAIL_RESULT_REDIS_TTL_SECONDS, remaining));
}

async function setRedisCache(entry: EmailResultCacheEntry): Promise<void> {
  const ttl = redisTtlFor(entry);
  if (ttl <= 0) return;
  try {
    await redis.set(resultRedisKey(entry.emailHash), JSON.stringify(entry), "EX", ttl);
  } catch {
    // PostgreSQL remains the source of truth. A Redis cache write failure must
    // not discard a completed verification result.
  }
}

function parseRedisEntry(raw: string, hash: string): EmailResultCacheEntry | null {
  try {
    const entry = JSON.parse(raw) as EmailResultCacheEntry;
    if (
      entry.emailHash !== hash ||
      !entry.result ||
      !entry.expiresAt ||
      Date.parse(entry.expiresAt) <= Date.now()
    ) return null;
    return entry;
  } catch {
    return null;
  }
}

export async function getFreshCachedResult(email: string): Promise<EmailResultCacheEntry | null> {
  const hash = emailCacheHash(email);
  const key = resultRedisKey(hash);

  try {
    const raw = await redis.get(key);
    if (raw) {
      const entry = parseRedisEntry(raw, hash);
      if (entry) return entry;
      await redis.del(key).catch(() => undefined);
    }
  } catch {
    // Continue to PostgreSQL if Redis is unavailable.
  }

  const found = await db.query<{
    email_hash: string;
    normalized_email: string;
    domain: string;
    result: VerificationResult | string;
    checked_at: Date | string;
    expires_at: Date | string;
    retention_until: Date | string;
  }>(
    `select email_hash, normalized_email, domain, result, checked_at, expires_at, retention_until
     from email_verification_cache
     where email_hash = $1 and expires_at > now()
     limit 1`,
    [hash]
  );

  const row = found.rows[0];
  if (!row) return null;
  const entry: EmailResultCacheEntry = {
    emailHash: row.email_hash,
    normalizedEmail: row.normalized_email,
    domain: row.domain,
    result: typeof row.result === "string" ? JSON.parse(row.result) as VerificationResult : row.result,
    checkedAt: row.checked_at instanceof Date ? row.checked_at.toISOString() : new Date(row.checked_at).toISOString(),
    expiresAt: row.expires_at instanceof Date ? row.expires_at.toISOString() : new Date(row.expires_at).toISOString(),
    retentionUntil: row.retention_until instanceof Date ? row.retention_until.toISOString() : new Date(row.retention_until).toISOString()
  };
  await setRedisCache(entry);
  return entry;
}

function createEntry(email: string, result: VerificationResult): EmailResultCacheEntry {
  const now = new Date();
  const ttl = ttlForStatus(result.status);
  return {
    emailHash: emailCacheHash(email),
    normalizedEmail: normalizedCacheInput(email),
    domain: result.domain,
    result,
    checkedAt: now.toISOString(),
    expiresAt: new Date(now.getTime() + ttl * 1000).toISOString(),
    retentionUntil: new Date(now.getTime() + config.EMAIL_RESULT_RETENTION_DAYS * 86400_000).toISOString()
  };
}

/**
 * Reuses a fresh Redis/PostgreSQL result. On a cache miss, a distributed
 * per-email lock ensures concurrent jobs share one SMTP/DNS verification.
 * PostgreSQL persistence is batched by the worker alongside job results.
 */
export async function verifyEmailWithCache(
  email: string,
  preloadedMx?: Parameters<typeof verifyEmail>[1]
): Promise<EmailResultCacheEntry> {
  const cached = await getFreshCachedResult(email);
  if (cached) return cached;

  const hash = emailCacheHash(email);
  return withRedisLock(
    resultLockKey(hash),
    config.EMAIL_RESULT_LOCK_TTL_MS,
    async () => {
      const afterLock = await getFreshCachedResult(email);
      if (afterLock) return afterLock;

      // Invalid inputs are cacheable too; valid inputs use the separately
      // cached domain/MX evidence inside verifyEmail/resolveMx.
      const parsed = parseEmail(email);
      const result = await verifyEmail(email, parsed ? preloadedMx : undefined);
      const entry = createEntry(email, result);
      await setRedisCache(entry);
      return entry;
    }
  );
}
