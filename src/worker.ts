import { Worker, Job } from "bullmq";
import { redis } from "./redis.ts";
import { config } from "./config.ts";
import { db } from "./db.ts";
// import { verifyEmail } from "./verifier.ts";
import { verifyEmailWithCache, getFreshCachedResult, type EmailResultCacheEntry } from "./resultCache.ts";
import { resolveMx } from "./dns.ts";
import { startOutboxDispatcher } from "./outbox.ts";
import { logger } from "./logger.ts";
import type {
  VerificationJobPayload,
  VerificationResult
} from "./types.ts";

type EmailRow = {
  id: string;
  email: string;
  normalized_email: string;
  domain: string;
};

const countersFor = (result: VerificationResult) => ({
  deliverable:
    result.status === "likely_deliverable" ||
    result.status === "deliverable"
      ? 1 : 0,
  undeliverable: result.status === "undeliverable" ? 1 : 0,
  catch_all: result.status === "catch_all" ? 1 : 0,
  no_mx: result.status === "no_mx" ? 1 : 0,
  invalid: result.status === "invalid" ? 1 : 0,
  unknown: result.status === "unknown" ? 1 : 0,
  temporary_error:
    result.status === "temporary_error" ? 1 : 0
});

async function bulkPersist(
  jobId: string,
  rows: Array<{ emailId: string; result: VerificationResult; cacheEntry: EmailResultCacheEntry }>
): Promise<void> {
  if (!rows.length) return;

  const values: unknown[] = [];
  const placeholders: string[] = [];

  rows.forEach((row, index) => {
    const base = index * 16;

    placeholders.push(
      `($${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5},` +
      `$${base + 6},$${base + 7},$${base + 8},$${base + 9},$${base + 10},` +
      `$${base + 11},$${base + 12},$${base + 13},$${base + 14},$${base + 15},$${base + 16})`
    );

    values.push(
      row.emailId,
      row.result.status,
      row.result.confidence,
      row.result.syntax,
      row.result.mx,
      row.result.smtp.result,
      row.result.smtp.code,
      row.result.smtp.response,
      row.result.catchAll,
      row.result.mxHost,
      row.result.mxPriority,
      row.result.errorCode,
      row.result.errorMessage,
      row.result.smtp.tls,
      row.result.smtp.attempts,
      row.cacheEntry.checkedAt
    );
  });

  await db.query(
    `
      insert into verification_results (
        email_id,
        status,
        confidence,
        syntax_result,
        mx_result,
        smtp_result,
        smtp_code,
        smtp_response,
        catch_all_result,
        mx_host,
        mx_priority,
        error_code,
        error_message,
        smtp_tls,
        smtp_attempts,
        verified_at
      )
      values ${placeholders.join(",")}
      on conflict (email_id)
      do update set
        status = excluded.status,
        confidence = excluded.confidence,
        syntax_result = excluded.syntax_result,
        mx_result = excluded.mx_result,
        smtp_result = excluded.smtp_result,
        smtp_code = excluded.smtp_code,
        smtp_response = excluded.smtp_response,
        catch_all_result = excluded.catch_all_result,
        mx_host = excluded.mx_host,
        mx_priority = excluded.mx_priority,
        error_code = excluded.error_code,
        error_message = excluded.error_message,
        smtp_tls = excluded.smtp_tls,
        smtp_attempts = excluded.smtp_attempts,
        verified_at = excluded.verified_at
    `,
    values
  );

  // Upsert the cross-job cache in batches with the per-job result write. Cache
  // hits keep their original checked_at/expires_at and therefore never extend
  // freshness just because a user requested the same email again.
  const cacheValues: unknown[] = [];
  const cachePlaceholders: string[] = [];
  rows.forEach((row, index) => {
    const base = index * 8;
    cachePlaceholders.push(
      `($${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5},$${base + 6},$${base + 7},$${base + 8}::jsonb)`
    );
    const entry = row.cacheEntry;
    cacheValues.push(
      entry.emailHash,
      entry.normalizedEmail,
      entry.domain,
      entry.result.status,
      entry.checkedAt,
      entry.expiresAt,
      entry.retentionUntil,
      JSON.stringify(entry.result)
    );
  });

  await db.query(
    `
      insert into email_verification_cache (
        email_hash, normalized_email, domain, status, checked_at,
        expires_at, retention_until, result
      )
      values ${cachePlaceholders.join(",")}
      on conflict (email_hash) do update set
        normalized_email = excluded.normalized_email,
        domain = excluded.domain,
        status = excluded.status,
        checked_at = excluded.checked_at,
        expires_at = excluded.expires_at,
        retention_until = excluded.retention_until,
        result = excluded.result,
        updated_at = now()
      where excluded.checked_at > email_verification_cache.checked_at
    `,
    cacheValues
  );
}

async function processJob(job: Job<VerificationJobPayload>) {
  const { jobId } = job.data;

  // Idempotency guard: completed jobs are never re-run accidentally.
  const started = await db.query(
    `
      update verification_jobs
      set status = 'processing',
          started_at = coalesce(started_at, now()),
          error = null
      where id = $1
        and status in ('queued', 'processing', 'failed')
      returning id
    `,
    [jobId]
  );

  if (!started.rowCount) {
    return { jobId, skipped: true };
  }

  const result = await db.query<EmailRow>(
    `
      select id, email, normalized_email, domain
      from verification_emails
      where job_id = $1
      order by id
    `,
    [jobId]
  );

  const emails = result.rows;


  // Check the email-result cache before doing domain work. Only cache misses
  // participate in MX prefetch; domains are still resolved once per job and
  // their independent DNS TTL is enforced by the engine's DNS cache.
  const cachedByEmailId = new Map<string, EmailResultCacheEntry | null>();
  let cacheIndex = 0;
  const cacheLookupLoop = async () => {
    while (true) {
      const current = cacheIndex++;
      if (current >= emails.length) return;
      const row = emails[current];
      cachedByEmailId.set(row.id, await getFreshCachedResult(row.email));
    }
  };
  await Promise.all(
    Array.from(
      { length: Math.min(10, config.DNS_CONCURRENCY, emails.length) },
      () => cacheLookupLoop()
    )
  );

  const missingRows = emails.filter(row => !cachedByEmailId.get(row.id));
  const uniqueDomains = [...new Set(missingRows.map(row => row.domain).filter(Boolean))];
  const mxByDomain = new Map<string, Awaited<ReturnType<typeof resolveMx>>>();
  let domainIndex = 0;

  const dnsLoop = async () => {
    while (true) {
      const current = domainIndex++;
      if (current >= uniqueDomains.length) return;

      const domain = uniqueDomains[current];
      mxByDomain.set(domain, await resolveMx(domain));
    }
  };

  await Promise.all(
    Array.from(
      { length: Math.min(config.DNS_CONCURRENCY, uniqueDomains.length) },
      () => dnsLoop()
    )
  );

  let processed = 0;
  let cancelled = false;

  const counters = {
    deliverable: 0,
    undeliverable: 0,
    catch_all: 0,
    no_mx: 0,
    invalid: 0,
    unknown: 0,
    temporary_error: 0
  };

  const batch: Array<{
    emailId: string;
    result: VerificationResult;
    cacheEntry: EmailResultCacheEntry;
  }> = [];

  let index = 0;

  const workerLoop = async () => {
    while (true) {
      const current = index++;
      if (current >= emails.length) return;

      if (cancelled) return;

      const row = emails[current];
      // const verification = await verifyEmail(row.email);
      // const verification = await verifyEmail(row.email, mxByDomain.get(row.domain));
      const cacheEntry = cachedByEmailId.get(row.id) ?? await verifyEmailWithCache(row.email, mxByDomain.get(row.domain));
      const verification = cacheEntry.result;
      console.log(".......worker.....169.....", verification)

      batch.push({
        emailId: row.id,
        result: verification,
        cacheEntry
      });

      const c = countersFor(verification);
      counters.deliverable += c.deliverable;
      counters.undeliverable += c.undeliverable;
      counters.catch_all += c.catch_all;
      counters.no_mx += c.no_mx;
      counters.invalid += c.invalid;
      counters.unknown += c.unknown;
      counters.temporary_error += c.temporary_error;

      processed++;

      if (
        batch.length >= config.RESULT_BATCH_SIZE ||
        processed === emails.length
      ) {
        const chunk = batch.splice(0, batch.length);
        await bulkPersist(jobId, chunk);
      }

      if (
        processed % config.PROGRESS_EVERY === 0 ||
        processed === emails.length
      ) {
        const percentage =
          emails.length === 0
            ? 100
            : Math.round((processed / emails.length) * 100);

        await job.updateProgress({
          processed,
          total: emails.length,
          percentage
        });

        const statusCheck = await db.query<{ status: string }>(
          `select status from verification_jobs where id = $1`,
          [jobId]
        );

        if (statusCheck.rows[0]?.status === "cancelled") {
          cancelled = true;
          return;
        }

        await db.query(
          `
            update verification_jobs
            set
              processed = $1,
              deliverable = $2,
              undeliverable = $3,
              catch_all = $4,
              no_mx = $5,
              invalid = $6,
              unknown = $7,
              temporary_error = $8
            where id = $9
          `,
          [
            processed,
            counters.deliverable,
            counters.undeliverable,
            counters.catch_all,
            counters.no_mx,
            counters.invalid,
            counters.unknown,
            counters.temporary_error,
            jobId
          ]
        );
      }
    }
  };

  await Promise.all(
    Array.from(
      { length: Math.min(config.SMTP_CONCURRENCY, emails.length) },
      () => workerLoop()
    )
  );

  if (batch.length) {
    const chunk = batch.splice(0, batch.length);
    await bulkPersist(jobId, chunk);
  }

  if (!cancelled) {
    const finalStatus = await db.query<{ status: string }>(
      `select status from verification_jobs where id = $1`,
      [jobId]
    );
    cancelled = finalStatus.rows[0]?.status === "cancelled";
  }

  if (cancelled) {
    await db.query(
      `
        update verification_jobs
        set status = 'cancelled', completed_at = coalesce(completed_at, now())
        where id = $1
          and status = 'cancelled'
      `,
      [jobId]
    );

    logger.info("Verification job stopped after cancellation", {
      jobId,
      processed,
      total: emails.length
    });

    return { jobId, processed, total: emails.length, cancelled: true };
  }

  await db.query(
    `
      update verification_jobs
      set
        status = 'completed',
        processed = $1,
        deliverable = $2,
        undeliverable = $3,
        catch_all = $4,
        no_mx = $5,
        invalid = $6,
        unknown = $7,
        temporary_error = $8,
        completed_at = now()
      where id = $9
    `,
    [
      processed,
      counters.deliverable,
      counters.undeliverable,
      counters.catch_all,
      counters.no_mx,
      counters.invalid,
      counters.unknown,
      counters.temporary_error,
      jobId
    ]
  );

  return {
    jobId,
    processed,
    total: emails.length
  };
}

const resultCacheCleanup = setInterval(() => {
  db.query(`delete from email_verification_cache where retention_until < now()`)
    .then(result => {
      if (result.rowCount) logger.info("Expired email result cache rows removed", { count: result.rowCount });
    })
    .catch(error => logger.warn("Email result cache cleanup failed", {
      error: error instanceof Error ? error.message : String(error)
    }));
}, 6 * 60 * 60 * 1000);
resultCacheCleanup.unref();

export const worker = new Worker<VerificationJobPayload>(
  config.QUEUE_NAME,
  processJob,
  {
    connection: redis,
    concurrency: 1
  }
);

worker.on("completed", job => {
  // console.log(`Job ${job.id} completed`);
  logger.info("BullMQ job completed", { jobId: job.data.jobId, queueJobId: job.id });
});

worker.on("failed", (job, error) => {
  // console.error(`Job ${job?.id} failed`, error);
  logger.error("BullMQ job failed", {
    jobId: job?.data.jobId,
    queueJobId: job?.id,
    error: error.message,
    attemptsMade: job?.attemptsMade
  });
  if (!job) return;

  const maxAttempts = Number(job.opts.attempts ?? 1);
  const finalAttempt = job.attemptsMade >= maxAttempts;

  // db.query(
  //   `
  //     update verification_jobs
  //     set status = 'failed',
  //         failed_at = now(),
  //         error = $1
  //     where id = $2
  //       and status <> 'completed'
  //   `,
  //   [error.message, job.data.jobId]
  // ).catch(console.error);
  db.query(
    finalAttempt
      ? `
          update verification_jobs
          set status = 'failed',
              failed_at = now(),
              error = $1
          where id = $2
            and status <> 'completed'
        `
      : `
          update verification_jobs
          set status = 'queued',
              error = $1
          where id = $2
            and status <> 'completed'
        `,
    [error.message, job.data.jobId]
  // ).catch(console.error);
  ).catch(error => logger.error("Failed to update job failure state", { error: error instanceof Error ? error.message : String(error) }));
});

const outboxDispatcher = startOutboxDispatcher();

async function shutdown(signal: string) {
  console.log(`Received ${signal}; shutting down worker`);
  clearInterval(outboxDispatcher);
  clearInterval(resultCacheCleanup);
  await worker.close();
  await redis.quit();
  await db.end();
  process.exit(0);
}

process.once("SIGTERM", () => void shutdown("SIGTERM"));
process.once("SIGINT", () => void shutdown("SIGINT"));

console.log("Email verification worker started");
