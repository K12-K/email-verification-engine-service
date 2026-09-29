import { Worker, Job } from "bullmq";
import { redis } from "./redis.ts";
import { config } from "./config.ts";
import { db } from "./db.ts";
import { verifyEmail } from "./verifier.ts";
import { resolveMx } from "./dns.js";
import { startOutboxDispatcher } from "./outbox.js";
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
  rows: Array<{ emailId: string; result: VerificationResult }>
): Promise<void> {
  if (!rows.length) return;

  const values: unknown[] = [];
  const placeholders: string[] = [];

  rows.forEach((row, index) => {
    const base = index * 15;

    placeholders.push(
      `($${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5},` +
      `$${base + 6},$${base + 7},$${base + 8},$${base + 9},$${base + 10},` +
      `$${base + 11},$${base + 12},$${base + 13},$${base + 14},$${base + 15})`
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
      row.result.smtp.attempts
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
        smtp_attempts
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
        verified_at = now()
    `,
    values
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

  // Resolve each unique domain once and share the result across all emails
  // in the job. This is the domain-level optimization required for bulk work.
  const uniqueDomains = [...new Set(emails.map(row => row.domain).filter(Boolean))];
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
  }> = [];

  let index = 0;

  const workerLoop = async () => {
    while (true) {
      const current = index++;
      if (current >= emails.length) return;

      const row = emails[current];
      // const verification = await verifyEmail(row.email);
      const verification = await verifyEmail(row.email, mxByDomain.get(row.domain));
      console.log(".......worker.....169.....", verification)

      batch.push({
        emailId: row.id,
        result: verification
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

export const worker = new Worker<VerificationJobPayload>(
  config.QUEUE_NAME,
  processJob,
  {
    connection: redis,
    concurrency: 1
  }
);

worker.on("completed", job => {
  console.log(`Job ${job.id} completed`);
});

worker.on("failed", (job, error) => {
  console.error(`Job ${job?.id} failed`, error);
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
  ).catch(console.error);
});

const outboxDispatcher = startOutboxDispatcher();

async function shutdown(signal: string) {
  console.log(`Received ${signal}; shutting down worker`);
  clearInterval(outboxDispatcher);
  await worker.close();
  await redis.quit();
  await db.end();
  process.exit(0);
}

process.once("SIGTERM", () => void shutdown("SIGTERM"));
process.once("SIGINT", () => void shutdown("SIGINT"));

console.log("Email verification worker started");
