import {
  Worker,
  Job
} from "bullmq";

import { redis } from "./redis.ts";
import { config } from "./config.ts";
import { db } from "./db.ts";
import { verifyEmail } from "./verifier.ts";
import type {
  VerificationJobPayload
} from "./types.ts";

async function processJob(
  job: Job<VerificationJobPayload>
) {

  const { jobId } = job.data;

  await db.query(
    `
      update verification_jobs
      set status = 'processing',
          started_at = now()
      where id = $1
    `,
    [jobId]
  );

  const result = await db.query(
    `
      select
        id,
        email,
        normalized_email,
        domain
      from verification_emails
      where job_id = $1
      order by id
    `,
    [jobId]
  );

  const emails = result.rows;

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

  /*
   * Conservative SMTP concurrency.
   */
  const concurrency =
    config.SMTP_CONCURRENCY;

  let index = 0;

  async function workerLoop() {

    while (true) {

      const current = index++;

      if (current >= emails.length) {
        return;
      }

      const row = emails[current];

      const result =
        await verifyEmail(row.email);

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
            mx_priority
          )
          values (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11
          )
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
            verified_at = now()
        `,
        [
          row.id,
          result.status,
          result.confidence,
          result.syntax,
          result.mx,
          result.smtp.result,
          result.smtp.code,
          result.smtp.response,
          result.catchAll,
          result.mxHost,
          result.mxPriority
        ]
      );

      processed++;

      if (
        result.status ===
        "likely_deliverable"
      ) {
        counters.deliverable++;
      } else if (
        result.status ===
        "undeliverable"
      ) {
        counters.undeliverable++;
      } else if (
        result.status ===
        "catch_all"
      ) {
        counters.catch_all++;
      } else if (
        result.status ===
        "no_mx"
      ) {
        counters.no_mx++;
      } else if (
        result.status ===
        "invalid"
      ) {
        counters.invalid++;
      } else if (
        result.status ===
        "temporary_error"
      ) {
        counters.temporary_error++;
      } else {
        counters.unknown++;
      }

      /*
       * Don't write progress for every email.
       * This reduces database/Redis traffic.
       */
      if (
        processed % 10 === 0 ||
        processed === emails.length
      ) {

        const percentage =
          emails.length === 0
            ? 100
            : Math.round(
                processed /
                emails.length *
                100
              );

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
  }

  await Promise.all(
    Array.from(
      {
        length: Math.min(
          concurrency,
          emails.length
        )
      },
      () => workerLoop()
    )
  );

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

export const worker =
  new Worker<VerificationJobPayload>(
    config.QUEUE_NAME,
    processJob,
    {
      connection: redis,
      concurrency: 1
    }
  );

worker.on(
  "completed",
  job => {
    console.log(
      `Job ${job.id} completed`
    );
  }
);

worker.on(
  "failed",
  (job, error) => {

    console.error(
      `Job ${job?.id} failed`,
      error
    );

    if (job) {
      db.query(
        `
          update verification_jobs
          set
            status = 'failed',
            failed_at = now(),
            error = $1
          where id = $2
        `,
        [
          error.message,
          job.data.jobId
        ]
      ).catch(console.error);
    }
  }
);

console.log(
  "Email verification worker started"
);
