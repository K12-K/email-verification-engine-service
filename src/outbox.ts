import { verificationQueue } from "./queue.ts";
import { db } from "./db.ts";

const MAX_DISPATCH_PER_PASS = 50;

export async function enqueueJob(jobId: string): Promise<void> {
  await verificationQueue.add(
    "verify",
    { jobId },
    {
      jobId,
      attempts: 3,
      backoff: { type: "exponential", delay: 5000 },
      removeOnComplete: { age: 86400, count: 1000 },
      removeOnFail: { age: 7 * 86400, count: 5000 }
    }
  );
}

export async function dispatchPendingJobs(): Promise<number> {
  const claimed = await db.query<{ id: string; job_id: string }>(
    `
      update verification_outbox o
      set
        status = 'dispatching',
        attempts = attempts + 1,
        updated_at = now()
      where o.id in (
        select id
        from verification_outbox
        where (
            status in ('pending', 'failed')
            or (status = 'dispatching' and updated_at < now() - interval '30 seconds')
          )
          and next_attempt_at <= now()
        order by created_at
        for update skip locked
        limit $1
      )
      returning id, job_id
    `,
    [MAX_DISPATCH_PER_PASS]
  );

  let dispatched = 0;

  for (const row of claimed.rows) {
    try {
      await enqueueJob(row.job_id);

      await db.query(
        `
          update verification_outbox
          set status = 'dispatched',
              dispatched_at = now(),
              updated_at = now(),
              last_error = null
          where id = $1
        `,
        [row.id]
      );

      dispatched++;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);

      await db.query(
        `
          update verification_outbox
          set status = 'failed',
              last_error = $1,
              next_attempt_at = now() + interval '5 seconds' * least(greatest(attempts, 1), 60),
              updated_at = now()
          where id = $2
        `,
        [message, row.id]
      );
    }
  }

  return dispatched;
}

export function startOutboxDispatcher(): NodeJS.Timeout {
  const interval = setInterval(() => {
    void dispatchPendingJobs().catch(error => {
      console.error("Outbox dispatch pass failed", error);
    });
  }, 5000);

  interval.unref();
  return interval;
}
