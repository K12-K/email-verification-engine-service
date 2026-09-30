import express from "express";
import { z } from "zod";
import { randomUUID, createHash } from "node:crypto";

import { config } from "./config.ts";
import { db } from "./db.ts";
import { redis } from "./redis.ts";
// import { verificationQueue } from "./queue.ts";
import { dispatchPendingJobs, startOutboxDispatcher } from "./outbox.ts";
import { checkApiRateLimit } from "./apiRateLimit.ts";
import { logger } from "./logger.ts";

const app = express();

app.disable("x-powered-by");
app.use(express.json({ limit: "2mb" }));

app.use((req, res, next) => {
  const requestId = req.header("x-request-id") || randomUUID();
  res.setHeader("x-request-id", requestId);
  res.locals.requestId = requestId;
  next();
});

function authenticate(
  req: express.Request,
  res: express.Response,
  next: express.NextFunction
) {
  const key = req.headers.authorization?.replace(/^Bearer\s+/i, "");

  if (!key || key !== config.API_KEY) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  next();
}

async function rateLimit(req: express.Request, res: express.Response, next: express.NextFunction) {
  try {
    const identifier = req.headers.authorization || req.ip || "unknown";
    const result = await checkApiRateLimit(identifier);

    res.setHeader("x-ratelimit-limit", config.API_RATE_LIMIT);
    res.setHeader("x-ratelimit-remaining", result.remaining);

    if (!result.allowed) {
      res.setHeader("retry-after", result.retryAfterSeconds);
      return res.status(429).json({
        error: "Rate limit exceeded",
        retryAfterSeconds: result.retryAfterSeconds,
        requestId: res.locals.requestId
      });
    }

    next();
  } catch (error) {
    // API rate limiting is protection, not the source of truth. If Redis is
    // unavailable, the API remains usable while the health endpoint reports it.
    logger.warn("API rate limiter unavailable", {
      requestId: res.locals.requestId,
      error: error instanceof Error ? error.message : String(error)
    });
    next();
  }
}

app.get("/health", async (_req, res) => {
  try {
    await db.query("select 1");
    await redis.ping();

    res.json({ status: "ok" });
  } catch (err) {
    console.log(err)
    res.status(503).json({ status: "unhealthy" });
  }
});

const createJobSchema = z.object({
  emails: z.array(z.string()).min(1).max(config.MAX_EMAILS_PER_JOB),
  clientReferenceId: z.string().trim().min(1).max(128).optional()
});

function canonicalizeEmails(emails: string[]): string[] {
  return [...new Set(emails.map(email => email.trim().toLowerCase()).filter(Boolean))].sort();
}

function requestHash(emails: string[], clientReferenceId?: string): string {
  const payload = JSON.stringify({
    emails: canonicalizeEmails(emails),
    clientReferenceId: clientReferenceId?.trim() || null
  });
  return createHash("sha256").update(payload).digest("hex");
}

// app.post("/v1/jobs", authenticate, async (req, res) => {
app.post("/v1/jobs", authenticate, rateLimit, async (req, res) => {
  const requestId = res.locals.requestId as string;
  const parsed = createJobSchema.safeParse(req.body);

  if (!parsed.success) {
    return res.status(400).json({
      error: "Invalid request",
      details: parsed.error.issues,
      requestId
    });
  }

  // const emails = [
  //   ...new Set(
  //     parsed.data.emails
  //       .map(email => email.trim())
  //       .filter(Boolean)
  //   )
  // ];
  const emails: string[] = [];
  const seenEmails = new Set<string>();

  for (const rawEmail of parsed.data.emails) {
    const email = rawEmail.trim();
    const normalized = email.toLowerCase();
    if (!email || seenEmails.has(normalized)) continue;
    seenEmails.add(normalized);
    emails.push(email);
  }

  if (!emails.length) {
    return res.status(400).json({
      error: "No usable emails supplied",
      requestId
    });
  }

  const idempotencyKey = req.header("idempotency-key")?.trim() || null;
  if (idempotencyKey && (idempotencyKey.length < 8 || idempotencyKey.length > 255)) {
    return res.status(400).json({
      error: "Invalid Idempotency-Key",
      message: "Idempotency-Key must contain 8 to 255 characters.",
      requestId
    });
  }

  const clientReferenceId = parsed.data.clientReferenceId ?? null;
  const hash = requestHash(emails, clientReferenceId ?? undefined);
  const client = await db.connect();

  try {
    await client.query("BEGIN");

    // const jobResult = await client.query(
    //   `
    //     insert into verification_jobs (total)
    //     values ($1)
    //     returning id
    //   `,
    //   [emails.length]
    // );

    if (idempotencyKey) {
      const existing = await client.query(
        `
          select id, total, status, request_hash
          from verification_jobs
          where idempotency_key = $1
          for update
        `,
        [idempotencyKey]
      );

      if (existing.rowCount) {
        const row = existing.rows[0];

        if (row.request_hash !== hash) {
          await client.query("ROLLBACK");
          return res.status(409).json({
            error: "Idempotency key already used with a different request",
            jobId: row.id,
            requestId
          });
        }

        await client.query("COMMIT");

        logger.info("Returning existing idempotent verification job", {
          requestId,
          jobId: row.id,
          total: row.total
        });

        return res.status(202).json({
          jobId: row.id,
          total: row.total,
          status: row.status,
          idempotent: true,
          requestId
        });
      }
    }

    const jobResult = await client.query(
      `
        insert into verification_jobs (
          total,
          idempotency_key,
          request_hash,
          client_reference_id
        )
        values ($1, $2, $3, $4)
        on conflict (idempotency_key) where idempotency_key is not null do nothing
        returning id, total, status
      `,
      [emails.length, idempotencyKey, hash, clientReferenceId]
    );

    if (!jobResult.rowCount) {
      const existing = await client.query(
        `
          select id, total, status, request_hash
          from verification_jobs
          where idempotency_key = $1
        `,
        [idempotencyKey]
      );

      if (!existing.rowCount) {
        throw new Error("Idempotency conflict could not be resolved");
      }

      const row = existing.rows[0];
      if (row.request_hash !== hash) {
        await client.query("ROLLBACK");
        return res.status(409).json({
          error: "Idempotency key already used with a different request",
          jobId: row.id,
          requestId
        });
      }

      await client.query("COMMIT");
      return res.status(202).json({
        jobId: row.id,
        total: row.total,
        status: row.status,
        idempotent: true,
        requestId
      });
    }

    const jobId = jobResult.rows[0].id;

    // One query for all emails instead of one INSERT per email.
    const values: unknown[] = [];
    const placeholders: string[] = [];

    emails.forEach((email, index) => {
      const normalized = email.toLowerCase();
      const at = normalized.lastIndexOf("@");
      const domain = at > 0 ? normalized.slice(at + 1) : "";
      const base = index * 4;

      placeholders.push(
        `($${base + 1},$${base + 2},$${base + 3},$${base + 4})`
      );

      values.push(jobId, email, normalized, domain);
    });

    await client.query(
      `
        insert into verification_emails (
          job_id,
          email,
          normalized_email,
          domain
        )
        values ${placeholders.join(",")}
        on conflict (job_id, normalized_email)
        do nothing
      `,
      values
    );

    await client.query(
      `
        insert into verification_outbox (job_id)
        values ($1)
        on conflict (job_id) do nothing
      `,
      [jobId]
    );

    await client.query("COMMIT");

    logger.info("Verification job created", {
      requestId,
      jobId,
      total: emails.length,
      clientReferenceId
    });

    // try {
    //   await verificationQueue.add(
    //     "verify",
    //     { jobId },
    //     {
    //       jobId,
    //       attempts: 3,
    //       backoff: {
    //         type: "exponential",
    //         delay: 5000
    //       },
    //       removeOnComplete: {
    //         age: 86400,
    //         count: 1000
    //       },
    //       removeOnFail: {
    //         age: 7 * 86400,
    //         count: 5000
    //       }
    //     }
    //   );
    // } catch (queueError) {
    //   await db.query(
    //     `
    //       update verification_jobs
    //       set status = 'failed',
    //           failed_at = now(),
    //           error = $1
    //       where id = $2
    //     `,
    //     [
    //       queueError instanceof Error
    //         ? queueError.message
    //         : String(queueError),
    //       jobId
    //     ]
    //   );

    //   return res.status(503).json({
    //     error: "Job was stored but could not be queued",
    //     jobId
    //   });
    // }

    // Best-effort immediate dispatch. The transactional outbox remains the
    // recovery path if Redis/BullMQ is temporarily unavailable.
    void dispatchPendingJobs().catch(error => {
      // console.error("Initial outbox dispatch failed", error);
      logger.error("Initial outbox dispatch failed", {
        requestId,
        jobId,
        error: error instanceof Error ? error.message : String(error)
      });
    });

    return res.status(202).json({
      jobId,
      total: emails.length,
      status: "queued",
      idempotent: false,
      // requestId: randomUUID(),
      requestId
    });
  } catch (error) {
    await client.query("ROLLBACK");
    // console.error(error);

    const pgError = error as { code?: string; constraint?: string };
    if (pgError.code === "23505" && pgError.constraint === "verification_jobs_client_reference_unique_idx") {
      return res.status(409).json({
        error: "clientReferenceId is already associated with another job",
        requestId
      });
    }

    logger.error("Failed to create job", {
      requestId,
      error: error instanceof Error ? error.message : String(error)
    });

    return res.status(500).json({
      error: "Failed to create job",
      requestId
    });
  } finally {
    client.release();
  }
});

// app.get("/v1/jobs/:id", authenticate, async (req, res) => {
app.get("/v1/jobs/:id", authenticate, rateLimit, async (req, res) => {
  const requestId = res.locals.requestId as string;
  const result = await db.query(
    `
      select
        id,
        status,
        total,
        processed,
        deliverable,
        undeliverable,
        catch_all,
        no_mx,
        invalid,
        unknown,
        temporary_error,
        created_at,
        started_at,
        completed_at,
        failed_at,
        error,
        client_reference_id
      from verification_jobs
      where id = $1
    `,
    [req.params.id]
  );

  if (!result.rowCount) {
    return res.status(404).json({ error: "Job not found", requestId });
  }

  const row = result.rows[0];
  const total = Number(row.total) || 0;
  const processed = Number(row.processed) || 0;

  return res.json({
    jobId: row.id,
    status: row.status,
    total,
    processed,
    progressPercent: total > 0 ? Math.min(100, Math.floor((processed / total) * 100)) : 0,
    counts: {
      deliverable: row.deliverable,
      undeliverable: row.undeliverable,
      catchAll: row.catch_all,
      noMx: row.no_mx,
      invalid: row.invalid,
      unknown: row.unknown,
      temporaryError: row.temporary_error
    },
    createdAt: row.created_at,
    startedAt: row.started_at,
    completedAt: row.completed_at,
    failedAt: row.failed_at,
    error: row.error,
    clientReferenceId: row.client_reference_id,
    requestId
  });
});

app.get("/v1/jobs/by-reference/:clientReferenceId", authenticate, rateLimit, async (req, res) => {
  const requestId = res.locals.requestId as string;
  const clientReferenceId = req.params.clientReferenceId?.trim();

  if (!clientReferenceId) {
    return res.status(400).json({ error: "clientReferenceId is required", requestId });
  }

  const result = await db.query(
    `
      select
        id,
        status,
        total,
        processed,
        deliverable,
        undeliverable,
        catch_all,
        no_mx,
        invalid,
        unknown,
        temporary_error,
        created_at,
        started_at,
        completed_at,
        failed_at,
        error,
        client_reference_id
      from verification_jobs
      where client_reference_id = $1
      order by created_at desc
      limit 2
    `,
    [clientReferenceId]
  );

  if (!result.rowCount) {
    return res.status(404).json({ error: "Job not found", requestId });
  }

  if (result.rowCount > 1) {
    return res.status(409).json({
      error: "Multiple jobs found for clientReferenceId",
      requestId
    });
  }

  const row = result.rows[0];
  const total = Number(row.total) || 0;
  const processed = Number(row.processed) || 0;

  return res.json({
    jobId: row.id,
    status: row.status,
    total,
    processed,
    progressPercent: total > 0 ? Math.min(100, Math.floor((processed / total) * 100)) : 0,
    counts: {
      deliverable: row.deliverable,
      undeliverable: row.undeliverable,
      catchAll: row.catch_all,
      noMx: row.no_mx,
      invalid: row.invalid,
      unknown: row.unknown,
      temporaryError: row.temporary_error
    },
    createdAt: row.created_at,
    startedAt: row.started_at,
    completedAt: row.completed_at,
    failedAt: row.failed_at,
    error: row.error,
    clientReferenceId: row.client_reference_id,
    requestId
  });
});

app.post("/v1/jobs/:id/cancel", authenticate, rateLimit, async (req, res) => {
  const requestId = res.locals.requestId as string;

  const result = await db.query(
    `
      update verification_jobs
      set status = 'cancelled',
          completed_at = coalesce(completed_at, now()),
          error = null
      where id = $1
        and status in ('queued', 'processing')
      returning id, status
    `,
    [req.params.id]
  );

  if (!result.rowCount) {
    const existing = await db.query(
      `select id, status from verification_jobs where id = $1`,
      [req.params.id]
    );

    if (!existing.rowCount) {
      return res.status(404).json({ error: "Job not found", requestId });
    }

    return res.status(409).json({
      error: "Job cannot be cancelled in its current state",
      status: existing.rows[0].status,
      requestId
    });
  }

  await db.query(
    `
      update verification_outbox
      set status = 'cancelled', updated_at = now()
      where job_id = $1
        and status in ('pending', 'failed', 'dispatching')
    `,
    [req.params.id]
  );

  logger.info("Verification job cancelled", {
    requestId,
    jobId: req.params.id
  });

  return res.json({
    jobId: req.params.id,
    status: "cancelled",
    requestId
  });
});

const resultStatuses = [
  "invalid",
  "no_mx",
  "undeliverable",
  "likely_deliverable",
  "deliverable",
  "catch_all",
  "temporary_error",
  "unknown"
] as const;

// app.get("/v1/jobs/:id/results", authenticate, async (req, res) => {
app.get("/v1/jobs/:id/results", authenticate, rateLimit, async (req, res) => {
  const requestId = res.locals.requestId as string;
  // const page = Math.max(1, Number(req.query.page || 1));
  // const limit = Math.min(
  //   100,
  //   Math.max(1, Number(req.query.limit || 50))
  // );
  const pageValue = Number(req.query.page || 1);
  const limitValue = Number(req.query.limit || 50);
  const status = typeof req.query.status === "string" ? req.query.status : undefined;

  const page = Number.isFinite(pageValue) ? Math.max(1, Math.floor(pageValue)) : 1;
  const limit = Number.isFinite(limitValue)
    ? Math.min(100, Math.max(1, Math.floor(limitValue)))
    : 50;

  if (status && !resultStatuses.includes(status as typeof resultStatuses[number])) {
    return res.status(400).json({
      error: "Invalid result status",
      allowed: resultStatuses,
      requestId
    });
  }

  const jobExists = await db.query(
    `select id from verification_jobs where id = $1`,
    [req.params.id]
  );

  if (!jobExists.rowCount) {
    return res.status(404).json({ error: "Job not found", requestId });
  }

  const offset = (page - 1) * limit;
  const statusFilter = status ? `and r.status = $4` : "";
  const params: unknown[] = [req.params.id, limit, offset];
  if (status) params.push(status);

  // const result = await db.query(
  //   `
  //     select
  //       e.id,
  //       e.email,
  //       e.domain,
  //       r.status,
  //       r.confidence,
  //       r.syntax_result,
  //       r.mx_result,
  //       r.smtp_result,
  //       r.smtp_code,
  //       r.smtp_response,
  //       r.catch_all_result,
  //       r.mx_host,
  //       r.mx_priority,
  //       r.error_code,
  //       r.error_message,
  //       r.smtp_tls,
  //       r.smtp_attempts,
  //       r.verified_at
  //     from verification_emails e
  //     left join verification_results r
  //       on r.email_id = e.id
  //     where e.job_id = $1
  //     order by e.id
  //     limit $2
  //     offset $3
  //   `,
  //   [req.params.id, limit, offset]
  // );
  const result = await db.query(
    `
      select
        e.id,
        e.email,
        e.domain,
        r.status,
        r.confidence,
        r.syntax_result,
        r.mx_result,
        r.smtp_result,
        r.smtp_code,
        r.smtp_response,
        r.catch_all_result,
        r.mx_host,
        r.mx_priority,
        r.error_code,
        r.error_message,
        r.smtp_tls,
        r.smtp_attempts,
        r.verified_at
      from verification_emails e
      left join verification_results r on r.email_id = e.id
      where e.job_id = $1
        ${statusFilter}
      order by e.id
      limit $2 offset $3
    `,
    params
  );

  const countParams: unknown[] = [req.params.id];
  if (status) countParams.push(status);
  // const countResult = await db.query(
  //   `
  //     select count(*)::int as total
  //     from verification_emails
  //     where job_id = $1
  //   `,
  //   [req.params.id]
  // );
  const countResult = await db.query(
    `
      select count(*)::int as total
      from verification_emails e
      left join verification_results r on r.email_id = e.id
      where e.job_id = $1
        ${status ? "and r.status = $2" : ""}
    `,
    countParams
  );

  const total = countResult.rows[0]?.total ?? 0;

  return res.json({
    jobId: req.params.id,
    page,
    limit,
    total,
    totalPages: Math.ceil(total / limit),
    status: status ?? null,
    results: result.rows,
    requestId
  });
});

const outboxDispatcher = startOutboxDispatcher();

const server = app.listen(config.PORT, () => {
  // console.log(`API listening on ${config.PORT}`);
  logger.info("API listening", { port: config.PORT });
});

async function shutdown(signal: string) {
  // console.log(`Received ${signal}; shutting down API`);
  logger.info("API shutting down", { signal });

  clearInterval(outboxDispatcher);

  server.close(async () => {
    await redis.quit();
    await db.end();
    process.exit(0);
  });
}

process.once("SIGTERM", () => void shutdown("SIGTERM"));
process.once("SIGINT", () => void shutdown("SIGINT"));
