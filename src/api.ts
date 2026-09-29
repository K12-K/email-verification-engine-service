import express from "express";
import { z } from "zod";
import { randomUUID } from "node:crypto";

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
  emails: z.array(z.string()).min(1).max(config.MAX_EMAILS_PER_JOB)
});

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

  const emails = [
    ...new Set(
      parsed.data.emails
        .map(email => email.trim())
        .filter(Boolean)
    )
  ];

  if (!emails.length) {
    return res.status(400).json({
      error: "No usable emails supplied",
      requestId
    });
  }

  const client = await db.connect();

  try {
    await client.query("BEGIN");

    const jobResult = await client.query(
      `
        insert into verification_jobs (total)
        values ($1)
        returning id
      `,
      [emails.length]
    );

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
      total: emails.length
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
      // requestId: randomUUID(),
      requestId
    });
  } catch (error) {
    await client.query("ROLLBACK");
    // console.error(error);
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
    const result = await db.query(
    `
      select *
      from verification_jobs
      where id = $1
    `,
    [req.params.id]
  );

  if (!result.rowCount) {
    return res.status(404).json({
      error: "Job not found",
      requestId: res.locals.requestId
    });
  }

  return res.json(result.rows[0]);
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

// app.get("/v1/jobs/:id/results", authenticate, async (req, res) => {
app.get("/v1/jobs/:id/results", authenticate, rateLimit, async (req, res) => {
  // const page = Math.max(1, Number(req.query.page || 1));
  // const limit = Math.min(
  //   100,
  //   Math.max(1, Number(req.query.limit || 50))
  // );
  const pageValue = Number(req.query.page || 1);
  const limitValue = Number(req.query.limit || 50);

  const page = Number.isFinite(pageValue) ? Math.max(1, Math.floor(pageValue)) : 1;
  const limit = Number.isFinite(limitValue)
    ? Math.min(100, Math.max(1, Math.floor(limitValue)))
    : 50;

  const offset = (page - 1) * limit;

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
      left join verification_results r
        on r.email_id = e.id
      where e.job_id = $1
      order by e.id
      limit $2
      offset $3
    `,
    [req.params.id, limit, offset]
  );

  const countResult = await db.query(
    `
      select count(*)::int as total
      from verification_emails
      where job_id = $1
    `,
    [req.params.id]
  );

  const total = countResult.rows[0]?.total ?? 0;

  return res.json({
    page,
    limit,
    total,
    totalPages: Math.ceil(total / limit),
    results: result.rows,
    requestId: res.locals.requestId
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
