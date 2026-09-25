import express from "express";
import { z } from "zod";

import { config } from "./config.ts";
import { db } from "./db.ts";
import { verificationQueue } from "./queue.ts";

const app = express();

app.use(
  express.json({
    limit: "2mb"
  })
);

function authenticate(
  req: express.Request,
  res: express.Response,
  next: express.NextFunction
) {

  const key =
    req.headers.authorization
      ?.replace(/^Bearer\s+/i, "");

  if (!key || key !== config.API_KEY) {
    return res.status(401).json({
      error: "Unauthorized"
    });
  }

  next();
}

app.get(
  "/health",
  async (_req, res) => {

    try {

      await db.query("select 1");

      res.json({
        status: "ok"
      });

    } catch (err) {
      console.log(err)
      res.status(503).json({
        status: "unhealthy"
      });
    }
  }
);

const createJobSchema = z.object({
  emails: z
    .array(z.string())
    .min(1)
    .max(config.MAX_EMAILS_PER_JOB)
});

app.post(
  "/v1/jobs",
  authenticate,
  async (req, res) => {

    const parsed =
      createJobSchema.safeParse(
        req.body
      );

    if (!parsed.success) {

      return res.status(400).json({
        error: "Invalid request",
        details: parsed.error.issues
      });
    }

    const emails = [
      ...new Set(
        parsed.data.emails
          .map(email => email.trim())
          .filter(Boolean)
      )
    ];

    const client =
      await db.connect();

    try {

      await client.query("BEGIN");

      const jobResult =
        await client.query(
          `
            insert into verification_jobs (
              total
            )
            values ($1)
            returning id
          `,
          [emails.length]
        );

      const jobId =
        jobResult.rows[0].id;

      for (const email of emails) {

        const normalized =
          email.trim().toLowerCase();

        const at =
          normalized.lastIndexOf("@");

        const domain =
          at > 0
            ? normalized.slice(at + 1)
            : "";

        await client.query(
          `
            insert into verification_emails (
              job_id,
              email,
              normalized_email,
              domain
            )
            values ($1,$2,$3,$4)
            on conflict (
              job_id,
              normalized_email
            )
            do nothing
          `,
          [
            jobId,
            email,
            normalized,
            domain
          ]
        );
      }

      await client.query("COMMIT");

      await verificationQueue.add(
        "verify",
        { jobId },
        {
          jobId,
          attempts: 3,
          backoff: {
            type: "exponential",
            delay: 5000
          },
          removeOnComplete: {
            age: 3600
          },
          removeOnFail: {
            age: 86400
          }
        }
      );

      return res.status(202).json({
        jobId,
        total: emails.length,
        status: "queued"
      });

    } catch (error) {

      await client.query("ROLLBACK");

      console.error(error);

      return res.status(500).json({
        error: "Failed to create job"
      });

    } finally {
      client.release();
    }
  }
);

app.get(
  "/v1/jobs/:id",
  authenticate,
  async (req, res) => {

    const result =
      await db.query(
        `
          select *
          from verification_jobs
          where id = $1
        `,
        [req.params.id]
      );

    if (!result.rowCount) {

      return res.status(404).json({
        error: "Job not found"
      });
    }

    return res.json(
      result.rows[0]
    );
  }
);

app.get(
  "/v1/jobs/:id/results",
  authenticate,
  async (req, res) => {

    const page =
      Math.max(
        1,
        Number(req.query.page || 1)
      );

    const limit =
      Math.min(
        100,
        Math.max(
          1,
          Number(req.query.limit || 50)
        )
      );

    const offset =
      (page - 1) * limit;

    const result =
      await db.query(
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
            r.verified_at
          from verification_emails e
          left join verification_results r
            on r.email_id = e.id
          where e.job_id = $1
          order by e.id
          limit $2
          offset $3
        `,
        [
          req.params.id,
          limit,
          offset
        ]
      );

    return res.json({
      page,
      limit,
      results: result.rows
    });
  }
);

app.listen(
  config.PORT,
  () => {
    console.log(
      `API listening on ${config.PORT}`
    );
  }
);
