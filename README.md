API:
npm run start:api

Worker:
npm run start:worker

----


# Email Verification Engine — Production Hardening

This revision keeps the existing architecture:

- Next.js = product/application
- Redis/BullMQ = queue + temporary coordination
- Supabase PostgreSQL = persistent source of truth
- Node.js worker = complete verification pipeline

## Production-hardening changes

1. DNS/MX cache is Redis-backed and uses the actual DNS record TTL when available.
2. DNS UDP queries fall back to TCP when the response is truncated.
3. NXDOMAIN/negative results use short TTLs.
4. Null MX is detected.
5. RFC 5321 implicit MX fallback to A/AAAA is handled when no MX exists.
6. SMTP supports STARTTLS when the MX advertises it.
7. EHLO is repeated after STARTTLS.
8. SMTP retries temporary/network failures with exponential backoff.
9. Multiple MX hosts can be attempted, with a configurable limit.
10. Redis provides a shared per-domain SMTP throttle across workers.
11. Catch-all detection is cached per domain and protected by a Redis lock.
12. Results are written to PostgreSQL in batches.
13. API email inserts are batched.
14. Worker processing is idempotent for completed jobs.
15. Graceful SIGTERM/SIGINT shutdown is included.
16. Health checks cover both PostgreSQL and Redis.

## Important limitation

SMTP `RCPT TO` is evidence, not proof of mailbox existence. The engine deliberately uses `likely_deliverable`, `catch_all`, `temporary_error`, and `unknown` instead of pretending every SMTP response is definitive.

## Install

```bash
npm install
```

Apply migrations in order:

```text
migrations/001_initial.sql
migrations/002_production_hardening.sql
```

Copy `.env.example` to `.env` and set real values.

## Run locally

```bash
npm run dev
```

Or separately:

```bash
npm run dev:api
npm run dev:worker
```

Build:

```bash
npm run build
```

## Railway

Create two services from the same repository/image:

### API
Start command:

```bash
npm run start:api
```

### Worker
Start command:

```bash
npm run start:worker
```

Both services use the same environment variables and Redis/PostgreSQL.

## SMTP identity

`SMTP_FROM` and `SMTP_EHLO` should use a domain you control. Do not forge arbitrary sender identities.

## Recommended initial limits

Start conservatively:

```text
DNS_CONCURRENCY=30
SMTP_CONCURRENCY=5
SMTP_DOMAIN_RATE_LIMIT=3
SMTP_DOMAIN_RATE_WINDOW_MS=1000
SMTP_MAX_ATTEMPTS_PER_HOST=2
SMTP_MAX_MX_HOSTS=3
```

Tune these only after observing real DNS/SMTP latency, temporary failures, throttling, and worker resource usage.
