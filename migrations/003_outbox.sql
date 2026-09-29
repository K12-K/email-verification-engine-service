create table if not exists verification_outbox (
    id uuid primary key default gen_random_uuid(),
    job_id uuid not null unique references verification_jobs(id) on delete cascade,
    status text not null default 'pending',
    attempts integer not null default 0,
    last_error text,
    next_attempt_at timestamptz not null default now(),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    dispatched_at timestamptz
);

create index if not exists verification_outbox_pending_idx
  on verification_outbox(status, next_attempt_at, created_at);
