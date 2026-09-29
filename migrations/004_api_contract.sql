alter table verification_jobs
  add column if not exists idempotency_key text,
  add column if not exists request_hash text,
  add column if not exists client_reference_id text;

create unique index if not exists verification_jobs_idempotency_key_idx
  on verification_jobs(idempotency_key)
  where idempotency_key is not null;

create index if not exists verification_jobs_client_reference_idx
  on verification_jobs(client_reference_id)
  where client_reference_id is not null;
