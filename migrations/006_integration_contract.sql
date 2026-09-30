-- v1.6 integration/reconciliation contract.
-- One application-side verification job should map to one engine job.
create unique index if not exists verification_jobs_client_reference_unique_idx
  on verification_jobs(client_reference_id)
  where client_reference_id is not null;
