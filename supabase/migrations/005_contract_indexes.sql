-- v1.5 API contract indexes.
-- The result endpoint can filter by status while retaining the job scope.
create index if not exists verification_results_email_status_idx
  on verification_results(email_id, status);

create index if not exists verification_emails_job_id_id_idx
  on verification_emails(job_id, id);
