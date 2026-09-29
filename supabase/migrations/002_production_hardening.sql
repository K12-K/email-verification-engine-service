alter table verification_results
  add column if not exists error_message text,
  add column if not exists smtp_tls boolean,
  add column if not exists smtp_attempts integer;

create index if not exists verification_emails_job_domain_idx
  on verification_emails(job_id, domain);

create index if not exists verification_results_email_status_idx
  on verification_results(email_id, status);
