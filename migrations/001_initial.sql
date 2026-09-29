create extension if not exists pgcrypto;

create table verification_jobs (
    id uuid primary key default gen_random_uuid(),

    status text not null default 'queued',

    total integer not null default 0,
    processed integer not null default 0,

    deliverable integer not null default 0,
    undeliverable integer not null default 0,
    catch_all integer not null default 0,
    no_mx integer not null default 0,
    invalid integer not null default 0,
    unknown integer not null default 0,
    temporary_error integer not null default 0,

    created_at timestamptz not null default now(),
    started_at timestamptz,
    completed_at timestamptz,
    failed_at timestamptz,

    error text
);

create index verification_jobs_status_idx
    on verification_jobs(status);

create index verification_jobs_created_at_idx
    on verification_jobs(created_at desc);

create table verification_emails (
    id uuid primary key default gen_random_uuid(),

    job_id uuid not null
        references verification_jobs(id)
        on delete cascade,

    email text not null,
    normalized_email text not null,
    domain text not null,

    created_at timestamptz not null default now()
);

create index verification_emails_job_idx
    on verification_emails(job_id);

create index verification_emails_domain_idx
    on verification_emails(domain);

create unique index verification_emails_job_email_idx
    on verification_emails(job_id, normalized_email);

create table verification_results (
    id uuid primary key default gen_random_uuid(),

    email_id uuid not null
        references verification_emails(id)
        on delete cascade,

    status text not null,
    confidence text not null,

    syntax_result boolean,
    mx_result boolean,

    smtp_result text,
    smtp_code integer,
    smtp_response text,

    catch_all_result boolean,

    mx_host text,
    mx_priority integer,

    error_code text,
    error_message text,

    verified_at timestamptz not null default now()
);

create unique index verification_results_email_idx
    on verification_results(email_id);

create index verification_results_status_idx
    on verification_results(status);

create index verification_results_verified_at_idx
    on verification_results(verified_at desc);
