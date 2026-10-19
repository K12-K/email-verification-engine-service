-- Cross-job email-result cache. The HMAC key is used for lookup/deduplication;
-- unlike a plain SHA-256 email hash, it is not directly enumerable without the
-- application secret. Per-job result rows remain the history for each job.
CREATE TABLE IF NOT EXISTS email_verification_cache (
  email_hash TEXT PRIMARY KEY,
  normalized_email TEXT NOT NULL,
  domain TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL CHECK (status IN (
    'invalid','no_mx','undeliverable','likely_deliverable',
    'deliverable','catch_all','temporary_error','unknown'
  )),
  result JSONB NOT NULL,
  checked_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  retention_until TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (expires_at >= checked_at),
  CHECK (retention_until >= checked_at)
);

CREATE INDEX IF NOT EXISTS email_verification_cache_expiry_idx
  ON email_verification_cache(expires_at);
CREATE INDEX IF NOT EXISTS email_verification_cache_retention_idx
  ON email_verification_cache(retention_until);
CREATE INDEX IF NOT EXISTS email_verification_cache_domain_idx
  ON email_verification_cache(domain);

COMMENT ON TABLE email_verification_cache IS
  'Cross-job email evidence cache. expires_at controls re-verification; retention_until controls data cleanup.';
