export type VerificationStatus =
  | "invalid"
  | "no_mx"
  | "undeliverable"
  | "likely_deliverable"
  | "deliverable"
  | "catch_all"
  | "temporary_error"
  | "unknown";

export type Confidence = "high" | "medium" | "low";

export interface VerificationJobPayload {
  jobId: string;
}

export interface EmailInput {
  id: string;
  email: string;
  normalizedEmail: string;
  domain: string;
}

export interface MxRecord {
  exchange: string;
  priority: number;
  ttl: number;
}

export interface MxResult {
  domain: string;
  records: MxRecord[];
  ttl: number | null;
  negative: boolean;
  nullMx: boolean;
  implicitMx: boolean;
  errorCode: string | null;
  errorMessage: string | null;
}

export interface SmtpResult {
  result: "accepted" | "rejected" | "temporary" | "unknown" | "error";
  code: number | null;
  response: string | null;
  host: string | null;
  tls: boolean;
  attempts: number;
}

export interface VerificationResult {
  email: string;
  domain: string;
  status: VerificationStatus;
  confidence: Confidence;
  syntax: boolean;
  mx: boolean;
  smtp: SmtpResult;
  catchAll: boolean | null;
  mxHost: string | null;
  mxPriority: number | null;
  errorCode: string | null;
  errorMessage: string | null;
}
