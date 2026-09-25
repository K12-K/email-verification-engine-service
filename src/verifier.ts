import { getHostname } from "tldts";
import type {
  Confidence,
  VerificationResult
} from "./types.ts";
import crypto from "node:crypto";
import { resolveMx } from "./dns.ts";
import { smtpVerify } from "./smtp.ts";
import type { MxResult } from "./types.ts";
import { config } from "./config.ts";

export interface ParsedEmail {
  email: string;
  localPart: string;
  domain: string;
}

export function parseEmail(
  input: unknown
): ParsedEmail | null {

  if (typeof input !== "string") {
    return null;
  }

  const email = input.trim().toLowerCase();

  if (!email || email.length > 254) {
    return null;
  }

  const at = email.lastIndexOf("@");

  if (at <= 0 || at === email.length - 1) {
    return null;
  }

  const localPart = email.slice(0, at);
  const rawDomain = email.slice(at + 1);

  if (
    localPart.startsWith(".") ||
    localPart.endsWith(".") ||
    localPart.includes("..")
  ) {
    return null;
  }

  const domain = getHostname(rawDomain);

  if (!domain) {
    return null;
  }

  return {
    email,
    localPart,
    domain: domain.toLowerCase()
  };
}

const catchAllCache = new Map<
  string,
  {
    value: boolean | null;
    expiresAt: number;
  }
>();

function randomAddress(domain: string) {

  const token = crypto
    .randomBytes(18)
    .toString("hex");

  return `verify-${token}@${domain}`;
}

export async function detectCatchAll(
  domain: string,
  mx: MxResult
): Promise<boolean | null> {

  const cached = catchAllCache.get(domain);

  if (
    cached &&
    cached.expiresAt > Date.now()
  ) {
    return cached.value;
  }

  if (!config.CATCH_ALL_ENABLED) {
    return null;
  }

  if (!mx.records.length) {
    return false;
  }

  const testEmail = randomAddress(domain);

  const result = await smtpVerify(
    mx.records[0],
    testEmail
  );

  let value: boolean | null = null;

  if (result.result === "accepted") {
    value = true;
  } else if (result.result === "rejected") {
    value = false;
  }

  catchAllCache.set(domain, {
    value,
    expiresAt: Date.now() + 60 * 60_000
  });

  return value;
}

export async function verifyEmail(
  email: string
): Promise<VerificationResult> {

  const parsed = parseEmail(email);

  if (!parsed) {

    return {
      email,
      status: "invalid",
      confidence: "high",

      syntax: false,
      mx: false,

      smtp: {
        result: "unknown",
        code: null,
        response: null,
        host: null
      },

      catchAll: false,

      mxHost: null,
      mxPriority: null
    };
  }

  const mx = await resolveMx(
    parsed.domain
  );

  if (!mx.records.length) {

    return {
      email: parsed.email,
      status: "no_mx",
      confidence: "high",

      syntax: true,
      mx: false,

      smtp: {
        result: "unknown",
        code: null,
        response: null,
        host: null
      },

      catchAll: false,

      mxHost: null,
      mxPriority: null
    };
  }

  const catchAll =
    await detectCatchAll(
      parsed.domain,
      mx
    );

  const smtp =
    await smtpVerify(
      mx.records[0],
      parsed.email
    );

  let status:
    VerificationResult["status"];

  let confidence:
    VerificationResult["confidence"];

  if (catchAll === true) {

    status = "catch_all";
    confidence = "high";

  } else if (smtp.result === "rejected") {

    status = "undeliverable";
    confidence = "high";

  } else if (smtp.result === "accepted") {

    status = "likely_deliverable";
    confidence = "medium";

  } else if (smtp.result === "temporary") {

    status = "temporary_error";
    confidence = "low";

  } else {

    status = "unknown";
    confidence = "low";
  }

  return {
    email: parsed.email,

    status,
    confidence,

    syntax: true,
    mx: true,

    smtp,

    catchAll,

    mxHost: mx.records[0].exchange,
    mxPriority: mx.records[0].priority
  };
}
