import crypto from "node:crypto";
import { getHostname } from "tldts";
import { config } from "./config.ts";
import { redis } from "./redis.ts";
import { resolveMx } from "./dns.ts";
import { smtpVerify } from "./smtp.ts";
import { withRedisLock } from "./throttle.ts";
import type {
  Confidence,
  MxResult,
  VerificationResult
} from "./types.ts";

export interface ParsedEmail {
  email: string;
  localPart: string;
  domain: string;
}

const CONSUMER_EMAIL_DOMAINS = new Set([
  "gmail.com",
  "googlemail.com",
  "outlook.com",
  "hotmail.com",
  "live.com",
  "yahoo.com",
  "icloud.com"
]);

export function parseEmail(input: unknown): ParsedEmail | null {
  if (typeof input !== "string") return null;

  const email = input.trim().toLowerCase();

  if (!email || email.length > 254) return null;

  const at = email.lastIndexOf("@");
  if (at <= 0 || at === email.length - 1) return null;

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
  if (!domain) return null;

  return {
    email,
    localPart,
    domain: domain.toLowerCase()
  };
}

function randomAddress(domain: string): string {
  const token = crypto.randomBytes(18).toString("hex");
  return `verify-${token}@${domain}`;
}

function catchAllKey(domain: string): string {
  return `${config.REDIS_KEY_PREFIX}:catch-all:${domain}`;
}

export async function detectCatchAll(
  domain: string,
  mx: MxResult
): Promise<boolean | null> {
  if (!config.CATCH_ALL_ENABLED) return null;
  // SMTP recipient acceptance is not reliable proof of catch-all
  // behavior for these consumer email providers.
  if (CONSUMER_EMAIL_DOMAINS.has(domain.toLowerCase())) return null;
  if (!mx.records.length || mx.nullMx) return false;

  const key = catchAllKey(domain);
  const cached = await redis.get(key);

  if (cached !== null) {
    return cached === "1";
  }

  return withRedisLock(
    `${key}:lock`,
    Math.max(10_000, config.SMTP_TIMEOUT_MS * 4),
    async () => {
      const again = await redis.get(key);
      if (again !== null) return again === "1";

      const result = await smtpVerify(
        mx.records,
        randomAddress(domain),
        domain
      );

      let value: boolean | null = null;

      if (result.result === "accepted") {
        value = true;
      } else if (result.result === "rejected") {
        value = false;
      }

      if (value !== null) {
        await redis.set(
          key,
          value ? "1" : "0",
          "EX",
          config.CATCH_ALL_CACHE_SECONDS
        );
      }

      return value;
    }
  );
}

function resultBase(
  email: string,
  domain: string,
  status: VerificationResult["status"],
  confidence: Confidence,
  syntax: boolean,
  mx: boolean,
  smtp: VerificationResult["smtp"],
  catchAll: boolean | null,
  mxHost: string | null,
  mxPriority: number | null,
  errorCode: string | null = null,
  errorMessage: string | null = null
): VerificationResult {
  return {
    email,
    domain,
    status,
    confidence,
    syntax,
    mx,
    smtp,
    catchAll,
    mxHost,
    mxPriority,
    errorCode,
    errorMessage
  };
}

// export async function verifyEmail(email: string): Promise<VerificationResult> {
export async function verifyEmail(
  email: string,
  preloadedMx?: MxResult
): Promise<VerificationResult> {
  const parsed = parseEmail(email);

  if (!parsed) {
    return resultBase(
      String(email),
      "",
      "invalid",
      "high",
      false,
      false,
      {
        result: "unknown",
        code: null,
        response: null,
        host: null,
        tls: false,
        attempts: 0
      },
      false,
      null,
      null,
      "INVALID_SYNTAX",
      "Email address failed normalization/syntax validation"
    );
  }

  // const mx = await resolveMx(parsed.domain);
  const mx = preloadedMx ?? await resolveMx(parsed.domain);

  if (mx.nullMx || mx.records.length === 0) {
    return resultBase(
      parsed.email,
      parsed.domain,
      "no_mx",
      "high",
      true,
      false,
      {
        result: "unknown",
        code: null,
        response: null,
        host: null,
        tls: false,
        attempts: 0
      },
      false,
      null,
      null,
      mx.errorCode,
      mx.errorMessage
    );
  }

  const smtp = await smtpVerify(
    mx.records,
    parsed.email,
    parsed.domain
  );
  console.log("....verifier, 197,...", mx.records, parsed.email, parsed.domain, smtp)

  // Only use catch-all classification after an actual recipient
  // was accepted. A rejected real recipient should remain rejected.
  let catchAll: boolean | null = null;
  if (smtp.result === "accepted") {
    catchAll = await detectCatchAll(parsed.domain, mx);
  }

  if (smtp.result === "rejected") {
    return resultBase(
      parsed.email,
      parsed.domain,
      "undeliverable",
      "high",
      true,
      true,
      smtp,
      catchAll,
      smtp.host ?? mx.records[0].exchange,
      mx.records[0].priority
    );
  }

  if (catchAll === true) {
    return resultBase(
      parsed.email,
      parsed.domain,
      "catch_all",
      "high",
      true,
      true,
      smtp,
      true,
      smtp.host ?? mx.records[0].exchange,
      mx.records[0].priority
    );
  }

  if (smtp.result === "accepted") {
    return resultBase(
      parsed.email,
      parsed.domain,
      "likely_deliverable",
      "medium",
      true,
      true,
      smtp,
      // false,
      catchAll,
      smtp.host ?? mx.records[0].exchange,
      mx.records[0].priority
    );
  }

  if (smtp.result === "temporary") {
    return resultBase(
      parsed.email,
      parsed.domain,
      "temporary_error",
      "low",
      true,
      true,
      smtp,
      catchAll,
      smtp.host ?? mx.records[0].exchange,
      mx.records[0].priority
    );
  }
  console.log("....veridier, 264,...", parsed.email, parsed.domain, smtp)

  return resultBase(
    parsed.email,
    parsed.domain,
    "unknown",
    "low",
    true,
    true,
    smtp,
    catchAll,
    smtp.host ?? mx.records[0].exchange,
    mx.records[0].priority
  );
}
