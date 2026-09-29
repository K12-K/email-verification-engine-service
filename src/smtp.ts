import { SMTPClient } from "smtp-client";
import { config } from "./config.ts";
import type { MxRecord, SmtpResult } from "./types.ts";
import { acquireDomainSlot } from "./throttle.ts";

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function parseCode(error: unknown): number | null {
  const message = error instanceof Error ? error.message : String(error);
  const match = message.match(/\b([245]\d{2})\b/);
  return match ? Number(match[1]) : null;
}

function parseResponse(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.slice(0, 1000);
}

function classifyCode(code: number | null): SmtpResult["result"] {
  if (!code) return "unknown";
  if (code >= 200 && code < 300) return "accepted";
  if (code >= 400 && code < 500) return "temporary";
  if (code >= 500 && code < 600) return "rejected";
  return "unknown";
}

async function safeQuit(client: SMTPClient): Promise<void> {
  try {
    await client.quit({ timeout: Math.min(config.SMTP_TIMEOUT_MS, 3000) });
  } catch {
    try {
      await client.close({ timeout: 1000 });
    } catch {}
  }
}

async function attempt(
  mx: MxRecord,
  email: string,
  domain: string
): Promise<SmtpResult> {
  await acquireDomainSlot(domain);

  const client = new SMTPClient({
    host: mx.exchange,
    port: 25
  });

  try {
    await client.connect({ timeout: config.SMTP_TIMEOUT_MS });

    await client.greet({
      hostname: config.SMTP_EHLO,
      timeout: config.SMTP_TIMEOUT_MS
    });

    let tls = client.isSecure();

    if (
      config.SMTP_USE_STARTTLS &&
      !tls &&
      client.hasExtension("STARTTLS")
    ) {
      await client.secure({
        timeout: config.SMTP_TIMEOUT_MS
      });

      // STARTTLS resets SMTP's extension state; EHLO must be sent again.
      await client.ehlo({
        hostname: config.SMTP_EHLO,
        timeout: config.SMTP_TIMEOUT_MS
      });

      tls = client.isSecure();
    }

    if (config.SMTP_REQUIRE_STARTTLS && !tls) {
      return {
        result: "unknown",
        code: null,
        response: "STARTTLS is required but unavailable",
        host: mx.exchange,
        tls: false,
        attempts: 1
      };
    }

    await client.mail({
      from: config.SMTP_FROM,
      timeout: config.SMTP_TIMEOUT_MS
    });

    try {
      await client.rcpt({
        to: email,
        timeout: config.SMTP_TIMEOUT_MS
      });

      return {
        result: "accepted",
        code: 250,
        response: "RCPT accepted",
        host: mx.exchange,
        tls,
        attempts: 1
      };
    } catch (error) {
      const code = parseCode(error);

      return {
        result: classifyCode(code),
        code,
        response: parseResponse(error),
        host: mx.exchange,
        tls,
        attempts: 1
      };
    }
  } catch (error) {
    const code = parseCode(error);

    return {
      result: code
        ? classifyCode(code)
        : "error",
      code,
      response: parseResponse(error),
      host: mx.exchange,
      tls: client.isSecure(),
      attempts: 1
    };
  } finally {
    await safeQuit(client);
  }
}

function isRetryable(result: SmtpResult): boolean {
  return (
    result.result === "temporary" ||
    result.result === "error" ||
    result.code === 421 ||
    result.code === 450 ||
    result.code === 451 ||
    result.code === 452
  );
}

export async function smtpVerify(
  mxHosts: MxRecord[],
  email: string,
  domain: string
): Promise<SmtpResult> {
  const hosts = mxHosts.slice(0, config.SMTP_MAX_MX_HOSTS);

  let last: SmtpResult = {
    result: "unknown",
    code: null,
    response: "No MX host available",
    host: null,
    tls: false,
    attempts: 0
  };

  for (const mx of hosts) {
    for (let attemptNumber = 1;
         attemptNumber <= config.SMTP_MAX_ATTEMPTS_PER_HOST;
         attemptNumber++) {
      const result = await attempt(mx, email, domain);
      result.attempts = attemptNumber;
      last = result;

      // A recipient rejection is meaningful and should not cause
      // repeated probing against the same host.
      if (!isRetryable(result)) {
        return result;
      }

      if (attemptNumber < config.SMTP_MAX_ATTEMPTS_PER_HOST) {
        const delay =
          config.SMTP_RETRY_BASE_DELAY_MS *
          2 ** (attemptNumber - 1);

        await sleep(delay);
      }
    }

    // For a temporary/network failure, try the next MX host.
  }

  return last;
}
