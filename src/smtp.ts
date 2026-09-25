import { SMTPClient } from "smtp-client";
import { config } from "./config.js";
import type {
  MxRecord,
  SmtpResult
} from "./types.js";

function timeout<T>(
  promise: Promise<T>,
  ms: number
): Promise<T> {

  return new Promise((resolve, reject) => {

    const timer = setTimeout(() => {
      reject(new Error("SMTP timeout"));
    }, ms);

    promise
      .then(value => {
        clearTimeout(timer);
        resolve(value);
      })
      .catch(error => {
        clearTimeout(timer);
        reject(error);
      });
  });
}

function parseCode(
  error: unknown
): number | null {

  const message =
    error instanceof Error
      ? error.message
      : String(error);

  const match = message.match(
    /\b([245]\d{2})\b/
  );

  return match
    ? Number(match[1])
    : null;
}

function classifyCode(
  code: number | null
): SmtpResult["result"] {

  if (!code) {
    return "unknown";
  }

  if (code >= 200 && code < 300) {
    return "accepted";
  }

  if (code >= 400 && code < 500) {
    return "temporary";
  }

  if (code >= 500 && code < 600) {
    return "rejected";
  }

  return "unknown";
}

export async function smtpVerify(
  mx: MxRecord,
  email: string
): Promise<SmtpResult> {

  const client = new SMTPClient({
    host: mx.exchange,
    port: 25,
    // timeout: config.SMTP_TIMEOUT_MS
  });

  try {

    await timeout(
      client.connect(),
      config.SMTP_TIMEOUT_MS
    );

    await timeout(
      client.greet({
        hostname: config.SMTP_EHLO
      }),
      config.SMTP_TIMEOUT_MS
    );

    await timeout(
      client.mail({
        from: config.SMTP_FROM
      }),
      config.SMTP_TIMEOUT_MS
    );

    try {

      await timeout(
        client.rcpt({
          to: email
        }),
        config.SMTP_TIMEOUT_MS
      );

      await safeQuit(client);

      return {
        result: "accepted",
        code: 250,
        response: "RCPT accepted",
        host: mx.exchange
      };

    } catch (error) {

      const code = parseCode(error);

      await safeQuit(client);

      return {
        result: classifyCode(code),
        code,
        response:
          error instanceof Error
            ? error.message
            : String(error),
        host: mx.exchange
      };
    }

  } catch (error) {

    await safeQuit(client);

    return {
      result: "error",
      code: parseCode(error),
      response:
        error instanceof Error
          ? error.message
          : String(error),
      host: mx.exchange
    };
  }
}

async function safeQuit(
  client: SMTPClient
) {

  try {
    await client.quit();
  } catch {
    try {
      client.close();
    } catch {}
  }
}
