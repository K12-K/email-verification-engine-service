import dns from "node:dns/promises";
import net from "node:net";
import { SMTPClient } from "smtp-client";
import { getHostname } from "tldts";

/*
 * ============================================================
 * Configuration
 * ============================================================
 */

const CONFIG = {
  dnsTimeout: 5000,
  smtpTimeout: 10000,

  // Keep this conservative.
  maxDnsConcurrency: 30,
  maxSmtpConcurrency: 5,

  // Used for catch-all testing.
  catchAllEnabled: true,

  // Never use a real customer address for catch-all testing.
  catchAllPrefix: "verify",

  // Use a domain you control for SMTP MAIL FROM.
  //
  // IMPORTANT:
  // Do not use a forged sender address.
  // Configure this as an actual address/domain you control.
  smtpFrom: process.env.VERIFIER_FROM || "verify@example.com",

  // EHLO hostname should ideally be your actual verifier hostname.
  smtpEhlo: process.env.SMTP_EHLO || "verifier.example.com"
};


/*
 * ============================================================
 * Generic concurrency limiter
 * ============================================================
 */

async function mapLimit(items, limit, worker) {
  const results = new Array(items.length);

  let nextIndex = 0;

  async function runner() {
    while (true) {
      const index = nextIndex++;

      if (index >= items.length) {
        return;
      }

      try {
        results[index] = await worker(items[index], index);
      } catch (error) {
        results[index] = {
          error: error?.message || String(error)
        };
      }
    }
  }

  const workers = Array.from(
    { length: Math.min(limit, items.length) },
    () => runner()
  );

  await Promise.all(workers);

  return results;
}


/*
 * ============================================================
 * Timeout helper
 * ============================================================
 */

function withTimeout(promise, ms, message = "Timeout") {
  let timer;

  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => {
      reject(new Error(message));
    }, ms);
  });

  return Promise.race([
    promise.finally(() => clearTimeout(timer)),
    timeout
  ]);
}


/*
 * ============================================================
 * Email parsing
 * ============================================================
 */

function normalizeEmail(email) {
  if (typeof email !== "string") {
    return null;
  }

  const value = email.trim().toLowerCase();

  // Basic sanity check.
  //
  // This is deliberately not attempting to implement the entire
  // RFC mailbox grammar.
  if (!value || value.length > 254) {
    return null;
  }

  const at = value.lastIndexOf("@");

  if (at <= 0 || at === value.length - 1) {
    return null;
  }

  const local = value.slice(0, at);
  const domain = value.slice(at + 1);

  if (!local || !domain) {
    return null;
  }

  // Basic local-part sanity.
  if (
    local.startsWith(".") ||
    local.endsWith(".") ||
    local.includes("..")
  ) {
    return null;
  }

  // Basic domain sanity.
  if (
    domain.startsWith(".") ||
    domain.endsWith(".") ||
    domain.includes("..")
  ) {
    return null;
  }

  const hostname = getHostname(domain);

  if (!hostname) {
    return null;
  }

  return {
    email: value,
    local,
    domain: hostname.toLowerCase()
  };
}


/*
 * ============================================================
 * DNS / MX
 * ============================================================
 *
 * Node's resolveMx() gives us MX records but not their TTL.
 *
 * For a first production version, this cache uses a configurable
 * TTL. If you require exact authoritative TTL extraction, put a
 * DNS packet resolver in front of this layer.
 *
 * The important architectural point is that DNS results are
 * cached separately from individual email results.
 */

const mxCache = new Map();

async function resolveMx(domain) {
  const cached = mxCache.get(domain);

  if (cached && cached.expiresAt > Date.now()) {
    return {
      ...cached.value,
      cache: "hit"
    };
  }

  try {
    const records = await withTimeout(
      dns.resolveMx(domain),
      CONFIG.dnsTimeout,
      "MX lookup timeout"
    );

    const mx = records
      .filter(r => r.exchange)
      .sort((a, b) => a.priority - b.priority)
      .map(r => ({
        exchange: r.exchange.replace(/\.$/, ""),
        priority: r.priority
      }));

    const value = {
      domain,
      hasMx: mx.length > 0,
      mx,
      error: null
    };

    /*
     * If you don't have authoritative TTL available, don't pretend
     * that this is the DNS TTL.
     *
     * 5 minutes is simply an application cache TTL.
     */
    const cacheTtl = 5 * 60 * 1000;

    mxCache.set(domain, {
      value,
      expiresAt: Date.now() + cacheTtl
    });

    return {
      ...value,
      cache: "miss",
      cacheTtlMs: cacheTtl
    };

  } catch (error) {
    const value = {
      domain,
      hasMx: false,
      mx: [],
      error: error?.code || error?.message || "DNS_ERROR"
    };

    /*
     * Negative caching should normally be shorter.
     */
    mxCache.set(domain, {
      value,
      expiresAt: Date.now() + 60_000
    });

    return {
      ...value,
      cache: "miss"
    };
  }
}


/*
 * ============================================================
 * SMTP classification
 * ============================================================
 */

function classifySmtpCode(code) {
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


/*
 * ============================================================
 * SMTP RCPT check
 * ============================================================
 */

async function smtpCheck(mxHost, email) {
  const client = new SMTPClient({
    host: mxHost,
    port: 25,
    // timeout: CONFIG.smtpTimeout
  });

  try {
    await withTimeout(
      client.connect(),
      CONFIG.smtpTimeout,
      "SMTP connect timeout"
    );

    await withTimeout(
      client.greet({
        hostname: CONFIG.smtpEhlo
      }),
      CONFIG.smtpTimeout,
      "SMTP EHLO timeout"
    );

    /*
     * MAIL FROM is required before RCPT TO.
     *
     * Use a sender domain/address that you control.
     */
    await withTimeout(
      client.mail({
        from: CONFIG.smtpFrom
      }),
      CONFIG.smtpTimeout,
      "SMTP MAIL FROM timeout"
    );

    const response = await withTimeout(
      client.rcpt({
        to: email
      }),
      CONFIG.smtpTimeout,
      "SMTP RCPT timeout"
    );

    /*
     * smtp-client normally throws on unsuccessful SMTP responses.
     * This branch exists mainly for implementations that expose
     * the response directly.
     */

    let code:any = null;

    if (typeof response === "string") {
      code = Number.parseInt(response.slice(0, 3), 10);
    }

    await safeQuit(client);

    return {
      host: mxHost,
      result: classifySmtpCode(code),
      code,
      response
    };

  } catch (error) {

    const code =
      error?.code && /^\d{3}$/.test(String(error.code))
        ? Number(error.code)
        : null;

    await safeQuit(client);

    /*
     * smtp-client may throw an error containing the server response.
     * Preserve it for diagnostics.
     */
    const message =
      error?.message ||
      String(error);

    if (code) {
      return {
        host: mxHost,
        result: classifySmtpCode(code),
        code,
        response: message
      };
    }

    return {
      host: mxHost,
      result: "error",
      code: null,
      response: message
    };
  }
}


async function safeQuit(client) {
  try {
    await client.quit();
  } catch {
    try {
      client.close();
    } catch {}
  }
}


/*
 * ============================================================
 * MX → SMTP
 * ============================================================
 */

async function checkEmailAgainstMx(email, mxRecords) {
  if (!mxRecords.length) {
    return {
      smtp: "not_checked",
      host: null
    };
  }

  /*
   * MX priority matters.
   *
   * Try the lowest-priority-number server first.
   * If it fails at the connection/protocol level, try another.
   */
  for (const mx of mxRecords) {
    const result = await smtpCheck(
      mx.exchange,
      email
    );

    if (result.result === "accepted") {
      return {
        smtp: "accepted",
        host: mx.exchange,
        code: result.code,
        response: result.response
      };
    }

    if (result.result === "rejected") {
      return {
        smtp: "rejected",
        host: mx.exchange,
        code: result.code,
        response: result.response
      };
    }

    /*
     * Temporary/error:
     * try the next MX if available.
     */
  }

  return {
    smtp: "unknown",
    host: null
  };
}


/*
 * ============================================================
 * Catch-all detection
 * ============================================================
 */

function randomCatchAllAddress(domain) {
  const random = cryptoRandomString(24);

  return `${CONFIG.catchAllPrefix}-${random}@${domain}`;
}


function cryptoRandomString(length) {
  const chars =
    "abcdefghijklmnopqrstuvwxyz0123456789";

  let result = "";

  for (let i = 0; i < length; i++) {
    result += chars[
      Math.floor(Math.random() * chars.length)
    ];
  }

  return result;
}


async function detectCatchAll(domain, mxRecords) {
  if (!CONFIG.catchAllEnabled) {
    return {
      checked: false,
      catchAll: null
    };
  }

  if (!mxRecords.length) {
    return {
      checked: false,
      catchAll: false
    };
  }

  const testEmail = randomCatchAllAddress(domain);

  const result = await checkEmailAgainstMx(
    testEmail,
    mxRecords
  );

  /*
   * A randomly generated address being accepted is evidence
   * of catch-all behavior, NOT proof.
   *
   * Some servers deliberately accept RCPT and reject later.
   */
  if (result.smtp === "accepted") {
    return {
      checked: true,
      catchAll: true,
      testEmail
    };
  }

  if (result.smtp === "rejected") {
    return {
      checked: true,
      catchAll: false,
      testEmail
    };
  }

  return {
    checked: true,
    catchAll: null,
    testEmail
  };
}


/*
 * ============================================================
 * Final status
 * ============================================================
 */

function classify({
  syntax,
  mx,
  smtp,
  catchAll
}) {

  if (!syntax) {
    return {
      status: "invalid",
      confidence: "high"
    };
  }

  if (!mx.hasMx) {
    return {
      status: "no_mx",
      confidence: "high"
    };
  }

  if (catchAll === true && smtp === "accepted") {
    return {
      status: "catch_all",
      confidence: "high"
    };
  }

  if (smtp === "rejected") {
    return {
      status: "undeliverable",
      confidence: "high"
    };
  }

  if (smtp === "accepted") {
    return {
      status: "likely_deliverable",
      confidence: "medium"
    };
  }

  if (smtp === "unknown") {
    return {
      status: "unknown",
      confidence: "low"
    };
  }

  return {
    status: "unknown",
    confidence: "low"
  };
}


/*
 * ============================================================
 * Single email
 * ============================================================
 */

export async function verifyEmail(email) {

  const parsed = normalizeEmail(email);

  if (!parsed) {
    return {
      email,
      status: "invalid",
      confidence: "high",

      checks: {
        syntax: false,
        mx: false,
        smtp: "not_checked",
        catchAll: false
      }
    };
  }

  const mx = await resolveMx(parsed.domain);

  if (!mx.hasMx) {
    return {
      email: parsed.email,
      domain: parsed.domain,

      status: "no_mx",
      confidence: "high",

      checks: {
        syntax: true,
        mx: false,
        smtp: "not_checked",
        catchAll: false
      },

      dns: mx
    };
  }

  /*
   * Detect catch-all once per domain in a real production system.
   * For this single-email function we perform the check here.
   */
  const catchAll = await detectCatchAll(
    parsed.domain,
    mx.mx
  );

  const smtpResult = await checkEmailAgainstMx(
    parsed.email,
    mx.mx
  );

  const classification = classify({
    syntax: true,
    mx,
    smtp: smtpResult.smtp,
    catchAll: catchAll.catchAll
  });

  return {
    email: parsed.email,
    domain: parsed.domain,

    status: classification.status,
    confidence: classification.confidence,

    checks: {
      syntax: true,
      mx: true,
      smtp: smtpResult.smtp,
      catchAll: catchAll.catchAll
    },

    dns: {
      mx: mx.mx,
      cache: mx.cache
    },

    smtp: smtpResult,

    catchAll
  };
}


/*
 * ============================================================
 * Bulk verification
 * ============================================================
 */

export async function verifyEmails(emails) {

  if (!Array.isArray(emails)) {
    throw new TypeError("emails must be an array");
  }

  /*
   * Protect your API from accidentally huge requests.
   */
  if (emails.length > 1000) {
    throw new Error("Maximum 1000 emails per request");
  }

  /*
   * Normalize first.
   */
  const parsed = emails.map(email => ({
    original: email,
    parsed: normalizeEmail(email)
  }));

  /*
   * Unique domains.
   */
  const domains = [
    ...new Set(
      parsed
        .map(x => x.parsed?.domain)
        .filter(Boolean)
    )
  ];

  /*
   * ----------------------------------------------------------
   * Step 1: DNS
   * ----------------------------------------------------------
   */

  const dnsResults = await mapLimit(
    domains,
    CONFIG.maxDnsConcurrency,
    domain => resolveMx(domain)
  );

  const dnsMap = new Map(
    dnsResults.map(result => [
      result.domain,
      result
    ])
  );

  /*
   * ----------------------------------------------------------
   * Step 2: Catch-all detection
   * ----------------------------------------------------------
   *
   * One test per DOMAIN, not per email.
   * ----------------------------------------------------------
   */

  const domainsWithMx = domains.filter(
    domain => dnsMap.get(domain)?.hasMx
  );

  const catchAllResults = await mapLimit(
    domainsWithMx,
    CONFIG.maxSmtpConcurrency,
    async domain => {

      const dnsResult = dnsMap.get(domain);

      return {
        domain,
        result: await detectCatchAll(
          domain,
          dnsResult.mx
        )
      };
    }
  );

  const catchAllMap = new Map(
    catchAllResults.map(x => [
      x.domain,
      x.result
    ])
  );

  /*
   * ----------------------------------------------------------
   * Step 3: SMTP recipient checks
   * ----------------------------------------------------------
   */

  const validEmails = parsed.filter(
    x => x.parsed && dnsMap.get(x.parsed.domain)?.hasMx
  );

  const smtpResults = await mapLimit(
    validEmails,
    CONFIG.maxSmtpConcurrency,
    async item => {

      const { parsed: email } = item;

      const dnsResult = dnsMap.get(email.domain);

      const smtp = await checkEmailAgainstMx(
        email.email,
        dnsResult.mx
      );

      return {
        email: email.email,
        smtp
      };
    }
  );

  const smtpMap = new Map(
    smtpResults.map(x => [
      x.email,
      x.smtp
    ])
  );

  /*
   * ----------------------------------------------------------
   * Step 4: Final result
   * ----------------------------------------------------------
   */

  return parsed.map(item => {

    if (!item.parsed) {
      return {
        email: item.original,
        status: "invalid",
        confidence: "high",

        checks: {
          syntax: false,
          mx: false,
          smtp: "not_checked",
          catchAll: false
        }
      };
    }

    const email = item.parsed;

    const dnsResult = dnsMap.get(email.domain);

    if (!dnsResult?.hasMx) {
      return {
        email: email.email,
        domain: email.domain,

        status: "no_mx",
        confidence: "high",

        checks: {
          syntax: true,
          mx: false,
          smtp: "not_checked",
          catchAll: false
        }
      };
    }

    const smtp = smtpMap.get(email.email);

    const catchAll =
      catchAllMap.get(email.domain);

    const classification = classify({
      syntax: true,
      mx: dnsResult,
      smtp: smtp?.smtp,
      catchAll: catchAll?.catchAll
    });

    return {
      email: email.email,
      domain: email.domain,

      status: classification.status,
      confidence: classification.confidence,

      checks: {
        syntax: true,
        mx: true,
        smtp: smtp?.smtp || "unknown",
        catchAll: catchAll?.catchAll ?? null
      },

      dns: {
        mx: dnsResult.mx,
        cache: dnsResult.cache
      },

      smtp: smtp || {
        smtp: "not_checked"
      },

      catchAll: catchAll || {
        checked: false,
        catchAll: null
      }
    };
  });
}


/*
 * ============================================================
 * Example
 * ============================================================
 */

if (import.meta.url === `file://${process.argv[1]}`) {

  const emails = [
    "contact@misecode.com",
    "nicn@misecode.com"
  ];

  const results = await verifyEmails(emails);

  console.dir(results, {
    depth: null
  });
}
