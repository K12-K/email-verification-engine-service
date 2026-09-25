import dns from "node:dns/promises";
import { config } from "./config.js";
import type { MxResult } from "./types.js";

interface CacheEntry {
  value: MxResult;
  expiresAt: number;
}

const cache = new Map<string, CacheEntry>();

function timeout<T>(
  promise: Promise<T>,
  ms: number
): Promise<T> {

  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error("DNS timeout"));
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

export async function resolveMx(
  domain: string
): Promise<MxResult> {

  const cached = cache.get(domain);

  if (
    cached &&
    cached.expiresAt > Date.now()
  ) {
    return cached.value;
  }

  try {
    const records = await timeout(
      dns.resolveMx(domain),
      config.DNS_TIMEOUT_MS
    );

    const result: MxResult = {
      domain,
      records: records
        .map(record => ({
          exchange: record.exchange.replace(/\.$/, ""),
          priority: record.priority
        }))
        .sort((a, b) =>
          a.priority - b.priority
        ),
      ttl: null,
      negative: false
    };

    /*
     * IMPORTANT:
     *
     * resolveMx() does not expose the DNS TTL.
     * Therefore this fallback cache duration is NOT
     * presented as the authoritative DNS TTL.
     *
     * The next iteration should use a raw DNS query
     * when exact TTL extraction is required.
     */

    cache.set(domain, {
      value: result,
      expiresAt: Date.now() + 5 * 60_000
    });

    return result;

  } catch (error) {

    const result: MxResult = {
      domain,
      records: [],
      ttl: null,
      negative: true
    };

    cache.set(domain, {
      value: result,
      expiresAt: Date.now() + 60_000
    });

    return result;
  }
}
