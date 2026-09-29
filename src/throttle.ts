import { config } from "./config.ts";
import { redis } from "./redis.ts";

const RELEASE_LOCK_SCRIPT = `
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
end
return 0
`;

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function randomToken(): string {
  return `${process.pid}-${Date.now()}-${Math.random()}`;
}

export async function acquireDomainSlot(domain: string): Promise<void> {
  const limit = config.SMTP_DOMAIN_RATE_LIMIT;
  const windowMs = config.SMTP_DOMAIN_RATE_WINDOW_MS;

  while (true) {
    const bucket = Math.floor(Date.now() / windowMs);
    const key = `${config.REDIS_KEY_PREFIX}:smtp-rate:${domain}:${bucket}`;

    const count = await redis.incr(key);

    if (count === 1) {
      await redis.pexpire(key, windowMs + 1000);
    }

    if (count <= limit) {
      return;
    }

    const remaining = windowMs - (Date.now() % windowMs);
    await sleep(Math.max(25, remaining));
  }
}

export async function withRedisLock<T>(
  key: string,
  ttlMs: number,
  fn: () => Promise<T>
): Promise<T> {
  const token = randomToken();
  const deadline = Date.now() + ttlMs;

  while (Date.now() < deadline) {
    const acquired = await redis.set(key, token, "PX", ttlMs, "NX");

    if (acquired === "OK") {
      try {
        return await fn();
      } finally {
        await redis.eval(RELEASE_LOCK_SCRIPT, 1, key, token);
      }
    }

    await sleep(100);
  }

  throw new Error(`Timed out acquiring Redis lock: ${key}`);
}
