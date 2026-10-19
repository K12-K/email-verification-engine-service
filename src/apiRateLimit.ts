import { createHash } from "node:crypto";
import { redis } from "./redis.ts";
import { config } from "./config.ts";

const INCREMENT_SCRIPT = `
local current = redis.call('INCR', KEYS[1])
if current == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
local ttl = redis.call('PTTL', KEYS[1])
return {current, ttl}
`;

export async function checkApiRateLimit(identifier: string): Promise<{
  allowed: boolean;
  remaining: number;
  retryAfterSeconds: number;
}> {
  const fingerprint = createHash("sha256").update(identifier).digest("hex");
  const key = `${config.REDIS_KEY_PREFIX}:api-rate:${fingerprint}`;
  const result = await redis.eval(
    INCREMENT_SCRIPT,
    1,
    key,
    String(config.API_RATE_WINDOW_MS)
  ) as [number, number];

  const current = Number(result[0]);
  const ttl = Math.max(0, Number(result[1]));

  return {
    allowed: current <= config.API_RATE_LIMIT,
    remaining: Math.max(0, config.API_RATE_LIMIT - current),
    retryAfterSeconds: Math.max(1, Math.ceil(ttl / 1000))
  };
}
