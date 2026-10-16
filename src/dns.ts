import dgram from "node:dgram";
import net from "node:net";
import dns from "node:dns";
import dnsPacket from "dns-packet";
import { redis } from "./redis.ts";
import { config } from "./config.ts";
import type { MxRecord, MxResult } from "./types.ts";

const DNS_PORT = 53;
const DNS_REDIS_PREFIX = `${config.REDIS_KEY_PREFIX}:dns:mx`;

// interface DecodedDns {
//   flags?: number;
//   answers?: Array<{
//     name: string;
//     type: string;
//     ttl?: number;
//     preference?: number;
//     exchange?: string;
//     data?: any;
//   }>;
//   authorities?: Array<{
//     name: string;
//     type: string;
//     ttl?: number;
//     data?: any;
//   }>;
// }

interface MxData {
  preference: number;
  exchange: string;
}

interface DecodedSoaData {
  minimum?: number;
}

interface DnsRecord {
  name: string;
  type: string;
  ttl?: number;
  data?: unknown;
}

interface DecodedDns {
  id?: number;
  flags?: number;

  answers?: DnsRecord[];

  authorities?: Array<{
    name: string;
    type: string;
    ttl?: number;
    data?: DecodedSoaData | Record<string, unknown>;
  }>;
}

function resolverAddress(): string {
  return config.DNS_SERVER || dns.getServers()[0] || "127.0.0.11";
}

function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("DNS timeout")), ms);
    promise.then(
      value => {
        clearTimeout(timer);
        resolve(value);
      },
      error => {
        clearTimeout(timer);
        reject(error);
      }
    );
  });
}

function queryUdp(server: string, packet: Buffer): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const socket = dgram.createSocket(net.isIPv6(server) ? "udp6" : "udp4");

    const cleanup = () => {
      try {
        socket.close();
      } catch {}
    };

    const timer = setTimeout(() => {
      cleanup();
      reject(new Error("DNS UDP timeout"));
    }, config.DNS_TIMEOUT_MS);

    socket.once("error", error => {
      clearTimeout(timer);
      cleanup();
      reject(error);
    });

    socket.once("message", message => {
      clearTimeout(timer);
      cleanup();
      resolve(message);
    });

    socket.send(packet, DNS_PORT, server, error => {
      if (error) {
        clearTimeout(timer);
        cleanup();
        reject(error);
      }
    });
  });
}

function queryTcp(server: string, packet: Buffer): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({
      host: server,
      port: DNS_PORT
    });

    let buffer = Buffer.alloc(0);
    let expectedLength: number | null = null;

    const timer = setTimeout(() => {
      socket.destroy();
      reject(new Error("DNS TCP timeout"));
    }, config.DNS_TIMEOUT_MS);

    const finish = (error?: Error) => {
      clearTimeout(timer);
      if (error) reject(error);
    };

    socket.once("error", error => finish(error));

    socket.on("data", (chunk: Buffer) => {
      buffer = Buffer.concat([buffer, chunk]);

      if (expectedLength === null && buffer.length >= 2) {
        expectedLength = buffer.readUInt16BE(0);
      }

      if (expectedLength !== null && buffer.length >= expectedLength + 2) {
        const body = buffer.subarray(2, expectedLength + 2);
        clearTimeout(timer);
        socket.end();
        resolve(body);
      }
    });

    socket.on("connect", () => {
      const framed = Buffer.alloc(packet.length + 2);
      framed.writeUInt16BE(packet.length, 0);
      packet.copy(framed, 2);
      socket.write(framed);
    });
  });
}

async function queryDns(
  name: string,
  type: "MX" | "A" | "AAAA"
): Promise<DecodedDns> {
  const id = Math.floor(Math.random() * 65536);

  const packet = dnsPacket.encode({
    type: "query",
    id,
    flags: dnsPacket.RECURSION_DESIRED,
    questions: [{ type, class: "IN", name }]
  });

  const server = resolverAddress();
  console.log("....dns....147.....", server)
  let response = await withTimeout(queryUdp(server, packet), config.DNS_TIMEOUT_MS);
  let decoded = dnsPacket.decode(response) as DecodedDns;

  if (decoded.flags !== undefined &&
      (decoded.flags & dnsPacket.TRUNCATED_RESPONSE) !== 0) {
    response = await withTimeout(queryTcp(server, packet), config.DNS_TIMEOUT_MS);
    decoded = dnsPacket.decode(response) as DecodedDns;
  }
  console.dir(
    {
      query: {
        name,
        type,
        server
      },
      decoded
    },
    { depth: null }
  );
  
  
  return decoded;
}

function responseCode(flags = 0): number {
  return flags & 0x000f;
}

function positiveTtl(records: Array<{ ttl?: number }> | undefined): number | null {
  const values = (records ?? [])
    .map(r => r.ttl)
    .filter((v): v is number => typeof v === "number" && Number.isFinite(v));

  return values.length ? Math.min(...values) : null;
}

function negativeTtl(authorities: DecodedDns["authorities"]): number | null {
  const soa = (authorities ?? []).find(r => r.type === "SOA");
  if (!soa || typeof soa.ttl !== "number") return null;

  const minimum =
    soa.data && typeof soa.data.minimum === "number"
      ? soa.data.minimum
      : soa.ttl;

  return Math.min(soa.ttl, minimum);
}

function clampPositiveTtl(ttl: number | null): number {
  const source = ttl ?? config.DNS_CACHE_MIN_TTL_SECONDS;
  return Math.max(
    config.DNS_CACHE_MIN_TTL_SECONDS,
    Math.min(source, config.DNS_CACHE_MAX_TTL_SECONDS)
  );
}

function clampNegativeTtl(ttl: number | null): number {
  const source = ttl ?? 60;
  return Math.max(
    1,
    Math.min(source, config.DNS_NEGATIVE_CACHE_MAX_TTL_SECONDS)
  );
}

function cacheKey(domain: string): string {
  return `${DNS_REDIS_PREFIX}:${domain}`;
}

function isMxRecord(
  record: DnsRecord
): record is DnsRecord & {
  type: "MX";
  data: {
    preference: number;
    exchange: string;
  };
} {
  return (
    record.type === "MX" &&
    typeof record.data === "object" &&
    record.data !== null &&
    "preference" in record.data &&
    "exchange" in record.data &&
    typeof record.data.preference === "number" &&
    typeof record.data.exchange === "string"
  );
}

export async function resolveMx(domain: string): Promise<MxResult> {
  const key = cacheKey(domain);
  const cached = await redis.get(key);

  if (cached) {
    return JSON.parse(cached) as MxResult;
  }

  try {
    const response = await queryDns(domain, "MX");
    const code = responseCode(response.flags);
    const answers = response.answers ?? [];

    if (code === 3) {
      const result: MxResult = {
        domain,
        records: [],
        ttl: null,
        negative: true,
        nullMx: false,
        implicitMx: false,
        errorCode: "NXDOMAIN",
        errorMessage: "Domain does not exist"
      };

      await redis.set(
        key,
        JSON.stringify(result),
        "EX",
        clampNegativeTtl(negativeTtl(response.authorities))
      );

      return result;
    }

    if (code !== 0) {
      const result: MxResult = {
        domain,
        records: [],
        ttl: null,
        negative: true,
        nullMx: false,
        implicitMx: false,
        errorCode: `DNS_RCODE_${code}`,
        errorMessage: `DNS resolver returned RCODE ${code}`
      };

      await redis.set(
        key,
        JSON.stringify(result),
        "EX",
        30
      );

      return result;
    }

    // const mxAnswers = answers.filter(
    //   record => record.type === "MX" && typeof record.exchange === "string"
    // );

    // const ttl = positiveTtl(mxAnswers);
    // const records: MxRecord[] = mxAnswers
    //   .map(record => ({
    //     exchange: record.exchange!.replace(/\.$/, "").toLowerCase(),
    //     priority: Number(record.preference ?? 0),
    //     ttl: Number(record.ttl ?? ttl ?? config.DNS_CACHE_MIN_TTL_SECONDS)
    //   }))
    //   .sort((a, b) => a.priority - b.priority);

    // const mxAnswers = answers.filter(
    //   record =>
    //     record.type === "MX" &&
    //     record.data &&
    //     typeof record.data.exchange === "string" &&
    //     typeof record.data.preference === "number"
    // );
    const mxAnswers = (answers ?? []).filter(isMxRecord);

    const ttl = positiveTtl(mxAnswers);
    
    const records: MxRecord[] = mxAnswers
      .map(record => ({
        exchange: record.data.exchange
          .replace(/\.$/, "")
          .toLowerCase(),
    
        priority: Number(record.data.preference),
    
        ttl: Number(
          record.ttl ??
          ttl ??
          config.DNS_CACHE_MIN_TTL_SECONDS
        )
      }))
      .sort((a, b) => a.priority - b.priority);
        
    const nullMx = records.length === 1 && records[0].exchange === ".";

    if (nullMx) {
      const result: MxResult = {
        domain,
        records: [],
        ttl,
        negative: false,
        nullMx: true,
        implicitMx: false,
        errorCode: "NULL_MX",
        errorMessage: "Domain explicitly publishes Null MX"
      };

      await redis.set(
        key,
        JSON.stringify(result),
        "EX",
        clampPositiveTtl(ttl)
      );

      return result;
    }

    if (records.length > 0) {
      const result: MxResult = {
        domain,
        records,
        ttl,
        negative: false,
        nullMx: false,
        implicitMx: false,
        errorCode: null,
        errorMessage: null
      };

      await redis.set(
        key,
        JSON.stringify(result),
        "EX",
        clampPositiveTtl(ttl)
      );

      return result;
    }

    // RFC 5321 permits implicit MX fallback to the domain's A/AAAA
    // when there is no MX record.
    const [aResponse, aaaaResponse] = await Promise.all([
      queryDns(domain, "A"),
      queryDns(domain, "AAAA")
    ]);

    const addressCount =
      (aResponse.answers ?? []).filter(r => r.type === "A").length +
      (aaaaResponse.answers ?? []).filter(r => r.type === "AAAA").length;

    if (addressCount > 0) {
      const implicitTtl = Math.min(
        positiveTtl(aResponse.answers) ?? config.DNS_CACHE_MIN_TTL_SECONDS,
        positiveTtl(aaaaResponse.answers) ?? config.DNS_CACHE_MIN_TTL_SECONDS
      );

      const result: MxResult = {
        domain,
        records: [{
          exchange: domain,
          priority: 0,
          ttl: implicitTtl
        }],
        ttl: implicitTtl,
        negative: false,
        nullMx: false,
        implicitMx: true,
        errorCode: null,
        errorMessage: null
      };

      await redis.set(
        key,
        JSON.stringify(result),
        "EX",
        clampPositiveTtl(implicitTtl)
      );

      return result;
    }

    const result: MxResult = {
      domain,
      records: [],
      ttl: null,
      negative: true,
      nullMx: false,
      implicitMx: false,
      errorCode: "NO_MX_OR_ADDRESS",
      errorMessage: "No MX, A, or AAAA record was found"
    };

    await redis.set(
      key,
      JSON.stringify(result),
      "EX",
      clampNegativeTtl(negativeTtl(response.authorities))
    );

    return result;
  } catch (error) {
    console.log("...dns..381u,,,", error)
    const message = error instanceof Error ? error.message : String(error);

    // Resolver/network failures are cached only briefly so a transient
    // resolver outage does not poison the domain for a long time.
    const result: MxResult = {
      domain,
      records: [],
      ttl: null,
      negative: true,
      nullMx: false,
      implicitMx: false,
      errorCode: "DNS_ERROR",
      errorMessage: message
    };

    await redis.set(key, JSON.stringify(result), "EX", 15);
    return result;
  }
}
