declare module "dns-packet" {
  export const RECURSION_DESIRED: number;
  export const TRUNCATED_RESPONSE: number;

  export interface Question {
    type: string;
    name: string;
    class?: string;
  }

  export interface Answer {
    name: string;
    type: string;
    class?: string;
    ttl?: number;
    preference?: number;
    exchange?: string;
    data?: any;
  }

  export interface Packet {
    type: string;
    id?: number;
    flags?: number;
    questions?: Question[];
    answers?: Answer[];
    authorities?: Answer[];
    additionals?: Answer[];
  }

  function encode(packet: Packet): Buffer;
  function decode(buffer: Buffer): Packet;
  function streamEncode(packet: Packet): Buffer;
  function streamDecode(buffer: Buffer): Packet;

  const dnsPacket: {
    RECURSION_DESIRED: number;
    TRUNCATED_RESPONSE: number;
    encode: typeof encode;
    decode: typeof decode;
    streamEncode: typeof streamEncode;
    streamDecode: typeof streamDecode;
  };

  export = dnsPacket;
}
