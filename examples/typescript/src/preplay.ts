import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";
import { decodeEntries, type Transaction } from "./entries.js";

const ENDPOINT = new URL(process.env.SODAE_PREPLAY_URL ?? "http://ams.rpc.sodae.io:10301");
const TOKEN = process.env.SODAE_TOKEN;
const PROTO = fileURLToPath(new URL("../../proto/shredstream.proto", import.meta.url));
const FATAL = new Set([
  "UNAUTHENTICATED",
  "NOT_ENTITLED",
  "IP_NOT_ALLOWED",
  "QUOTA_EXCEEDED",
  "AUTH_RATE_LIMITED",
]);

interface EntriesMessage {
  slot: string;
  entries: Buffer;
}

const definition = protoLoader.loadSync(PROTO, { longs: String });
const shredstream = grpc.loadPackageDefinition(definition).shredstream as grpc.GrpcObject;
const ShredstreamProxy = shredstream.ShredstreamProxy as grpc.ServiceClientConstructor;

function handle(message: EntriesMessage, program: string | undefined): void {
  let entries;
  try {
    entries = decodeEntries(message.entries);
  } catch (error) {
    console.error(`slot ${message.slot}: could not decode entries: ${error}`);
    return;
  }
  const transactions = entries.flatMap((entry) => entry.transactions);
  if (!program) {
    console.log(message.slot, `entries=${entries.length}`, `transactions=${transactions.length}`);
    return;
  }
  for (const tx of transactions) {
    if (tx.accountKeys.includes(program)) printTransaction(message.slot, tx);
  }
}

function printTransaction(slot: string, tx: Transaction): void {
  console.log(
    slot,
    tx.signatures[0],
    `signer=${tx.accountKeys[0]}`,
    `version=${tx.version}`,
    `lookups=${tx.addressTableLookups.length}`,
  );
  for (const ix of tx.instructions) {
    console.log(
      `  ${tx.accountKeys[ix.programIdIndex]}`,
      `accounts=${ix.accounts.length}`,
      `data=${ix.data.length}B`,
    );
  }
}

function subscribe(program: string | undefined, onConnected: () => void): Promise<void> {
  const credentials =
    ENDPOINT.protocol === "https:"
      ? grpc.credentials.createSsl()
      : grpc.credentials.createInsecure();
  const client = new ShredstreamProxy(ENDPOINT.host, credentials, {
    "grpc.max_receive_message_length": 64 * 1024 * 1024,
  });
  const metadata = new grpc.Metadata();
  metadata.set("x-token", TOKEN ?? "");
  const call = client.SubscribeEntries({}, metadata) as grpc.ClientReadableStream<EntriesMessage>;
  return new Promise((resolve, reject) => {
    call.on("data", (message: EntriesMessage) => {
      onConnected();
      handle(message, program);
    });
    call.on("error", (error) => {
      client.close();
      reject(error);
    });
    call.on("end", () => {
      client.close();
      resolve();
    });
  });
}

function errorCode(error: unknown): string | undefined {
  const fromMetadata = (error as grpc.ServiceError).metadata?.get("x-error-code")[0];
  if (fromMetadata) return String(fromMetadata);
  const message = error instanceof Error ? error.message : String(error);
  return /\(code: ([A-Z_]+)\)/.exec(message)?.[1];
}

async function main(): Promise<void> {
  if (!TOKEN) throw new Error("set SODAE_TOKEN to your API token");
  const program = process.argv[2];
  let delay = 1_000;
  for (;;) {
    try {
      await subscribe(program, () => (delay = 1_000));
      console.error("stream closed by the server");
    } catch (error) {
      const code = errorCode(error);
      const details = (error as grpc.ServiceError).details ?? String(error);
      console.error(`stream error ${code ?? "-"}: ${details}`);
      if (code && FATAL.has(code)) process.exit(1);
    }
    console.error(`reconnecting in ${delay / 1000}s`);
    await new Promise((resolve) => setTimeout(resolve, delay));
    delay = Math.min(delay * 2, 30_000);
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
