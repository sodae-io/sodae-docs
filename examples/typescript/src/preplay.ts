import Client, {
  type SubscribeDeshredRequest,
  type SubscribeUpdateDeshred,
  type SubscribeUpdateDeshredTransactionInfo,
} from "@triton-one/yellowstone-grpc";
import bs58 from "bs58";

const ENDPOINT = process.env.SODAE_PREPLAY_URL ?? "http://ams.rpc.sodae.io:10301";
const TOKEN = process.env.SODAE_TOKEN;
const FATAL = new Set([
  "UNAUTHENTICATED",
  "NOT_ENTITLED",
  "IP_NOT_ALLOWED",
  "QUOTA_EXCEEDED",
  "AUTH_RATE_LIMITED",
]);

function buildRequest(program: string | undefined): SubscribeDeshredRequest {
  if (program !== undefined && bs58.decode(program).length !== 32) {
    throw new Error("the argument must be a program id");
  }
  return {
    deshredTransactions: {
      preplay: {
        vote: false,
        accountInclude: program ? [program] : [],
        accountExclude: [],
        accountRequired: [],
      },
    },
    slots: {},
  };
}

function printTransaction(slot: string, tx: SubscribeUpdateDeshredTransactionInfo): void {
  const message = tx.transaction?.message;
  if (!message) return;
  const key = (i: number) => (message.accountKeys[i] ? bs58.encode(message.accountKeys[i]) : "");
  const version = !message.versioned ? "legacy" : message.config ? "1" : "0";
  console.log(
    slot,
    bs58.encode(tx.signature),
    `signer=${key(0)}`,
    `version=${version}`,
    `lookups=${message.addressTableLookups.length}`,
    `loaded=${tx.loadedWritableAddresses.length + tx.loadedReadonlyAddresses.length}`,
  );
  for (const ix of message.instructions) {
    console.log(`  ${key(ix.programIdIndex)}`, `accounts=${ix.accounts.length}`, `data=${ix.data.length}B`);
  }
}

async function subscribe(program: string | undefined, onConnected: () => void): Promise<void> {
  const client = new Client(ENDPOINT, TOKEN, { grpcMaxDecodingMessageSize: 64 * 1024 * 1024 });
  await client.connect();
  const stream = await client.subscribeDeshred();
  stream.write(buildRequest(program));
  const perSlot = new Map<string, number>();
  await new Promise<void>((resolve, reject) => {
    stream.on("data", (update: SubscribeUpdateDeshred) => {
      onConnected();
      if (update.ping) {
        stream.write({ deshredTransactions: {}, slots: {}, ping: { id: 1 } });
        return;
      }
      const received = update.deshredTransaction;
      if (!received?.transaction) return;
      if (program) {
        printTransaction(received.slot, received.transaction);
        return;
      }
      perSlot.set(received.slot, (perSlot.get(received.slot) ?? 0) + 1);
      while (perSlot.size > 2) {
        const oldest = [...perSlot.keys()].reduce((a, b) => (BigInt(a) < BigInt(b) ? a : b));
        console.log(oldest, `transactions=${perSlot.get(oldest)}`);
        perSlot.delete(oldest);
      }
    });
    stream.on("error", reject);
    stream.on("end", resolve);
  });
}

function errorCode(error: unknown): string | undefined {
  const message = error instanceof Error ? error.message : String(error);
  return /\(code: ([A-Z_]+)\)/.exec(message)?.[1];
}

async function main(): Promise<void> {
  if (!TOKEN) throw new Error("set SODAE_TOKEN to your API token");
  const program = process.argv[2];
  buildRequest(program);
  let delay = 1_000;
  for (;;) {
    try {
      await subscribe(program, () => (delay = 1_000));
      console.error("stream closed by the server");
    } catch (error) {
      const code = errorCode(error);
      console.error(`stream error ${code ?? "-"}: ${error instanceof Error ? error.message : error}`);
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
