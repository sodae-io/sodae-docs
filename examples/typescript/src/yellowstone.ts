import Client, {
  CommitmentLevel,
  SlotStatus,
  type SubscribeRequest,
  type SubscribeUpdate,
} from "@triton-one/yellowstone-grpc";
import bs58 from "bs58";

const ENDPOINT = process.env.SODAE_YELLOWSTONE_URL ?? "http://ams.rpc.sodae.io:10201";
const TOKEN = process.env.SODAE_TOKEN;
const PUMP_AMM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const FATAL = new Set([
  "UNAUTHENTICATED",
  "NOT_ENTITLED",
  "IP_NOT_ALLOWED",
  "QUOTA_EXCEEDED",
  "AUTH_RATE_LIMITED",
]);

function emptyRequest(): SubscribeRequest {
  return {
    accounts: {},
    slots: {},
    transactions: {},
    transactionsStatus: {},
    blocks: {},
    blocksMeta: {},
    entry: {},
    accountsDataSlice: [],
  };
}

function buildRequest([mode = "transactions", ...targets]: string[]): SubscribeRequest {
  const request = { ...emptyRequest(), commitment: CommitmentLevel.PROCESSED };
  switch (mode) {
    case "transactions":
      request.transactions.transactions = {
        vote: false,
        failed: false,
        accountInclude: targets.length > 0 ? targets : [PUMP_AMM],
        accountExclude: [],
        accountRequired: [],
      };
      return request;
    case "accounts":
      if (targets.length === 0) throw new Error("usage: yellowstone accounts <pubkey>...");
      request.accounts.accounts = { account: targets, owner: [], filters: [] };
      return request;
    case "slots":
      request.slots.slots = { filterByCommitment: false };
      return request;
    default:
      throw new Error(`unknown mode ${mode}; use transactions, accounts or slots`);
  }
}

function print(update: SubscribeUpdate): void {
  if (update.transaction?.transaction) {
    const { slot, transaction } = update.transaction;
    console.log(slot, bs58.encode(transaction.signature));
  } else if (update.account?.account) {
    const { slot, account } = update.account;
    console.log(
      slot,
      bs58.encode(account.pubkey),
      `lamports=${account.lamports}`,
      `data=${account.data.length}B`,
      `owner=${bs58.encode(account.owner)}`,
    );
  } else if (update.slot) {
    console.log(update.slot.slot, SlotStatus[update.slot.status] ?? "UNKNOWN");
  }
}

async function subscribe(request: SubscribeRequest, onConnected: () => void): Promise<void> {
  const client = new Client(ENDPOINT, TOKEN, { grpcMaxDecodingMessageSize: 64 * 1024 * 1024 });
  await client.connect();
  const stream = await client.subscribe(request);
  await new Promise<void>((resolve, reject) => {
    stream.on("data", (update: SubscribeUpdate) => {
      onConnected();
      if (update.ping) {
        stream.write({ ...emptyRequest(), ping: { id: 1 } });
      } else {
        print(update);
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
  const request = buildRequest(process.argv.slice(2));
  let delay = 1_000;
  for (;;) {
    try {
      await subscribe(request, () => (delay = 1_000));
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
