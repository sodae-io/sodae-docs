import bs58 from "bs58";

export interface Instruction {
  programIdIndex: number;
  accounts: number[];
  data: Uint8Array;
}

export interface AddressTableLookup {
  accountKey: string;
  writableIndexes: number[];
  readonlyIndexes: number[];
}

export interface TransactionConfig {
  priorityFee?: bigint;
  computeUnitLimit?: number;
  loadedAccountsDataSizeLimit?: number;
  heapSize?: number;
}

export interface Transaction {
  signatures: string[];
  version: "legacy" | 0 | 1;
  header: {
    numRequiredSignatures: number;
    numReadonlySignedAccounts: number;
    numReadonlyUnsignedAccounts: number;
  };
  accountKeys: string[];
  recentBlockhash: string;
  instructions: Instruction[];
  addressTableLookups: AddressTableLookup[];
  config: TransactionConfig;
}

export interface Entry {
  numHashes: bigint;
  hash: string;
  transactions: Transaction[];
}

const V0_PREFIX = 0x80;
const V1_PREFIX = 0x81;
const KNOWN_CONFIG_BITS = 0b11111;

class Reader {
  private offset = 0;
  private readonly view: DataView;

  constructor(private readonly buffer: Uint8Array) {
    this.view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
  }

  take(length: number): Uint8Array {
    if (this.offset + length > this.buffer.length) throw new RangeError("truncated entries");
    const slice = this.buffer.subarray(this.offset, this.offset + length);
    this.offset += length;
    return slice;
  }

  peek(): number {
    if (this.offset >= this.buffer.length) throw new RangeError("truncated entries");
    return this.buffer[this.offset];
  }

  u8(): number {
    return this.take(1)[0];
  }

  u16(): number {
    const value = this.view.getUint16(this.offset, true);
    this.take(2);
    return value;
  }

  u32(): number {
    const value = this.view.getUint32(this.offset, true);
    this.take(4);
    return value;
  }

  u64(): bigint {
    const value = this.view.getBigUint64(this.offset, true);
    this.take(8);
    return value;
  }

  length(): number {
    let value = 0;
    for (let shift = 0; shift < 21; shift += 7) {
      const byte = this.u8();
      value |= (byte & 0x7f) << shift;
      if ((byte & 0x80) === 0) return value;
    }
    throw new RangeError("invalid compact length");
  }

  bytes(): Uint8Array {
    return this.take(this.length());
  }

  key(): string {
    return bs58.encode(this.take(32));
  }

  signature(): string {
    return bs58.encode(this.take(64));
  }

  list<T>(read: () => T): T[] {
    return Array.from({ length: this.length() }, read);
  }

  header(): Transaction["header"] {
    return {
      numRequiredSignatures: this.u8(),
      numReadonlySignedAccounts: this.u8(),
      numReadonlyUnsignedAccounts: this.u8(),
    };
  }
}

function readV1Transaction(reader: Reader): Transaction {
  reader.u8();
  const header = reader.header();
  const mask = reader.u32();
  if ((mask & ~KNOWN_CONFIG_BITS) !== 0 || (mask & 0b11) === 0b01 || (mask & 0b11) === 0b10) {
    throw new Error(`invalid transaction config mask ${mask}`);
  }
  const recentBlockhash = reader.key();
  const numInstructions = reader.u8();
  const numAddresses = reader.u8();
  const accountKeys = Array.from({ length: numAddresses }, () => reader.key());
  const config: TransactionConfig = {};
  if ((mask & 0b11) === 0b11) config.priorityFee = reader.u64();
  if (mask & 0b100) config.computeUnitLimit = reader.u32();
  if (mask & 0b1000) config.loadedAccountsDataSizeLimit = reader.u32();
  if (mask & 0b10000) config.heapSize = reader.u32();
  const layouts = Array.from({ length: numInstructions }, () => ({
    programIdIndex: reader.u8(),
    numAccounts: reader.u8(),
    dataLength: reader.u16(),
  }));
  const instructions = layouts.map((layout) => ({
    programIdIndex: layout.programIdIndex,
    accounts: Array.from(reader.take(layout.numAccounts)),
    data: reader.take(layout.dataLength),
  }));
  const signatures = Array.from({ length: header.numRequiredSignatures }, () => reader.signature());
  return {
    signatures,
    version: 1,
    header,
    accountKeys,
    recentBlockhash,
    instructions,
    addressTableLookups: [],
    config,
  };
}

function readTransaction(reader: Reader): Transaction {
  if (reader.peek() === V1_PREFIX) return readV1Transaction(reader);
  const signatures = reader.list(() => reader.signature());
  const prefix = reader.peek();
  if ((prefix & 0x80) !== 0 && prefix !== V0_PREFIX) {
    throw new Error(`unsupported transaction version ${prefix & 0x7f}`);
  }
  const versioned = prefix === V0_PREFIX;
  if (versioned) reader.u8();
  const header = reader.header();
  const accountKeys = reader.list(() => reader.key());
  const recentBlockhash = reader.key();
  const instructions = reader.list(() => ({
    programIdIndex: reader.u8(),
    accounts: Array.from(reader.bytes()),
    data: reader.bytes(),
  }));
  const addressTableLookups = versioned
    ? reader.list(() => ({
        accountKey: reader.key(),
        writableIndexes: Array.from(reader.bytes()),
        readonlyIndexes: Array.from(reader.bytes()),
      }))
    : [];
  return {
    signatures,
    version: versioned ? 0 : "legacy",
    header,
    accountKeys,
    recentBlockhash,
    instructions,
    addressTableLookups,
    config: {},
  };
}

export function decodeEntries(bytes: Uint8Array): Entry[] {
  const reader = new Reader(bytes);
  return Array.from({ length: Number(reader.u64()) }, () => ({
    numHashes: reader.u64(),
    hash: reader.key(),
    transactions: Array.from({ length: Number(reader.u64()) }, () => readTransaction(reader)),
  }));
}
