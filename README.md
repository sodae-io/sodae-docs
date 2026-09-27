# Sodae docs

Customer documentation for Sodae, built with [Mintlify](https://mintlify.com), plus runnable client examples.

## Layout

| Path | Contents |
|---|---|
| `docs.json` | Mintlify site configuration and navigation |
| `*.mdx`, `yellowstone/`, `preplay/`, `rpc/` | Pages |
| `examples/rust` | Rust clients: `cargo run --example yellowstone` / `preplay` |
| `examples/typescript` | TypeScript clients: `npm run yellowstone` / `preplay` |
| `examples/go` | Go clients: `go run ./yellowstone` / `./preplay` |
| `examples/proto` | `shredstream.proto`, plus `geyser.proto` and `solana-storage.proto` from [yellowstone-grpc](https://github.com/rpcpool/yellowstone-grpc) (Apache-2.0) |

## Preview

```bash
npm i -g mint
mint dev
```

## Examples

Every example reads `SODAE_TOKEN`. The endpoints default to Amsterdam and can be overridden with `SODAE_YELLOWSTONE_URL` and `SODAE_PREPLAY_URL`.

The Go code under `examples/go/proto` is generated:

```bash
cd examples/go
protoc -I ../proto \
  --go_out=. --go_opt=module=sodae-examples \
  --go-grpc_out=. --go-grpc_opt=module=sodae-examples \
  --go_opt=Mgeyser.proto=sodae-examples/proto/geyser \
  --go_opt=Msolana-storage.proto=sodae-examples/proto/geyser \
  --go_opt=Mshredstream.proto=sodae-examples/proto/shredstream \
  --go-grpc_opt=Mgeyser.proto=sodae-examples/proto/geyser \
  --go-grpc_opt=Msolana-storage.proto=sodae-examples/proto/geyser \
  --go-grpc_opt=Mshredstream.proto=sodae-examples/proto/shredstream \
  geyser.proto solana-storage.proto shredstream.proto
```
