# clients

Protocol-client crates for the polaris-trade market-data stack, in one Cargo workspace. Both are backend-generic over [`transport_core`](https://github.com/polaris-trade/transport-core) and share one dependency table.

| Crate | Path | Protocol |
| --- | --- | --- |
| `client_moldudp` | [`crates/moldudp`](crates/moldudp) | MoldUDP64 downstream: wire codec, sequence reassembler, gap re-request, A/B arbiter |
| `client_soupbintcp` | [`crates/soupbintcp`](crates/soupbintcp) | SoupBinTCP 3.0: login handshake, heartbeats, sequenced framing, optional compressed variant |

Each crate is generic over a `transport_core` backend (the async tokio backend, the runtime-free mio backend, or a busy-poll sync backend), so the same client runs on any transport with no per-backend code.

## Using a crate

Both crates set `publish = false`; depend on them by git tag. Per-crate tags follow `<crate>-vX.Y.Z`:

```toml
[dependencies]
client_soupbintcp = { git = "https://github.com/polaris-trade/clients", tag = "client_soupbintcp-v0.4.0" }
```

### Depending on both crates: pin one shared tag

Cargo keys a git source by `(url, tag)`. If you depend on **both** `client_moldudp` and `client_soupbintcp`, pin them to the **same tag** — two different tags check out two copies of the shared `transport_core`, and a type crossing between them fails to compile (`E0308`). Pin both to the newest tag whose commit carries the versions you need:

```toml
client_moldudp    = { git = "https://github.com/polaris-trade/clients", tag = "client_soupbintcp-v0.4.0" }
client_soupbintcp = { git = "https://github.com/polaris-trade/clients", tag = "client_soupbintcp-v0.4.0" }
```

## Development

```bash
just check                                    # fmt-check + clippy + nextest
cargo nextest run --workspace --all-features
```

Format uses nightly rustfmt (unstable import options); build/clippy/test stay on the pinned stable toolchain. Install hooks once per clone with `just hooks`.

## History

`client_moldudp` and `client_soupbintcp` were previously separate repositories (`client-moldudp`, `client-soupbintcp`), merged here with history preserved so shared-dependency bumps and CI/release scaffolding are maintained once. The old repositories are archived; their `client_*-v*` tags remain resolvable for historical pins.

## License

`MIT OR Apache-2.0`.
