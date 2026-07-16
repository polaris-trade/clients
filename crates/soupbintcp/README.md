# client_soupbintcp

SoupBinTCP 3.0 market-data client: login handshake, sequenced and unsequenced framing, heartbeats, and an optional compressed variant, over any stream backend.

## What it is

`SoupBinClient<T>` runs the SoupBinTCP 3.0 session state machine over any `transport_core::StreamSource`. It performs the login handshake, tracks sequence numbers, drives heartbeats both ways, and yields sequenced data messages. It is generic over the transport, so the same client runs on the async tokio backend or any other stream backend with no per-backend code.

## Design

Ingest lands transport bytes directly into the decode buffer's spare capacity via `StreamSource::recv_into`, so the stream path has exactly one copy; `BytesMut` framing (`split_to`) stays refcount-free after. The optional `compressed` feature adds a staging inflate step, which needs a contiguous compressed frame, ahead of the same framing; both paths share one packet reader.

## Features

- `compressed` (off by default): SoupBinTCP compressed variant, via `flate2`.

## Usage

```rust
use client_soupbintcp::{SoupBinClient, SoupBinClientConfig};

// caller supplies a connected transport_core::StreamSource (e.g. a TCP TokioTransport)
let cfg = SoupBinClientConfig {
    username: "user".into(),
    password: "pass".into(),
    requested_session: String::new(),   // empty = current session
    requested_sequence_number: 1,
    ..Default::default()
};
let mut client = SoupBinClient::connect(transport, cfg).await?;

while let Ok(_msg) = client.recv().await {
    // _msg borrows the decode buffer; refcount-free after framing
}
```

`connect` completes the login handshake; `recv` yields sequenced data, `send_unsequenced` writes an unsequenced packet, `tick_heartbeat` maintains liveness, and `logout` ends the session.

## Protocol specification

SoupBinTCP and its compressed variant are Nasdaq protocols. Obtain the
specifications from
[Nasdaq market data specifications](https://data.nasdaq.com/market-data-specifications).
Spec documents are not redistributed in this repository.

## Building and testing

```bash
cargo test                       # uncompressed path, transport_tokio mock TCP
cargo test --features compressed # compressed variant
```

MSRV 1.96.1.

## Dependency

`publish = false`; distribution is git-tag only.

```toml
[dependencies]
client_soupbintcp = { git = "https://github.com/polaris-trade/client-soupbintcp", tag = "client_soupbintcp-v0.2.0" }
```

## Logging

This crate emits [`tracing`](https://docs.rs/tracing) events on error and lifecycle paths (never per message). Install any subscriber to see them, e.g. `tracing_subscriber::fmt::init()`. Filter per crate with `RUST_LOG=client_soupbintcp=debug`. Disable at compile time with `tracing`'s `release_max_level_off` feature in your binary.

## License

Dual-licensed under either [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE), at your option.
