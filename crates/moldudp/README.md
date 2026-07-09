# client_moldudp

MoldUDP64 market-data client: wire codec, sequence reassembler, gap re-request, and A/B line arbitration behind one backend-generic receiver.

## What it is

`MoldUdpReceiver<T>` runs the full MoldUDP64 downstream receive path over any `transport_core::DatagramSource`. It parses the 20-byte downstream header, iterates message blocks straight out of the datagram, reorders by sequence, re-requests gaps as rate-limited Request Packets, and (for redundant A/B feeds) arbitrates first-arrival. It is generic over the transport, so the same receiver runs on the async tokio backend, the runtime-free mio backend, or a busy-poll sync backend with no per-backend code.

## Design

Recv is driven by `DatagramSource::recv_burst`, which hands back owned frames. A datagram whose leading sequence is already `expected_next` drains inline, its messages borrowed straight from the still-owned frame with zero per-message allocation; a datagram that lands ahead promotes its frame to a single `Arc` and buffers `MessageView`s until the gap fills, then cascade-drains. `recv()` returns a borrowed `MoldUdpOutcome`; `recv_owned()` returns an owned one (`Send + 'static`) so a message can move to a sharded engine thread without copying.

The recv pool is sized at the reorder window plus burst headroom and asserted at construction, so an undersized pool fails fast instead of stalling a live feed.

## Usage

```rust
use client_moldudp::{MoldUdpReceiver, MoldUdpReceiverConfig, MoldUdpOutcome, StreamConfig};
use transport_tokio::TokioTransport;

let stream = StreamConfig { bind_addr: "127.0.0.1:0".parse()?, ..Default::default() };
let cfg = MoldUdpReceiverConfig { streams: vec![stream], ..Default::default() };
let mut rx = MoldUdpReceiver::<TokioTransport>::new(cfg).await?;

match rx.recv().await? {
    MoldUdpOutcome::Frame(frame) => { let _ = frame.payload(); } // borrows the frame
    MoldUdpOutcome::Owned(msg)   => { /* Send + 'static; hand to another thread */ }
    MoldUdpOutcome::Event(_)     => {}                           // heartbeat / end-of-session
}
```

Configure one `StreamConfig` per feed (A/B lines become two streams); set the stream's multicast address to join a group.

## Protocol specification

MoldUDP64 is a Nasdaq protocol. Obtain the specification from
[Nasdaq market data specifications](https://data.nasdaq.com/market-data-specifications).
Spec documents are not redistributed in this repository.

## Building and testing

```bash
cargo test    # over the transport_tokio localhost mock, any OS
```

MSRV 1.96.1.

## Dependency

`publish = false`; distribution is git-tag only.

```toml
[dependencies]
client_moldudp = { git = "https://github.com/polaris-trade/client-moldudp", tag = "client_moldudp-v0.2.0" }
```

## License

Dual-licensed under either [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE), at your option.
