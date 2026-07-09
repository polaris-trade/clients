This directory defines the high-level concepts, business logic, and architecture of this project using markdown. It is managed by [lat.md](https://www.npmjs.com/package/lat.md) — a tool that anchors source code to these definitions. Install the `lat` command with `npm i -g lat.md` and run `lat --help`.

# client-moldudp

MoldUDP64 client: wire codec, sequence reassembler, gap re-request, A/B arbiter, and a backend-generic receiver over `transport_core::DatagramSource`.

## Wire codec

Parses the 20-byte MoldUDP64 downstream header and iterates message blocks straight out of the datagram slice, zero-alloc.

- [[src/wire.rs#DownstreamHeader]] — session, sequence, message count.
- [[src/wire.rs#parse_header]] — rejects short/oversized datagrams.
- [[src/wire.rs#PacketKind]] — heartbeat / end-of-session / data classification.
- [[src/wire.rs#MessageBlockIter]] — borrowed message block iterator; yields `(offset, block)`, `offset` being the block's byte position within the datagram so a slot can retain a slab view instead of a copy.

## Sequence reassembler

Fixed-capacity slot ring keyed by `seq % capacity`; drains contiguous runs, drops stale duplicates, and rejects inserts that would clobber a pending slot.

- [[src/reassembly.rs#SequenceReassembler]] — insert/drain over an owned slab handle `S` (the receiver instantiates `S = MessageView`). `advance_expected` moves `expected_next` for the in-order fast path without an insert, so an in-order datagram never forces an `Arc`; `drain_ready` cascade-drains views buffered ahead once that advance fills the gap.
- [[src/reassembly.rs#DrainCursor]] — lazy drain iterator, finishes on drop like `Vec::drain`.

## Gap handling

Tracks missing sequence ranges and turns them into rate-limited MoldUDP64 Request Packets so re-request traffic can't flood the server.

Gaps come from three places: a data packet whose leading sequence jumps past `expected_next`, and heartbeat / end-of-session packets whose next-expected sequence exceeds ours (tail loss during quiet traffic).

- [[src/gap.rs#GapRequestHandler]] — records/coalesces/clears missing ranges.
- [[src/gap.rs#GapRequestEmitter]] — per-gap rate-limited unicast re-request.

## A/B arbiter

Sliding bitmap window that dedupes redundant A/B feeds (first arrival wins) and withholds gap confirmation until every stream has missed a sequence for a grace window.

The receiver stages detected gaps via `note_missing_range` and drains `confirmed_gaps` into the gap handler each poll, so a lagging leg gets its window before a re-request fires.

- [[src/ab.rs#AbArbiter]] — observe (dedup), note_missing_range / confirmed_gaps (grace-windowed gap staging), rebase (anchor base for mid-session join), stats.
- [[src/ab.rs#ArbiterVerdict]] — Forward / Duplicate / OutOfWindow.

## Receiver

Assembles wire codec, reassembler, gap tracking, and optional arbiter into one `DatagramSource`-generic receiver driven by `recv_burst`. Base construction needs `TransportBind + PoolAccess`; multicast join adds `UdpTransport`.

- [[src/receiver.rs#MoldUdpReceiver]]: `new` (binds legs, joins multicast when configured), `recv` (borrowed), `recv_owned` (cross-thread handoff), `stats`, `emit_pending_gaps`. `expected_next` anchors to `cfg.start_sequence` when set, else to the first packet's sequence, so a mid-session join does not treat the backlog as one giant gap. A datagram whose leading sequence is already `expected_next` drains inline, borrowed from the still-owned frame (zero alloc); one landing ahead promotes its frame to a single `Arc` and buffers `MessageView`s until the gap fills, cascade-draining on fill. The recv pool is sized at the reorder window plus burst headroom and asserted at construction.
- [[src/receiver.rs#MoldUdpOutcome]] — what `recv`/`recv_owned` hand back: `Frame` (borrowed) / `Owned` (moves a message to another thread) / `Event`.

## Error, config, event, frame types

Shared shapes: typed errors, serde-first receiver config, control events, and the borrowed consumer-facing frame.

- [[src/error.rs#MoldUdpError]] — typed failure kinds, `Transport` wraps `transport_core::TransportError`.
- [[src/config.rs#MoldUdpReceiverConfig]] — serde `Default`-backed receiver config; `start_sequence` sets the initial expected sequence for a mid-session resume.
- [[src/event.rs#MoldUdpEvent]] — `Heartbeat { next_expected }` / `EndOfSession { next_expected }`, both carrying the server's next-expected for tail-loss detection; no `SessionOpen` (session id stays internal).
- [[src/frame.rs#Frame]] — borrowed consumer-facing frame, implements `AsPayload`.
- [[src/frame.rs#MessageView]] — refcounted view into a datagram slab (`Arc<F>` + offset/len), the owned reassembler slot that replaces per-message `to_vec`.
- [[src/frame.rs#OwnedFrame]] — owned message handle carried by `MoldUdpOutcome::Owned` for cross-thread handoff.
