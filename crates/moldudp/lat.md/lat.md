This directory defines the high-level concepts, business logic, and architecture of this project using markdown. It is managed by [lat.md](https://www.npmjs.com/package/lat.md) — a tool that anchors source code to these definitions. Install the `lat` command with `npm i -g lat.md` and run `lat --help`.

# client-moldudp

MoldUDP64 client: wire codec, sequence reassembler, gap re-request, A/B arbiter, and a backend-generic receiver over `transport_core::Transport`.

## Wire codec

Parses the 20-byte MoldUDP64 downstream header and iterates message blocks straight out of the datagram slice, zero-alloc.

- [[src/wire.rs#DownstreamHeader]] — session, sequence, message count.
- [[src/wire.rs#parse_header]] — rejects short/oversized datagrams.
- [[src/wire.rs#PacketKind]] — heartbeat / end-of-session / data classification.
- [[src/wire.rs#MessageBlockIter]] — borrowed message block iterator.

## Sequence reassembler

Fixed-capacity slot ring keyed by `seq % capacity`; drains contiguous runs, drops stale duplicates, and rejects inserts that would clobber a pending slot.

- [[src/reassembly.rs#SequenceReassembler]] — insert/drain over an owned slab handle `S`.
- [[src/reassembly.rs#DrainCursor]] — lazy drain iterator, finishes on drop like `Vec::drain`.

## Gap handling

Tracks missing sequence ranges and turns them into rate-limited MoldUDP64 Request Packets so re-request traffic can't flood the server.

- [[src/gap.rs#GapRequestHandler]] — records/coalesces/clears missing ranges.
- [[src/gap.rs#GapRequestEmitter]] — per-gap rate-limited unicast re-request.

## A/B arbiter

Sliding bitmap window that dedupes redundant A/B feeds (first arrival wins) and withholds gap confirmation until every stream has missed a sequence for a grace window.

- [[src/ab.rs#AbArbiter]] — observe/confirmed_gaps/stats.
- [[src/ab.rs#ArbiterVerdict]] — Forward / Duplicate / OutOfWindow.

## Receiver

Assembles wire codec, reassembler, gap tracking, and optional arbiter into one `Transport`-generic receiver. `new` binds a leg per stream and joins `multicast_addr` when set, so it needs `TransportBind + UdpTransport`.

- [[src/receiver.rs#MoldUdpReceiver]]: `new` (binds legs, joins multicast when configured), `recv`, `stats`, `emit_pending_gaps`. The recv loop decodes message blocks straight from the borrowed transport frame via a disjoint field borrow, so the whole datagram is never copied; per-message payloads are owned only once they are buffered in reassembler slots.

## Error, config, event, frame types

Shared shapes: typed errors, serde-first receiver config, control events, and the borrowed consumer-facing frame.

- [[src/error.rs#MoldUdpError]] — typed failure kinds, `Transport` wraps `transport_core::TransportError`.
- [[src/config.rs#MoldUdpReceiverConfig]] — serde `Default`-backed receiver config.
- [[src/event.rs#MoldUdpEvent]] — `Heartbeat` / `EndOfSession`, no `SessionOpen` (session id stays internal).
- [[src/frame.rs#Frame]] — borrowed consumer-facing frame, implements `AsPayload`.
