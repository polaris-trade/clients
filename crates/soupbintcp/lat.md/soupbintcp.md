# SoupBinTCP Client

SoupBinTCP v3.0 session protocol client: wire codec, login/heartbeat/logout state machine, and an optional NASDAQ compressed-feed variant, generic over any `transport_core::StreamSource` backend, with `AsyncReady` optional for async login/recv.

## Wire codec

Parses the logical packet framing `Length[2 BE] + Type[1] + Payload` from a raw byte buffer, holding partial packets until more bytes arrive.

`PacketType` decodes the type byte via `TryFrom<u8>`, rejecting unknown bytes as a structured error instead of panicking. `parse_packet` is pure: no I/O, no config, callers own the accumulation buffer.

See [[src/wire.rs#PacketType]], [[src/wire.rs#parse_packet]].

## Client state machine and login

`SoupBinClient<T: StreamSource>` drives one session over an already-connected transport, from login through streaming.

The struct and its base `impl` (send, heartbeat, `poll_recv`) need only `StreamSource`. A second `impl<T: StreamSource + AsyncReady>` block adds `connect`/`recv`, gated behind the readiness adapter.

`connect` sends `Login Request`, awaits `Login Accepted` / `Login Rejected` / timeout, then transitions Disconnected -> Authenticating -> Streaming. Sequenced data increments an internal counter (seeded from the login response) so reconnect can request the right resume point via `next_expected_sequence`.

Two ways to drain the streaming state: `recv` (async, needs `AsyncReady`) awaits transport readiness between packets. `poll_recv` (sync, needs only `StreamSource`) makes one non-blocking dispatch-or-`recv_into` attempt and returns `Ok(None)` on no progress, so a sync-only backend can busy-spin it. Both share one packet-dispatch helper so framing logic isn't duplicated.

See [[src/client.rs#SoupBinClient]], [[src/frame.rs#Frame]].

## One-landing stream ingest

Bytes come off the wire via `StreamSource::recv_into`, not a resident transport buffer, so `ingest_transport_frame` owns exactly where each byte lands.

Uncompressed: reserves `decode_buf_capacity` spare bytes on `decode_buf` then lands `recv_into` straight into that spare region (`advance_mut` marks the exact count init), one copy, no intermediate buffer, and `decode_buf`'s own `BytesMut::split_to` framing downstream stays refcount-free. The `reserve` before `spare_capacity_mut` is load-bearing: `take_one_packet`'s `split_to` permanently shrinks spare capacity, so skipping it starves `recv_into` to a zero-length slice over repeated packets. Compressed (`compressed` feature): `recv_into` still lands once, but into a separate `recv_staging` buffer first, since `inflate.feed` needs a contiguous compressed frame; the inflate step then copies decoded bytes into `decode_buf`, an unavoidable second copy that the uncompressed path skips.

See [[src/client.rs#SoupBinClient]].

## Heartbeat, logout, end of session

Bidirectional heartbeat plus clean session teardown: client-driven ticks, server logout, and end-of-session handling.

`tick_heartbeat` is driven by the caller's own timer: it sends `Client Heartbeat` after `heartbeat_interval` of silence and reports `HeartbeatTimeout` once the server has been silent past `heartbeat_timeout`. `logout` sends `Logout Request` and closes; an `End of Session` packet from the server closes the client and makes further `recv` calls return `EndOfSession`.

See [[src/client.rs#SoupBinClient]], [[src/event.rs#SoupBinEvent]].

## Error and config

One error enum for every failure path, one config struct with working defaults for every field.

`SoupBinError` covers `Transport`, login/heartbeat failures, and protocol violations, with source chains preserved via `#[from]`. `SoupBinClientConfig` is `Serialize + Deserialize + Default` with `#[serde(default)]` so partial JSON/TOML configs still load, and durations use `humantime_serde` for human-readable values like `"30s"`.

See [[src/error.rs#SoupBinError]], [[src/config.rs#SoupBinClientConfig]].

## Compressed variant

Optional NASDAQ compressed-feed support: server-to-client bytes flow through a streaming zlib inflate before packet parsing.

Under the `compressed` feature, `CompressedReader` wraps the transport's read side with `Decompress::new(true)` (zlib framing, not raw deflate). Upstream writes (login, heartbeats, unsequenced data, logout) always bypass the inflator: compression is server-to-client only.

See [[src/compressed.rs#CompressedReader]].
