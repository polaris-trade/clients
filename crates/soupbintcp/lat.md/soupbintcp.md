# SoupBinTCP Client

SoupBinTCP v3.0 session protocol client: wire codec, login/heartbeat/logout state machine, and an optional NASDAQ compressed-feed variant, generic over any `transport_core::Transport` backend.

## Wire codec

Parses the logical packet framing `Length[2 BE] + Type[1] + Payload` from a raw byte buffer, holding partial packets until more bytes arrive.

`PacketType` decodes the type byte via `TryFrom<u8>`, rejecting unknown bytes as a structured error instead of panicking. `parse_packet` is pure: no I/O, no config, callers own the accumulation buffer.

See [[src/wire.rs#PacketType]], [[src/wire.rs#parse_packet]].

## Client state machine and login

`SoupBinClient<T: Transport>` drives one session over an already-connected transport, from login through streaming.

`connect` sends `Login Request`, awaits `Login Accepted` / `Login Rejected` / timeout, then transitions Disconnected -> Authenticating -> Streaming. Sequenced data increments an internal counter (seeded from the login response) so reconnect can request the right resume point via `next_expected_sequence`.

See [[src/client.rs#SoupBinClient]], [[src/frame.rs#Frame]].

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
