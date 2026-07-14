# client-soupbintcp

SoupBinTCP v3.0 client crate: wire codec, session state machine, heartbeats, logout/end-of-session, and an optional NASDAQ compressed-feed variant.

This directory defines the high-level concepts, business logic, and architecture of this project using markdown. It is managed by [lat.md](https://www.npmjs.com/package/lat.md) — a tool that anchors source code to these definitions. Install the `lat` command with `npm i -g lat.md` and run `lat --help`.

## Sections

Architecture docs for this crate, one topic per link below.

- [[soupbintcp]] — wire codec, client state machine and login, heartbeat/logout/end-of-session, error/config, compressed variant, telemetry.
