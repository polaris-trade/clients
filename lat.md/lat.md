This directory defines the high-level concepts, business logic, and architecture of this project using markdown. It is managed by [lat.md](https://www.npmjs.com/package/lat.md) — a tool that anchors source code to these definitions. Install the `lat` command with `npm i -g lat.md` and run `lat --help`.

# clients

Two protocol-client crates in one Cargo workspace, both backend-generic over `transport_core` and sharing one dependency table: MoldUDP64 (`client_moldudp`) and SoupBinTCP v3.0 (`client_soupbintcp`).

- [[moldudp]]: MoldUDP64 wire codec, sequence reassembler, gap re-request, A/B arbiter, and a `DatagramSource`-generic receiver.
- [[soupbintcp]]: SoupBinTCP v3.0 wire codec, session state machine, heartbeats, end-of-session, and the optional compressed variant.
