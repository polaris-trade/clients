# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).
## [0.4.1](https://github.com/polaris-trade/clients/compare/client_moldudp-v0.4.0...client_moldudp-v0.4.1) (2026-07-15)


### Bug fixes

* **deps:** source transport_core and transport_tokio from the transports workspace ([#4](https://github.com/polaris-trade/clients/issues/4)) ([bca1972](https://github.com/polaris-trade/clients/commit/bca19729f06eff4f97ee578afa58aa86faeaf9e6))

## [0.4.0](https://github.com/polaris-trade/clients/compare/client_moldudp-v0.3.0...client_moldudp-v0.4.0) (2026-07-14)


### Features

* **moldudp:** MoldUDP64 client with reassembly, gap recovery, and A/B arbitration ([155f5d3](https://github.com/polaris-trade/clients/commit/155f5d3e9a0fe404aaf3603a2fcb8b331b67a1d3))
* **recv:** migrate receiver to owned-frame recv seam ([#2](https://github.com/polaris-trade/clients/issues/2)) ([cfc251c](https://github.com/polaris-trade/clients/commit/cfc251c265ec77ff729616b1fece46805065590b))
* **telemetry:** add recv-counter ([#10](https://github.com/polaris-trade/clients/issues/10)) ([45397a3](https://github.com/polaris-trade/clients/commit/45397a3decc876da7038836faa82041ca115b3f4))


### Bug fixes

* **ci:** point reusable workflows at polaris-trade/ci ([56f4b64](https://github.com/polaris-trade/clients/commit/56f4b64a96a53ad590c7898bffcc484e05a767ea))
* **receiver:** gap detection from heartbeats and mid-session anchor ([#8](https://github.com/polaris-trade/clients/issues/8)) ([1ef3c20](https://github.com/polaris-trade/clients/commit/1ef3c20462cdbc9d25145cab9646ef098bad2e1e))


### Refactor

* rename crate moldudp to client_moldudp, folder to client-moldudp ([a58c557](https://github.com/polaris-trade/clients/commit/a58c557739b20aaf92aa46ad5d0130d25ba88ab0))


### Tests

* **client-moldudp:** prove zero-alloc recv, owned handoff, pool-size guard ([#6](https://github.com/polaris-trade/clients/issues/6)) ([09612c6](https://github.com/polaris-trade/clients/commit/09612c633e1ce3540b35d54c564e81301557ecb6))


### Build

* **deps:** bump deps and update README ([#4](https://github.com/polaris-trade/clients/issues/4)) ([15fb321](https://github.com/polaris-trade/clients/commit/15fb321a678d9c42fd6877eb0f36843537c04573))
* **workspace:** merge client-soupbintcp and client-moldudp ([#1](https://github.com/polaris-trade/clients/issues/1)) ([7d76be9](https://github.com/polaris-trade/clients/commit/7d76be9521d69a5bb5c4cb0a248aed4cd6069106))

## [0.3.0](https://github.com/polaris-trade/client-moldudp/compare/client_moldudp-v0.2.3...client_moldudp-v0.3.0) (2026-07-11)


### Features

* **telemetry:** add recv-counter ([#10](https://github.com/polaris-trade/client-moldudp/issues/10)) ([26c4e01](https://github.com/polaris-trade/client-moldudp/commit/26c4e01a85ddeaecc95a22ddac1f0f7bb2e624a4))

## [0.2.3](https://github.com/polaris-trade/client-moldudp/compare/client_moldudp-v0.2.2...client_moldudp-v0.2.3) (2026-07-09)


### Bug fixes

* **receiver:** gap detection from heartbeats and mid-session anchor ([#8](https://github.com/polaris-trade/client-moldudp/issues/8)) ([db8d2b9](https://github.com/polaris-trade/client-moldudp/commit/db8d2b9c007fbc1e454f0b1a6d24f52d2a776cf5))

## [0.2.2](https://github.com/polaris-trade/client-moldudp/compare/client_moldudp-v0.2.1...client_moldudp-v0.2.2) (2026-07-09)


### Tests

* **client-moldudp:** prove zero-alloc recv, owned handoff, pool-size guard ([#6](https://github.com/polaris-trade/client-moldudp/issues/6)) ([1de022f](https://github.com/polaris-trade/client-moldudp/commit/1de022f92a812900f1829df69a6f83813472a1e2))

## [0.2.1](https://github.com/polaris-trade/client-moldudp/compare/client_moldudp-v0.2.0...client_moldudp-v0.2.1) (2026-07-09)


### Build

* **deps:** bump deps and update README ([#4](https://github.com/polaris-trade/client-moldudp/issues/4)) ([ac968de](https://github.com/polaris-trade/client-moldudp/commit/ac968dec2527173cdc0512653372a791d8562b78))

## [0.2.0](https://github.com/polaris-trade/client-moldudp/compare/client_moldudp-v0.1.0...client_moldudp-v0.2.0) (2026-07-09)


### Features

* **recv:** migrate receiver to owned-frame recv seam ([#2](https://github.com/polaris-trade/client-moldudp/issues/2)) ([8cbb2f2](https://github.com/polaris-trade/client-moldudp/commit/8cbb2f2bb08fea160828cc6b99c5a09db7538916))

## [0.1.0](https://github.com/polaris-trade/client-moldudp/releases/tag/client_moldudp-v0.1.0) - 2026-07-07

### Bug fixes

- *(ci)* Point reusable workflows at polaris-trade/ci


### Features

- *(moldudp)* MoldUDP64 client with reassembly, gap recovery, and A/B arbitration


  Backend-generic MoldUDP64 receiver over transport_core::Transport:
- wire codec: downstream header parse + message-block iterator, packet-kind classification
- SequenceReassembler: in-order drain with out-of-order slot buffering
- gap detection + re-request emitter for missing sequence ranges
- A/B line arbiter: dedupes redundant feeds, tracks per-stream stats
- per-stream multicast join, serde config, typed error taxonomy


### Refactor

- Rename crate moldudp to client_moldudp, folder to client-moldudp
