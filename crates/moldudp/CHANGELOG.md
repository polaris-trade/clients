# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).
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
