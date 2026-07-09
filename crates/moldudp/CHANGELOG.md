# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).
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
