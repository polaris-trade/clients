# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).
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

