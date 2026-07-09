# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).
## [0.3.0](https://github.com/polaris-trade/client-soupbintcp/compare/client_soupbintcp-v0.2.1...client_soupbintcp-v0.3.0) (2026-07-09)


### Features

* **client-soupbintcp:** support sync-only backends via poll_recv ([#7](https://github.com/polaris-trade/client-soupbintcp/issues/7)) ([7c9d03f](https://github.com/polaris-trade/client-soupbintcp/commit/7c9d03fd11cae92c93406cdcd1076076aa2f56f6))

## [0.2.1](https://github.com/polaris-trade/client-soupbintcp/compare/client_soupbintcp-v0.2.0...client_soupbintcp-v0.2.1) (2026-07-09)


### Build

* **deps:** bump deps and update README ([#5](https://github.com/polaris-trade/client-soupbintcp/issues/5)) ([3f96eb7](https://github.com/polaris-trade/client-soupbintcp/commit/3f96eb79f2e5f5b7125328ec42e4ff781ae21803))

## [0.2.0](https://github.com/polaris-trade/client-soupbintcp/compare/client_soupbintcp-v0.1.0...client_soupbintcp-v0.2.0) (2026-07-09)


### Features

* **recv:** land ingest directly into the decode buffer ([#3](https://github.com/polaris-trade/client-soupbintcp/issues/3)) ([9581ddc](https://github.com/polaris-trade/client-soupbintcp/commit/9581ddc030df7307031c30bd6b096ce893a98dd7))

## [0.1.0](https://github.com/polaris-trade/client-soupbintcp/releases/tag/client_soupbintcp-v0.1.0) - 2026-07-07

### Bug fixes

- *(ci)* Point reusable workflows at polaris-trade/ci


### Features

- *(soupbintcp)* SoupBinTCP 3.0 session client with compressed-feed support ([#1](https://github.com/polaris-trade/client-soupbintcp/pull/1))


  Backend-generic SoupBinClient over transport_core::Transport:
- login handshake with timeout, sequenced/unsequenced data, server + client heartbeats
- zero-copy packet decode via BytesMut split; resident send buffer reused per write
- optional `compressed` feature: zlib-inflate NASDAQ compressed variant
- session state machine, serde config, typed error taxonomy with source chains


### Refactor

- Rename crate soupbintcp to client_soupbintcp, folder to client-soupbintcp
