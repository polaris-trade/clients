# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).
## [0.5.0](https://github.com/polaris-trade/clients/compare/client_soupbintcp-v0.4.0...client_soupbintcp-v0.5.0) (2026-07-14)


### Features

* **client-soupbintcp:** support sync-only backends via poll_recv ([#7](https://github.com/polaris-trade/clients/issues/7)) ([1db456f](https://github.com/polaris-trade/clients/commit/1db456f121039f1b6c6dcdf4665559e32781855a))
* **recv:** land ingest directly into the decode buffer ([#3](https://github.com/polaris-trade/clients/issues/3)) ([f8ca4e8](https://github.com/polaris-trade/clients/commit/f8ca4e8b4f823e64ada663996e626629ab7662a1))
* **soupbintcp:** SoupBinTCP 3.0 session client with compressed-feed support ([#1](https://github.com/polaris-trade/clients/issues/1)) ([c3249f1](https://github.com/polaris-trade/clients/commit/c3249f14150aa9f584187215dc2842bfe33b08cf))
* **telemetry:** add recv-counter ([#11](https://github.com/polaris-trade/clients/issues/11)) ([7a4a540](https://github.com/polaris-trade/clients/commit/7a4a540e58dc70d3aea749724d26a49613ef7e42))


### Bug fixes

* **ci:** point reusable workflows at polaris-trade/ci ([c8dc597](https://github.com/polaris-trade/clients/commit/c8dc5971dd9da6a77cfa4b89a1d7294902506e57))
* **client:** tolerate login heartbeat and default to full-session replay ([#9](https://github.com/polaris-trade/clients/issues/9)) ([4579974](https://github.com/polaris-trade/clients/commit/45799741684f09b8d0e7de0c1972c8d56b6fd218))


### Refactor

* rename crate soupbintcp to client_soupbintcp, folder to client-soupbintcp ([4eef8e6](https://github.com/polaris-trade/clients/commit/4eef8e65867fbb84562f8a1be22e46bad63f36ba))


### Build

* **deps:** bump deps and update README ([#5](https://github.com/polaris-trade/clients/issues/5)) ([e9c807c](https://github.com/polaris-trade/clients/commit/e9c807cded6d6c58165b487a87970fcc460762da))
* **workspace:** merge client-soupbintcp and client-moldudp ([#1](https://github.com/polaris-trade/clients/issues/1)) ([7d76be9](https://github.com/polaris-trade/clients/commit/7d76be9521d69a5bb5c4cb0a248aed4cd6069106))

## [0.4.0](https://github.com/polaris-trade/client-soupbintcp/compare/client_soupbintcp-v0.3.1...client_soupbintcp-v0.4.0) (2026-07-11)


### Features

* **telemetry:** add recv-counter ([#11](https://github.com/polaris-trade/client-soupbintcp/issues/11)) ([0b84e0d](https://github.com/polaris-trade/client-soupbintcp/commit/0b84e0d2094126511542e282e3311383963961e6))

## [0.3.1](https://github.com/polaris-trade/client-soupbintcp/compare/client_soupbintcp-v0.3.0...client_soupbintcp-v0.3.1) (2026-07-09)


### Bug fixes

* **client:** tolerate login heartbeat and default to full-session replay ([#9](https://github.com/polaris-trade/client-soupbintcp/issues/9)) ([371ee44](https://github.com/polaris-trade/client-soupbintcp/commit/371ee449d75f32dff655a43cc46be15f84fac16d))

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
