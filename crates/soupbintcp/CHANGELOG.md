# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).
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

