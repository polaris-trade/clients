//! SoupBinTCP v3.0 client crate: wire codec, `SoupBinClient` state machine, heartbeats, `compressed` feature.
//!
//! Generic over `transport_core::StreamSource`; `AsyncReady` is optional, gating
//! `connect`/`recv` only. No backend imports here.
#[cfg(feature = "compressed")]
pub mod compressed;

pub mod client;
pub mod config;
pub mod error;
pub mod event;
pub mod frame;
pub mod wire;

pub use client::{ClientState, SoupBinClient};
#[cfg(feature = "compressed")]
pub use compressed::CompressedReader;
pub use config::SoupBinClientConfig;
pub use error::SoupBinError;
pub use event::{SoupBinEvent, SoupBinMessage};
pub use frame::Frame;
pub use wire::{PacketFrame, PacketType, parse_packet};
