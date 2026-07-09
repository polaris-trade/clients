//! MoldUDP64 client crate: wire codec, `SequenceReassembler`, `GapRequestHandler`,
//! `AbArbiter`, `MoldUdpReceiver`.
//!
//! Backend-agnostic over `transport_core::DatagramSource`: no `tokio`/`mio`
//! imports here, no feature gates per backend. See each module for wire
//! format, reassembly, gap recovery, and A/B arbitration details.

pub mod ab;
pub mod config;
pub mod error;
pub mod event;
pub mod frame;
pub mod gap;
pub mod reassembly;
pub mod receiver;
pub mod wire;

pub use ab::{AbArbiter, ArbiterStats, ArbiterVerdict, StreamStats};
pub use config::{MoldUdpReceiverConfig, StreamConfig};
pub use error::MoldUdpError;
pub use event::MoldUdpEvent;
pub use frame::{Frame, MessageView, OwnedFrame};
pub use gap::{GapRequest, GapRequestEmitter, GapRequestHandler};
pub use reassembly::{DrainCursor, SequenceReassembler, Slot};
pub use receiver::{MoldUdpOutcome, MoldUdpReceiver, ReceiverStats};
pub use wire::{DownstreamHeader, MessageBlockIter, PacketKind, parse_header};
