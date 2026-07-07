//! `MoldUdpError`: every failure path in this crate returns one of these variants.
//! I/O and backend failures bubble through `Transport` from `transport_core::TransportError`.

use thiserror::Error;
use transport_core::TransportError;

/// Failure kind for MoldUDP64 wire decode, reassembly, session, and transport paths.
/// `#[non_exhaustive]` since new failure kinds may land alongside future REQ work.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MoldUdpError {
    #[error(transparent)]
    Transport(#[from] TransportError),

    #[error("session mismatch: expected {expected:02x?}, got {got:02x?}")]
    SessionMismatch { expected: [u8; 10], got: [u8; 10] },

    #[error("gap detected")]
    GapDetected,

    #[error("reassembly buffer full (capacity {capacity})")]
    ReassemblyBufferFull { capacity: usize },

    #[error("invalid sequence")]
    InvalidSequence,

    #[error("packet too short")]
    PacketTooShort,

    #[error("packet too large")]
    PacketTooLarge,
}
