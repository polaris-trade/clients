//! `SoupBinError`: every failure path in this crate resolves to one variant here.
//! `Transport` wraps backend I/O failures via `#[from]`, rest are protocol-level.

use std::time::Duration;
use thiserror::Error;

/// Failure kinds for SoupBinTCP session handling.
///
/// `#[non_exhaustive]`: new protocol edge cases may add variants later.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SoupBinError {
    #[error(transparent)]
    Transport(#[from] transport_core::TransportError),

    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    #[error("login rejected: {code}")]
    LoginRejected { code: String },

    #[error("login timeout after {timeout:?}")]
    LoginTimeout { timeout: Duration },

    #[error("auth failed: {0}")]
    AuthFailed(String),

    #[error("heartbeat timeout")]
    HeartbeatTimeout,

    #[error("frame too large: {size} bytes (max {max})")]
    FrameTooLarge { size: usize, max: usize },

    #[error("protocol violation: {0}")]
    ProtocolViolation(String),

    #[error("unknown packet type: 0x{0:02x}")]
    UnknownPacketType(u8),

    #[error("end of session")]
    EndOfSession,
}
