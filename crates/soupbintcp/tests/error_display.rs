//! One `Display` assertion per `SoupBinError` variant.

use std::time::Duration;

use client_soupbintcp::SoupBinError;
use transport_core::TransportError;

#[test]
fn transport_displays_inner_error() {
    let err = SoupBinError::Transport(TransportError::Io(std::io::Error::other("boom")));
    assert_eq!(err.to_string(), "I/O error: boom");
}

#[test]
fn connection_failed_displays_reason() {
    let err = SoupBinError::ConnectionFailed("refused".into());
    assert_eq!(err.to_string(), "connection failed: refused");
}

#[test]
fn login_rejected_displays_code() {
    let err = SoupBinError::LoginRejected { code: "A".into() };
    assert_eq!(err.to_string(), "login rejected: A");
}

#[test]
fn login_timeout_displays_duration() {
    let err = SoupBinError::LoginTimeout {
        timeout: Duration::from_secs(30),
    };
    assert_eq!(err.to_string(), "login timeout after 30s");
}

#[test]
fn auth_failed_displays_reason() {
    let err = SoupBinError::AuthFailed("bad creds".into());
    assert_eq!(err.to_string(), "auth failed: bad creds");
}

#[test]
fn heartbeat_timeout_displays() {
    assert_eq!(
        SoupBinError::HeartbeatTimeout.to_string(),
        "heartbeat timeout"
    );
}

#[test]
fn frame_too_large_displays_sizes() {
    let err = SoupBinError::FrameTooLarge { size: 100, max: 64 };
    assert_eq!(err.to_string(), "frame too large: 100 bytes (max 64)");
}

#[test]
fn protocol_violation_displays_reason() {
    let err = SoupBinError::ProtocolViolation("bad state".into());
    assert_eq!(err.to_string(), "protocol violation: bad state");
}

#[test]
fn unknown_packet_type_displays_hex() {
    let err = SoupBinError::UnknownPacketType(0x3f);
    assert_eq!(err.to_string(), "unknown packet type: 0x3f");
}

#[test]
fn end_of_session_displays() {
    assert_eq!(SoupBinError::EndOfSession.to_string(), "end of session");
}
