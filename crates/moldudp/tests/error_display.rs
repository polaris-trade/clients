//! One Display assertion per `MoldUdpError` variant: callers match on kind,
//! but the message itself still needs to carry diagnostic detail.

use client_moldudp::MoldUdpError;
use transport_core::TransportError;

#[test]
fn transport_variant_displays_source_error() {
    let err = MoldUdpError::Transport(TransportError::Io(std::io::Error::other("boom")));
    assert!(err.to_string().contains("boom"));
}

#[test]
fn session_mismatch_displays_both_ids() {
    let err = MoldUdpError::SessionMismatch {
        expected: [0xAA; 10],
        got: [0xBB; 10],
    };
    let msg = err.to_string();
    assert!(msg.contains("session mismatch"));
    assert!(msg.contains("aa"));
    assert!(msg.contains("bb"));
}

#[test]
fn gap_detected_displays() {
    assert_eq!(MoldUdpError::GapDetected.to_string(), "gap detected");
}

#[test]
fn reassembly_buffer_full_displays_capacity() {
    let err = MoldUdpError::ReassemblyBufferFull { capacity: 4096 };
    assert!(err.to_string().contains("4096"));
}

#[test]
fn invalid_sequence_displays() {
    assert_eq!(
        MoldUdpError::InvalidSequence.to_string(),
        "invalid sequence"
    );
}

#[test]
fn packet_too_short_displays() {
    assert_eq!(MoldUdpError::PacketTooShort.to_string(), "packet too short");
}

#[test]
fn packet_too_large_displays() {
    assert_eq!(MoldUdpError::PacketTooLarge.to_string(), "packet too large");
}
