//! Wire-level parser tests: fragmentation, unknown types, zero-length guard.

use client_soupbintcp::{PacketType, SoupBinError, parse_packet};

fn build(ty: u8, payload: &[u8]) -> Vec<u8> {
    let len = 1 + payload.len();
    let mut out = Vec::new();
    out.extend_from_slice(&(len as u16).to_be_bytes());
    out.push(ty);
    out.extend_from_slice(payload);
    out
}

#[test]
fn partial_packet_held() {
    let full = build(b'S', b"hello");

    // fewer than 3 bytes: not even a full length + type prefix yet
    assert!(parse_packet(&full[..2]).unwrap().is_none());

    // length + type known, but body still short one byte
    assert!(parse_packet(&full[..full.len() - 1]).unwrap().is_none());

    // full packet parses once all bytes present
    let (frame, consumed) = parse_packet(&full).unwrap().unwrap();
    assert_eq!(frame.ty, PacketType::SequencedData);
    assert_eq!(frame.payload, b"hello");
    assert_eq!(consumed, full.len());
}

#[test]
fn unknown_type_rejected() {
    let buf = build(b'?', b"x");
    match parse_packet(&buf).unwrap_err() {
        SoupBinError::UnknownPacketType(b) => assert_eq!(b, b'?'),
        other => panic!("expected UnknownPacketType, got {other:?}"),
    }
}

#[test]
fn debug_packet_dropped() {
    // wire layer decodes Debug packets like any other type; SoupBinClient::recv
    // is what actually discards them before a consumer ever sees them.
    let buf = build(b'+', b"trace info");
    let (frame, consumed) = parse_packet(&buf).unwrap().unwrap();
    assert_eq!(frame.ty, PacketType::Debug);
    assert_eq!(frame.payload, b"trace info");
    assert_eq!(consumed, buf.len());
}

#[test]
fn zero_length_rejected() {
    // 3 bytes present (the minimum parse_packet needs to read the length
    // field at all), length field itself claims 0 -> rejected, not held as partial.
    let buf = vec![0u8, 0u8, 0u8];
    match parse_packet(&buf).unwrap_err() {
        SoupBinError::ProtocolViolation(_) => {}
        other => panic!("expected ProtocolViolation, got {other:?}"),
    }
}
