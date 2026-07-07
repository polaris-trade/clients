//! Session enforcement over a real loopback UDP transport: first datagram
//! locks the session id, a later datagram carrying a different id is rejected.

use client_moldudp::{
    MoldUdpError, MoldUdpOutcome, MoldUdpReceiver, MoldUdpReceiverConfig, StreamConfig,
};
use tokio::net::UdpSocket;
use transport_tokio::TokioTransport;

fn build_datagram(session: &[u8; 10], seq: u64, payload: &[u8]) -> Vec<u8> {
    let mut packet = Vec::new();
    packet.extend_from_slice(session);
    packet.extend_from_slice(&seq.to_be_bytes());
    packet.extend_from_slice(&1u16.to_be_bytes());
    packet.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    packet.extend_from_slice(payload);
    packet
}

fn bound_addr(receiver: &MoldUdpReceiver<TokioTransport>) -> std::net::SocketAddr {
    match &receiver.transports()[0] {
        TokioTransport::Udp(u) => u.local_addr().expect("local addr"),
        _ => panic!("expected udp transport"),
    }
}

#[tokio::test]
async fn later_session_mismatch_rejected_after_first_locks_it() {
    let cfg = MoldUdpReceiverConfig {
        streams: vec![StreamConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut receiver = MoldUdpReceiver::<TokioTransport>::new(cfg)
        .await
        .expect("bind receiver");
    let addr = bound_addr(&receiver);
    let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();

    let session_a = *b"SESSION_A0";
    let session_b = *b"SESSION_B0";

    sender
        .send_to(&build_datagram(&session_a, 1, b"first"), addr)
        .await
        .unwrap();
    match receiver.recv().await.expect("first recv") {
        MoldUdpOutcome::Frame(frame) => {
            assert_eq!(frame.payload, b"first");
        }
        MoldUdpOutcome::Event(_) => panic!("unexpected event"),
    }

    sender
        .send_to(&build_datagram(&session_b, 2, b"second"), addr)
        .await
        .unwrap();
    let err = receiver
        .recv()
        .await
        .expect_err("session mismatch expected");
    match err {
        MoldUdpError::SessionMismatch { expected, got } => {
            assert_eq!(expected, session_a);
            assert_eq!(got, session_b);
        }
        other => panic!("expected SessionMismatch, got {other}"),
    }
}
