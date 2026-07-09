//! Two mock sender streams share session + sequence space; the receiver's
//! A/B arbiter merges them so each seq is delivered exactly once, in order,
//! even though stream A is missing a message that only stream B delivers.

use client_moldudp::{MoldUdpOutcome, MoldUdpReceiver, MoldUdpReceiverConfig, StreamConfig};
use tokio::net::UdpSocket;
use transport_tokio::TokioTransport;

fn encode(session: &[u8; 10], seq: u64, payload: &[u8]) -> Vec<u8> {
    let mut packet = Vec::new();
    packet.extend_from_slice(session);
    packet.extend_from_slice(&seq.to_be_bytes());
    packet.extend_from_slice(&1u16.to_be_bytes());
    packet.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    packet.extend_from_slice(payload);
    packet
}

fn bound_addr(transport: &TokioTransport) -> std::net::SocketAddr {
    match transport {
        TokioTransport::Udp(u) => u.local_addr().expect("local addr"),
        _ => panic!("expected udp transport"),
    }
}

#[tokio::test]
async fn each_sequence_delivered_exactly_once_via_ab_merge() {
    let cfg = MoldUdpReceiverConfig {
        streams: vec![
            StreamConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                ..Default::default()
            },
            StreamConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let mut receiver = MoldUdpReceiver::<TokioTransport>::new(cfg)
        .await
        .expect("bind receiver");
    let addr_a = bound_addr(&receiver.transports()[0]);
    let addr_b = bound_addr(&receiver.transports()[1]);

    let session = *b"SESSIONAB1";
    let sender_a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let sender_b = UdpSocket::bind("127.0.0.1:0").await.unwrap();

    // stream A is missing seq 3; stream B has the full run 1..=5.
    for seq in [1u64, 2, 4, 5] {
        let payload = format!("msg-{seq}").into_bytes();
        sender_a
            .send_to(&encode(&session, seq, &payload), addr_a)
            .await
            .unwrap();
    }
    for seq in 1u64..=5 {
        let payload = format!("msg-{seq}").into_bytes();
        sender_b
            .send_to(&encode(&session, seq, &payload), addr_b)
            .await
            .unwrap();
    }

    let mut received = Vec::new();
    while received.len() < 5 {
        match receiver.recv().await {
            Ok(MoldUdpOutcome::Frame(frame)) => {
                received.push((frame.sequence, frame.payload.to_vec()))
            }
            Ok(MoldUdpOutcome::Event(_)) => {}
            Ok(MoldUdpOutcome::Owned(_)) => panic!("recv() must not yield an owned frame"),
            Err(client_moldudp::MoldUdpError::GapDetected) => {}
            Err(e) => panic!("unexpected error: {e}"),
        }
    }

    assert_eq!(received.len(), 5, "each seq must be delivered exactly once");
    for (i, (seq, payload)) in received.iter().enumerate() {
        let expected_seq = (i + 1) as u64;
        assert_eq!(*seq, expected_seq);
        assert_eq!(payload, format!("msg-{expected_seq}").as_bytes());
    }
}
