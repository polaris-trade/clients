//! End-to-end loopback: a real UDP sender feeds N mold packets to a receiver
//! bound over `transport_tokio::TokioTransport`, asserting in-order delivery.

use client_moldudp::{MoldUdpOutcome, MoldUdpReceiver, MoldUdpReceiverConfig, StreamConfig};
use tokio::net::UdpSocket;
use transport_tokio::TokioTransport;

const N: usize = 5;

#[tokio::test]
async fn loopback_delivers_n_frames_in_order() {
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
    let addr = match &receiver.transports()[0] {
        TokioTransport::Udp(u) => u.local_addr().expect("local addr"),
        _ => panic!("expected udp transport"),
    };

    let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let session = *b"SESSIONID1";
    for i in 0..N {
        let seq = (i + 1) as u64;
        let payload = format!("msg-{i}").into_bytes();
        let mut packet = Vec::new();
        packet.extend_from_slice(&session);
        packet.extend_from_slice(&seq.to_be_bytes());
        packet.extend_from_slice(&1u16.to_be_bytes());
        packet.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        packet.extend_from_slice(&payload);
        sender.send_to(&packet, addr).await.unwrap();
    }

    let mut received = Vec::new();
    for _ in 0..N {
        match receiver.recv().await.expect("recv") {
            MoldUdpOutcome::Frame(frame) => received.push((frame.sequence, frame.payload.to_vec())),
            MoldUdpOutcome::Event(_) => panic!("unexpected event"),
            MoldUdpOutcome::Owned(_) => panic!("recv() must not yield an owned frame"),
        }
    }

    assert_eq!(received.len(), N);
    for (i, (seq, payload)) in received.iter().enumerate() {
        assert_eq!(*seq, (i + 1) as u64);
        assert_eq!(payload, format!("msg-{i}").as_bytes());
    }
}
