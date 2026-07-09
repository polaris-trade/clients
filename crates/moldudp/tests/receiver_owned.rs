//! `recv_owned` proof: the outcome crosses a real thread boundary (`Send`)
//! and its bytes match what a real UDP sender put on the wire.

use client_moldudp::{MoldUdpOutcome, MoldUdpReceiver, MoldUdpReceiverConfig, StreamConfig};
use tokio::net::UdpSocket;
use transport_core::AsPayload;
use transport_tokio::TokioTransport;

#[tokio::test]
async fn recv_owned_crosses_thread_boundary() {
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
    let payload = b"owned-payload".to_vec();
    let mut packet = Vec::new();
    packet.extend_from_slice(&session);
    packet.extend_from_slice(&1u64.to_be_bytes());
    packet.extend_from_slice(&1u16.to_be_bytes());
    packet.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    packet.extend_from_slice(&payload);
    sender.send_to(&packet, addr).await.unwrap();

    let owned = match receiver.recv_owned().await.expect("recv_owned") {
        MoldUdpOutcome::Owned(owned) => owned,
        other => panic!("expected owned frame, got {other:?}"),
    };
    assert_eq!(owned.sequence, 1);

    // Move the owned frame to another thread: proves `OwnedFrame` is `Send`.
    let handle = std::thread::spawn(move || owned.payload().to_vec());
    let bytes = handle.join().expect("owned frame crossed thread boundary");
    assert_eq!(bytes, payload);
}
