//! Multicast group join through `MoldUdpReceiver::new` against
//! `transport_tokio::TokioTransport`, which implements
//! `transport_core::UdpTransport`. Joining a group via `IP_ADD_MEMBERSHIP` is
//! an unprivileged socket op, so this runs without `--ignored`.

use std::net::{IpAddr, Ipv4Addr};

use client_moldudp::{MoldUdpReceiver, MoldUdpReceiverConfig, StreamConfig};
use transport_tokio::TokioTransport;

#[tokio::test]
async fn joins_multicast_group() {
    let cfg = MoldUdpReceiverConfig {
        streams: vec![StreamConfig {
            bind_addr: "0.0.0.0:0".parse().unwrap(),
            ..Default::default()
        }],
        multicast_addr: Some(IpAddr::V4(Ipv4Addr::new(239, 192, 0, 1))),
        ..Default::default()
    };
    let receiver = MoldUdpReceiver::<TokioTransport>::new(cfg).await;
    assert!(
        receiver.is_ok(),
        "multicast join failed: {:?}",
        receiver.err()
    );
}
