//! Construction-time guard: a backend reporting a recv pool smaller than the
//! reorder window plus burst headroom must fail fast with a config error,
//! not panic or stall live once the reorder window fills.

mod support;

use client_moldudp::{MoldUdpError, MoldUdpReceiver, MoldUdpReceiverConfig, StreamConfig};
use support::{UndersizedPoolTransport, block_on};
use transport_core::TransportError;

#[test]
fn undersized_pool_fails_construction_not_recv() {
    let cfg = MoldUdpReceiverConfig {
        streams: vec![StreamConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..Default::default()
        }],
        ..Default::default()
    };

    let result = block_on(MoldUdpReceiver::<UndersizedPoolTransport>::new(cfg));
    let err = match result {
        Ok(_) => panic!("undersized pool must fail construction"),
        Err(e) => e,
    };
    match err {
        MoldUdpError::Transport(TransportError::BackendUnavailable { name, reason }) => {
            assert_eq!(name, "mock");
            assert!(reason.contains("below required"));
        }
        other => panic!("expected BackendUnavailable, got {other:?}"),
    }
}
