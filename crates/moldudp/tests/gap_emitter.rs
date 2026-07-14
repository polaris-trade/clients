//! `GapRequestEmitter` rate limiting: caps re-request traffic per gap so a
//! gap that never fills can't flood the re-request server.

use std::time::Duration;

use client_moldudp::{GapRequest, GapRequestEmitter};
use transport_core::{TransportCore, TransportError};

/// Minimal `TransportCore` double that just records every buffer handed to `send`.
struct RecordingTransport {
    sent: Vec<Vec<u8>>,
}

impl TransportCore for RecordingTransport {
    fn name(&self) -> &'static str {
        "recording"
    }

    async fn send(&mut self, buf: &[u8]) -> Result<(), TransportError> {
        self.sent.push(buf.to_vec());
        Ok(())
    }
}

#[tokio::test]
async fn rate_limits_repeated_requests_for_the_same_gap() {
    let mut transport = RecordingTransport { sent: Vec::new() };
    let addr = "127.0.0.1:9000".parse().unwrap();
    // default rate: 4 requests/sec/gap => 250ms minimum spacing.
    let mut emitter = GapRequestEmitter::new(addr, 4);
    let session = *b"SESSIONID1";
    let gap = GapRequest {
        start_seq: 100,
        count: 5,
    };

    let mut total_sent = 0usize;
    for _ in 0..10 {
        total_sent += emitter
            .emit(std::slice::from_ref(&gap), session, &mut transport)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(111)).await;
    }

    // Real-clock rate limiter (Instant::now) over ten 111ms-spaced emits in a
    // ~1.1s window at 4/s. Ideal timing yields 4; coarse timers and scheduler
    // jitter on CI (Windows ~15ms granularity) shift it, so assert the contract
    // (capped well below 10, still emitting near the rate) not an exact count.
    assert!(
        (3..=6).contains(&total_sent),
        "rate limiter should cap a persistent gap near 4/s, got {total_sent} of 10"
    );
    assert_eq!(transport.sent.len(), total_sent);
}
