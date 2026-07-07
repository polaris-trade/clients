//! `GapRequestEmitter` rate limiting: caps re-request traffic per gap so a
//! gap that never fills can't flood the re-request server.

use std::task::{Context, Poll};
use std::time::Duration;

use client_moldudp::{GapRequest, GapRequestEmitter};
use transport_core::{AsPayload, Transport, TransportError};

/// Minimal `Transport` double that just records every buffer handed to `send`.
struct RecordingTransport {
    sent: Vec<Vec<u8>>,
}

struct NoFrame;
impl AsPayload for NoFrame {
    fn payload(&self) -> &[u8] {
        &[]
    }
    fn sequence(&self) -> u64 {
        0
    }
    fn stream_id(&self) -> u8 {
        0
    }
}

impl Transport for RecordingTransport {
    type Frame<'a> = NoFrame;
    type Event = ();

    fn poll_event(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), TransportError>> {
        Poll::Pending
    }

    fn next_frame(&self) -> Option<Self::Frame<'_>> {
        None
    }

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

    assert_eq!(
        total_sent, 4,
        "expected exactly 4 of 10 requests in ~1s window"
    );
    assert_eq!(transport.sent.len(), 4);
}
