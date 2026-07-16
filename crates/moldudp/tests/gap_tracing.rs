//! Verifies `MoldUdpReceiver` gap events go through `tracing`: a warn fires
//! once per discontinuity at detection, never per packet, and an in-order
//! feed emits nothing.

mod support;

use std::sync::{Arc, Mutex};

use client_moldudp::{
    MoldUdpError, MoldUdpEvent, MoldUdpOutcome, MoldUdpReceiver, MoldUdpReceiverConfig,
    StreamConfig,
};
use support::{AllocProofTransport, block_on, mold_heartbeat, mold_packet};
use tracing::{
    Event, Level, Metadata, Subscriber,
    field::{Field, Visit},
    span,
};

fn cfg() -> MoldUdpReceiverConfig {
    MoldUdpReceiverConfig {
        streams: vec![StreamConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// One dispatched event: level plus its `message` field text.
#[derive(Debug, Clone)]
struct CapturedEvent {
    level: Level,
    message: String,
}

#[derive(Default)]
struct MessageVisitor {
    message: String,
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        }
    }
}

/// Minimal hand-rolled `Subscriber`: no spans used on this path, so span
/// bookkeeping is a fixed no-op id. Only `event` matters for this test.
#[derive(Clone, Default)]
struct CapturingSubscriber {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
}

impl Subscriber for CapturingSubscriber {
    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _span: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(1)
    }

    fn record(&self, _span: &span::Id, _values: &span::Record<'_>) {}

    fn record_follows_from(&self, _span: &span::Id, _follows: &span::Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        self.events.lock().unwrap().push(CapturedEvent {
            level: *event.metadata().level(),
            message: visitor.message,
        });
    }

    fn enter(&self, _span: &span::Id) {}
    fn exit(&self, _span: &span::Id) {}
}

#[test]
fn in_order_sequence_emits_no_gap_event() {
    let session = *b"SESSIONID1";
    let subscriber = CapturingSubscriber::default();
    let events = subscriber.events.clone();

    tracing::subscriber::with_default(subscriber, || {
        let mut receiver =
            block_on(MoldUdpReceiver::<AllocProofTransport>::new(cfg())).expect("bind receiver");
        let transport = &receiver.transports()[0];
        for seq in 1u64..=3 {
            transport.seed(mold_packet(&session, seq, format!("m{seq}").as_bytes()));
        }
        for _ in 0..3 {
            match block_on(receiver.recv()) {
                Ok(MoldUdpOutcome::Frame(_)) => {}
                other => panic!("unexpected outcome: {other:?}"),
            }
        }
    });

    assert!(
        events.lock().unwrap().is_empty(),
        "in-order feed must not emit any tracing event"
    );
}

#[test]
fn tail_gap_discontinuity_emits_exactly_one_warn() {
    // Heartbeat carrying next-expected 4 after only seq 1 was seen is the
    // detection transition (`note_tail_gap`): must fire once, not per poll.
    let session = *b"SESSIONID1";
    let subscriber = CapturingSubscriber::default();
    let events = subscriber.events.clone();

    tracing::subscriber::with_default(subscriber, || {
        let mut receiver =
            block_on(MoldUdpReceiver::<AllocProofTransport>::new(cfg())).expect("bind receiver");
        let transport = &receiver.transports()[0];
        transport.seed(mold_packet(&session, 1, b"one"));
        transport.seed(mold_heartbeat(&session, 4));

        let mut saw_heartbeat = false;
        for _ in 0..4 {
            match block_on(receiver.recv()) {
                Ok(MoldUdpOutcome::Frame(_)) => {}
                Ok(MoldUdpOutcome::Event(MoldUdpEvent::Heartbeat { .. })) => {
                    saw_heartbeat = true;
                    break;
                }
                Ok(other) => panic!("unexpected outcome: {other:?}"),
                Err(MoldUdpError::GapDetected) => {}
                Err(e) => panic!("unexpected recv error: {e}"),
            }
        }
        assert!(saw_heartbeat);
    });

    let captured = events.lock().unwrap();
    assert_eq!(captured.len(), 1, "exactly one gap event, got {captured:?}");
    assert_eq!(captured[0].level, Level::WARN);
    assert!(
        captured[0].message.contains("sequence gap detected"),
        "unexpected message: {}",
        captured[0].message
    );
}
