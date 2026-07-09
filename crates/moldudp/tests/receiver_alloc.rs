//! Allocation proof for the receiver's borrowed recv path: in-order messages
//! decode without touching the heap, and a gap-buffered message promotes its
//! datagram to `Arc` at most once per buffered datagram, not once per
//! message drained from it.

mod support;

use client_moldudp::{
    MoldUdpError, MoldUdpOutcome, MoldUdpReceiver, MoldUdpReceiverConfig, StreamConfig,
};
use support::{AllocProofTransport, block_on, mold_multi_packet, mold_packet};
use transport_core::{AsyncReady, DatagramSource};

/// Drive `recv()` until `n` data frames have been collected, skipping over
/// `GapDetected` errors (each out-of-order block re-flags the still-open
/// gap; that is expected noise here, not a failure).
fn drain_n_frames<T: DatagramSource + AsyncReady>(
    receiver: &mut MoldUdpReceiver<T>,
    n: usize,
) -> Vec<u64> {
    let mut sequences = Vec::with_capacity(n);
    while sequences.len() < n {
        match block_on(receiver.recv()) {
            Ok(MoldUdpOutcome::Frame(frame)) => sequences.push(frame.sequence),
            Ok(other) => panic!("expected a data frame, got {other:?}"),
            Err(MoldUdpError::GapDetected) => continue,
            Err(e) => panic!("unexpected recv error: {e}"),
        }
    }
    sequences
}

fn cfg() -> MoldUdpReceiverConfig {
    MoldUdpReceiverConfig {
        streams: vec![StreamConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[test]
fn recv_in_order_burst_is_allocation_free() {
    const N: u64 = 8;
    let session = *b"SESSIONID1";

    let mut receiver =
        block_on(MoldUdpReceiver::<AllocProofTransport>::new(cfg())).expect("bind receiver");
    let transport = &receiver.transports()[0];
    for seq in 1..=N {
        let payload = format!("msg-{seq}").into_bytes();
        transport.seed(mold_packet(&session, seq, &payload));
    }

    let info = allocation_counter::measure(|| {
        for seq in 1..=N {
            match block_on(receiver.recv()).expect("recv") {
                MoldUdpOutcome::Frame(frame) => assert_eq!(frame.sequence, seq),
                other => panic!("expected borrowed frame, got {other:?}"),
            }
        }
    });
    assert_eq!(info.count_total, 0, "in-order recv must not allocate");
}

/// Gap-tracking bookkeeping (`GapRequestHandler`'s `BTreeMap`) allocates too,
/// independent of the reassembler's `Arc` promotion under test here. A raw
/// `count_total <= 1` bound would conflate the two. Instead compare a
/// single-message out-of-order datagram against a same-shaped datagram
/// carrying 3 out-of-order messages: both record the same one gap range, so
/// gap-tracking cost is identical between the two; only a message-count-scaled
/// regression in `arc_current` (once-per-datagram, not once-per-message)
/// would make the 3-message case cost more.
#[test]
fn recv_gap_then_fill_arc_promotion_is_amortized_per_datagram_not_per_message() {
    let session = *b"SESSIONID1";

    let single_message_total = {
        let mut receiver =
            block_on(MoldUdpReceiver::<AllocProofTransport>::new(cfg())).expect("bind receiver");
        let transport = &receiver.transports()[0];
        // Arrival order 1, 3, 2: seq 3 lands ahead of `expected_next` (2) and
        // must buffer; seq 2 then fills the gap and cascade-drains seq 3.
        transport.seed(mold_packet(&session, 1, b"one"));
        transport.seed(mold_packet(&session, 3, b"three"));
        transport.seed(mold_packet(&session, 2, b"two"));

        allocation_counter::measure(|| {
            // Compare against a fixed-size array, not `vec![]`: the
            // measured region must not pay for the assertion's own alloc.
            let sequences = drain_n_frames(&mut receiver, 3);
            assert_eq!(sequences, [1u64, 2, 3]);
        })
        .count_total
    };

    let multi_message_total = {
        let mut receiver =
            block_on(MoldUdpReceiver::<AllocProofTransport>::new(cfg())).expect("bind receiver");
        let transport = &receiver.transports()[0];
        // Same shape, but the out-of-order datagram carries 3 messages (seq
        // 3, 4, 5) instead of 1; seq 2 cascade-drains all three at once.
        transport.seed(mold_packet(&session, 1, b"one"));
        transport.seed(mold_multi_packet(
            &session,
            3,
            &[b"three", b"four", b"five"],
        ));
        transport.seed(mold_packet(&session, 2, b"two"));

        allocation_counter::measure(|| {
            let sequences = drain_n_frames(&mut receiver, 5);
            assert_eq!(sequences, [1u64, 2, 3, 4, 5]);
        })
        .count_total
    };

    assert!(
        multi_message_total <= single_message_total,
        "buffering 3 out-of-order messages from one datagram ({multi_message_total} allocs) \
         must not cost more than buffering 1 ({single_message_total} allocs): the datagram's \
         slab must promote to `Arc` once per datagram, not once per message"
    );
}
