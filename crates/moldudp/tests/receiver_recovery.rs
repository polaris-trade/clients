//! Gap-recovery and sequence-anchoring behavior of `MoldUdpReceiver`:
//! mid-session join, configured start, heartbeat/end-of-session tail-gap
//! detection, and single-gap-per-packet recording. All single-stream (no
//! arbiter), so gaps record inline and surface via `stats().pending_gaps`.

mod support;

use client_moldudp::{
    GapRequest, MoldUdpError, MoldUdpEvent, MoldUdpOutcome, MoldUdpReceiver, MoldUdpReceiverConfig,
    StreamConfig,
};
use support::{
    AllocProofTransport, block_on, mold_end_of_session, mold_heartbeat, mold_multi_packet,
    mold_packet,
};
use transport_core::{AsyncReady, DatagramSource};

fn cfg() -> MoldUdpReceiverConfig {
    MoldUdpReceiverConfig {
        streams: vec![StreamConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Pull `n` data frames, skipping `GapDetected` and control events.
fn drain_n_frames<T: DatagramSource + AsyncReady>(
    receiver: &mut MoldUdpReceiver<T>,
    n: usize,
) -> Vec<u64> {
    let mut seqs = Vec::with_capacity(n);
    while seqs.len() < n {
        match block_on(receiver.recv()) {
            Ok(MoldUdpOutcome::Frame(f)) => seqs.push(f.sequence),
            Ok(_) => continue,
            Err(MoldUdpError::GapDetected) => continue,
            Err(e) => panic!("unexpected recv error: {e}"),
        }
    }
    seqs
}

#[test]
fn cold_start_adopts_first_packet_sequence_no_phantom_gap() {
    // Joining a live feed mid-session at seq 5000 must anchor there, not treat
    // 1..5000 as one giant gap.
    let session = *b"SESSIONID1";
    let mut receiver =
        block_on(MoldUdpReceiver::<AllocProofTransport>::new(cfg())).expect("bind receiver");
    let transport = &receiver.transports()[0];
    for seq in 5000u64..=5003 {
        transport.seed(mold_packet(&session, seq, format!("m{seq}").as_bytes()));
    }

    let seqs = drain_n_frames(&mut receiver, 4);
    assert_eq!(seqs, [5000, 5001, 5002, 5003]);
    assert!(
        receiver.stats().pending_gaps.is_empty(),
        "no phantom gap below the first sequence seen"
    );
}

#[test]
fn configured_start_sequence_is_honored() {
    let session = *b"SESSIONID1";
    let mut c = cfg();
    c.start_sequence = Some(100);
    let mut receiver =
        block_on(MoldUdpReceiver::<AllocProofTransport>::new(c)).expect("bind receiver");
    let transport = &receiver.transports()[0];
    transport.seed(mold_packet(&session, 100, b"hundred"));

    let seqs = drain_n_frames(&mut receiver, 1);
    assert_eq!(seqs, [100]);
    assert!(receiver.stats().pending_gaps.is_empty());
}

#[test]
fn heartbeat_ahead_of_expected_records_tail_gap() {
    // Server heartbeat carrying next-expected 4 after we have only seen seq 1
    // means 2 and 3 were lost during quiet traffic: record them.
    let session = *b"SESSIONID1";
    let mut receiver =
        block_on(MoldUdpReceiver::<AllocProofTransport>::new(cfg())).expect("bind receiver");
    let transport = &receiver.transports()[0];
    transport.seed(mold_packet(&session, 1, b"one"));
    transport.seed(mold_heartbeat(&session, 4));

    let mut saw_frame = false;
    let mut saw_gap = false;
    let mut saw_heartbeat = false;
    for _ in 0..4 {
        match block_on(receiver.recv()) {
            Ok(MoldUdpOutcome::Frame(f)) => {
                assert_eq!(f.sequence, 1);
                saw_frame = true;
            }
            Ok(MoldUdpOutcome::Event(MoldUdpEvent::Heartbeat { next_expected })) => {
                assert_eq!(next_expected, 4);
                saw_heartbeat = true;
                break;
            }
            Ok(other) => panic!("unexpected outcome: {other:?}"),
            Err(MoldUdpError::GapDetected) => saw_gap = true,
            Err(e) => panic!("unexpected recv error: {e}"),
        }
    }
    assert!(saw_frame && saw_gap && saw_heartbeat);
    assert_eq!(
        receiver.stats().pending_gaps,
        vec![GapRequest {
            start_seq: 2,
            count: 2
        }]
    );
}

#[test]
fn end_of_session_ahead_of_expected_records_tail_gap() {
    // End-of-session is the last chance to re-request: a next-expected past our
    // own position means the tail was never delivered.
    let session = *b"SESSIONID1";
    let mut receiver =
        block_on(MoldUdpReceiver::<AllocProofTransport>::new(cfg())).expect("bind receiver");
    let transport = &receiver.transports()[0];
    transport.seed(mold_packet(&session, 1, b"one"));
    transport.seed(mold_end_of_session(&session, 3));

    let mut saw_eos = false;
    for _ in 0..4 {
        match block_on(receiver.recv()) {
            Ok(MoldUdpOutcome::Frame(f)) => assert_eq!(f.sequence, 1),
            Ok(MoldUdpOutcome::Event(MoldUdpEvent::EndOfSession { next_expected })) => {
                assert_eq!(next_expected, 3);
                saw_eos = true;
                break;
            }
            Ok(other) => panic!("unexpected outcome: {other:?}"),
            Err(MoldUdpError::GapDetected) => {}
            Err(e) => panic!("unexpected recv error: {e}"),
        }
    }
    assert!(saw_eos);
    assert_eq!(
        receiver.stats().pending_gaps,
        vec![GapRequest {
            start_seq: 2,
            count: 1
        }]
    );
}

#[test]
fn multi_block_ahead_records_one_gap_not_per_block() {
    // A packet whose blocks are all ahead of expected must record only the
    // leading jump as a gap, not re-request the blocks it actually carries.
    let session = *b"SESSIONID1";
    let mut receiver =
        block_on(MoldUdpReceiver::<AllocProofTransport>::new(cfg())).expect("bind receiver");
    let transport = &receiver.transports()[0];
    transport.seed(mold_packet(&session, 1, b"one"));
    // expected becomes 2; this packet carries seq 5,6,7 (2,3,4 are the gap).
    transport.seed(mold_multi_packet(&session, 5, &[b"five", b"six", b"seven"]));

    match block_on(receiver.recv()).expect("recv") {
        MoldUdpOutcome::Frame(f) => assert_eq!(f.sequence, 1),
        other => panic!("unexpected outcome: {other:?}"),
    }
    assert!(matches!(
        block_on(receiver.recv()),
        Err(MoldUdpError::GapDetected)
    ));
    // Gap is {2,3,4} only. The pre-fix bug widened it to {2,3,4,5,6}.
    assert_eq!(
        receiver.stats().pending_gaps,
        vec![GapRequest {
            start_seq: 2,
            count: 3
        }]
    );
}
