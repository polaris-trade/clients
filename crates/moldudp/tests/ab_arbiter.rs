//! A/B arbiter: first-arrival dedupe, window eviction, stats, and the gap
//! grace period that withholds re-request until every stream has missed.

use std::time::{Duration, Instant};

use client_moldudp::{AbArbiter, ArbiterVerdict};

#[test]
fn first_arrival_wins() {
    let t0 = Instant::now();
    let mut ab = AbArbiter::new(2, 64, 5);
    assert_eq!(ab.observe(0, 1, t0), ArbiterVerdict::Forward);
    assert_eq!(ab.observe(1, 1, t0), ArbiterVerdict::Duplicate);
}

#[test]
fn duplicate_dropped() {
    let t0 = Instant::now();
    let mut ab = AbArbiter::new(2, 64, 5);
    assert_eq!(ab.observe(0, 1, t0), ArbiterVerdict::Forward);
    assert_eq!(ab.observe(0, 1, t0), ArbiterVerdict::Duplicate);
}

#[test]
fn out_of_window_dropped() {
    let t0 = Instant::now();
    let mut ab = AbArbiter::new(2, 4, 5);
    // seq 100 forces the window far past seq 1, which nobody ever delivered:
    // seq 1 becomes a pending gap candidate.
    ab.observe(0, 100, t0);
    let confirmed = ab.confirmed_gaps(t0 + Duration::from_millis(10));
    assert!(confirmed.contains(&1));

    // seq 1 arrives super late, after its gap was already confirmed/expired.
    let verdict = ab.observe(1, 1, t0 + Duration::from_millis(20));
    assert_eq!(verdict, ArbiterVerdict::OutOfWindow);
}

#[test]
fn stats_track_win_loss() {
    let t0 = Instant::now();
    let mut ab = AbArbiter::new(2, 64, 5);
    ab.observe(0, 1, t0); // stream 0 wins
    ab.observe(1, 1, t0); // stream 1 loses to peer
    let stats = ab.stats();
    assert_eq!(stats.streams[0].packets_won, 1);
    assert_eq!(stats.streams[0].packets_lost_to_peer, 0);
    assert_eq!(stats.streams[1].packets_won, 0);
    assert_eq!(stats.streams[1].packets_lost_to_peer, 1);
}

#[test]
fn gap_withheld_until_all_miss() {
    let t0 = Instant::now();
    let mut ab = AbArbiter::new(2, 4, 5);
    ab.observe(0, 100, t0); // slides window, seq 1 never delivered by any stream
    // no grace period has elapsed yet: gap must not be reported.
    let confirmed = ab.confirmed_gaps(t0);
    assert!(confirmed.is_empty());
}

#[test]
fn gap_confirmed_after_window() {
    let t0 = Instant::now();
    let mut ab = AbArbiter::new(2, 4, 5);
    ab.observe(0, 100, t0);

    let too_soon = ab.confirmed_gaps(t0 + Duration::from_millis(4));
    assert!(too_soon.is_empty());

    let confirmed = ab.confirmed_gaps(t0 + Duration::from_millis(6));
    assert!(confirmed.contains(&1));
}
