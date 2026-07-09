//! A/B (or A/B/C...) stream arbiter: dedupes redundant feeds via a sliding
//! bitmap window (first arrival wins, O(1) lookup) and withholds gap
//! confirmation until a sequence has gone unseen across every stream for
//! `gap_confirm_window_ms`, so one stream's transient lag doesn't fire a
//! spurious re-request.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use bitvec::vec::BitVec;

/// Outcome of observing one (stream, seq) arrival.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArbiterVerdict {
    /// First arrival for this seq; forward to the reassembler.
    Forward,
    /// Seq already merged from an earlier arrival; drop.
    Duplicate,
    /// Seq is outside the tracked window and not a recoverable pending gap; drop.
    OutOfWindow,
}

/// Per-stream liveness and race-outcome counters.
#[derive(Debug, Clone, Default)]
pub struct StreamStats {
    pub packets_won: u64,
    pub packets_lost_to_peer: u64,
    pub last_recv_at: Option<Instant>,
    pub max_seq_seen: u64,
    pub sequences_behind_leader: u64,
}

/// Snapshot returned by [`AbArbiter::stats`].
#[derive(Debug, Clone, Default)]
pub struct ArbiterStats {
    pub streams: Vec<StreamStats>,
}

/// Sliding bitmap window over `[window_base, window_base + capacity)`. A set
/// bit means some stream already delivered that seq. Circular indexing
/// (`seq % capacity`) keeps both lookup and slide O(1) per position advanced.
pub struct AbArbiter {
    window_base: u64,
    window_bits: BitVec,
    stream_stats: Vec<StreamStats>,
    gap_confirm_window_ms: u64,
    pending_gap_confirms: HashMap<u64, Instant>,
}

impl AbArbiter {
    pub fn new(stream_count: usize, window_capacity: usize, gap_confirm_window_ms: u64) -> Self {
        assert!(window_capacity > 0, "window_capacity must be non-zero");
        Self {
            window_base: 0,
            window_bits: BitVec::repeat(false, window_capacity),
            stream_stats: vec![StreamStats::default(); stream_count],
            gap_confirm_window_ms,
            pending_gap_confirms: HashMap::new(),
        }
    }

    fn idx(&self, seq: u64) -> usize {
        (seq % self.window_bits.len() as u64) as usize
    }

    /// Observe `seq` arriving on `stream_id`. Slides the window forward if
    /// `seq` is beyond it, marking any never-delivered evicted position as a
    /// pending gap candidate (seen by construction: it's still unset when
    /// forced out, so no stream has delivered it yet).
    pub fn observe(&mut self, stream_id: u8, seq: u64, now: Instant) -> ArbiterVerdict {
        let capacity = self.window_bits.len() as u64;

        if seq < self.window_base {
            if self.pending_gap_confirms.remove(&seq).is_some() {
                self.record_delivery(stream_id, seq, now, false);
                return ArbiterVerdict::Forward;
            }
            return ArbiterVerdict::OutOfWindow;
        }

        if seq >= self.window_base + capacity {
            let target_base = seq - capacity + 1;
            while self.window_base < target_base {
                let idx = self.idx(self.window_base);
                if !self.window_bits[idx] {
                    self.pending_gap_confirms
                        .entry(self.window_base)
                        .or_insert(now);
                }
                self.window_bits.set(idx, false);
                self.window_base += 1;
            }
        }

        let idx = self.idx(seq);
        if self.window_bits[idx] {
            self.record_delivery(stream_id, seq, now, true);
            return ArbiterVerdict::Duplicate;
        }
        self.window_bits.set(idx, true);
        self.pending_gap_confirms.remove(&seq);
        self.record_delivery(stream_id, seq, now, false);
        ArbiterVerdict::Forward
    }

    fn record_delivery(&mut self, stream_id: u8, seq: u64, now: Instant, lost_race: bool) {
        let stats = &mut self.stream_stats[stream_id as usize];
        if lost_race {
            stats.packets_lost_to_peer += 1;
        } else {
            stats.packets_won += 1;
        }
        stats.last_recv_at = Some(now);
        stats.max_seq_seen = stats.max_seq_seen.max(seq);
        self.recompute_leader_lag();
    }

    fn recompute_leader_lag(&mut self) {
        let leader = self
            .stream_stats
            .iter()
            .map(|s| s.max_seq_seen)
            .max()
            .unwrap_or(0);
        for s in &mut self.stream_stats {
            s.sequences_behind_leader = leader.saturating_sub(s.max_seq_seen);
        }
    }

    /// Drain and return pending gaps whose grace period has elapsed as of `now`.
    pub fn confirmed_gaps(&mut self, now: Instant) -> Vec<u64> {
        let window = Duration::from_millis(self.gap_confirm_window_ms);
        let mut confirmed: Vec<u64> = self
            .pending_gap_confirms
            .iter()
            .filter(|&(_, &first_seen)| now.duration_since(first_seen) >= window)
            .map(|(&seq, _)| seq)
            .collect();
        confirmed.sort_unstable();
        for seq in &confirmed {
            self.pending_gap_confirms.remove(seq);
        }
        confirmed
    }

    pub fn stats(&self) -> ArbiterStats {
        ArbiterStats {
            streams: self.stream_stats.clone(),
        }
    }
}
