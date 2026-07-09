//! Gap tracking and rate-limited unicast re-request emission.
//!
//! `GapRequestHandler` records missing sequence ranges as they're detected and
//! clears them as messages arrive. `GapRequestEmitter` turns pending gaps into
//! MoldUDP64 Request Packets, capped per-gap so a stuck gap can't flood the
//! re-request server.

use std::{
    collections::{BTreeMap, HashMap},
    time::{Duration, Instant},
};

use transport_core::TransportCore;

use crate::error::MoldUdpError;

/// One contiguous run of missing sequence numbers, ready to re-request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GapRequest {
    pub start_seq: u64,
    pub count: u16,
}

/// Tracks missing sequence ranges as `start -> end (exclusive)`. Coalesces
/// adjacent/overlapping ranges on record; splits a range on partial fill.
#[derive(Debug, Default)]
pub struct GapRequestHandler {
    gaps: BTreeMap<u64, u64>,
}

impl GapRequestHandler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record `[start, end)` as missing. `start >= end` is a no-op.
    pub fn record_missing_range(&mut self, start: u64, end_exclusive: u64) {
        if start >= end_exclusive {
            return;
        }
        let merged_end = self
            .gaps
            .get(&start)
            .copied()
            .unwrap_or(start)
            .max(end_exclusive);
        self.gaps.insert(start, merged_end);
    }

    /// Record a single missing sequence number.
    pub fn record_gap(&mut self, seq: u64) {
        self.record_missing_range(seq, seq + 1);
    }

    /// Clear `seq` from whichever pending range currently covers it, splitting
    /// that range if `seq` falls in its interior.
    pub fn mark_received(&mut self, seq: u64) {
        let Some((&start, &end)) = self.gaps.range(..=seq).next_back() else {
            return;
        };
        if seq >= end {
            return;
        }
        self.gaps.remove(&start);
        if start < seq {
            self.gaps.insert(start, seq);
        }
        if seq + 1 < end {
            self.gaps.insert(seq + 1, end);
        }
    }

    /// Expand tracked ranges into re-request-sized chunks (`count` fits `u16`).
    pub fn pending_gaps(&self) -> Vec<GapRequest> {
        let mut out = Vec::new();
        for (&start, &end) in &self.gaps {
            let mut cur = start;
            while cur < end {
                let len = (end - cur).min(u64::from(u16::MAX));
                out.push(GapRequest {
                    start_seq: cur,
                    count: len as u16,
                });
                cur += len;
            }
        }
        out
    }
}

/// Sends MoldUDP64 Request Packets for pending gaps, rate-limited per gap
/// start sequence so a gap that never fills can't flood the re-request server.
pub struct GapRequestEmitter {
    pub server_addr: std::net::SocketAddr,
    last_sent: HashMap<u64, Instant>,
    max_per_gap_per_sec: u32,
}

impl GapRequestEmitter {
    pub fn new(server_addr: std::net::SocketAddr, max_per_gap_per_sec: u32) -> Self {
        Self {
            server_addr,
            last_sent: HashMap::new(),
            max_per_gap_per_sec: max_per_gap_per_sec.max(1),
        }
    }

    /// Send a Request Packet for each gap not currently rate-limited. Returns
    /// how many were actually sent.
    pub async fn emit<T: TransportCore>(
        &mut self,
        gaps: &[GapRequest],
        session: [u8; 10],
        transport: &mut T,
    ) -> Result<usize, MoldUdpError> {
        let interval = Duration::from_secs(1) / self.max_per_gap_per_sec;
        let now = Instant::now();
        let mut sent = 0usize;
        for gap in gaps {
            let allowed = match self.last_sent.get(&gap.start_seq) {
                Some(&last) => now.duration_since(last) >= interval,
                None => true,
            };
            if !allowed {
                continue;
            }
            let packet = encode_request_packet(session, gap.start_seq, gap.count);
            transport.send(&packet).await?;
            self.last_sent.insert(gap.start_seq, now);
            sent += 1;
        }
        Ok(sent)
    }
}

/// Request Packet: `Session[10]`, `Sequence[8 BE]`, `RequestedMessageCount[2 BE]`.
fn encode_request_packet(session: [u8; 10], start_seq: u64, count: u16) -> [u8; 20] {
    let mut packet = [0u8; 20];
    packet[0..10].copy_from_slice(&session);
    packet[10..18].copy_from_slice(&start_seq.to_be_bytes());
    packet[18..20].copy_from_slice(&count.to_be_bytes());
    packet
}
