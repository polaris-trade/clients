//! Hot receive path: burst reap, sequence reassembly, gap detection, drain into
//! `recv`/`recv_owned`.

use std::{future::Future, task::Poll, time::Instant};

use smallvec::SmallVec;
use transport_core::{AsPayload, AsyncReady, DatagramSource};

use super::{
    MAX_INFLIGHT_BURST, MoldUdpOutcome, MoldUdpReceiver, ReadyItem, record_gap, record_message,
};
use crate::{
    ab::ArbiterVerdict,
    error::MoldUdpError,
    event::MoldUdpEvent,
    frame::{Frame, MessageView, OwnedFrame},
    wire::{self, PacketKind},
};

impl<T: DatagramSource + AsyncReady> MoldUdpReceiver<T> {
    /// Next data frame or control event, borrowed from the receiver. On a
    /// gap, returns `Err(MoldUdpError::GapDetected)` for that call only
    /// (already recorded in the gap handler / arbiter) so the caller can log
    /// and keep calling `recv` without losing reassembly progress.
    pub async fn recv(&mut self) -> Result<MoldUdpOutcome<'_, T::Frame>, MoldUdpError> {
        // Retire the previous call's item up front: by the time `recv` can
        // run again, the borrow checker guarantees the caller's prior
        // `Frame<'_>` is dead, so it is safe to release its backing storage
        // now. Deferring this past `poll_once` would let `process_next_pending`
        // clobber `current_datagram` for a new datagram while the old one's
        // `current_pending` count is still outstanding.
        if let Some(prev) = self.current.take() {
            self.retire_current(&prev);
        }
        loop {
            if let Some(item) = self.ready.pop_front() {
                self.current = Some(item);
                break;
            }
            self.poll_once()?;
            if self.ready.is_empty() {
                self.wait_ready().await?;
            }
        }
        match self.current.as_ref().expect("just populated above") {
            ReadyItem::Inline {
                sequence,
                stream_id,
                offset,
                len,
            } => {
                let payload = self.resolve_current(*offset, *len);
                record_message();
                Ok(MoldUdpOutcome::Frame(Frame {
                    payload,
                    sequence: *sequence,
                    stream_id: *stream_id,
                }))
            }
            ReadyItem::View {
                view,
                sequence,
                stream_id,
            } => {
                record_message();
                Ok(MoldUdpOutcome::Frame(Frame {
                    payload: view.as_ref(),
                    sequence: *sequence,
                    stream_id: *stream_id,
                }))
            }
            ReadyItem::Event(ev) => Ok(MoldUdpOutcome::Event(*ev)),
            ReadyItem::Gap => Err(MoldUdpError::GapDetected),
        }
    }

    /// Owned counterpart to [`MoldUdpReceiver::recv`] for cross-thread
    /// handoff (a sharded engine core): materializes an `Arc`-backed
    /// [`OwnedFrame`] instead of a borrow.
    pub async fn recv_owned(&mut self) -> Result<MoldUdpOutcome<'static, T::Frame>, MoldUdpError> {
        // See `recv`'s comment: retire any borrowed item left over from a
        // prior `recv` call before touching `current_datagram`/`current_arc`.
        if let Some(prev) = self.current.take() {
            self.retire_current(&prev);
        }
        loop {
            if let Some(item) = self.ready.pop_front() {
                return self.materialize_owned(item);
            }
            self.poll_once()?;
            if self.ready.is_empty() {
                self.wait_ready().await?;
            }
        }
    }

    fn materialize_owned(
        &mut self,
        item: ReadyItem<T::Frame>,
    ) -> Result<MoldUdpOutcome<'static, T::Frame>, MoldUdpError> {
        match item {
            ReadyItem::Inline {
                sequence,
                stream_id,
                offset,
                len,
            } => {
                let arc = self.arc_current();
                self.current_pending -= 1;
                if self.current_pending == 0 {
                    self.current_datagram = None;
                    self.current_arc = None;
                }
                let view = MessageView::new(arc, offset, len);
                record_message();
                Ok(MoldUdpOutcome::Owned(OwnedFrame {
                    view,
                    sequence,
                    stream_id,
                }))
            }
            ReadyItem::View {
                view,
                sequence,
                stream_id,
            } => {
                record_message();
                Ok(MoldUdpOutcome::Owned(OwnedFrame {
                    view,
                    sequence,
                    stream_id,
                }))
            }
            ReadyItem::Event(ev) => Ok(MoldUdpOutcome::Event(ev)),
            ReadyItem::Gap => Err(MoldUdpError::GapDetected),
        }
    }

    /// Reap and decode until `ready` has something or every leg is drained.
    /// Pure sync spin, no wait: an empty `ready` on return means nothing was
    /// reapable anywhere, and the caller should wait on readiness.
    fn poll_once(&mut self) -> Result<(), MoldUdpError> {
        loop {
            if !self.ready.is_empty() {
                return Ok(());
            }
            if self.pending_datagrams.is_empty() {
                let mut reaped_any = false;
                for idx in 0..self.transports.len() {
                    reaped_any |= self.reap_burst(idx)?;
                }
                if !reaped_any {
                    return Ok(());
                }
            }
            self.process_next_pending()?;
            self.drain_confirmed_gaps();
        }
    }

    /// Record a tail gap discovered from a heartbeat / end-of-session
    /// next-expected sequence: anything between our `expected_next` and the
    /// server's `next_expected` was lost during quiet traffic and never seen.
    fn note_tail_gap(&mut self, next_expected: u64) {
        let expected = self.reassembler.expected_next();
        if next_expected > expected {
            self.gap_handler
                .record_missing_range(expected, next_expected);
            tracing::warn!(
                expected,
                next_expected,
                "sequence gap detected; queueing re-request"
            );
            self.ready.push_back(ReadyItem::Gap);
            record_gap();
        }
    }

    /// Promote arbiter gap candidates whose confirm window has elapsed into the
    /// gap handler. Only the multi-stream path stages candidates; single-stream
    /// records gaps inline in `process_next_pending`, so this is a no-op there.
    fn drain_confirmed_gaps(&mut self) {
        let Some(arbiter) = self.arbiter.as_mut() else {
            return;
        };
        let confirmed = arbiter.confirmed_gaps(Instant::now());
        for seq in confirmed {
            self.gap_handler.record_gap(seq);
            self.ready.push_back(ReadyItem::Gap);
            record_gap();
        }
    }

    /// Block until some leg becomes readable, then reap it. Only entered when
    /// a sync spin found nothing, so the per-leg `ready()` futures this boxes
    /// never touch the hot path.
    async fn wait_ready(&mut self) -> Result<(), MoldUdpError> {
        let transports = &mut self.transports;
        let mut waiters: Vec<_> = transports.iter_mut().map(|t| Box::pin(t.ready())).collect();
        std::future::poll_fn(|cx| {
            for w in waiters.iter_mut() {
                if w.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(());
                }
            }
            Poll::Pending
        })
        .await;
        drop(waiters);
        Ok(())
    }

    /// Reap up to `MAX_INFLIGHT_BURST` datagrams from leg `idx` into
    /// `pending_datagrams`, tagged with their stream id. Returns whether
    /// anything landed.
    fn reap_burst(&mut self, idx: usize) -> Result<bool, MoldUdpError> {
        let n = self.transports[idx].recv_burst(&mut self.recv_batch, MAX_INFLIGHT_BURST)?;
        if n == 0 {
            return Ok(false);
        }
        for frame in self.recv_batch.drain() {
            self.pending_datagrams.push_back((idx as u8, frame));
        }
        Ok(true)
    }

    /// Decode the next pending datagram into `ready` items. In-order
    /// messages drain inline (no `Arc`); a message landing ahead of
    /// `expected_next` promotes the datagram to a shared `Arc` (at most once
    /// per datagram) and buffers a `MessageView` in the reassembler.
    fn process_next_pending(&mut self) -> Result<(), MoldUdpError> {
        let Some((stream_id, frame)) = self.pending_datagrams.pop_front() else {
            return Ok(());
        };
        let header = wire::parse_header(frame.payload())?;

        match self.session.get() {
            None => {
                let _ = self.session.set(header.session);
            }
            Some(&expected) if expected != header.session => {
                return Err(MoldUdpError::SessionMismatch {
                    expected,
                    got: header.session,
                });
            }
            Some(_) => {}
        }

        // Cold start / mid-session join: anchor the expected sequence to the
        // first packet seen rather than assuming seq 1, so joining a live feed
        // does not treat the whole backlog as one giant gap. Heartbeat and
        // end-of-session both carry the server's next-expected, so any first
        // packet anchors correctly. Configured `start_sequence` anchors at
        // construction instead.
        if !self.seq_anchored {
            self.reassembler.reset_expected(header.sequence);
            if let Some(arbiter) = self.arbiter.as_mut() {
                arbiter.rebase(header.sequence);
            }
            self.seq_anchored = true;
        }

        match header.kind() {
            PacketKind::Heartbeat => {
                let next_expected = header.sequence;
                self.note_tail_gap(next_expected);
                self.ready
                    .push_back(ReadyItem::Event(MoldUdpEvent::Heartbeat { next_expected }));
                return Ok(());
            }
            PacketKind::EndOfSession => {
                let next_expected = header.sequence;
                self.note_tail_gap(next_expected);
                self.ready
                    .push_back(ReadyItem::Event(MoldUdpEvent::EndOfSession {
                        next_expected,
                    }));
                return Ok(());
            }
            PacketKind::Data => {}
        }

        // Collect (seq, offset, len) before moving `frame`: the iterator
        // borrows `frame.payload()`, which must finish before ownership moves.
        let mut blocks: SmallVec<[(u64, usize, usize); 8]> = SmallVec::new();
        for (seq, block) in (header.sequence..).zip(header.blocks(frame.payload())) {
            let (offset, bytes) = block?;
            blocks.push((seq, offset, bytes.len()));
        }

        // A packet's blocks are numbered contiguously, so the only gap is the
        // jump from `expected_next` to this packet's leading sequence. Record
        // it once, not once per block (which would re-widen the range over
        // already-received blocks and re-request them). Single stream records
        // inline; A/B stages the range in the arbiter so a lagging leg gets
        // its confirm window before a re-request fires.
        let expected0 = self.reassembler.expected_next();
        if header.sequence > expected0 {
            match self.arbiter.as_mut() {
                Some(arbiter) => {
                    arbiter.note_missing_range(expected0, header.sequence, Instant::now());
                }
                None => {
                    self.gap_handler
                        .record_missing_range(expected0, header.sequence);
                    self.ready.push_back(ReadyItem::Gap);
                    record_gap();
                }
            }
        }

        self.current_datagram = Some(frame);
        self.current_arc = None;
        self.current_pending = 0;

        for (seq, offset, len) in blocks {
            if let Some(arbiter) = self.arbiter.as_mut() {
                match arbiter.observe(stream_id, seq, Instant::now()) {
                    ArbiterVerdict::Duplicate | ArbiterVerdict::OutOfWindow => continue,
                    ArbiterVerdict::Forward => {}
                }
            }

            self.gap_handler.mark_received(seq);

            match seq.cmp(&self.reassembler.expected_next()) {
                std::cmp::Ordering::Equal => {
                    self.reassembler.advance_expected(1);
                    self.ready.push_back(ReadyItem::Inline {
                        sequence: seq,
                        stream_id,
                        offset,
                        len,
                    });
                    self.current_pending += 1;
                    for (next_seq, view) in (seq + 1..).zip(self.reassembler.drain_ready()) {
                        self.ready.push_back(ReadyItem::View {
                            view,
                            sequence: next_seq,
                            stream_id,
                        });
                    }
                }
                std::cmp::Ordering::Greater => {
                    let arc = self.arc_current();
                    let view = MessageView::new(arc, offset, len);
                    if let Some(cursor) = self.reassembler.insert(seq, view)? {
                        for (next_seq, view) in (seq..).zip(cursor) {
                            self.ready.push_back(ReadyItem::View {
                                view,
                                sequence: next_seq,
                                stream_id,
                            });
                        }
                    }
                }
                std::cmp::Ordering::Less => {} // stale duplicate, drop
            }
        }

        if self.current_pending == 0 {
            self.current_datagram = None;
            self.current_arc = None;
        }
        Ok(())
    }
}
