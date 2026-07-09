//! Assembles wire codec, reassembler, gap tracking, and optional A/B arbiter
//! into one `DatagramSource`-generic receiver. Base construction only needs
//! `TransportBind` + `PoolAccess`; multicast join needs the extra
//! `UdpTransport` bound (`new_with_multicast`), since not every backend
//! supports it.
//!
//! Recv drives `DatagramSource::recv_burst` for owned frames. A datagram
//! whose leading sequence is already `expected_next` drains inline, borrowed
//! straight from the still-owned frame (no allocation); a datagram that lands
//! ahead of `expected_next` promotes its frame to one `Arc` and buffers
//! [`MessageView`]s in the reassembler until the gap fills.

use std::{
    cell::OnceCell, collections::VecDeque, future::Future, sync::Arc, task::Poll, time::Instant,
};

use smallvec::SmallVec;
use transport_core::{
    AffinityConfig, AsPayload, AsyncReady, BatchConfig, BindConfig, BufferPool, DatagramSource,
    FrameBatch, MulticastInterface, PoolAccess, RecvBufConfig, RingConfig, SendBufConfig,
    TransportBind, TransportCore, TransportError, UdpTransport,
};

use crate::{
    ab::{AbArbiter, ArbiterStats, ArbiterVerdict},
    config::MoldUdpReceiverConfig,
    error::MoldUdpError,
    event::MoldUdpEvent,
    frame::{Frame, MessageView, OwnedFrame},
    gap::{GapRequest, GapRequestEmitter, GapRequestHandler},
    reassembly::SequenceReassembler,
    wire::{self, PacketKind},
};

/// Ring capacity for both the sequence reassembler and the A/B arbiter window.
const RING_CAPACITY: usize = 4096;

/// Burst depth per `recv_burst` call. Also the headroom added on top of
/// `RING_CAPACITY` when sizing the backend's recv pool: worst case, a full
/// reorder window is pinned by buffered messages while a fresh burst lands.
const MAX_INFLIGHT_BURST: usize = 64;

/// One drained item waiting to be handed to a caller via `recv`/`recv_owned`.
/// `Inline` borrows from whichever of `current_datagram`/`current_arc` is
/// currently backing it (zero-alloc, in-order fast path); `View` carries its
/// own `Arc` (gap-buffered or cascade-drained from a slot the reassembler had
/// already been holding across earlier calls).
enum ReadyItem<F> {
    Inline {
        sequence: u64,
        stream_id: u8,
        offset: usize,
        len: usize,
    },
    View {
        view: MessageView<F>,
        sequence: u64,
        stream_id: u8,
    },
    Event(MoldUdpEvent),
    Gap,
}

/// What [`MoldUdpReceiver::recv`] hands back on a data or control packet.
/// [`MoldUdpReceiver::recv_owned`] uses the same type via the `Owned` variant
/// so a caller can move a message to another thread.
pub enum MoldUdpOutcome<'a, F> {
    Frame(Frame<'a>),
    Owned(OwnedFrame<F>),
    Event(MoldUdpEvent),
}

// Manual impl: deriving would add an `F: Debug` bound backend frame types
// (e.g. `UdpFrame`) don't carry.
impl<'a, F> std::fmt::Debug for MoldUdpOutcome<'a, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MoldUdpOutcome::Frame(frame) => f.debug_tuple("Frame").field(frame).finish(),
            MoldUdpOutcome::Owned(owned) => f
                .debug_struct("Owned")
                .field("sequence", &owned.sequence)
                .field("stream_id", &owned.stream_id)
                .finish(),
            MoldUdpOutcome::Event(ev) => f.debug_tuple("Event").field(ev).finish(),
        }
    }
}

/// Snapshot of receiver-level health: arbiter stats (multi-stream only) and
/// whatever gaps are still outstanding.
#[derive(Debug, Clone, Default)]
pub struct ReceiverStats {
    pub arbiter: Option<ArbiterStats>,
    pub pending_gaps: Vec<GapRequest>,
}

/// Backend-generic MoldUDP64 receiver over any `transport_core::DatagramSource`.
/// ```
/// # use client_moldudp::{MoldUdpReceiver, MoldUdpReceiverConfig, StreamConfig};
/// # use transport_tokio::TokioTransport;
/// # let stream = StreamConfig { bind_addr: "127.0.0.1:0".parse().unwrap(), ..Default::default() };
/// # let cfg = MoldUdpReceiverConfig { streams: vec![stream], ..Default::default() };
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// MoldUdpReceiver::<TokioTransport>::new(cfg).await.unwrap();
/// # });
/// ```
pub struct MoldUdpReceiver<T: DatagramSource> {
    transports: SmallVec<[T; 2]>,
    session: OnceCell<[u8; 10]>,
    reassembler: SequenceReassembler<MessageView<T::Frame>>,
    gap_handler: GapRequestHandler,
    gap_emitter: Option<GapRequestEmitter>,
    arbiter: Option<AbArbiter>,
    cfg: MoldUdpReceiverConfig,
    ready: VecDeque<ReadyItem<T::Frame>>,
    current: Option<ReadyItem<T::Frame>>,
    /// Datagrams reaped by `recv_burst` but not yet decoded into `ready`.
    pending_datagrams: VecDeque<(u8, T::Frame)>,
    /// The one still-owned (not yet `Arc`'d) datagram backing outstanding
    /// `Inline` items, if any.
    current_datagram: Option<T::Frame>,
    /// Set once a message inside `current_datagram` needed buffering; from
    /// then on outstanding `Inline` items resolve through this instead.
    current_arc: Option<Arc<T::Frame>>,
    /// Count of outstanding `Inline` items still referencing
    /// `current_datagram`/`current_arc`; both get dropped (pool slab
    /// reclaimed) once this reaches zero.
    current_pending: usize,
    /// Whether `expected_next` has been anchored yet. Configured
    /// `start_sequence` anchors at construction; otherwise the first packet's
    /// sequence anchors it (cold start), so a mid-session join does not treat
    /// the backlog below the first seen sequence as one giant gap.
    seq_anchored: bool,
    /// Preallocated, reused across `recv_burst` calls; no per-call heap
    /// allocation on the steady-state recv path.
    recv_batch: FrameBatch<T::Frame>,
}

impl<T: DatagramSource + TransportBind + PoolAccess> MoldUdpReceiver<T> {
    async fn bind_streams(cfg: MoldUdpReceiverConfig) -> Result<Self, MoldUdpError> {
        let mut transports: SmallVec<[T; 2]> = SmallVec::new();
        let required_slabs = RING_CAPACITY + MAX_INFLIGHT_BURST;
        // `RingConfig` is `#[non_exhaustive]`: mutate the default in place
        // rather than a struct-update literal.
        let mut ring = RingConfig::default();
        ring.slab_count = required_slabs;
        for stream in &cfg.streams {
            let bind = BindConfig::new(stream.bind_addr);
            let t = T::bind_udp(
                bind,
                RecvBufConfig::default(),
                SendBufConfig::default(),
                ring.clone(),
                BatchConfig::default(),
                AffinityConfig::default(),
            )
            .await?;
            // The reorder window pins at most one datagram slab per buffered
            // message; a fresh burst must still find a free slab on top of
            // that. Undersized pools fail fast here, not as a live stall.
            let cap = t.pool().capacity();
            if cap < required_slabs {
                return Err(MoldUdpError::Transport(
                    TransportError::BackendUnavailable {
                        name: t.name(),
                        reason: format!(
                            "recv pool capacity {cap} below required {required_slabs} \
                         (reorder window {RING_CAPACITY} + burst headroom {MAX_INFLIGHT_BURST})"
                        ),
                    },
                ));
            }
            transports.push(t);
        }
        Ok(Self::assemble(cfg, transports))
    }

    fn assemble(cfg: MoldUdpReceiverConfig, transports: SmallVec<[T; 2]>) -> Self {
        let mut arbiter = if transports.len() > 1 {
            let window_ms = cfg.gap_confirm_window.as_millis() as u64;
            Some(AbArbiter::new(transports.len(), RING_CAPACITY, window_ms))
        } else {
            None
        };
        let mut reassembler = SequenceReassembler::new(RING_CAPACITY);
        // Configured start anchors deterministically now; otherwise the first
        // packet seen anchors it in `process_next_pending`.
        let seq_anchored = if let Some(start) = cfg.start_sequence {
            reassembler.reset_expected(start);
            if let Some(arb) = arbiter.as_mut() {
                arb.rebase(start);
            }
            true
        } else {
            false
        };
        let gap_emitter = cfg
            .rerequest_server_addr
            .map(|addr| GapRequestEmitter::new(addr, cfg.max_rerequests_per_gap_per_sec));
        Self {
            transports,
            session: OnceCell::new(),
            reassembler,
            gap_handler: GapRequestHandler::new(),
            gap_emitter,
            arbiter,
            cfg,
            ready: VecDeque::with_capacity(MAX_INFLIGHT_BURST),
            current: None,
            pending_datagrams: VecDeque::with_capacity(MAX_INFLIGHT_BURST),
            current_datagram: None,
            current_arc: None,
            current_pending: 0,
            seq_anchored,
            recv_batch: FrameBatch::with_capacity(MAX_INFLIGHT_BURST),
        }
    }
}

impl<T: DatagramSource + TransportBind + PoolAccess + UdpTransport> MoldUdpReceiver<T> {
    /// Bind one `T` per configured stream leg; when `cfg.multicast_addr` is
    /// set, join that group on every leg. The `T: UdpTransport` bound gates
    /// the join, so every backend used here must support multicast.
    pub async fn new(cfg: MoldUdpReceiverConfig) -> Result<Self, MoldUdpError> {
        let group = cfg.multicast_addr;
        let interfaces: Vec<MulticastInterface> = cfg
            .streams
            .iter()
            .map(|s| MulticastInterface {
                v4: s.interface_v4,
                v6_scope_id: s.interface_v6_scope_id,
            })
            .collect();
        let mut receiver = Self::bind_streams(cfg).await?;
        if let Some(group) = group {
            for (t, iface) in receiver.transports.iter_mut().zip(interfaces) {
                t.join_multicast(group, iface).await?;
            }
        }
        Ok(receiver)
    }
}

impl<T: DatagramSource> MoldUdpReceiver<T> {
    /// Raw access to the bound transports, for callers that need
    /// backend-specific introspection (e.g. the local bound address in tests).
    pub fn transports(&self) -> &[T] {
        &self.transports
    }

    pub fn stats(&self) -> ReceiverStats {
        ReceiverStats {
            arbiter: self.arbiter.as_ref().map(|a| a.stats()),
            pending_gaps: self.gap_handler.pending_gaps(),
        }
    }

    /// Send Request Packets for currently-pending gaps over `transport`
    /// (typically a small unicast socket pointed at `rerequest_server_addr`,
    /// separate from the multicast receive legs). No-op until session id is
    /// known, re-request is enabled, and a rerequest server is configured.
    pub async fn emit_pending_gaps<Tx: TransportCore>(
        &mut self,
        transport: &mut Tx,
    ) -> Result<usize, MoldUdpError> {
        if !self.cfg.rerequest_enabled {
            return Ok(0);
        }
        let Some(emitter) = self.gap_emitter.as_mut() else {
            return Ok(0);
        };
        let Some(&session) = self.session.get() else {
            return Ok(0);
        };
        let gaps = self.gap_handler.pending_gaps();
        if gaps.is_empty() {
            return Ok(0);
        }
        emitter.emit(&gaps, session, transport).await
    }

    /// Resolve an `Inline` item's bytes against whichever of
    /// `current_datagram`/`current_arc` currently backs it.
    fn resolve_current(&self, offset: usize, len: usize) -> &[u8] {
        if let Some(arc) = &self.current_arc {
            return &arc.payload()[offset..offset + len];
        }
        let frame = self
            .current_datagram
            .as_ref()
            .expect("Inline ready item pending without a backing datagram");
        &frame.payload()[offset..offset + len]
    }

    /// Release whatever backs an outgoing `Inline` item once no more
    /// outstanding items reference it.
    fn retire_current(&mut self, item: &ReadyItem<T::Frame>) {
        if matches!(item, ReadyItem::Inline { .. }) {
            self.current_pending -= 1;
            if self.current_pending == 0 {
                self.current_datagram = None;
                self.current_arc = None;
            }
        }
    }

    /// Promote `current_datagram` to a shared `Arc`, or clone the existing one
    /// if this datagram already needed buffering for an earlier message. At
    /// most one `Arc::new` per datagram.
    fn arc_current(&mut self) -> Arc<T::Frame> {
        if let Some(arc) = &self.current_arc {
            return Arc::clone(arc);
        }
        let frame = self
            .current_datagram
            .take()
            .expect("current datagram must be set while its blocks are being processed");
        let arc = Arc::new(frame);
        self.current_arc = Some(Arc::clone(&arc));
        arc
    }
}

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
            } => Ok(MoldUdpOutcome::Frame(Frame {
                payload: view.as_ref(),
                sequence: *sequence,
                stream_id: *stream_id,
            })),
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
            } => Ok(MoldUdpOutcome::Owned(OwnedFrame {
                view,
                sequence,
                stream_id,
            })),
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
            self.ready.push_back(ReadyItem::Gap);
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
