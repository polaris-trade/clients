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

use std::{cell::OnceCell, collections::VecDeque, sync::Arc};

use smallvec::SmallVec;
use transport_core::{DatagramSource, FrameBatch};

use crate::{
    ab::{AbArbiter, ArbiterStats},
    config::MoldUdpReceiverConfig,
    event::MoldUdpEvent,
    frame::{Frame, MessageView, OwnedFrame},
    gap::{GapRequest, GapRequestEmitter, GapRequestHandler},
    reassembly::SequenceReassembler,
};

/// Ring capacity for both the sequence reassembler and the A/B arbiter window.
const RING_CAPACITY: usize = 4096;

/// Burst depth per `recv_burst` call. Also the headroom added on top of
/// `RING_CAPACITY` when sizing the backend's recv pool: worst case, a full
/// reorder window is pinned by buffered messages while a fresh burst lands.
const MAX_INFLIGHT_BURST: usize = 64;

/// Record one message actually yielded to the caller: gated thread-local
/// `count_msg` plus a 1-in-8192 sampled `merge_local` so a flusher on another
/// thread eventually sees the total without the caller wiring a merge tick.
/// No-op (single `Cell` read) when the metrics gate is off.
#[inline]
fn record_message() {
    if observability_core::metrics_enabled() {
        observability_core::count_msg();
        if observability_core::should_sample(observability_core::SAMPLE_1_IN_8192) {
            observability_core::merge_local();
        }
    }
}

/// Record one client-visible gap: tail loss from a heartbeat/end-of-session,
/// a single-stream sequence jump, or a confirmed multi-stream miss. Called
/// once per `ReadyItem::Gap` pushed, so the counter matches the number of
/// `MoldUdpError::GapDetected` a caller actually observes.
#[inline]
fn record_gap() {
    if observability_core::metrics_enabled() {
        metrics::counter!("client.gaps", "protocol" => "moldudp").increment(1);
    }
}

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

mod base;
mod construct;
mod recv;
