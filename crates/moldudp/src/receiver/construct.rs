//! Stream binding, receiver assembly, and multicast join.

use std::{cell::OnceCell, collections::VecDeque};

use smallvec::SmallVec;
use transport_core::{
    AffinityConfig, BatchConfig, BindConfig, BufferPool, DatagramSource, FrameBatch,
    MulticastInterface, PoolAccess, RecvBufConfig, RingConfig, SendBufConfig, TransportBind,
    TransportError, UdpTransport,
};

use super::{MAX_INFLIGHT_BURST, MoldUdpReceiver, RING_CAPACITY};
use crate::{
    ab::AbArbiter,
    config::MoldUdpReceiverConfig,
    error::MoldUdpError,
    gap::{GapRequestEmitter, GapRequestHandler},
    reassembly::SequenceReassembler,
};

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
