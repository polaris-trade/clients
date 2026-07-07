//! Assembles wire codec, reassembler, gap tracking, and optional A/B arbiter
//! into one `Transport`-generic receiver. Base construction only needs
//! `TransportBind`; multicast join needs the extra `UdpTransport` bound
//! (`new_with_multicast`), since not every backend supports it.

use std::cell::OnceCell;
use std::collections::VecDeque;
use std::task::Poll;
use std::time::Instant;

use smallvec::SmallVec;
use transport_core::{
    AsPayload, BatchConfig, BindConfig, MulticastInterface, RecvBufConfig, RingConfig,
    SendBufConfig, Transport, TransportBind, UdpTransport,
};

use crate::ab::{AbArbiter, ArbiterStats, ArbiterVerdict};
use crate::config::MoldUdpReceiverConfig;
use crate::error::MoldUdpError;
use crate::event::MoldUdpEvent;
use crate::frame::Frame;
use crate::gap::{GapRequest, GapRequestEmitter, GapRequestHandler};
use crate::reassembly::SequenceReassembler;
use crate::wire::{self, PacketKind};

/// Ring capacity for both the sequence reassembler and the A/B arbiter window.
const RING_CAPACITY: usize = 4096;

enum ReadyItem {
    Frame {
        sequence: u64,
        stream_id: u8,
        payload: Vec<u8>,
    },
    Event(MoldUdpEvent),
    Gap,
}

/// What [`MoldUdpReceiver::recv`] hands back on a data or control packet.
#[derive(Debug, Clone, Copy)]
pub enum MoldUdpOutcome<'a> {
    Frame(Frame<'a>),
    Event(MoldUdpEvent),
}

/// Snapshot of receiver-level health: arbiter stats (multi-stream only) and
/// whatever gaps are still outstanding.
#[derive(Debug, Clone, Default)]
pub struct ReceiverStats {
    pub arbiter: Option<ArbiterStats>,
    pub pending_gaps: Vec<GapRequest>,
}

/// Backend-generic MoldUDP64 receiver over any `transport_core::Transport`.
/// ```
/// # use client_moldudp::{MoldUdpReceiver, MoldUdpReceiverConfig, StreamConfig};
/// # use transport_tokio::TokioTransport;
/// # let stream = StreamConfig { bind_addr: "127.0.0.1:0".parse().unwrap(), ..Default::default() };
/// # let cfg = MoldUdpReceiverConfig { streams: vec![stream], ..Default::default() };
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// MoldUdpReceiver::<TokioTransport>::new(cfg).await.unwrap();
/// # });
/// ```
pub struct MoldUdpReceiver<T: Transport> {
    transports: SmallVec<[T; 2]>,
    session: OnceCell<[u8; 10]>,
    reassembler: SequenceReassembler<Vec<u8>>,
    gap_handler: GapRequestHandler,
    gap_emitter: Option<GapRequestEmitter>,
    arbiter: Option<AbArbiter>,
    cfg: MoldUdpReceiverConfig,
    ready: VecDeque<ReadyItem>,
    current: Option<ReadyItem>,
}

impl<T: TransportBind> MoldUdpReceiver<T> {
    async fn bind_streams(cfg: MoldUdpReceiverConfig) -> Result<Self, MoldUdpError> {
        let mut transports: SmallVec<[T; 2]> = SmallVec::new();
        for stream in &cfg.streams {
            let bind = BindConfig::new(stream.bind_addr);
            let t = T::bind_udp(
                bind,
                RecvBufConfig::default(),
                SendBufConfig::default(),
                RingConfig::default(),
                BatchConfig::default(),
            )
            .await?;
            transports.push(t);
        }
        Ok(Self::assemble(cfg, transports))
    }

    fn assemble(cfg: MoldUdpReceiverConfig, transports: SmallVec<[T; 2]>) -> Self {
        let arbiter = if transports.len() > 1 {
            let window_ms = cfg.gap_confirm_window.as_millis() as u64;
            Some(AbArbiter::new(transports.len(), RING_CAPACITY, window_ms))
        } else {
            None
        };
        let gap_emitter = cfg
            .rerequest_server_addr
            .map(|addr| GapRequestEmitter::new(addr, cfg.max_rerequests_per_gap_per_sec));
        Self {
            transports,
            session: OnceCell::new(),
            reassembler: SequenceReassembler::new(RING_CAPACITY),
            gap_handler: GapRequestHandler::new(),
            gap_emitter,
            arbiter,
            cfg,
            ready: VecDeque::new(),
            current: None,
        }
    }
}

impl<T: TransportBind + UdpTransport> MoldUdpReceiver<T> {
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

impl<T: Transport> MoldUdpReceiver<T> {
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
    pub async fn emit_pending_gaps<Tx: Transport>(
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

    /// Next data frame or control event. Polls every stream leg; on a gap,
    /// returns `Err(MoldUdpError::GapDetected)` for that call only (already
    /// recorded in the gap handler / arbiter) so the caller can log and keep
    /// calling `recv` without losing reassembly progress.
    pub async fn recv(&mut self) -> Result<MoldUdpOutcome<'_>, MoldUdpError> {
        loop {
            if let Some(item) = self.ready.pop_front() {
                self.current = Some(item);
                break;
            }
            self.poll_once().await?;
        }
        match self.current.as_ref().expect("just populated above") {
            ReadyItem::Frame {
                sequence,
                stream_id,
                payload,
            } => Ok(MoldUdpOutcome::Frame(Frame {
                payload,
                sequence: *sequence,
                stream_id: *stream_id,
            })),
            ReadyItem::Event(ev) => Ok(MoldUdpOutcome::Event(*ev)),
            ReadyItem::Gap => Err(MoldUdpError::GapDetected),
        }
    }

    async fn poll_once(&mut self) -> Result<(), MoldUdpError> {
        let transports = &mut self.transports;
        let (idx, result) = std::future::poll_fn(|cx| {
            for (i, t) in transports.iter_mut().enumerate() {
                if let Poll::Ready(res) = t.poll_event(cx) {
                    return Poll::Ready((i, res));
                }
            }
            Poll::Pending
        })
        .await;
        result.map_err(MoldUdpError::Transport)?;
        // Disjoint field borrow: decode straight from the borrowed transport
        // frame while mutating decode state. Avoids copying the whole datagram;
        // per-message payloads are still owned when they land in reassembler
        // slots, since frames outlive their source datagram across recv calls.
        let Self {
            transports,
            session,
            reassembler,
            gap_handler,
            arbiter,
            ready,
            ..
        } = &mut *self;
        let Some(frame) = transports[idx].next_frame() else {
            return Ok(());
        };
        process_datagram(
            session,
            reassembler,
            gap_handler,
            arbiter,
            ready,
            idx as u8,
            frame.payload(),
        )
    }
}

fn process_datagram(
    session: &mut OnceCell<[u8; 10]>,
    reassembler: &mut SequenceReassembler<Vec<u8>>,
    gap_handler: &mut GapRequestHandler,
    arbiter: &mut Option<AbArbiter>,
    ready: &mut VecDeque<ReadyItem>,
    stream_id: u8,
    datagram: &[u8],
) -> Result<(), MoldUdpError> {
    let header = wire::parse_header(datagram)?;

    match session.get() {
        None => {
            let _ = session.set(header.session);
        }
        Some(&expected) if expected != header.session => {
            return Err(MoldUdpError::SessionMismatch {
                expected,
                got: header.session,
            });
        }
        Some(_) => {}
    }

    match header.kind() {
        PacketKind::Heartbeat => {
            ready.push_back(ReadyItem::Event(MoldUdpEvent::Heartbeat));
            return Ok(());
        }
        PacketKind::EndOfSession => {
            ready.push_back(ReadyItem::Event(MoldUdpEvent::EndOfSession {
                next_expected: header.sequence,
            }));
            return Ok(());
        }
        PacketKind::Data => {}
    }

    for (seq, block) in (header.sequence..).zip(header.blocks(datagram)) {
        let payload = block?;
        handle_message(reassembler, gap_handler, arbiter, ready, stream_id, seq, payload)?;
    }
    Ok(())
}

fn handle_message(
    reassembler: &mut SequenceReassembler<Vec<u8>>,
    gap_handler: &mut GapRequestHandler,
    arbiter: &mut Option<AbArbiter>,
    ready: &mut VecDeque<ReadyItem>,
    stream_id: u8,
    seq: u64,
    payload: &[u8],
) -> Result<(), MoldUdpError> {
    if let Some(arbiter) = arbiter {
        match arbiter.observe(stream_id, seq, Instant::now()) {
            ArbiterVerdict::Duplicate | ArbiterVerdict::OutOfWindow => return Ok(()),
            ArbiterVerdict::Forward => {}
        }
    }

    let expected = reassembler.expected_next();
    if seq > expected {
        gap_handler.record_missing_range(expected, seq);
        ready.push_back(ReadyItem::Gap);
    }
    gap_handler.mark_received(seq);

    if let Some(cursor) = reassembler.insert(seq, payload.to_vec())? {
        for (offset, item) in cursor.enumerate() {
            ready.push_back(ReadyItem::Frame {
                sequence: seq + offset as u64,
                stream_id,
                payload: item,
            });
        }
    }
    Ok(())
}
