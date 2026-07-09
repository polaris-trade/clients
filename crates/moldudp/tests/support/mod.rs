//! Shared test-only in-process `DatagramSource` double. Lets receiver tests
//! drive `MoldUdpReceiver` without a real socket and, via `PoolSizing`,
//! toggle pool capacity to exercise the undersized-pool config-error path.
//! Not every helper here is used by every test binary that includes this
//! module (each integration-test file compiles its own copy).
#![allow(dead_code)]

use std::{
    collections::VecDeque,
    future::Future,
    marker::PhantomData,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
};

use transport_core::{
    AffinityConfig, AsPayload, AsyncReady, BatchConfig, BindConfig, BufferPool, DatagramSource,
    FrameBatch, MulticastInterface, PoolAccess, RecvBufConfig, RingConfig, SendBufConfig,
    TransportBind, TransportCore, TransportError, UdpTransport,
};

/// Controls how a [`MockTransport`]'s pool capacity relates to the
/// `RingConfig` it was bound with. `Honest` mirrors a real backend (pool
/// sized to what the receiver requested); `Undersized` simulates a backend
/// that ignores the request and reports a too-small pool.
pub trait PoolSizing: Send + Sync + 'static {
    fn capacity(requested: usize) -> usize;
}

pub struct Honest;
impl PoolSizing for Honest {
    fn capacity(requested: usize) -> usize {
        requested
    }
}

pub struct Undersized;
impl PoolSizing for Undersized {
    fn capacity(_requested: usize) -> usize {
        4 // fixed, well below any real reorder-window requirement
    }
}

/// One received datagram's bytes. The `Arc` is built once when a test seeds
/// it, before the measured region; `recv_burst` only moves it.
pub struct MockFrame {
    bytes: Arc<Vec<u8>>,
}

impl AsPayload for MockFrame {
    fn payload(&self) -> &[u8] {
        &self.bytes
    }

    fn sequence(&self) -> u64 {
        0
    }

    fn stream_id(&self) -> u8 {
        0
    }
}

pub struct MockPool {
    capacity: usize,
}

impl BufferPool for MockPool {
    type Slab = Vec<u8>;

    fn acquire(&self, _len: usize) -> Option<Self::Slab> {
        None // unused: MockTransport hands out pre-seeded frames directly
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn in_use(&self) -> usize {
        0
    }
}

/// In-process `DatagramSource` double. Datagrams are seeded via
/// [`MockTransport::seed`] before driving the receiver; `recv_burst` pops
/// them without allocating. `M` picks whether `pool().capacity()` honors the
/// requested `RingConfig::slab_count` or reports a fixed undersized pool.
pub struct MockTransport<M: PoolSizing> {
    pending: Mutex<VecDeque<Arc<Vec<u8>>>>,
    pool: MockPool,
    _mode: PhantomData<M>,
}

pub type AllocProofTransport = MockTransport<Honest>;
pub type UndersizedPoolTransport = MockTransport<Undersized>;

impl<M: PoolSizing> MockTransport<M> {
    /// Queue one pre-built datagram for a later `recv_burst` to reap. Call
    /// before entering a measured region: this allocates (the `Arc`),
    /// `recv_burst` itself never does.
    pub fn seed(&self, datagram: Vec<u8>) {
        self.pending.lock().unwrap().push_back(Arc::new(datagram));
    }
}

impl<M: PoolSizing> TransportCore for MockTransport<M> {
    fn name(&self) -> &'static str {
        "mock"
    }

    async fn send(&mut self, _buf: &[u8]) -> Result<(), TransportError> {
        Ok(())
    }
}

impl<M: PoolSizing> DatagramSource for MockTransport<M> {
    type Frame = MockFrame;

    fn recv_burst(
        &mut self,
        out: &mut FrameBatch<MockFrame>,
        max: usize,
    ) -> Result<usize, TransportError> {
        let mut pending = self.pending.lock().unwrap();
        let mut n = 0;
        while n < max {
            match pending.pop_front() {
                Some(bytes) => {
                    out.push(MockFrame { bytes });
                    n += 1;
                }
                None => break,
            }
        }
        Ok(n)
    }
}

impl<M: PoolSizing> PoolAccess for MockTransport<M> {
    type Pool = MockPool;

    fn pool(&self) -> &MockPool {
        &self.pool
    }
}

impl<M: PoolSizing> AsyncReady for MockTransport<M> {
    async fn ready(&mut self) -> Result<(), TransportError> {
        // Tests only ever seed data ahead of time, so `poll_once` always
        // finds something and this never actually gets awaited.
        Ok(())
    }
}

impl<M: PoolSizing> UdpTransport for MockTransport<M> {
    async fn join_multicast(
        &mut self,
        _group: IpAddr,
        _interface: MulticastInterface,
    ) -> Result<(), TransportError> {
        Ok(())
    }

    async fn send_to(&mut self, _buf: &[u8], _addr: SocketAddr) -> Result<(), TransportError> {
        Ok(())
    }
}

impl<M: PoolSizing> TransportBind for MockTransport<M> {
    async fn bind_udp(
        _bind: BindConfig,
        _rx: RecvBufConfig,
        _tx: SendBufConfig,
        ring: RingConfig,
        _batch: BatchConfig,
        _affinity: AffinityConfig,
    ) -> Result<Self, TransportError> {
        Ok(Self {
            pending: Mutex::new(VecDeque::new()),
            pool: MockPool {
                capacity: M::capacity(ring.slab_count),
            },
            _mode: PhantomData,
        })
    }

    async fn connect_tcp(
        _bind: BindConfig,
        _rx: RecvBufConfig,
        _tx: SendBufConfig,
        _ring: RingConfig,
        _affinity: AffinityConfig,
    ) -> Result<Self, TransportError> {
        Err(TransportError::Unsupported {
            name: "mock",
            reason: "MockTransport is datagram-only",
        })
    }
}

/// Build one single-message MoldUDP64 downstream packet: 20-byte header plus
/// one length-prefixed block. Mirrors `loopback.rs`'s wire construction.
pub fn mold_packet(session: &[u8; 10], sequence: u64, payload: &[u8]) -> Vec<u8> {
    mold_multi_packet(session, sequence, &[payload])
}

/// Build one MoldUDP64 downstream packet carrying `messages.len()`
/// length-prefixed blocks, sequenced `first_sequence..first_sequence +
/// messages.len()`.
pub fn mold_multi_packet(session: &[u8; 10], first_sequence: u64, messages: &[&[u8]]) -> Vec<u8> {
    let mut packet = Vec::new();
    packet.extend_from_slice(session);
    packet.extend_from_slice(&first_sequence.to_be_bytes());
    packet.extend_from_slice(&(messages.len() as u16).to_be_bytes());
    for m in messages {
        packet.extend_from_slice(&(m.len() as u16).to_be_bytes());
        packet.extend_from_slice(m);
    }
    packet
}

/// Build a MoldUDP64 heartbeat packet: 20-byte header only, `message_count ==
/// 0`, carrying `next_expected` in the sequence field.
pub fn mold_heartbeat(session: &[u8; 10], next_expected: u64) -> Vec<u8> {
    mold_control(session, next_expected, 0)
}

/// Build a MoldUDP64 end-of-session packet: 20-byte header only,
/// `message_count == 0xFFFF`, carrying `next_expected` in the sequence field.
pub fn mold_end_of_session(session: &[u8; 10], next_expected: u64) -> Vec<u8> {
    mold_control(session, next_expected, 0xFFFF)
}

fn mold_control(session: &[u8; 10], sequence: u64, message_count: u16) -> Vec<u8> {
    let mut packet = Vec::with_capacity(20);
    packet.extend_from_slice(session);
    packet.extend_from_slice(&sequence.to_be_bytes());
    packet.extend_from_slice(&message_count.to_be_bytes());
    packet
}

/// Poll `fut` to completion on the current thread with a no-op waker. No
/// executor, no allocation: only safe because every mock recv path here
/// resolves on the first poll (data is always pre-seeded, `MockTransport`
/// never actually suspends on `AsyncReady::ready`).
pub fn block_on<T>(fut: impl Future<Output = T>) -> T {
    let mut fut = std::pin::pin!(fut);
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    loop {
        if let std::task::Poll::Ready(out) = fut.as_mut().poll(&mut cx) {
            return out;
        }
    }
}
