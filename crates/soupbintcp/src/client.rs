//! `SoupBinClient<T>`: session state machine over any `transport_core::StreamSource`.
//! `AsyncReady` is optional: it unlocks `connect`/`recv` (async login and
//! spin-free await). Sync-only backends drive the base API instead, polling
//! with `poll_recv`. Owns login, sequenced-data delivery, heartbeats, logout,
//! end-of-session. No backend imports here: generic over `T`.

use std::{
    future::{Future, poll_fn},
    pin::pin,
    task::Poll,
    time::Instant,
};

use bytes::{BufMut, BytesMut};
use transport_core::{AsyncReady, StreamSource};

#[cfg(feature = "compressed")]
use crate::compressed::CompressedReader;
use crate::{
    config::SoupBinClientConfig,
    error::SoupBinError,
    event::{SoupBinEvent, SoupBinMessage},
    frame::Frame,
    wire::{self, PacketType},
};

/// Session lifecycle stage. Streaming is the only stage where `recv` yields `Frame`s.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientState {
    Disconnected,
    Authenticating,
    Streaming,
    Closed,
}

/// One dispatch pass over buffered packets, as owned data rather than a
/// borrow of `self`: keeps `dispatch_buffered` safe to call more than once
/// (with another `&mut self` call in between) from the same caller frame,
/// which a `self`-borrowing return type would rule out. Callers turn this
/// into a `SoupBinMessage` themselves, right at the point they return it, so
/// the `&self.last_frame` borrow stays scoped to that one branch.
enum PacketOutcome {
    Data { sequence: u64 },
    Event(SoupBinEvent),
    NoProgress,
}

/// SoupBinTCP v3.0 session over transport `T`. Caller connects the transport,
/// then hands it to [`SoupBinClient::connect`] to run the login handshake.
///
/// Base methods (send, heartbeat, `poll_recv`) need only `T: StreamSource`.
/// `connect` and `recv` need `T: AsyncReady` too; see the second `impl` block.
pub struct SoupBinClient<T: StreamSource> {
    transport: T,
    state: ClientState,
    decode_buf: BytesMut,
    last_frame: BytesMut,
    // resident scratch for outbound frames; cleared per write_packet.
    send_buf: BytesMut,
    session: String,
    next_expected_sequence: u64,
    last_send: Instant,
    last_recv: Instant,
    cfg: SoupBinClientConfig,
    #[cfg(feature = "compressed")]
    inflate: CompressedReader,
    // raw compressed bytes land here before inflate; inflate needs a
    // contiguous compressed frame, so this is a second, unavoidable copy.
    #[cfg(feature = "compressed")]
    recv_staging: BytesMut,
}

impl<T: StreamSource> SoupBinClient<T> {
    fn from_transport(transport: T, cfg: SoupBinClientConfig) -> Self {
        let cap = cfg.decode_buf_capacity;
        let send_cap = cfg.max_frame_size;
        Self {
            transport,
            state: ClientState::Disconnected,
            decode_buf: BytesMut::with_capacity(cap),
            last_frame: BytesMut::new(),
            send_buf: BytesMut::with_capacity(send_cap),
            session: String::new(),
            next_expected_sequence: 0,
            last_send: Instant::now(),
            last_recv: Instant::now(),
            #[cfg(feature = "compressed")]
            inflate: CompressedReader::new(cap),
            #[cfg(feature = "compressed")]
            recv_staging: BytesMut::with_capacity(cap),
            cfg,
        }
    }

    /// Sequence number the server will assign to the next `Sequenced Data` packet.
    /// Use this to populate `requested_sequence_number` on reconnect.
    pub fn next_expected_sequence(&self) -> u64 {
        self.next_expected_sequence
    }

    /// Session id assigned by the server at login.
    pub fn session(&self) -> &str {
        &self.session
    }

    pub fn state(&self) -> ClientState {
        self.state
    }

    /// Non-blocking single attempt: dispatches an already-buffered packet if
    /// one is ready, else tries one `recv_into` and dispatches again.
    /// `Ok(None)` means no progress this call (nothing buffered, nothing on
    /// the wire yet); caller decides whether to spin. Debug packets decode
    /// and drop silently, same as `recv`. For backends without `AsyncReady`;
    /// async callers should prefer `recv` instead of spinning this.
    pub fn poll_recv(&mut self) -> Result<Option<SoupBinMessage<'_>>, SoupBinError> {
        if self.state == ClientState::Closed {
            return Err(SoupBinError::EndOfSession);
        }
        let outcome = match self.dispatch_buffered()? {
            PacketOutcome::NoProgress => {
                self.ingest_transport_frame()?;
                self.dispatch_buffered()?
            }
            outcome => outcome,
        };
        match outcome {
            PacketOutcome::Data { sequence } => Ok(Some(SoupBinMessage::Data(Frame {
                payload: &self.last_frame,
                sequence,
            }))),
            PacketOutcome::Event(event) => Ok(Some(SoupBinMessage::Event(event))),
            PacketOutcome::NoProgress => Ok(None),
        }
    }

    /// Drains packets already sitting in `decode_buf`, dispatching the first
    /// non-Debug one as an owned [`PacketOutcome`] (never borrows `self`, so
    /// callers can chain another `&mut self` call after it). Debug packets
    /// decode and drop silently, looping to the next buffered packet.
    /// `NoProgress` once `decode_buf` yields no more full packets. Shared
    /// framing/dispatch for `recv` and `poll_recv`.
    fn dispatch_buffered(&mut self) -> Result<PacketOutcome, SoupBinError> {
        while let Some((ty, bytes)) = self.take_one_packet()? {
            match ty {
                PacketType::SequencedData => {
                    let sequence = self.next_expected_sequence;
                    self.next_expected_sequence += 1;
                    self.last_frame = bytes;
                    return Ok(PacketOutcome::Data { sequence });
                }
                PacketType::ServerHeartbeat => {
                    return Ok(PacketOutcome::Event(SoupBinEvent::HeartbeatReceived));
                }
                PacketType::EndOfSession => {
                    self.state = ClientState::Closed;
                    return Ok(PacketOutcome::Event(SoupBinEvent::EndOfSession));
                }
                PacketType::Debug => continue,
                other => {
                    return Err(SoupBinError::ProtocolViolation(format!(
                        "unexpected packet type in streaming state: {other:?}"
                    )));
                }
            }
        }
        Ok(PacketOutcome::NoProgress)
    }

    /// Wraps `payload` as `Unsequenced Data (U)` and writes it, plain, without
    /// waiting on a server ack. Always uncompressed even under the `compressed`
    /// feature: upstream never deflates.
    pub async fn send_unsequenced(&mut self, payload: &[u8]) -> Result<(), SoupBinError> {
        if self.state == ClientState::Closed {
            return Err(SoupBinError::EndOfSession);
        }
        self.write_packet(b'U', payload).await
    }

    /// Sends `Logout Request (O)` and closes. Idempotent: no-op if already closed.
    pub async fn logout(&mut self) -> Result<(), SoupBinError> {
        if self.state == ClientState::Closed {
            return Ok(());
        }
        self.write_packet(b'O', &[]).await?;
        self.state = ClientState::Closed;
        Ok(())
    }

    /// Sends `Client Heartbeat (R)` if `cfg.heartbeat_interval` passed since the
    /// last client send, and checks server silence against `cfg.heartbeat_timeout`.
    /// Call this on the consumer's own timer; the client has no internal clock task.
    pub async fn tick_heartbeat(&mut self) -> Result<Option<SoupBinEvent>, SoupBinError> {
        if self.state == ClientState::Closed {
            return Err(SoupBinError::EndOfSession);
        }
        if self.last_recv.elapsed() > self.cfg.heartbeat_timeout {
            self.state = ClientState::Closed;
            return Err(SoupBinError::HeartbeatTimeout);
        }
        if self.last_send.elapsed() > self.cfg.heartbeat_interval {
            self.write_packet(b'R', &[]).await?;
            return Ok(Some(SoupBinEvent::HeartbeatSent));
        }
        Ok(None)
    }

    /// Peeks the length prefix (guarding `max_frame_size`) and tries `wire::parse_packet`.
    /// On a full packet, splits it out of `decode_buf` (zero-copy via `BytesMut`).
    fn take_one_packet(&mut self) -> Result<Option<(PacketType, BytesMut)>, SoupBinError> {
        if self.decode_buf.len() >= 2 {
            let len = u16::from_be_bytes([self.decode_buf[0], self.decode_buf[1]]) as usize;
            let total = 2 + len;
            if total > self.cfg.max_frame_size {
                return Err(SoupBinError::FrameTooLarge {
                    size: total,
                    max: self.cfg.max_frame_size,
                });
            }
        }
        let Some((frame, consumed)) = wire::parse_packet(&self.decode_buf)? else {
            return Ok(None);
        };
        let ty = frame.ty;
        let payload_len = frame.payload.len();
        let mut packet = self.decode_buf.split_to(consumed);
        let payload = packet.split_off(consumed - payload_len);
        Ok(Some((ty, payload)))
    }

    /// Lands one `recv_into` chunk. Uncompressed: writes straight into
    /// `decode_buf` spare capacity, one landing, no intermediate copy.
    /// Compressed: lands into a staging buffer first (inflate needs a
    /// contiguous compressed frame), then extends `decode_buf` with the
    /// inflated bytes, a second unavoidable copy. Updates `last_recv` either way.
    fn ingest_transport_frame(&mut self) -> Result<(), SoupBinError> {
        #[cfg(feature = "compressed")]
        {
            self.recv_staging.clear();
            self.recv_staging.reserve(self.cfg.decode_buf_capacity);
            let spare = self.recv_staging.spare_capacity_mut();
            let n = self.transport.recv_into(spare)?;
            // SAFETY: recv_into returns the exact count of bytes it wrote into `spare`.
            unsafe {
                self.recv_staging.advance_mut(n);
            }
            let inflated = self.inflate.feed(&self.recv_staging)?;
            self.decode_buf.extend_from_slice(inflated);
        }
        #[cfg(not(feature = "compressed"))]
        {
            // reserve before spare_capacity_mut: take_one_packet's split_to shrinks
            // spare permanently, so a starved reserve would zero-length the slice.
            self.decode_buf.reserve(self.cfg.decode_buf_capacity);
            let spare = self.decode_buf.spare_capacity_mut();
            let n = self.transport.recv_into(spare)?;
            // SAFETY: recv_into returns the exact count of bytes it wrote into `spare`.
            unsafe {
                self.decode_buf.advance_mut(n);
            }
        }
        self.last_recv = Instant::now();
        Ok(())
    }

    async fn write_packet(&mut self, ty: u8, payload: &[u8]) -> Result<(), SoupBinError> {
        self.send_buf.clear();
        encode_packet_into(&mut self.send_buf, ty, payload);
        self.transport.send(&self.send_buf).await?;
        self.last_send = Instant::now();
        Ok(())
    }
}

/// Async-only surface: login and streaming `recv` both spin on `AsyncReady::ready`
/// rather than a bare sync poll loop. Sync-only backends use the base `impl`
/// block above (`poll_recv`) instead.
impl<T: StreamSource + AsyncReady> SoupBinClient<T> {
    /// Runs the login handshake over an already-connected `transport`.
    ///
    /// On `Login Accepted`: captures session + sequence, transitions to streaming.
    /// On `Login Rejected`: returns `LoginRejected`. On no response within
    /// `cfg.login_timeout`: returns `LoginTimeout`. Either way the transport is
    /// dropped with `self` on the error path, closing the socket.
    pub async fn connect(transport: T, cfg: SoupBinClientConfig) -> Result<Self, SoupBinError> {
        let mut client = Self::from_transport(transport, cfg);
        client.login().await?;
        Ok(client)
    }

    /// Waits for the next sequenced data frame or lifecycle event.
    ///
    /// Debug packets are decoded and silently dropped, never surfaced here.
    /// After an `EndOfSession` event, further calls return `Err(EndOfSession)`.
    pub async fn recv(&mut self) -> Result<SoupBinMessage<'_>, SoupBinError> {
        if self.state == ClientState::Closed {
            return Err(SoupBinError::EndOfSession);
        }
        loop {
            match self.dispatch_buffered()? {
                PacketOutcome::Data { sequence } => {
                    return Ok(SoupBinMessage::Data(Frame {
                        payload: &self.last_frame,
                        sequence,
                    }));
                }
                PacketOutcome::Event(event) => return Ok(SoupBinMessage::Event(event)),
                PacketOutcome::NoProgress => {}
            }
            self.await_more_bytes().await?;
        }
    }

    async fn login(&mut self) -> Result<(), SoupBinError> {
        self.state = ClientState::Authenticating;
        let payload = build_login_request(&self.cfg);
        self.write_packet(b'L', &payload).await?;

        let deadline = Instant::now() + self.cfg.login_timeout;
        loop {
            if let Some((ty, bytes)) = self.take_one_packet()? {
                match ty {
                    PacketType::LoginAccepted => {
                        if bytes.len() < 30 {
                            return Err(SoupBinError::ProtocolViolation(
                                "login accepted payload shorter than Session+SequenceNumber".into(),
                            ));
                        }
                        self.session = parse_ascii_field(&bytes[0..10])?.to_string();
                        self.next_expected_sequence = parse_ascii_numeric(&bytes[10..30])?;
                        self.last_recv = Instant::now();
                        self.state = ClientState::Streaming;
                        return Ok(());
                    }
                    PacketType::LoginRejected => {
                        let code = parse_ascii_field(&bytes)?.to_string();
                        return Err(SoupBinError::LoginRejected { code });
                    }
                    PacketType::Debug => continue,
                    other => {
                        return Err(SoupBinError::ProtocolViolation(format!(
                            "unexpected packet type during login: {other:?}"
                        )));
                    }
                }
            }
            if Instant::now() >= deadline {
                return Err(SoupBinError::LoginTimeout {
                    timeout: self.cfg.login_timeout,
                });
            }
            self.await_more_bytes_with_deadline(deadline).await?;
        }
    }

    /// Blocks on the transport until bytes arrive. No deadline: heartbeat
    /// silence is caught by `tick_heartbeat`, driven by the caller's own timer.
    async fn await_more_bytes(&mut self) -> Result<(), SoupBinError> {
        self.transport.ready().await?;
        self.ingest_transport_frame()
    }

    /// Same as `await_more_bytes`, but self-wakes on `Pending` to recheck the
    /// wall clock each scheduler tick. No timer dependency in this crate, so
    /// login timeout is enforced by polling the deadline, not by sleeping.
    async fn await_more_bytes_with_deadline(
        &mut self,
        deadline: Instant,
    ) -> Result<(), SoupBinError> {
        let login_timeout = self.cfg.login_timeout;
        // scoped block: pin!'s hidden local (and the &mut self.transport borrow
        // it holds) must drop before ingest_transport_frame borrows self again.
        let result = {
            let ready = self.transport.ready();
            let mut ready = pin!(ready);
            poll_fn(|cx| match ready.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
                Poll::Ready(Err(e)) => Poll::Ready(Err(SoupBinError::from(e))),
                Poll::Pending => {
                    if Instant::now() >= deadline {
                        Poll::Ready(Err(SoupBinError::LoginTimeout {
                            timeout: login_timeout,
                        }))
                    } else {
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                }
            })
            .await
        };
        result?;
        self.ingest_transport_frame()
    }
}

fn encode_packet_into(buf: &mut BytesMut, ty: u8, payload: &[u8]) {
    let len = 1 + payload.len();
    buf.extend_from_slice(&(len as u16).to_be_bytes());
    buf.extend_from_slice(&[ty]);
    buf.extend_from_slice(payload);
}

fn build_login_request(cfg: &SoupBinClientConfig) -> Vec<u8> {
    let mut payload = Vec::with_capacity(6 + 10 + 10 + 20);
    payload.extend_from_slice(&ascii_left_justify(&cfg.username, 6));
    payload.extend_from_slice(&ascii_left_justify(&cfg.password, 10));
    payload.extend_from_slice(&ascii_left_justify(&cfg.requested_session, 10));
    payload.extend_from_slice(&ascii_right_justify_numeric(
        cfg.requested_sequence_number,
        20,
    ));
    payload
}

/// Left-justifies `s` in `width` bytes, space-padded on the right, truncated if longer.
fn ascii_left_justify(s: &str, width: usize) -> Vec<u8> {
    let mut buf = vec![b' '; width];
    let bytes = s.as_bytes();
    let n = bytes.len().min(width);
    buf[..n].copy_from_slice(&bytes[..n]);
    buf
}

/// Right-justifies `n` as ASCII digits in `width` bytes, space-padded on the left.
fn ascii_right_justify_numeric(n: u64, width: usize) -> Vec<u8> {
    let digits = n.to_string();
    let mut buf = vec![b' '; width];
    let bytes = digits.as_bytes();
    let take = bytes.len().min(width);
    let start = width - take;
    buf[start..start + take].copy_from_slice(&bytes[bytes.len() - take..]);
    buf
}

fn parse_ascii_field(bytes: &[u8]) -> Result<&str, SoupBinError> {
    std::str::from_utf8(bytes)
        .map(str::trim)
        .map_err(|_| SoupBinError::ProtocolViolation("non-ASCII field".into()))
}

fn parse_ascii_numeric(bytes: &[u8]) -> Result<u64, SoupBinError> {
    let s = parse_ascii_field(bytes)?;
    if s.is_empty() {
        return Ok(0);
    }
    s.parse::<u64>()
        .map_err(|_| SoupBinError::ProtocolViolation(format!("bad numeric field: {s:?}")))
}

// one-landing ingest is the uncompressed branch only (see ingest_transport_frame);
// the mock and its tests are scoped to that branch.
#[cfg(all(test, not(feature = "compressed")))]
mod tests {
    use core::mem::MaybeUninit;

    use transport_core::{TransportCore, TransportError};

    use super::*;

    /// Copies buffered bytes into the caller's `dst` on `recv_into`, tracking
    /// the last `dst` length seen. Drives the ingest path without a socket.
    struct MockStream {
        pending: Vec<u8>,
        last_dst_len: usize,
    }

    impl TransportCore for MockStream {
        fn name(&self) -> &'static str {
            "mock-stream"
        }

        async fn send(&mut self, _buf: &[u8]) -> Result<(), TransportError> {
            Ok(())
        }
    }

    impl StreamSource for MockStream {
        fn recv_into(&mut self, dst: &mut [MaybeUninit<u8>]) -> Result<usize, TransportError> {
            self.last_dst_len = dst.len();
            let n = self.pending.len().min(dst.len());
            for (slot, byte) in dst[..n].iter_mut().zip(self.pending.drain(..n)) {
                slot.write(byte);
            }
            Ok(n)
        }
    }

    impl AsyncReady for MockStream {
        async fn ready(&mut self) -> Result<(), TransportError> {
            Ok(())
        }
    }

    #[test]
    fn ingest_lands_once_with_no_extra_allocation() {
        let payload = b"steady-state payload, sized well under decode_buf_capacity".to_vec();
        let transport = MockStream {
            pending: payload.clone(),
            last_dst_len: 0,
        };
        let cfg = SoupBinClientConfig {
            decode_buf_capacity: 4096,
            ..Default::default()
        };
        let mut client = SoupBinClient::from_transport(transport, cfg);
        // warm decode_buf to steady state so ingest's own reserve() is a no-op.
        client.decode_buf.reserve(client.cfg.decode_buf_capacity);

        let info = allocation_counter::measure(|| {
            client.ingest_transport_frame().unwrap();
        });
        assert_eq!(info.count_total, 0, "steady-state ingest must not allocate");
        assert_eq!(&client.decode_buf[..], &payload[..]);
    }

    #[test]
    fn ingest_reserve_keeps_spare_available_after_repeated_consume() {
        // regression: skip reserve() before spare_capacity_mut() and take_one_packet's
        // split_to eventually starves recv_into to a zero-length slice.
        let mut pending = Vec::new();
        for _ in 0..20 {
            pending.extend_from_slice(b"0123456789");
        }
        let transport = MockStream {
            pending,
            last_dst_len: 0,
        };
        let cfg = SoupBinClientConfig {
            decode_buf_capacity: 64,
            ..Default::default()
        };
        let mut client = SoupBinClient::from_transport(transport, cfg);

        for _ in 0..20 {
            client.ingest_transport_frame().unwrap();
            assert!(
                client.transport.last_dst_len > 0,
                "recv_into starved to a zero-length slice; reserve() ordering regressed"
            );
            // mimic take_one_packet's BytesMut::split_to consuming decode_buf's front.
            let take = client.decode_buf.len().min(10);
            let _ = client.decode_buf.split_to(take);
        }
    }
}
