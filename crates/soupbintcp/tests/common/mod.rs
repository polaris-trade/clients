//! Shared test-only helpers: mock TCP server bytes, tokio TCP transport wiring.
//! Not a test binary itself (lives under tests/common/, cargo skips it).
//! Each `tests/*.rs` binary includes this module separately and uses only a
//! subset of it, so unused-per-binary warnings here are false positives.
#![allow(dead_code)]

use std::{net::SocketAddr, time::Duration};

#[cfg(feature = "compressed")]
use flate2::{Compress, Compression, FlushCompress};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use transport_core::{
    AffinityConfig, BindConfig, RecvBufConfig, RingConfig, SendBufConfig, TransportBind,
};
use transport_tokio::TokioTransport;

pub async fn bind_listener() -> (TcpListener, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    (listener, addr)
}

/// Wraps the mock server's socket. Client-to-server bytes (login, heartbeats,
/// unsequenced) always stay plain, so `sock` is exposed directly for those reads.
/// Server writes go through `write_packet`, which zlib-frames them under the
/// `compressed` feature (one shared `Compress` state per connection, matching
/// the client's own persistent `Decompress` state) and writes bytes as-is otherwise.
pub struct MockServer {
    pub sock: TcpStream,
    #[cfg(feature = "compressed")]
    encoder: Compress,
}

impl MockServer {
    pub fn new(sock: TcpStream) -> Self {
        Self {
            sock,
            #[cfg(feature = "compressed")]
            encoder: Compress::new(Compression::default(), true),
        }
    }

    pub async fn write_packet(&mut self, plain: &[u8]) {
        #[cfg(feature = "compressed")]
        {
            // compress_vec only writes into already-reserved spare capacity,
            // it never grows the vec itself (flate2 contract).
            let mut out = Vec::with_capacity(plain.len() + 128);
            self.encoder
                .compress_vec(plain, &mut out, FlushCompress::Sync)
                .expect("zlib compress");
            self.sock.write_all(&out).await.expect("write compressed");
        }
        #[cfg(not(feature = "compressed"))]
        {
            self.sock.write_all(plain).await.expect("write plain");
        }
    }

    pub async fn read(&mut self, buf: &mut [u8]) -> usize {
        self.sock.read(buf).await.expect("read")
    }
}

pub async fn connect_client(addr: SocketAddr) -> TokioTransport {
    let bind = BindConfig::new(addr);
    TokioTransport::connect_tcp(
        bind,
        RecvBufConfig::default(),
        SendBufConfig::default(),
        RingConfig::default(),
        AffinityConfig::default(),
    )
    .await
    .expect("connect_tcp")
}

/// Builds one logical SoupBinTCP packet: `Length[2 BE] + Type[1] + payload`.
pub fn build_packet(ty: u8, payload: &[u8]) -> Vec<u8> {
    let len = 1 + payload.len();
    let mut out = Vec::with_capacity(2 + len);
    out.extend_from_slice(&(len as u16).to_be_bytes());
    out.push(ty);
    out.extend_from_slice(payload);
    out
}

fn ascii_left_justify(s: &str, width: usize) -> Vec<u8> {
    let mut buf = vec![b' '; width];
    let bytes = s.as_bytes();
    let n = bytes.len().min(width);
    buf[..n].copy_from_slice(&bytes[..n]);
    buf
}

fn ascii_right_justify_numeric(n: u64, width: usize) -> Vec<u8> {
    let digits = n.to_string();
    let mut buf = vec![b' '; width];
    let bytes = digits.as_bytes();
    let take = bytes.len().min(width);
    let start = width - take;
    buf[start..start + take].copy_from_slice(&bytes[bytes.len() - take..]);
    buf
}

pub fn login_accepted_packet(session: &str, sequence: u64) -> Vec<u8> {
    let mut payload = Vec::with_capacity(30);
    payload.extend_from_slice(&ascii_left_justify(session, 10));
    payload.extend_from_slice(&ascii_right_justify_numeric(sequence, 20));
    build_packet(b'A', &payload)
}

pub fn login_rejected_packet(code: &str) -> Vec<u8> {
    build_packet(b'J', code.as_bytes())
}

pub fn sequenced_data_packet(payload: &[u8]) -> Vec<u8> {
    build_packet(b'S', payload)
}

pub fn end_of_session_packet() -> Vec<u8> {
    build_packet(b'Z', &[])
}

/// Small, fast-timer config for tests: real defaults would make heartbeat and
/// login-timeout tests take 15-30s each.
pub fn test_config() -> client_soupbintcp::SoupBinClientConfig {
    client_soupbintcp::SoupBinClientConfig {
        username: "user01".into(),
        password: "pass12345".into(),
        requested_session: "sess001".into(),
        requested_sequence_number: 1,
        login_timeout: Duration::from_millis(200),
        heartbeat_interval: Duration::from_millis(50),
        heartbeat_timeout: Duration::from_millis(300),
        max_frame_size: 4096,
        decode_buf_capacity: 4096,
    }
}
