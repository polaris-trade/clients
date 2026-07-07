//! `compressed` feature: server->client zlib inflate, client->server stays plain.

#![cfg(feature = "compressed")]

mod common;

use std::io::Write;

use client_soupbintcp::{CompressedReader, SoupBinClient};
use common::MockServer;
use flate2::Compression;
use flate2::write::ZlibEncoder;

fn zlib_compress(data: &[u8]) -> Vec<u8> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

#[test]
fn zlib_roundtrip() {
    let plaintext = b"NASDAQ compressed SoupBinTCP feed fixture payload";
    let compressed = zlib_compress(plaintext);

    let mut reader = CompressedReader::new(4096);
    let inflated = reader.feed(&compressed).expect("inflate");
    assert_eq!(inflated, &plaintext[..]);
}

#[tokio::test]
async fn upstream_stays_plain() {
    // send_unsequenced must write the plain U packet even with `compressed`
    // enabled: the inflator only ever wraps the read side.
    let (listener, addr) = common::bind_listener().await;
    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let mut mock = MockServer::new(sock);
        let mut buf = [0u8; 128];
        let _ = mock.read(&mut buf).await;
        mock.write_packet(&common::login_accepted_packet("sess001", 1))
            .await;

        let mut buf2 = [0u8; 32];
        let n = mock.read(&mut buf2).await;
        // upstream bypasses the inflator entirely: bytes on the wire are plain.
        assert_eq!(&buf2[..n], &common::build_packet(b'U', b"hello")[..]);
    });

    let transport = common::connect_client(addr).await;
    let mut client = SoupBinClient::connect(transport, common::test_config())
        .await
        .unwrap();
    client.send_unsequenced(b"hello").await.unwrap();

    server.await.unwrap();
}
