//! `recv_managed` (tokio feature): a consumer that loops only this one call
//! keeps the session alive by sending client heartbeats on its own, then still
//! delivers the next data frame.
#![cfg(feature = "tokio")]

mod common;

use std::time::Duration;

use client_soupbintcp::{SoupBinClient, SoupBinMessage};
use common::MockServer;
use transport_core::AsPayload;

#[tokio::test]
async fn recv_managed_sends_heartbeats_then_delivers_data() {
    let (listener, addr) = common::bind_listener().await;
    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let mut mock = MockServer::new(sock);
        let mut buf = [0u8; 128];
        let _ = mock.read(&mut buf).await; // login request
        mock.write_packet(&common::login_accepted_packet("sess001", 1))
            .await;

        // Client is driven only by recv_managed. With no data for longer than
        // heartbeat_interval (50ms in test_config), it must send a client
        // heartbeat 'R' unprompted, or the server would drop it.
        let mut hb = [0u8; 16];
        let n = mock.read(&mut hb).await;
        assert!(
            hb[..n].starts_with(&common::build_packet(b'R', &[])),
            "expected a client heartbeat, got {:?}",
            &hb[..n]
        );

        mock.write_packet(&common::sequenced_data_packet(b"after-hb"))
            .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    });

    let transport = common::connect_client(addr).await;
    let mut client = SoupBinClient::connect(transport, common::test_config())
        .await
        .expect("login");

    // One call: internally sends the heartbeat and returns the data frame.
    match client.recv_managed().await.expect("recv_managed") {
        SoupBinMessage::Data(frame) => assert_eq!(frame.payload(), b"after-hb"),
        SoupBinMessage::Event(e) => panic!("expected data frame, got event {e:?}"),
    }

    server.await.unwrap();
}
