//! Bidirectional heartbeat: client sends `R` on silence, detects server silence.

mod common;

use std::time::Duration;

use client_soupbintcp::{SoupBinClient, SoupBinError, SoupBinEvent};
use common::MockServer;

#[tokio::test]
async fn client_sends_r_on_1s_silent() {
    let (listener, addr) = common::bind_listener().await;
    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let mut mock = MockServer::new(sock);
        let mut buf = [0u8; 128];
        let _ = mock.read(&mut buf).await;
        mock.write_packet(&common::login_accepted_packet("sess001", 1))
            .await;

        let mut buf2 = [0u8; 16];
        let n = mock.read(&mut buf2).await;
        assert_eq!(&buf2[..n], &common::build_packet(b'R', &[])[..]);
    });

    let transport = common::connect_client(addr).await;
    let mut cfg = common::test_config();
    cfg.heartbeat_interval = Duration::from_millis(20);
    let mut client = SoupBinClient::connect(transport, cfg).await.unwrap();

    // stay silent past heartbeat_interval, then tick
    tokio::time::sleep(Duration::from_millis(30)).await;
    let event = client.tick_heartbeat().await.unwrap();
    assert_eq!(event, Some(SoupBinEvent::HeartbeatSent));

    server.await.unwrap();
}

#[tokio::test]
async fn server_silent_15s_timeout() {
    let (listener, addr) = common::bind_listener().await;
    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let mut mock = MockServer::new(sock);
        let mut buf = [0u8; 128];
        let _ = mock.read(&mut buf).await;
        mock.write_packet(&common::login_accepted_packet("sess001", 1))
            .await;
        // go silent past the client's heartbeat_timeout
        tokio::time::sleep(Duration::from_millis(200)).await;
    });

    let transport = common::connect_client(addr).await;
    let mut cfg = common::test_config();
    cfg.heartbeat_timeout = Duration::from_millis(30);
    let mut client = SoupBinClient::connect(transport, cfg).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    let err = client.tick_heartbeat().await.unwrap_err();
    assert!(matches!(err, SoupBinError::HeartbeatTimeout));

    server.await.unwrap();
}
