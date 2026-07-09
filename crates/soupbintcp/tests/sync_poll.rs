//! Sync-drive path: `poll_recv` over a real loopback socket, spun by the
//! caller in a plain loop, never touching `AsyncReady::ready` or `recv`.

mod common;

use std::time::{Duration, Instant};

use client_soupbintcp::{SoupBinClient, SoupBinEvent, SoupBinMessage};
use common::MockServer;
use transport_core::AsPayload;

#[tokio::test]
async fn poll_recv_drains_sequenced_data_without_ready() {
    let (listener, addr) = common::bind_listener().await;
    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let mut mock = MockServer::new(sock);
        let mut buf = [0u8; 128];
        let _ = mock.read(&mut buf).await;
        mock.write_packet(&common::login_accepted_packet("sess001", 1))
            .await;
        // stagger the data packet so poll_recv observes at least one
        // no-progress Ok(None) before data lands.
        tokio::time::sleep(Duration::from_millis(30)).await;
        mock.write_packet(&common::sequenced_data_packet(b"sync-tick"))
            .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    });

    let transport = common::connect_client(addr).await;
    let mut client = SoupBinClient::connect(transport, common::test_config())
        .await
        .expect("login should succeed");

    // bounded busy-spin: never calls AsyncReady::ready or recv, only poll_recv.
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match client.poll_recv().expect("poll_recv") {
            Some(SoupBinMessage::Data(frame)) => {
                assert_eq!(frame.payload(), b"sync-tick");
                assert_eq!(frame.sequence(), 1);
                break;
            }
            Some(SoupBinMessage::Event(e)) => panic!("expected data frame, got {e:?}"),
            None => {
                assert!(Instant::now() < deadline, "poll_recv spin timed out");
                // yield so the mock server task (same executor) gets to run.
                // poll_recv itself stays a plain sync call, not this sleep.
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }
    }

    server.await.unwrap();
}

#[tokio::test]
async fn poll_recv_reports_no_progress_before_data_arrives() {
    let (listener, addr) = common::bind_listener().await;
    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let mut mock = MockServer::new(sock);
        let mut buf = [0u8; 128];
        let _ = mock.read(&mut buf).await;
        mock.write_packet(&common::login_accepted_packet("sess001", 1))
            .await;
        tokio::time::sleep(Duration::from_millis(80)).await;
        mock.write_packet(&common::end_of_session_packet()).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    });

    let transport = common::connect_client(addr).await;
    let mut client = SoupBinClient::connect(transport, common::test_config())
        .await
        .expect("login should succeed");

    // immediately after login, server hasn't sent EndOfSession yet: at least
    // one poll_recv call must observe no progress.
    let saw_no_progress = client.poll_recv().expect("poll_recv").is_none();
    assert!(
        saw_no_progress,
        "expected first poll_recv to see no data yet"
    );

    // bounded busy-spin, same as above, never calls ready() or recv().
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match client.poll_recv().expect("poll_recv") {
            Some(SoupBinMessage::Event(SoupBinEvent::EndOfSession)) => break,
            Some(other) => panic!("expected EndOfSession, got {other:?}"),
            None => {
                assert!(Instant::now() < deadline, "poll_recv spin timed out");
                // yield so the mock server task (same executor) gets to run.
                // poll_recv itself stays a plain sync call, not this sleep.
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }
    }

    server.await.unwrap();
}
