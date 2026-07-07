//! Login handshake + logout + end-of-session over a loopback mock TCP server.

mod common;

use std::time::{Duration, Instant};

use client_soupbintcp::{SoupBinClient, SoupBinError, SoupBinEvent, SoupBinMessage};
use common::MockServer;
use transport_core::AsPayload;

#[tokio::test]
async fn login_accepted() {
    let (listener, addr) = common::bind_listener().await;
    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let mut mock = MockServer::new(sock);
        let mut buf = [0u8; 128];
        let n = mock.read(&mut buf).await;
        assert_eq!(buf[2], b'L', "expected Login Request");
        mock.write_packet(&common::login_accepted_packet("sess001", 5))
            .await;
        let _ = n;
        tokio::time::sleep(Duration::from_millis(50)).await;
    });

    let transport = common::connect_client(addr).await;
    let client = SoupBinClient::connect(transport, common::test_config())
        .await
        .expect("login should succeed");
    assert_eq!(client.session(), "sess001");
    assert_eq!(client.next_expected_sequence(), 5);

    server.await.unwrap();
}

#[tokio::test]
async fn login_rejected() {
    let (listener, addr) = common::bind_listener().await;
    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let mut mock = MockServer::new(sock);
        let mut buf = [0u8; 128];
        let _ = mock.read(&mut buf).await;
        mock.write_packet(&common::login_rejected_packet("A")).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    });

    let transport = common::connect_client(addr).await;
    match SoupBinClient::connect(transport, common::test_config()).await {
        Err(SoupBinError::LoginRejected { code }) => assert_eq!(code, "A"),
        Err(other) => panic!("expected LoginRejected, got {other:?}"),
        Ok(_) => panic!("expected LoginRejected, got Ok"),
    }

    server.await.unwrap();
}

#[tokio::test]
async fn login_timeout() {
    let (listener, addr) = common::bind_listener().await;
    let server = tokio::spawn(async move {
        let (_sock, _) = listener.accept().await.unwrap();
        // never respond: hold the connection open past the client's login_timeout
        tokio::time::sleep(Duration::from_millis(500)).await;
    });

    let transport = common::connect_client(addr).await;
    let mut cfg = common::test_config();
    cfg.login_timeout = Duration::from_millis(60);

    let start = Instant::now();
    let result = SoupBinClient::connect(transport, cfg).await;
    assert!(start.elapsed() >= Duration::from_millis(60));
    match result {
        Err(SoupBinError::LoginTimeout { timeout }) => {
            assert_eq!(timeout, Duration::from_millis(60))
        }
        Err(other) => panic!("expected LoginTimeout, got {other:?}"),
        Ok(_) => panic!("expected LoginTimeout, got Ok"),
    }

    server.await.unwrap();
}

#[tokio::test]
async fn seq_tracked_from_login() {
    let (listener, addr) = common::bind_listener().await;
    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let mut mock = MockServer::new(sock);
        let mut buf = [0u8; 128];
        let _ = mock.read(&mut buf).await;
        mock.write_packet(&common::login_accepted_packet("sess001", 7))
            .await;
        mock.write_packet(&common::sequenced_data_packet(b"payload-a"))
            .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    });

    let transport = common::connect_client(addr).await;
    let mut client = SoupBinClient::connect(transport, common::test_config())
        .await
        .unwrap();
    assert_eq!(client.next_expected_sequence(), 7);

    match client.recv().await.unwrap() {
        SoupBinMessage::Data(frame) => {
            assert_eq!(frame.payload(), b"payload-a");
            assert_eq!(frame.sequence(), 7);
        }
        SoupBinMessage::Event(e) => panic!("expected data frame, got event {e:?}"),
    }
    assert_eq!(client.next_expected_sequence(), 8);

    server.await.unwrap();
}

#[tokio::test]
async fn end_of_session_closes_socket() {
    let (listener, addr) = common::bind_listener().await;
    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let mut mock = MockServer::new(sock);
        let mut buf = [0u8; 128];
        let _ = mock.read(&mut buf).await;
        mock.write_packet(&common::login_accepted_packet("sess001", 1))
            .await;
        mock.write_packet(&common::end_of_session_packet()).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    });

    let transport = common::connect_client(addr).await;
    let mut client = SoupBinClient::connect(transport, common::test_config())
        .await
        .unwrap();

    match client.recv().await.unwrap() {
        SoupBinMessage::Event(SoupBinEvent::EndOfSession) => {}
        SoupBinMessage::Event(other) => panic!("expected EndOfSession event, got {other:?}"),
        SoupBinMessage::Data(_) => panic!("expected EndOfSession event, got a data frame"),
    }

    let err = client.recv().await.unwrap_err();
    assert!(
        matches!(err, SoupBinError::EndOfSession),
        "recv must refuse after EoS"
    );

    server.await.unwrap();
}

#[tokio::test]
async fn logout_sends_o() {
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
        assert_eq!(&buf2[..n], &common::build_packet(b'O', &[])[..]);
    });

    let transport = common::connect_client(addr).await;
    let mut client = SoupBinClient::connect(transport, common::test_config())
        .await
        .unwrap();
    client.logout().await.unwrap();

    server.await.unwrap();
}

#[tokio::test]
async fn unsequenced_send_writes_u() {
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
        assert_eq!(&buf2[..n], &common::build_packet(b'U', b"hello")[..]);
    });

    let transport = common::connect_client(addr).await;
    let mut client = SoupBinClient::connect(transport, common::test_config())
        .await
        .unwrap();
    client.send_unsequenced(b"hello").await.unwrap();

    server.await.unwrap();
}
