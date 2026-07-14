//! End-to-end loopback: login -> sequenced data -> heartbeat -> end of session,
//! driving `SoupBinClient<TokioTransport>` over a real `127.0.0.1:0` TCP socket.

mod common;

use std::time::Duration;

use client_soupbintcp::{SoupBinClient, SoupBinEvent, SoupBinMessage};
use common::MockServer;
use transport_core::AsPayload;

#[tokio::test]
async fn drives_login_data_heartbeat_eos_in_order() {
    let (listener, addr) = common::bind_listener().await;
    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let mut mock = MockServer::new(sock);

        let mut buf = [0u8; 128];
        let n = mock.read(&mut buf).await;
        assert_eq!(buf[2], b'L', "expected Login Request");
        let _ = n;

        mock.write_packet(&common::login_accepted_packet("sess001", 1))
            .await;
        mock.write_packet(&common::sequenced_data_packet(b"tick-1"))
            .await;
        mock.write_packet(&common::build_packet(b'H', &[])).await;
        mock.write_packet(&common::end_of_session_packet()).await;

        tokio::time::sleep(Duration::from_millis(50)).await;
    });

    let transport = common::connect_client(addr).await;
    let mut client = SoupBinClient::connect(transport, common::test_config())
        .await
        .expect("login should succeed");

    match client.recv().await.unwrap() {
        SoupBinMessage::Data(frame) => {
            assert_eq!(frame.payload(), b"tick-1");
            assert_eq!(frame.sequence(), 1);
        }
        SoupBinMessage::Event(e) => panic!("expected data frame first, got {e:?}"),
    }

    match client.recv().await.unwrap() {
        SoupBinMessage::Event(SoupBinEvent::HeartbeatReceived) => {}
        SoupBinMessage::Event(other) => panic!("expected HeartbeatReceived second, got {other:?}"),
        SoupBinMessage::Data(_) => panic!("expected HeartbeatReceived second, got a data frame"),
    }

    match client.recv().await.unwrap() {
        SoupBinMessage::Event(SoupBinEvent::EndOfSession) => {}
        SoupBinMessage::Event(other) => panic!("expected EndOfSession third, got {other:?}"),
        SoupBinMessage::Data(_) => panic!("expected EndOfSession third, got a data frame"),
    }

    let err = client.recv().await.unwrap_err();
    assert!(matches!(err, client_soupbintcp::SoupBinError::EndOfSession));

    server.await.unwrap();
}
