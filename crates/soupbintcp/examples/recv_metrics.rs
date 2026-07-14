//! Drives one bounded soupbintcp session (login -> 3 data frames -> end of
//! session) over a real `TokioTransport` against an in-process mock server,
//! with observability live. Scrape http://127.0.0.1:9464/metrics while this
//! runs; it exits on its own once the mock server sends `Z`.
//!
//! `tests/common/mod.rs` is a test-only module (cargo does not build it as a
//! target on its own). It is pulled in here via an explicit `#[path]` so the
//! same `MockServer` + packet builders back both the integration tests and
//! this example, rather than duplicating them.
#[path = "../tests/common/mod.rs"]
mod common;

use client_soupbintcp::{SoupBinClient, SoupBinEvent, SoupBinMessage};
use transport_core::AsPayload;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _obs = observability::init(observability::ObsConfig {
        pipeline: observability::PipelineConfig {
            service_name: "client-soupbintcp-example".into(),
            level: "info".into(),
            otlp: None,
            logging: Some(observability::LoggingConfig {
                sinks: vec![observability::LogSink {
                    kind: observability::LogSinkKind::Stdout,
                    format: observability::LogFormat::Pretty,
                    level: None,
                }],
            }),
            tracing: None,
        },
        metrics: Some(observability::MetricsConfig {
            exporter: observability::MetricsExporter::Prometheus {
                bind: "127.0.0.1:9464".parse()?,
            },
        }),
    })?;
    // gate via observability-core directly, not the observability:: re-export:
    // git-tag identity drives cargo dedup, so that copy is a separate instance
    // from this crate's runtime observability-core, with its own METRICS_ON.
    observability_core::set_metrics_enabled(true);
    observability_core::refresh_thread_gate();
    observability_core::spawn_flusher("client.latency", "client.messages");

    let (listener, addr) = common::bind_listener().await;
    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.expect("accept");
        let mut mock = common::MockServer::new(sock);
        let mut buf = [0u8; 128];
        let _ = mock.read(&mut buf).await; // login request, ignored

        mock.write_packet(&common::login_accepted_packet("sess001", 1))
            .await;
        for i in 0..3u32 {
            let payload = format!("msg-{i}");
            mock.write_packet(&common::sequenced_data_packet(payload.as_bytes()))
                .await;
        }
        mock.write_packet(&common::end_of_session_packet()).await;
    });

    let transport = common::connect_client(addr).await;
    let mut client = SoupBinClient::connect(transport, common::test_config())
        .await
        .expect("login");

    let mut received = 0u32;
    loop {
        match client.recv().await? {
            SoupBinMessage::Data(frame) => {
                received += 1;
                println!(
                    "recv seq={} bytes={}",
                    frame.sequence(),
                    frame.payload().len()
                );
            }
            SoupBinMessage::Event(event) => {
                println!("event {event:?}");
                if matches!(event, SoupBinEvent::EndOfSession) {
                    break;
                }
            }
        }
    }

    server.await?;
    println!("done: {received} sequenced-data messages delivered");
    println!("metrics were live on http://127.0.0.1:9464/metrics during this run");
    Ok(())
}
