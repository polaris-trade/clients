//! Bounded demo of the receiver's gated recv instrumentation. Drives a mock
//! transport through `count_msg` (per yielded message) and `client.gaps`
//! (per detected gap), then serves a Prometheus scrape at
//! `http://127.0.0.1:9464/metrics` briefly before exiting.
//!
//! Run: `cargo run --example recv_metrics`

// Reuses the crate's own mock `DatagramSource` + wire-fixture builders
// instead of duplicating them; `tests/support/mod.rs` has no test-only deps
// (only `transport_core`, already a runtime dependency), so it compiles fine
// as a plain module here.
#[path = "../tests/support/mod.rs"]
mod support;

use std::time::Duration;

use client_moldudp::{
    MoldUdpError, MoldUdpOutcome, MoldUdpReceiver, MoldUdpReceiverConfig, StreamConfig,
};
use support::{AllocProofTransport, mold_packet};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _obs = observability::init(observability::ObsConfig {
        pipeline: observability::PipelineConfig {
            service_name: "client-moldudp-example".into(),
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
    // Call through our own direct `observability-core` dep (flush-tokio
    // enabled there), not `observability`'s re-export: that crate's own
    // internal observability-core copy doesn't request the feature, so
    // spawn_flusher is compiled out of it.
    observability_core::spawn_flusher("client.latency", "client.messages");

    let session = *b"SESSIONID1";
    let cfg = MoldUdpReceiverConfig {
        streams: vec![StreamConfig {
            bind_addr: "127.0.0.1:0".parse().expect("loopback addr"),
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut receiver = MoldUdpReceiver::<AllocProofTransport>::new(cfg).await?;
    let transport = &receiver.transports()[0];
    transport.seed(mold_packet(&session, 1, b"one"));
    transport.seed(mold_packet(&session, 2, b"two"));
    // Sequence 3 is skipped: the datagram carrying sequence 4 lands ahead of
    // `expected_next`, so this exercises the client.gaps counter too.
    transport.seed(mold_packet(&session, 4, b"four"));

    for _ in 0..2 {
        match receiver.recv().await? {
            MoldUdpOutcome::Frame(frame) => {
                println!("recv message sequence={}", frame.sequence);
            }
            other => panic!("expected data frame, got {other:?}"),
        }
    }
    match receiver.recv().await {
        Err(MoldUdpError::GapDetected) => {
            println!("gap detected, client.gaps incremented");
        }
        Ok(other) => panic!("expected gap, got {other:?}"),
        Err(e) => panic!("unexpected recv error: {e}"),
    }

    // Give the 100ms flusher tick one pass so a scrape mid-sleep shows data.
    tokio::time::sleep(Duration::from_millis(150)).await;
    println!("scrape http://127.0.0.1:9464/metrics for client.messages / client.gaps");
    Ok(())
}
