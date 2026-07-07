//! Config serde roundtrips: guards on-disk schema stability for JSON and TOML.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use client_moldudp::{MoldUdpReceiverConfig, StreamConfig};

fn sample_config() -> MoldUdpReceiverConfig {
    MoldUdpReceiverConfig {
        streams: vec![
            StreamConfig {
                bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 30_000),
                interface_v4: Some(Ipv4Addr::new(10, 0, 0, 1)),
                interface_v6_scope_id: None,
            },
            StreamConfig::default(),
        ],
        multicast_addr: Some(IpAddr::V4(Ipv4Addr::new(239, 192, 0, 1))),
        rerequest_server_addr: Some(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
            40_000,
        )),
        rerequest_enabled: true,
        max_rerequests_per_gap_per_sec: 8,
        gap_confirm_window: Duration::from_millis(7),
    }
}

#[test]
fn json_roundtrip_preserves_all_fields() {
    let cfg = sample_config();
    let json = serde_json::to_string(&cfg).expect("serialize json");
    let back: MoldUdpReceiverConfig = serde_json::from_str(&json).expect("deserialize json");
    assert_eq!(cfg, back);
}

#[test]
fn toml_roundtrip_preserves_all_fields() {
    let cfg = sample_config();
    let text = toml::to_string(&cfg).expect("serialize toml");
    let back: MoldUdpReceiverConfig = toml::from_str(&text).expect("deserialize toml");
    assert_eq!(cfg, back);
}

#[test]
fn default_config_has_documented_defaults() {
    let cfg = MoldUdpReceiverConfig::default();
    assert!(cfg.streams.is_empty());
    assert!(cfg.multicast_addr.is_none());
    assert!(cfg.rerequest_server_addr.is_none());
    assert!(!cfg.rerequest_enabled);
    assert_eq!(cfg.max_rerequests_per_gap_per_sec, 4);
    assert_eq!(cfg.gap_confirm_window, Duration::from_millis(5));
}

#[test]
fn minimal_json_fills_in_defaults() {
    // only the mandatory stream bind address is present; everything else
    // must fall back to MoldUdpReceiverConfig::default().
    let json = r#"{"streams":[{"bind_addr":"127.0.0.1:30000"}]}"#;
    let cfg: MoldUdpReceiverConfig = serde_json::from_str(json).expect("minimal config decodes");
    assert_eq!(cfg.streams.len(), 1);
    assert_eq!(cfg.max_rerequests_per_gap_per_sec, 4);
    assert_eq!(cfg.gap_confirm_window, Duration::from_millis(5));
}
