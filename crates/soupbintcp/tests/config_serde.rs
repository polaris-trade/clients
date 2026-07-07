//! `SoupBinClientConfig` JSON + TOML roundtrip, and `Default` on partial input.

use std::time::Duration;

use client_soupbintcp::SoupBinClientConfig;

fn sample() -> SoupBinClientConfig {
    SoupBinClientConfig {
        username: "user01".into(),
        password: "pass12345".into(),
        requested_session: "sess001".into(),
        requested_sequence_number: 42,
        login_timeout: Duration::from_secs(30),
        heartbeat_interval: Duration::from_secs(1),
        heartbeat_timeout: Duration::from_secs(15),
        max_frame_size: 65536,
        decode_buf_capacity: 65536,
    }
}

#[test]
fn json_roundtrip() {
    let cfg = sample();
    let json = serde_json::to_string(&cfg).expect("serialize");
    let back: SoupBinClientConfig = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back.username, cfg.username);
    assert_eq!(
        back.requested_sequence_number,
        cfg.requested_sequence_number
    );
    assert_eq!(back.login_timeout, cfg.login_timeout);
    assert_eq!(back.heartbeat_interval, cfg.heartbeat_interval);
    assert_eq!(back.heartbeat_timeout, cfg.heartbeat_timeout);
}

#[test]
fn toml_roundtrip() {
    let cfg = sample();
    let toml_str = toml::to_string(&cfg).expect("serialize");
    let back: SoupBinClientConfig = toml::from_str(&toml_str).expect("deserialize");
    assert_eq!(back.requested_session, cfg.requested_session);
    assert_eq!(back.login_timeout, cfg.login_timeout);
    assert_eq!(back.max_frame_size, cfg.max_frame_size);
}

#[test]
fn toml_durations_are_human_readable() {
    // humantime_serde must write "30s", not a raw nanos struct: this is the
    // whole reason the config uses it instead of derived Duration serde.
    let cfg = sample();
    let toml_str = toml::to_string(&cfg).expect("serialize");
    assert!(toml_str.contains("login_timeout = \"30s\""), "{toml_str}");
}

#[test]
fn missing_fields_fall_back_to_default() {
    // #[serde(default)] on the struct: a config naming only username/password
    // still deserializes, with every omitted field taking its Default value.
    let json = r#"{ "username": "u", "password": "p" }"#;
    let cfg: SoupBinClientConfig = serde_json::from_str(json).expect("deserialize");
    let default_cfg = SoupBinClientConfig::default();
    assert_eq!(cfg.username, "u");
    assert_eq!(cfg.password, "p");
    assert_eq!(cfg.login_timeout, default_cfg.login_timeout);
    assert_eq!(cfg.heartbeat_interval, default_cfg.heartbeat_interval);
    assert_eq!(cfg.max_frame_size, default_cfg.max_frame_size);
}
