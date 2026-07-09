//! Receiver config: serde-first so deployments load JSON/TOML without hand
//! rolled parsing. Every optional field defaults so a minimal file suffices.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use serde::{Deserialize, Serialize};

/// One A/B feed leg: its own bind address and, for multicast, its own NIC
/// selection (A and B often ride different interfaces).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamConfig {
    pub bind_addr: SocketAddr,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interface_v4: Option<Ipv4Addr>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interface_v6_scope_id: Option<u32>,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            interface_v4: None,
            interface_v6_scope_id: None,
        }
    }
}

/// Top-level `MoldUdpReceiver` config: one or more stream legs sharing a
/// session/sequence space, plus gap re-request and multicast knobs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MoldUdpReceiverConfig {
    pub streams: Vec<StreamConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub multicast_addr: Option<IpAddr>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rerequest_server_addr: Option<SocketAddr>,
    pub rerequest_enabled: bool,
    pub max_rerequests_per_gap_per_sec: u32,
    #[serde(with = "humantime_serde")]
    pub gap_confirm_window: Duration,
}

impl Default for MoldUdpReceiverConfig {
    fn default() -> Self {
        Self {
            streams: Vec::new(),
            multicast_addr: None,
            rerequest_server_addr: None,
            rerequest_enabled: false,
            max_rerequests_per_gap_per_sec: 4,
            gap_confirm_window: Duration::from_millis(5),
        }
    }
}
