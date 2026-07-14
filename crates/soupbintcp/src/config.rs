//! `SoupBinClientConfig`: knobs for login handshake, heartbeat cadence, frame limits.
//! Durations use `humantime_serde` so config files write `"30s"` not nanos.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Config for one `SoupBinClient` session. `Default` gives working values for
/// every field; `#[serde(default)]` means a config file can omit any subset
/// of fields and still deserialize, falling back to those defaults.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct SoupBinClientConfig {
    pub username: String,
    pub password: String,
    pub requested_session: String,
    /// Sequence to request first at login. `1` replays the session from the
    /// start (default); `0` starts at the most recently generated message
    /// (live tail, no backlog); on reconnect set this to
    /// `next_expected_sequence()` to resume where the dropped socket left off.
    pub requested_sequence_number: u64,

    #[serde(with = "humantime_serde")]
    pub login_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub heartbeat_interval: Duration,
    #[serde(with = "humantime_serde")]
    pub heartbeat_timeout: Duration,

    /// Max total packet size (2-byte length prefix + body) before `FrameTooLarge`.
    pub max_frame_size: usize,
    /// Initial capacity for the decode buffer (and, under `compressed`, the inflate buffer).
    pub decode_buf_capacity: usize,
}

impl Default for SoupBinClientConfig {
    fn default() -> Self {
        Self {
            username: String::new(),
            password: String::new(),
            requested_session: String::new(),
            requested_sequence_number: 1,
            login_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(1),
            heartbeat_timeout: Duration::from_secs(15),
            max_frame_size: 64 * 1024,
            decode_buf_capacity: 64 * 1024,
        }
    }
}
