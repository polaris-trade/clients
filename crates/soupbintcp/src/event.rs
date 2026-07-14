//! Non-data signals `SoupBinClient` surfaces alongside sequenced `Frame`s.

/// Session lifecycle / liveness signal, distinct from a data `Frame`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SoupBinEvent {
    /// Server sent `Z` (End of Session). Socket closed, `recv` refuses further calls.
    EndOfSession,
    /// Server sent `H` (Server Heartbeat).
    HeartbeatReceived,
    /// Client sent `R` (Client Heartbeat) via `tick_heartbeat`.
    HeartbeatSent,
}

/// What `SoupBinClient::recv` hands back: either sequenced data or a lifecycle event.
#[derive(Debug)]
pub enum SoupBinMessage<'a> {
    Data(crate::frame::Frame<'a>),
    Event(SoupBinEvent),
}
