//! Control events surfaced instead of a `Frame` for non-data downstream packets.

/// Non-data downstream packet, classified by [`crate::wire::PacketKind`].
/// No `SessionOpen` variant: session id capture stays internal receiver state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoldUdpEvent {
    /// Server heartbeat. `next_expected` is the sequence the server will send
    /// next; if it exceeds the receiver's own expected sequence, the tail was
    /// lost during quiet traffic and gets recorded as a gap. This is why
    /// MoldUDP64 heartbeats carry a sequence: loss detection when idle.
    Heartbeat {
        next_expected: u64,
    },
    EndOfSession {
        next_expected: u64,
    },
}
