//! Control events surfaced instead of a `Frame` for non-data downstream packets.

/// Non-data downstream packet, classified by [`crate::wire::PacketKind`].
/// No `SessionOpen` variant: session id capture stays internal receiver state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoldUdpEvent {
    Heartbeat,
    EndOfSession { next_expected: u64 },
}
