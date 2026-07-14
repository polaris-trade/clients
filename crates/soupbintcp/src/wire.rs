//! Logical packet framing: `Length[2 BE u16]` + `Type[1]` + `Payload[Length-1]`.
//! Pure parser, no I/O, no config: caller holds partial packets in its own buffer
//! and re-calls `parse_packet` once more bytes land.

use crate::error::SoupBinError;

/// One SoupBinTCP packet type byte. Fixed set per protocol v3.0, won't grow.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketType {
    LoginRequest = b'L',
    LoginAccepted = b'A',
    LoginRejected = b'J',
    SequencedData = b'S',
    ServerHeartbeat = b'H',
    EndOfSession = b'Z',
    Debug = b'+',
    UnsequencedData = b'U',
    ClientHeartbeat = b'R',
    LogoutRequest = b'O',
}

impl TryFrom<u8> for PacketType {
    type Error = SoupBinError;

    fn try_from(b: u8) -> Result<Self, SoupBinError> {
        match b {
            b'L' => Ok(Self::LoginRequest),
            b'A' => Ok(Self::LoginAccepted),
            b'J' => Ok(Self::LoginRejected),
            b'S' => Ok(Self::SequencedData),
            b'H' => Ok(Self::ServerHeartbeat),
            b'Z' => Ok(Self::EndOfSession),
            b'+' => Ok(Self::Debug),
            b'U' => Ok(Self::UnsequencedData),
            b'R' => Ok(Self::ClientHeartbeat),
            b'O' => Ok(Self::LogoutRequest),
            other => Err(SoupBinError::UnknownPacketType(other)),
        }
    }
}

/// One decoded packet: type byte plus payload slice borrowed from the caller's buffer.
#[derive(Debug)]
pub struct PacketFrame<'a> {
    pub ty: PacketType,
    pub payload: &'a [u8],
}

/// Parses one logical packet from the front of `buf`.
///
/// Returns `Ok(None)` when `buf` holds only a partial packet (need more bytes).
/// Returns `Ok(Some((frame, consumed)))` when a full packet decoded; `consumed`
/// is the total byte count (`2 + Length`) the caller should drop from its buffer.
pub fn parse_packet<'a>(buf: &'a [u8]) -> Result<Option<(PacketFrame<'a>, usize)>, SoupBinError> {
    if buf.len() < 3 {
        return Ok(None);
    }
    let len = u16::from_be_bytes([buf[0], buf[1]]) as usize;
    if len < 1 {
        return Err(SoupBinError::ProtocolViolation("zero-length packet".into()));
    }
    let total = 2 + len;
    if buf.len() < total {
        return Ok(None);
    }
    let ty = PacketType::try_from(buf[2])?;
    let payload = &buf[3..total];
    Ok(Some((PacketFrame { ty, payload }, total)))
}
