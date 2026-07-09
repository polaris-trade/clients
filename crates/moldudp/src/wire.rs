//! MoldUDP64 downstream packet wire codec. Header parse plus message block
//! iteration, both zero-alloc: everything borrows straight from the caller's
//! datagram slice.

use crate::error::MoldUdpError;

/// 20-byte Downstream Packet Header: `Session[10]`, `Sequence[8 BE]`, `MessageCount[2 BE]`.
pub const HEADER_LEN: usize = 20;

/// Above this, a datagram exceeds any sane single-UDP-packet MoldUDP64 payload.
const MAX_DOWNSTREAM_DATAGRAM: usize = 64 * 1024;

/// Parsed MoldUDP64 downstream packet header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DownstreamHeader {
    pub session: [u8; 10],
    pub sequence: u64,
    pub message_count: u16,
}

/// What a downstream packet's `MessageCount` field classifies it as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketKind {
    /// `MessageCount == 0`; `sequence` carries next-expected, no message blocks.
    Heartbeat,
    /// `MessageCount == 0xFFFF`; `sequence` carries next-expected, no message blocks.
    EndOfSession,
    /// `MessageCount in 1..=0x7FFE`; `sequence` is the first message's seq.
    Data,
}

impl DownstreamHeader {
    /// Classify by `message_count` per MoldUDP64 heartbeat/end-of-session convention.
    pub fn kind(&self) -> PacketKind {
        match self.message_count {
            0 => PacketKind::Heartbeat,
            0xFFFF => PacketKind::EndOfSession,
            _ => PacketKind::Data,
        }
    }

    /// Iterate this header's message blocks out of `datagram` (the full packet
    /// this header was parsed from, header bytes included).
    pub fn blocks<'a>(&self, datagram: &'a [u8]) -> MessageBlockIter<'a> {
        let body = datagram.get(HEADER_LEN..).unwrap_or(&[]);
        MessageBlockIter::new(body, self.message_count)
    }
}

/// Reject datagrams shorter than [`HEADER_LEN`] or larger than any sane single
/// UDP datagram before touching the byte layout.
pub fn parse_header(buf: &[u8]) -> Result<DownstreamHeader, MoldUdpError> {
    if buf.len() < HEADER_LEN {
        return Err(MoldUdpError::PacketTooShort);
    }
    if buf.len() > MAX_DOWNSTREAM_DATAGRAM {
        return Err(MoldUdpError::PacketTooLarge);
    }
    let mut session = [0u8; 10];
    session.copy_from_slice(&buf[0..10]);
    // unwrap: slice lengths fixed by range bounds above, conversion cannot fail.
    let sequence = u64::from_be_bytes(buf[10..18].try_into().unwrap());
    let message_count = u16::from_be_bytes(buf[18..20].try_into().unwrap());
    Ok(DownstreamHeader {
        session,
        sequence,
        message_count,
    })
}

/// Borrows message block payloads straight from the datagram slice; no copy.
/// Each block is `Length[2 BE]` followed by `Length` payload bytes. Yields
/// `(offset, block)`, `offset` being the block's byte position within the
/// *full* datagram (header included), so a caller can build a
/// [`crate::frame::MessageView`] without re-deriving pointer arithmetic.
pub struct MessageBlockIter<'a> {
    remaining: &'a [u8],
    blocks_left: u16,
    truncated: bool,
    next_offset: usize,
}

impl<'a> MessageBlockIter<'a> {
    /// `payload` is the packet body after the 20-byte header.
    pub fn new(payload: &'a [u8], message_count: u16) -> Self {
        Self {
            remaining: payload,
            blocks_left: message_count,
            truncated: false,
            next_offset: HEADER_LEN,
        }
    }
}

impl<'a> Iterator for MessageBlockIter<'a> {
    type Item = Result<(usize, &'a [u8]), MoldUdpError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.truncated || self.blocks_left == 0 {
            return None;
        }
        if self.remaining.len() < 2 {
            self.truncated = true;
            return Some(Err(MoldUdpError::PacketTooShort));
        }
        let len = u16::from_be_bytes([self.remaining[0], self.remaining[1]]) as usize;
        let rest = &self.remaining[2..];
        if rest.len() < len {
            self.truncated = true;
            return Some(Err(MoldUdpError::PacketTooShort));
        }
        let (block, rest) = rest.split_at(len);
        let offset = self.next_offset + 2;
        self.next_offset += 2 + len;
        self.remaining = rest;
        self.blocks_left -= 1;
        Some(Ok((offset, block)))
    }
}
