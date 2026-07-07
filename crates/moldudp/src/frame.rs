//! Protocol-side frame handed to consumers. Borrows straight from the
//! receiver's owned message bytes, no per-message copy at hand-off time.

use transport_core::AsPayload;

/// One reassembled, in-order MoldUDP64 message.
#[derive(Debug, Clone, Copy)]
pub struct Frame<'a> {
    pub payload: &'a [u8],
    pub sequence: u64,
    pub stream_id: u8,
}

impl<'a> AsPayload for Frame<'a> {
    fn payload(&self) -> &[u8] {
        self.payload
    }

    fn sequence(&self) -> u64 {
        self.sequence
    }

    fn stream_id(&self) -> u8 {
        self.stream_id
    }
}
