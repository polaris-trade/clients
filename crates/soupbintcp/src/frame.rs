//! `Frame<'a>`: the protocol-side payload type `SoupBinClient::recv` hands back,
//! borrowed from the client's decode buffer. SoupBinTCP has no A/B feeds, so
//! `stream_id` is always 0.

use transport_core::AsPayload;

#[derive(Debug)]
pub struct Frame<'a> {
    pub(crate) payload: &'a [u8],
    pub(crate) sequence: u64,
}

impl<'a> AsPayload for Frame<'a> {
    fn payload(&self) -> &[u8] {
        self.payload
    }

    fn sequence(&self) -> u64 {
        self.sequence
    }

    fn stream_id(&self) -> u8 {
        0
    }
}
