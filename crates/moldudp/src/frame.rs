//! Protocol-side frame handed to consumers. `Frame` borrows straight from the
//! receiver's still-owned datagram; `MessageView`/`OwnedFrame` share ownership
//! of it via `Arc` for buffering or cross-thread handoff.

use std::sync::Arc;

use transport_core::{AsPayload, RecvFrame};

/// One reassembled, in-order MoldUDP64 message, borrowed from the receiver.
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

/// A message that shares ownership of its backing datagram `F` via `Arc`,
/// instead of borrowing it. Built when a message must outlive the datagram's
/// original owner: reorder buffering (out-of-order arrival) or a cross-thread
/// handoff. Cheap to clone (refcount bump, no copy of the bytes).
pub struct MessageView<F> {
    datagram: Arc<F>,
    offset: usize,
    len: usize,
}

impl<F> MessageView<F> {
    pub fn new(datagram: Arc<F>, offset: usize, len: usize) -> Self {
        Self {
            datagram,
            offset,
            len,
        }
    }
}

impl<F> Clone for MessageView<F> {
    fn clone(&self) -> Self {
        Self {
            datagram: Arc::clone(&self.datagram),
            offset: self.offset,
            len: self.len,
        }
    }
}

impl<F: RecvFrame> AsRef<[u8]> for MessageView<F> {
    fn as_ref(&self) -> &[u8] {
        &self.datagram.payload()[self.offset..self.offset + self.len]
    }
}

/// Owned counterpart to [`Frame`]: carries a [`MessageView`] instead of a
/// borrow, so it is `Send` and can cross a thread boundary (e.g. into a
/// sharded engine core) without copying the message bytes.
pub struct OwnedFrame<F> {
    pub view: MessageView<F>,
    pub sequence: u64,
    pub stream_id: u8,
}

impl<F: RecvFrame> AsPayload for OwnedFrame<F> {
    fn payload(&self) -> &[u8] {
        self.view.as_ref()
    }

    fn sequence(&self) -> u64 {
        self.sequence
    }

    fn stream_id(&self) -> u8 {
        self.stream_id
    }
}
