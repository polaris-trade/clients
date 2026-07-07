//! Sequence reassembler: a fixed-capacity slot ring keyed by `seq % capacity`.
//! Slots hold an owned, backend-agnostic slab handle `S`, not a private copy,
//! so drain never re-copies bytes; it just moves ownership out of the slot.

use crate::error::MoldUdpError;

/// One ring slot. `payload` is `None` when the slot is empty; `seq` is only
/// meaningful while `payload.is_some()`.
pub struct Slot<S> {
    pub seq: u64,
    pub payload: Option<S>,
}

/// O(1)-insert reassembler over a fixed ring of `capacity` slots. Drains
/// contiguous runs starting at `expected_next`; drops stale duplicates below
/// it; rejects an insert that would silently clobber a still-pending slot.
pub struct SequenceReassembler<S> {
    slots: Vec<Slot<S>>,
    capacity: u64,
    expected_next: u64,
}

impl<S> SequenceReassembler<S> {
    /// MoldUDP64 sessions start sequence numbering at 1.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be non-zero");
        let slots = (0..capacity)
            .map(|_| Slot {
                seq: 0,
                payload: None,
            })
            .collect();
        Self {
            slots,
            capacity: capacity as u64,
            expected_next: 1,
        }
    }

    pub fn expected_next(&self) -> u64 {
        self.expected_next
    }

    fn slot_index(&self, seq: u64) -> usize {
        (seq % self.capacity) as usize
    }
}

impl<S: AsRef<[u8]> + 'static> SequenceReassembler<S> {
    /// Insert `seq`'s slab. Stale duplicates (`seq < expected_next`) are
    /// dropped silently. An out-of-order insert that collides with a
    /// different still-pending slot returns `ReassemblyBufferFull` instead of
    /// evicting the existing entry. A successful insert that lands exactly on
    /// `expected_next` returns a [`DrainCursor`] over the newly contiguous run.
    pub fn insert(
        &mut self,
        seq: u64,
        slab: S,
    ) -> Result<Option<DrainCursor<'_, S>>, MoldUdpError> {
        if seq < self.expected_next {
            return Ok(None);
        }
        let idx = self.slot_index(seq);
        let slot = &self.slots[idx];
        if slot.payload.is_some() {
            if slot.seq == seq {
                return Ok(None); // exact duplicate already buffered
            }
            return Err(MoldUdpError::ReassemblyBufferFull {
                capacity: self.capacity as usize,
            });
        }
        let slot = &mut self.slots[idx];
        slot.seq = seq;
        slot.payload = Some(slab);
        if seq == self.expected_next {
            Ok(Some(DrainCursor { inner: self }))
        } else {
            Ok(None)
        }
    }
}

/// Lazily drains the contiguous run starting at the reassembler's
/// `expected_next` at the time [`SequenceReassembler::insert`] returned it.
/// Mirrors `Vec::drain`: dropping the cursor without fully iterating it still
/// finishes advancing `expected_next` past the whole run.
pub struct DrainCursor<'a, S> {
    inner: &'a mut SequenceReassembler<S>,
}

impl<'a, S> Iterator for DrainCursor<'a, S> {
    type Item = S;

    fn next(&mut self) -> Option<S> {
        let idx = self.inner.slot_index(self.inner.expected_next);
        let slot = &mut self.inner.slots[idx];
        if slot.seq != self.inner.expected_next {
            return None;
        }
        let payload = slot.payload.take()?;
        self.inner.expected_next += 1;
        Some(payload)
    }
}

impl<'a, S> Drop for DrainCursor<'a, S> {
    fn drop(&mut self) {
        while self.next().is_some() {}
    }
}
