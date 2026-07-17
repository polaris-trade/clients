//! Streaming zlib inflate for the NASDAQ compressed SoupBinTCP feed variant.
//! Server->client only: upstream writes in `client.rs` bypass this entirely.

use bytes::BytesMut;
use flate2::{Decompress, FlushDecompress, Status};

use crate::error::SoupBinError;

/// Wraps a `flate2::Decompress` (zlib framing) over the transport read side.
/// `feed` inflates one chunk at a time; `inflated` holds only the latest
/// chunk's output, mirroring the transport's own per-call frame semantics.
pub struct CompressedReader {
    inflator: Decompress,
    inflated: BytesMut,
    // hard cap on one feed's inflated output; a zlib bomb hits this instead of
    // growing scratch without bound. sized from the client decode budget.
    max_inflated: usize,
}

impl CompressedReader {
    /// `inflated_capacity` is both the starting buffer size and the hard ceiling
    /// on one `feed`'s output. An inflate that exceeds it errors rather than
    /// allocating without bound, so a hostile or corrupt zlib stream cannot
    /// exhaust memory. Raise the client decode-buffer config if legitimate reads
    /// ever need more headroom.
    pub fn new(inflated_capacity: usize) -> Self {
        Self {
            inflator: Decompress::new(true), // zlib framing (header + adler32), not raw deflate
            inflated: BytesMut::with_capacity(inflated_capacity),
            max_inflated: inflated_capacity,
        }
    }

    /// Inflates `compressed` and returns the decoded bytes produced by this call.
    /// Valid only until the next `feed` call.
    pub fn feed(&mut self, compressed: &[u8]) -> Result<&[u8], SoupBinError> {
        self.inflated.clear();
        let mut scratch = Vec::with_capacity(self.inflated.capacity().max(4096));
        let mut input = compressed;
        let mut guard = 0usize;
        loop {
            guard += 1;
            if guard > 1_000_000 {
                return Err(SoupBinError::ProtocolViolation(
                    "zlib inflate made no progress".into(),
                ));
            }
            if scratch.len() == scratch.capacity() {
                scratch.reserve(scratch.capacity().max(4096));
            }
            let before_in = self.inflator.total_in();
            let status = self
                .inflator
                .decompress_vec(input, &mut scratch, FlushDecompress::None)
                .map_err(|e| {
                    SoupBinError::ProtocolViolation(format!("zlib inflate failed: {e}"))
                })?;
            let consumed = (self.inflator.total_in() - before_in) as usize;
            input = &input[consumed..];
            if scratch.len() > self.max_inflated {
                return Err(SoupBinError::FrameTooLarge {
                    size: scratch.len(),
                    max: self.max_inflated,
                });
            }
            if status == Status::StreamEnd || input.is_empty() {
                break;
            }
        }
        self.inflated.extend_from_slice(&scratch);
        Ok(&self.inflated)
    }
}
