#![no_main]
//! Compressed-variant fuzz: hostile bytes through `CompressedReader::feed`.
//! Corrupt input must return structured `SoupBinError`, never panic. Accepted
//! input's inflated output stays within deflate's format expansion bound.
//!
//! Crate documents no byte cap on `feed` output: `compressed.rs` guards only
//! loop progress (1_000_000 iterations), packet size cap `max_frame_size`
//! (64 KiB default) lands later, in `client.rs::take_one_packet`. Nearest hard
//! bound is deflate format itself: max expansion ~1032:1 (one length/distance
//! pair emits at most 258 bytes, costs at least ~2 bits of input).

use client_soupbintcp::compressed::CompressedReader;
use libfuzzer_sys::fuzz_target;

const MAX_INFLATE_RATIO: usize = 1032;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    // first byte picks split point; two feeds exercise persistent inflate state
    let rest = &data[1..];
    let cut = usize::from(data[0]) % (rest.len() + 1);
    let mut reader = CompressedReader::new(4096);
    let mut total_out = 0usize;
    for chunk in [&rest[..cut], &rest[cut..]] {
        if chunk.is_empty() {
            continue;
        }
        match reader.feed(chunk) {
            Ok(out) => total_out += out.len(),
            // structured reject on corrupt stream; later feeds pointless
            Err(_) => return,
        }
    }
    assert!(
        total_out <= rest.len() * MAX_INFLATE_RATIO + 1024,
        "inflated {total_out} bytes from {} input, past deflate expansion bound",
        rest.len()
    );
});
