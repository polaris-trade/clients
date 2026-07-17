# Fuzz targets

cargo-fuzz workspace under `fuzz/`: detached crate with its own `[workspace]` table, excluded from the parent workspace, carrying three libFuzzer targets over both crates' byte-level framing surfaces. Build with `cargo +nightly fuzz build`.

Corpus policy: `fuzz/corpus/<target>/` holds committed synthetic seeds plus minimized regression inputs. This repo is public, so capture-derived bytes are never committed. Seeds mirror the crates' own test packet builders (`crates/moldudp/tests/support/mod.rs`, `crates/soupbintcp/tests/common/mod.rs`). Every fixed crash lands here as a named regression input so each PR replays it via `cargo fuzz run <target> -- -runs=0`.

## moldudp_wire

Arbitrary datagram bytes into [[crates/moldudp/src/wire.rs#parse_header]] and the block walk via [[crates/moldudp/src/wire.rs#DownstreamHeader#blocks]].

Oracle: rejects return structured [[crates/moldudp/src/error.rs#MoldUdpError]], never a panic. On accept, the walk terminates, every yielded block sits fully inside the datagram past the header and its 2-byte length prefix, and the yield count never exceeds the header's declared `message_count`.

## soupbintcp_framing

Same byte stream parsed contiguous vs chunked through [[crates/soupbintcp/src/wire.rs#parse_packet]] accumulation, mirroring the client's own decode-buffer loop. Split points derive from leading input bytes, no RNG.

Oracle: both passes must produce the identical frame sequence, identical consumed byte total, and the identical terminal error. Sensitivity proven by a temporary off-by-one in the chunk-feed loop tripping the equality assert on committed seeds.

## soupbintcp_compressed

Hostile bytes into [[crates/soupbintcp/src/compressed.rs#CompressedReader#feed]] (feature `compressed`), split into two feeds so the persistent zlib inflate state carries across calls.

Oracle: corrupt input returns a structured [[crates/soupbintcp/src/error.rs#SoupBinError]]; every accepted feed's inflated output stays within the reader's hard cap (the capacity passed to `CompressedReader::new`). A zlib bomb hits `FrameTooLarge`, a structured error, rather than growing without bound.
