# soupbintcp

SoupBinTCP v3.0 client with wire codec, session state, heartbeats, and compressed variant

## Protocol specification

SoupBinTCP and its compressed variant are Nasdaq protocols. Obtain the
specifications from
[Nasdaq market data specifications](https://data.nasdaq.com/market-data-specifications).
Spec documents are not redistributed in this repository.

## Usage

```rust
use soupbintcp::{DefaultGreeter, Greeter};

let g = DefaultGreeter;
let msg = g.greet("world").unwrap();
assert_eq!(msg, "hello, world");
```

## License

Dual-licensed under either [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE), at your option.
