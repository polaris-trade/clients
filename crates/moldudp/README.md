# moldudp

MoldUDP64 protocol codec, sequence reassembler, gap request handler, and A/B arbiter

## Protocol specification

MoldUDP64 is a Nasdaq protocol. Obtain the specification from
[Nasdaq market data specifications](https://data.nasdaq.com/market-data-specifications).
Spec documents are not redistributed in this repository.

## Usage

```rust
use moldudp::{DefaultGreeter, Greeter};

let g = DefaultGreeter;
let msg = g.greet("world").unwrap();
assert_eq!(msg, "hello, world");
```

## License

Dual-licensed under either [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE), at your option.
