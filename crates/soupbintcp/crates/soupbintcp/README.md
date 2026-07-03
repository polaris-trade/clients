# soupbintcp

SoupBinTCP v3.0 client with wire codec, session state, heartbeats, and compressed variant

## Usage

```rust
use soupbintcp::{DefaultGreeter, Greeter};

let g = DefaultGreeter;
let msg = g.greet("world").unwrap();
assert_eq!(msg, "hello, world");
```

## License

MIT — see the workspace-root `LICENSE`.
