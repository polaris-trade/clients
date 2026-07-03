# moldudp

MoldUDP64 protocol codec, sequence reassembler, gap request handler, and A/B arbiter

## Usage

```rust
use moldudp::{DefaultGreeter, Greeter};

let g = DefaultGreeter;
let msg = g.greet("world").unwrap();
assert_eq!(msg, "hello, world");
```

## License

MIT — see the workspace-root `LICENSE`.
