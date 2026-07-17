# Before starting work

Visibility: **PUBLIC (OSS)**. No runtime dep on, dev-dep on, or mention of any PRIVATE crate (`transport_io_uring`, `transport_afxdp`, `transport_dpdk`). See root `AGENTS.md#OSS/Private module discipline`. Rustdoc doctests may reference only public backend crates.

- Run `lat locate` to find sections relevant to your task. Read them to understand the design intent before writing code.
- Run `lat expand` on user prompts to expand any `[[refs]]` — this resolves section names to file locations and provides context.

# Post-task checklist (REQUIRED — do not skip)

After EVERY task, before responding to the user:

- [ ] Update `lat.md/` if you added or changed any functionality, architecture, tests, or behavior
- [ ] Run `lat check` — all wiki links and code refs must pass
- [ ] Do not skip these steps. Do not consider your task done until both are complete.

---

# What is lat.md?

This project uses [lat.md](https://www.npmjs.com/package/lat.md) to maintain a structured knowledge graph of its architecture, design decisions, and test specs in the `lat.md/` directory. It is a set of cross-linked markdown files that describe **what** this project does and **why** — the domain concepts, key design decisions, business logic, and test specifications. Use it to ground your work in the actual architecture rather than guessing.

# Commands

```bash
lat locate "Section Name"      # find a section by name (exact, fuzzy)
lat refs "file#Section"        # find what references a section
lat expand "user prompt text"  # expand [[refs]] to resolved locations
lat check                      # validate all links and code refs
```

Run `lat --help` when in doubt about available commands or options.

# Syntax primer

- **Section ids**: `lat.md/path/to/file#Heading#SubHeading` — full form uses project-root-relative path (e.g. `lat.md/tests/search#RAG Replay Tests`). Short form uses bare file name when unique (e.g. `search#RAG Replay Tests`, `cli#search#Indexing`).
- **Wiki links**: `[[target]]` or `[[target|alias]]` — cross-references between sections. Can also reference source code: `[[src/foo.ts#myFunction]]`.
- **Source code links**: Wiki links in `lat.md/` files can reference functions, classes, constants, and methods in TypeScript/JavaScript/Python/Rust/Go/C files. Use the full path: `[[src/config.ts#getConfigDir]]`, `[[src/server.ts#App#listen]]` (class method), `[[lib/utils.py#parse_args]]`, `[[src/lib.rs#Greeter#greet]]` (Rust impl method), `[[src/app.go#Greeter#Greet]]` (Go method), `[[src/app.h#Greeter]]` (C struct). `lat check` validates these exist.
- **Code refs**: `// @lat: [[section-id]]` (JS/TS/Rust/Go/C) or `# @lat: [[section-id]]` (Python) — ties source code to concepts

# Test specs

Key tests can be described as sections in `lat.md/` files (e.g. `tests.md`). Add frontmatter to require that every leaf section is referenced by a `// @lat:` or `# @lat:` comment in test code:

```markdown
---
lat:
  require-code-mention: true
---
# Tests

Authentication and authorization test specifications.

## User login

Verify credential validation and error handling for the login endpoint.

### Rejects expired tokens
Tokens past their expiry timestamp are rejected with 401, even if otherwise valid.

### Handles missing password
Login request without a password field returns 400 with a descriptive error.
```

Every section MUST have a description — at least one sentence explaining what the test verifies and why. Empty sections with just a heading are not acceptable. (This is a specific case of the general leading paragraph rule below.)

Each test in code should reference its spec with exactly one comment placed next to the relevant test — not at the top of the file:

```python
# @lat: [[tests#User login#Rejects expired tokens]]
def test_rejects_expired_tokens():
    ...

# @lat: [[tests#User login#Handles missing password]]
def test_handles_missing_password():
    ...
```

Do not duplicate refs. One `@lat:` comment per spec section, placed at the test that covers it. `lat check` will flag any spec section not covered by a code reference, and any code reference pointing to a nonexistent section.

# Section structure

Every section in `lat.md/` **must** have a leading paragraph — at least one sentence immediately after the heading, before any child headings or other block content. The first paragraph must be ≤250 characters (excluding `[[wiki link]]` content). This paragraph serves as the section's overview and is used in search results, command output, and RAG context — keeping it concise guarantees the section's essence is always captured.

```markdown
# Good Section

Brief overview of what this section documents and why it matters.

More detail can go in subsequent paragraphs, code blocks, or lists.

## Child heading

Details about this child topic.
```

```markdown
# Bad Section

## Child heading

Details about this child topic.
```

The second example is invalid because `Bad Section` has no leading paragraph. `lat check` validates this rule and reports errors for missing or overly long leading paragraphs.

---

# Memory And Search Protocol (MANDATORY)

All agents (Conductor, subagents, standalone) MUST follow this order before planning, implementation, review, investigation, writing code, or delegating research:

1. Call `agentmemory/memory_recall` with task, file, and module keywords when available.
2. Use `lat locate` or `lat expand` for architecture and design context when `lat.md/` exists.
3. Use Semble for semantic code search: `uvx --from "semble[mcp]" semble search "query" .`.
4. Use exposed `fff-mcp` MCP tools (`fff-grep`, `fff-find_files`, `fff-multi_grep`) for exact/file search.
5. Use `rust-analyzer` for Rust definitions, references, hover, diagnostics.
6. Fall back to regular search/read tools if preferred tools are missing, fail, or lack needed capability. State fallback reason.

Fallback rule: if preferred tool is missing, fails, or lacks needed capability, use regular tools and state reason in response or handoff.

Subagents should try exposed `fff-mcp` tools before fallback. If unavailable, use `rg` or `find` and state reason.

Conductor prompts must repeat memory/search protocol and fallback behavior for subagents.

This duplicates Claude's own global `~/.claude/CLAUDE.md` protocol on purpose — Copilot (VS Code and CLI) has no reliable user-global config inheritance, so this file is the only place Copilot will ever see it.

# Code Comment Rules (MANDATORY — WRITING, NOT REVIEW)

**Every agent writing ANY code, doc comment, or inline comment MUST follow these rules. Violations block merge.**

## Banned In All Comments

NEVER write any of these in code comments, doc comments (`///`, `//!`), or inline comments (`//`):

- `REQ-*` — requirement IDs (e.g. `REQ-P2-001`, `REQ-ARCH-022`)
- `TASK-*` — task IDs (e.g. `TASK-P2-004`)
- `AC-*` — acceptance criteria IDs
- `Phase N` or `Phase X` — phase references
- `milestone Y` — milestone references
- `work unit N` — work unit references
- Em dash `—` (U+2014)

## Allowed

- Cross-crate references: `// see pipeline-sinks::pg::raw`
- Short annotations: `TODO`, `FIXME`, `HACK`, `NOTE`, `WARNING`, `PERF`, `SECURITY`, `BUG`
- `// SAFETY:` blocks with invariant justification
- Inline `//` runs up to 4 lines when stating a non-obvious invariant or contract (never to restate code). Reviewers must not flag length alone within that bound. Recorded 2026-07-14; matches existing repo style.

## Why

Spec IDs leak process into permanent code. Git log and PR capture process history. Comments must stand alone post-merge.

## Subagent Relay (MANDATORY)

**Conductor MUST include the full "Banned In All Comments" list above in EVERY subagent handoff packet.** Subagents do not auto-load project instruction files. The handoff packet is their only source of truth for comment rules.

Implement-subagent packet must include:

```
CODE COMMENT RULES (MANDATORY — DO NOT VIOLATE):
NEVER write REQ-*, TASK-*, AC-*, Phase N, milestone Y, work unit N, or em dash (—) in any code comment, doc comment, or inline comment.
Allowed: cross-crate refs (// see crate::module), TODO, FIXME, HACK, NOTE, WARNING, PERF, SECURITY, BUG, SAFETY.
```

Code-review-subagent packet must include:

```
CODE COMMENT AUDIT (MANDATORY):
Flag every REQ-*, TASK-*, AC-*, Phase N, milestone Y, work unit N, and em dash (—) found in code comments. Any hit = NEEDS_REVISION.
```

# Logging Rules (MANDATORY: libraries emit, binaries subscribe)

Every crate logs through the [`tracing`](https://docs.rs/tracing) facade. Libraries emit events; only binaries install a subscriber. No `println!`/`eprintln!` in library code.

## Level semantics

| Level | Use for |
| ----- | ------- |
| `error` | an operation failed and the caller loses data or a connection; a human should look |
| `warn`  | degraded but continuing: a recoverable fault, a fallback taken, a gap detected |
| `info`  | coarse lifecycle: session start/end, reconnect, config resolved. Not per message |
| `debug` | detailed flow for diagnosis: re-request ticks, retry cadence, state transitions |
| `trace` | firehose, per item; off in every normal build |

## Libraries

- Emit `tracing::{error,warn,info,debug,trace}!` events only. Never install a subscriber.
- No spans on the hot path: a span allocates and takes a dispatcher lock even when no subscriber is attached. Use plain events.
- No per-message events. Log state transitions (gap detected, reconnect, session end), never once per packet/row/message. A per-message event floods and defeats filtering.
- Prefer structured fields over interpolation: `tracing::warn!(stream, %err, "...")`, not a preformatted string. Fields are filterable and become OTLP attributes for free.
- Depend on `tracing` unconditionally when the crate has something to log. Pure-decode crates that never log add no dependency.

## Binaries

- Install exactly one subscriber, once, at startup, before any work begins.
- Honor `RUST_LOG`. A binary with an observability pipeline routes through it; a plain binary installs a `tracing_subscriber::fmt` subscriber on stderr with an env filter defaulting to `warn`.
- Consumers of this workspace's libraries install their own subscriber; the libraries stay silent until they do (standard Rust).

## Lint enforcement

Every library crate root (`lib.rs`) carries, as its first inner attribute:

```rust
#![cfg_attr(not(test), deny(clippy::print_stdout, clippy::print_stderr))]
```

The `cfg_attr(not(test), ...)` form leaves unit-test code free to print. Restriction lints are off by default, so this attribute is what enables the ban; lefthook `pre-commit` and CI `-D warnings` then enforce it. Binaries, examples, benches, and integration tests are separate targets and are unaffected.

## Sanctioned print exceptions

`println!`/`eprintln!` are allowed only in:

- binary CLI product output (`main.rs` and its bin-target modules), the program's actual stdout product;
- a binary's pre-subscriber-init usage or fatal-startup `eprintln!` (before any subscriber exists);
- `build.rs` `cargo:` directives;
- the observability crate's own pre-subscriber-init stderr notices (it cannot log through a subscriber it has not installed yet).

# Post-Task Checklist (MANDATORY — ALL AGENTS, RUN BEFORE REPORTING DONE)

1. `cargo test --workspace --no-fail-fast` — must pass
2. `cargo clippy --workspace -- -D warnings` — must pass
3. `lat check` — must pass
4. `rg -n 'REQ-|TASK-|AC-' -g '*.rs' -g '!**/target/**'` — must be empty
5. `rg -n '—' -g '*.rs' -g '!**/target/**'` — must be empty
6. Update spec progress in `specs/<task-slug>/tasks.md` if any task changed state, or `../specs/<task-slug>/tasks.md` at the workspace root for cross-module specs
7. Update `lat.md/` if any module/type/function was added, removed, or renamed

If any step fails: fix it. Do NOT skip. Do NOT report done until all pass.

# Commit Message Convention

Use Conventional Commits: `type(scope): subject`.

- Always include a scope for `feat`, `fix`, `refactor`, and `perf` commits.
- Valid types: `build`, `chore`, `ci`, `docs`, `feat`, `fix`, `perf`, `refactor`, `revert`, `style`, `test`.
- Keep header length at 100 characters or less.
- Use lowercase subject style, not start-case, PascalCase, or upper-case.
- Do not suggest merge commits.

The `pr-title` workflow enforces this via `amannn/action-semantic-pull-request` — commits themselves are advisory unless you also install a local `commitlint` hook.

# Unit Test Rules

**Unit tests MUST NOT connect to external services** — databases (PostgreSQL, MSSQL), APIs, or network resources.

- **No real service connections in unit tests** — no DB connections, HTTP clients, external APIs.
- **Use `#[ignore]` for integration tests** — tests requiring real PostgreSQL/MSSQL/network services must be annotated with `#[ignore]` and only run via `cargo test -- --ignored`.
- **Use mockall for mocking** — prefer the [mockall](https://docs.rs/mockall/latest/mockall/) crate for mock implementations of traits and functions.
- **Localhost mock servers acceptable** — tests that bind to `127.0.0.1:0` with ephemeral ports and implement mock protocol servers in-process are acceptable.
- **E2E tests are exempt** — only apply these rules to unit/integration tests, not when the user explicitly asks for e2e tests.
- Run unit tests using `cargo nextest` for faster feedback loops.

When writing new tests:

1. Default to pure unit tests using test doubles/mocks.
2. Add mockall to dev-dependencies if mocking is needed: `mockall = { workspace = true }`.
3. Gate any DB/API tests with `#[ignore]`.
4. Document in test comments when `--ignored` flag is required.
5. **Test behavior, not language features** — do not write tests that verify language semantics (`Option::is_some()`, type casts, serde deserialization, default trait values). Tests should verify project-specific business logic.


---

# Code Growth Discipline (MANDATORY pre-write gate)

Workspace-wide standard (mirrors root AGENTS.md and the operator's global config). Apply before writing, not during review.

- New code: sketch module layout first. One concern per file; name the seam (trait impl, venue/channel, config vs logic). Projected >600 non-test LOC or a second concern: start as mod dir, never "split later".
- Feature work: if the result would be too coupled or push a file past ~800 non-test LOC, land a split-first refactor commit (pure moves + `pub use` re-exports, gates green, zero behavior diff), then implement. Two commits, never one mixed.
- Cohesive single impl block: fine at any size. Trigger is mixed concerns, not LOC.
- Test mod >40% of file: sibling `tests.rs`.
- Split mechanics: inherent impls split across files freely; one trait impl per file; child modules see parent-private fields, siblings need `pub(super)`; move by exact line-range extraction, never retype, never uniform-dedent (raw-string fixtures corrupt silently); keep external paths via `pub use`.
- Hot-path inline: a new concrete (non-generic) fn on a per-message/per-record path reachable across a git-dep boundary gets `#[inline]` at write time. Generic fns: nothing, MIR export covers them. `#[inline(always)]`: only with a measured delta cited.
- Validation honesty: a gate must compile the code it claims to validate. Feature-gated modules get `--features` in every validation command, spec, and CI caller.

# Useless Test Ban (MANDATORY)

A test must be able to fail from a project bug. Never write:

- Derive/stdlib restatement: thiserror `#[error]` string equality, `#[derive(Default)]` variant choice, `#[from]` conversion in isolation, derived Clone/Debug/Eq works, plain serde roundtrip with no custom impl, `Option::is_some` after `Some`.
- Field echo: constructor-stores-argument, struct-literal readback, a constant restated from the definition.
- Duplicate coverage: the branch is already covered; name the covering test and skip.
- Mock-call-count-only asserts with no observable output checked.
- False confidence: assertion weaker than the test name claims. Verify the actual contract via readback, or delete.
- Copy-pasted test doubles/encoders: they live once in `tests/support/`, per crate.
- Per-impl re-proof of a generic contract: one `assert_contract<T: Trait>` helper, called per implementation.

Keep (contract gates, not useless): wire/on-disk layout pins (size, alignment, discriminant, padding, format magic), codegen drift gates, determinism/replay gates, `Display` tests with a documented ops/log-matching rationale.

Review rule: any new test matching a banned class = NEEDS_REVISION. Relay packets for test-writing subagents must include this ban list.
