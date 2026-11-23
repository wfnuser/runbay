# Claude Code / Pi local verification

Verified 2026-11-23 on macOS 15.6.1, Node.js 24.18.0 and Rust 1.95.0.
Baseline: `main@362b4a00b49ff0e7005f76f9a78dbf5c34c93aad`.

## Installed clients

- Claude Code 2.1.296.
- Pi 1.6.0 from `@earendil-works/pi-coding-agent`.
- No host login/configuration was imported into sandbox homes or committed.

## Automated verification

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo run -p runbayd --locked -- self-check
cargo build -p runbayd -p runbay-beta-bench --release --locked
target/release/beta-bench \
  --runbayd-bin target/release/runbayd \
  --traces beta-bench/fixtures/recorded_llm_traces.json \
  --out target/local-smoke/beta-bench.json
```

Results: formatting and Clippy passed; 39 tests passed; all five self-check
booleans were true. Release build passed. The beta-bench correctness oracle,
direct-internet-denial check, and live daemon HTTP execution check passed.

The real-client E2E test launches both installed CLIs through the real daemon,
MVCC workspace, and macOS sandbox, replacing only the paid upstream model with
a local Anthropic protocol fixture. Both returned `RUNBAY_CLI_OK`. The gateway
received only scoped ephemeral tokens and the fixture upstream received the
host-injected test credential. Credential replacement, revocation, model scopes,
and redirect refusal have regression coverage.

Pi JSON mode can exit 0 after API failure. A real Pi run against a synthetic 401
response reproduced false success before the native-outcome fix and is now
reported as failed. Deterministic subprocess tests also cover long (>12 KB)
completion events, aborted/error/length outcomes, successful retry replacing
earlier failure, missing completion evidence, and Claude failure with exit 0.

## Live provider smoke

Two separate minimal no-tool tasks were sent through Runbay using an
Anthropic-compatible MiniMax-M3 upstream:

| Client | Verified assistant text | Run state |
| --- | --- | --- |
| Claude Code | `RUNBAY_REAL_CLAUDE_OK` | `succeeded` |
| Pi | `RUNBAY_REAL_PI_OK` | `succeeded` |

Both runs reported `sandbox_enforced`, `network_isolated`, and
`secretless_gateway` as true. These were real provider calls and consumed its
quota; the output check inspected assistant/result records, not echoed prompts.
Claude emitted an `unrecognized_model` diagnostic for the third-party model but
completed successfully. This tests Claude Code as the client, not an Anthropic
Claude model or subscription login.

## Limits

- Linux real-CLI execution is deliberately unsupported/fail-closed. Linux CI is
  separate from this macOS verification; it was not run locally.
- The tests do not certify native resume, hot process pooling, unrestricted
  shell execution, or automatic propagation of writes to the source workspace.
- Real-CLI E2E tests skip absent executables. Deterministic unit/protocol tests
  remain independent of installed CLI credentials.
- Anthropic-specific gateway usage/cache/prewarm telemetry is not implemented;
  its current counters must not be treated as billing measurements.
