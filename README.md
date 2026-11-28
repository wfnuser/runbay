# Runbay

[![CI](https://github.com/wfnuser/runbay/actions/workflows/ci.yml/badge.svg)](https://github.com/wfnuser/runbay/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**A local-first Rust runtime daemon and systems substrate for isolating, supervising, and scheduling coding-agent sessions.**

Runbay provides an authenticated loopback execution daemon (`runbayd`) over HTTP + SSE backed by OS-level subprocess sandboxing, copy-on-write MVCC workspace snapshots, a host-side secretless model gateway, and bounded live execution supervision.

---

## Implementation Status (Implemented vs Simulated vs Planned)

| Area | Status | Notes |
| :--- | :--- | :--- |
| Loopback Control Daemon (`runbayd serve`) | **Implemented** | Constant-time Bearer auth (`rb_ctl_*`), body/ID/workspace boundary checks, durable SQLite registry, `/health`, `/v1/status`, `/agents`, `/runs`, SSE `/events` with `Last-Event-ID` replay, `POST /runs/:id/cancel` |
| Runtime Execution & Process-Group Cleanup | **Implemented** | Bounded live concurrency & queue (`429` when full), isolated per-run MVCC CoW workspace + RAII `SnapshotLease`, POSIX process-group (`setpgid` + `killpg`) cancel/timeout reaping, explicit `daemon_restarted` startup recovery |
| Runtime Adapters | **Implemented (`one_shot`)** | `codex`, `claude` (Claude Code), and `pi` CLI adapters through the secretless gateway on macOS; TCP-only CLI adapters fail closed on Linux `--unshare-net`. Opt-in `--enable-fixture-adapter` deterministic worker for tests/CI. See [Claude and Pi](#claude-code-and-pi) for installation and limits. |
| Cross-Platform OS Sandbox (`runbay-sandbox`) | **Implemented** | macOS Seatbelt (`sandbox-exec`) with loopback TCP gateway allowlist; Linux bubblewrap or native Landlock + seccomp with a private network namespace and a bound Unix-domain-socket gateway (fails closed if TCP loopback is requested with `allow_network=false`) |
| Secretless Model Gateway (`runbay-gateway`) | **Implemented** | OpenAI-compatible routes and Anthropic `/v1/messages` over TCP loopback + Unix socket; real keys injected only on outbound hops, 256-bit `rb_*` tokens, route/model allowlists, immediate revocation, and no upstream redirects |
| Three-Layer MVCC / CoW Store (`runbay-mvcc`) | **Implemented** | L0 CAS hardlinks, L1 APFS `clonefile(2)` / Linux `FICLONE` CoW snapshots & branching, L2 shared SQLite FTS5 + dense index |
| Policy Scheduler (`runbay-scheduler`) | **Live primitives + Simulated policies** | `CancelToken` and `GuardedRunSink` are live in `runbayd`; `fifo` / `priority_aging` / `warm_env_affinity` / `dual_cache_ttl_aware` are currently discrete-event simulations in `beta-bench` |
| Hot / Warm / Cold Process Pool & Native Duplex Resume | **Planned** | Tracked in [Issue #1](https://github.com/wfnuser/runbay/issues/1) and [Issue #5](https://github.com/wfnuser/runbay/issues/5) |
| Speculative Workspace Branch HTTP APIs (`/workspaces/*`) | **Planned** | Library primitives exist in `runbay-mvcc`; HTTP routes tracked in [Issue #2](https://github.com/wfnuser/runbay/issues/2) |

---

## Implemented Control API (`runbayd serve`)

All endpoints except `GET /health` require `Authorization: Bearer <control_token>`.

```text
GET    /health                  Unauthenticated liveness probe
GET    /v1/status               Daemon capabilities, adapters, sandbox & queue metrics
POST   /agents                  Register agent {agent_id, runtime, workspace_dir, config?}
GET    /agents                  List registered agents and MVCC lease state
GET    /agents/:id              Inspect one agent
DELETE /agents/:id              Cancel active runs and delete agent state
POST   /agents/:id/run          Submit a run {prompt, priority?, required_rev?, timeout_secs?, allowed_model?, sandbox_mode?, resource_limits?}
GET    /agents/:id/events       SSE stream scoped to agent
GET    /runs                    List recent runs (?agent_id=...)
GET    /runs/:run_id            Query run state, exit code, sandbox/gateway flags, and event counts
POST   /runs/:run_id/cancel     Idempotently cancel queued/running run, kill+reap process group, revoke token
GET    /runs/:run_id/events     SSE event stream with Last-Event-ID / ?after_seq= replay (disconnect-safe)
```

---

## Platform Support & Limits

| Capability | macOS (Apple Silicon / Intel) | Linux (x86_64 / arm64) |
| :--- | :--- | :--- |
| OS Subprocess Sandbox | `sandbox-exec` (Seatbelt) | bubblewrap or native Landlock + seccomp-BPF |
| Secretless Gateway Under `allow_network=false` | Loopback TCP (`localhost:<port>`) + Unix socket | One bound Unix socket in a private network namespace. TCP-only loopback requests fail closed |
| `codex` One-Shot Adapter (Gateway Mode) | Supported (`codex exec --json --ephemeral` via Seatbelt TCP loopback gateway) | Fails closed when `allow_network=false` (Codex CLI requires TCP loopback, not Unix sockets) |
| `claude` / `pi` One-Shot Adapters | Supported through an Anthropic-compatible gateway | Fails closed; these CLIs require TCP rather than the isolated Unix socket channel |
| Per-run CPU, memory and task limits | Unavailable | Optional delegated cgroup v2 subtree |
| L1 CoW Cloning | `clonefile(2)` (APFS) | `ioctl(FICLONE)` (Btrfs/XFS) / fallback copy |
| L0 CAS Hardlink Sharing & L2 MVCC Index | Supported | Supported |

---

## Quick Start

Requires Rust stable (`rustc` 1.80+). Linux uses bubblewrap when available and can also use the native launcher described below.

```bash
# 1. Run all workspace unit, OS sandbox, and black-box daemon E2E tests (zero API cost)
cargo test --workspace --locked

# 2. Run the daemon self-check (verifies MVCC, OS sandbox, gateway, and cancel guard)
cargo run -p runbayd --locked -- self-check

# 3. Start the authenticated loopback daemon (auto-generates control token in state-dir/control/control.token)
RUNBAY_UPSTREAM_API_KEY="your-upstream-key" \
cargo run -p runbayd --release --locked -- serve \
  --port 7310 \
  --state-dir ~/.runbay \
  --allow-workspace-root "$PWD" \
  --upstream-url https://api.openai.com/v1

# 4. Run the reproducible Beta Bench + live HTTP daemon check
cargo build -p runbayd -p runbay-beta-bench --release --locked
target/release/beta-bench \
  --runbayd-bin target/release/runbayd \
  --traces beta-bench/fixtures/recorded_llm_traces.json \
  --out beta-bench/artifacts/beta_bench_results.json
```

See [docs/DESIGN.md](docs/DESIGN.md) for architecture details and [beta-bench/README.md](beta-bench/README.md) for benchmark methodology and measurement categories.

## Claude Code and Pi

Install Claude Code separately and install the maintained Pi package (Node.js 22.19+):

```bash
npm install -g @earendil-works/pi-coding-agent@1.6.0
claude --version
pi --version
```

Verified live-run CLI versions: Claude Code **2.1.239**, Pi **1.6.0**. Older CLIs may not support the isolation flags used here. The deprecated `@mariozechner/pi-coding-agent` package is not the tested target. Check `/v1/status` for the exact executable selected by the daemon's PATH, which may differ from an interactive shell.

Runbay discovers the executables on its host PATH. Optional `RUNBAY_CLAUDE_BIN` and `RUNBAY_PI_BIN` environment variables select explicit executables; an invalid override fails instead of silently choosing another installation. npm scripts are launched with the host's absolute Node executable so the sandbox's minimal PATH works with nvm installs.

Start the daemon with an **Anthropic-compatible** upstream and a host-side API key:

```bash
# Set RUNBAY_UPSTREAM_API_KEY through your usual secret-management mechanism.
cargo run -p runbayd --locked -- serve \
  --port 7310 \
  --state-dir /absolute/path/to/runbay-state \
  --allow-workspace-root /absolute/path/to/test-workspace \
  --upstream-url https://api.anthropic.com/v1
```

For a proxy whose Claude base URL is `https://provider.example/anthropic`, use `--upstream-url https://provider.example/anthropic/v1`. One daemon currently has one upstream; it does not route different providers independently. Claude Code is the client, not a guarantee that the upstream model is made by Anthropic.

Using the generated control token, register either runtime and submit a task (replace paths and model ID):

```bash
# RUNBAY_CONTROL_TOKEN must match runbay-state/control/control.token.
curl -H "Authorization: Bearer $RUNBAY_CONTROL_TOKEN" \
  --json '{"agent_id":"pi-demo","runtime":"pi","workspace_dir":"/absolute/path/to/test-workspace"}' \
  http://127.0.0.1:7310/agents

curl -H "Authorization: Bearer $RUNBAY_CONTROL_TOKEN" \
  --json '{"prompt":"Reply with RUNBAY_OK only","allowed_model":"<exact-upstream-model-id>","timeout_secs":60}' \
  http://127.0.0.1:7310/agents/pi-demo/run

# For Claude Code, register another agent with runtime "claude".
# Use the returned run_id to stream output or cancel the run:
curl -N -H "Authorization: Bearer $RUNBAY_CONTROL_TOKEN" \
  http://127.0.0.1:7310/runs/<run_id>/events
curl -X POST -H "Authorization: Bearer $RUNBAY_CONTROL_TOKEN" \
  http://127.0.0.1:7310/runs/<run_id>/cancel
```

Limits and security behavior:

- These are **one-shot, headless JSON** runs, not terminal windows or persistent conversations. SSE carries each CLI's native JSON in `agent.event` records.
- Native completion results are checked in addition to process exit codes. Pi JSON-mode API errors, aborts, and output-length limits are failures even when the CLI exits with code 0; a later successful retry supersedes an earlier error.
- `allowed_model` is required and must be the exact model ID the upstream accepts, without client-only aliases or context-window suffixes.
- The default `read-only` mode **disables all agent tools**. Explicit `sandbox_mode: "workspace-write"` enables file read/edit/write tools in the isolated CoW workspace; shell tools remain disabled. Changes are not automatically applied to the original workspace.
- Host CLI login/OAuth, plugins, hooks, MCP servers, and Pi extensions are not imported. CLI config is isolated per run; the sandbox receives only a revocable gateway token. Pi's generated model configuration stores an environment-variable reference, not the real API key. Your normal Claude/Pi configuration is unchanged.
- Gateway messages preserve Anthropic request fields and protocol headers; OpenAI `stream_options` is added only to chat-completion requests. Redirects are rejected to prevent forwarding credentials to a different origin.
- Pi uses text input, no thinking, a 131072-token context declaration and a 1024-token output cap for this minimal adapter. It does not auto-discover upstream model capabilities.
- Gateway usage/cache counters and tool-prewarm detection currently parse OpenAI events only. Anthropic calls are forwarded, but these gateway metrics are not valid Anthropic usage or billing measurements.
- Real-CLI integration tests run on macOS only when the corresponding binary is installed; they use a local simulated model with no API cost. Tests print a skip notice when a CLI is absent. Live provider calls require separately configured credentials and consume the provider's quota.

```bash
cargo test -p runbayd --locked test_real_claude_and_pi_via_anthropic_gateway_when_installed -- --nocapture
```

## Linux sandbox selection

`RUNBAY_SANDBOX_BACKEND=auto` probes bubblewrap and selects the native launcher when bubblewrap is missing or unusable. Set it to `bwrap` or `landlock` on the daemon to select one explicitly. The selected backend appears in `runbayd self-check`, `GET /v1/status`, and the gateway transport in run events. Requesting an unavailable backend fails the run before the worker executes.

The native launcher supports Linux x86-64 and ARM64 with Landlock ABI 1 or newer, seccomp filters, and unprivileged user, mount, PID and network namespaces. Cargo builds an optional static launcher when the target C compiler, Linux UAPI headers and static libc are available. Bubblewrap also works on installations without static libc. For deployment, install the launcher from the same checkout as the daemon. The daemon checks the launcher and policy source fingerprint before running the worker. Set `RUNBAY_LANDLOCK_LAUNCHER` to an explicit matching executable to override discovery. The install command also loads a profile scoped to its root-owned path when Ubuntu restricts user namespaces.

```bash
sudo sh scripts/install-linux-launcher.sh
RUNBAY_SANDBOX_BACKEND=landlock cargo run -p runbayd --locked -- self-check
```

The worker sees a private filesystem with read-only runtime files, the run's workspace, explicitly supplied data and the gateway socket. Runbay pins mounts before attaching them and drops capabilities before executing the worker. Read-only mounts protect shared inputs from truncation on Landlock ABI 1. The launcher confirms that setup and exec succeeded before the daemon publishes `run.started`. Private writable directories must own all names of their hard-linked files. Checking writable roots visits each directory entry at launch. Export large shared dependency stores (including hard-linked pnpm packages) as read-only inputs. The daemon creates its workspaces from MVCC snapshots.

Individual CPU, memory and task budgets require a delegated cgroup v2 subtree exposing `cgroup.kill` and `memory.swap.max`. The native filesystem and network backend can operate on Linux 5.13 independently of these optional controls. See [sandbox verification and timings](docs/sandbox-verification.md) for the tested platforms and reproduction commands.

## Linux per-run resource limits

Pass `--sandbox-cgroup-root DIR` to `runbayd serve`, or set `RUNBAY_CGROUP_ROOT` to a delegated cgroup v2 directory with `cpu`, `memory` and `pids` enabled in `cgroup.subtree_control`. Each run gets its own child cgroup before the sandbox starts. Its children and threads share the budget. The kernel must expose `cgroup.kill` and `memory.swap.max`. The daemon disables swap for the run and releases its cgroup before reporting completion, cancellation or timeout. A kernel-recorded memory limit kill is reported as `oom_killed`. Restarting with the same state directory and delegated parent cleans up interrupted runs.

| Daemon option | Default when enabled | Meaning |
| --- | --- | --- |
| `--sandbox-cpu-percent` | 100 | CPU quota, with 100 equal to one core |
| `--sandbox-memory-mib` | 256 | Aggregate memory in MiB |
| `--sandbox-max-pids` | 64 | Total processes and threads, including the sandbox wrapper |

A plain `POST /agents/:id/run` uses all three daemon defaults. A caller may tighten them by supplying all three positive integers, for example `"resource_limits": {"cpu_percent": 50, "memory_mib": 96, "max_pids": 32}`. The daemon rejects requests above its ceilings or requests for limits on an unconfigured host. Read the configured ceilings from `GET /v1/status` and the applied limits from the `run.started` event.

To try the kernel and daemon tests as a regular user in a fresh systemd service, use the following command. It exercises CPU throttling, memory and task caps, a concurrent fixture gateway request, cancellation and timeout. The service owns the temporary cgroup subtree.

```bash
systemd-run --user --quiet --wait --pipe --collect --property=Delegate=yes \
  --working-directory="$PWD" --setenv="PATH=$PATH" \
  sh scripts/with-delegated-cgroup.sh cargo test --workspace --locked cgroup -- --ignored --nocapture
```

To run a daemon with these defaults, replace the `cargo test …` portion with `target/debug/runbayd serve --enable-fixture-adapter` after building the binary.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
