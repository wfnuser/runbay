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
| Runtime Adapters | **Implemented (`one_shot`)** | Honest `one_shot` `codex` adapter (`codex exec --json --ephemeral` through secretless loopback gateway on macOS; fails closed on Linux `--unshare-net`) + opt-in `--enable-fixture-adapter` deterministic protocol worker for tests/CI |
| Cross-Platform OS Sandbox (`runbay-sandbox`) | **Implemented** | macOS Seatbelt (`sandbox-exec`) with loopback TCP gateway allowlist; Linux bubblewrap or native Landlock + seccomp with a private network namespace and a bound Unix-domain-socket gateway (fails closed if TCP loopback is requested with `allow_network=false`) |
| Secretless Model Gateway (`runbay-gateway`) | **Implemented** | `tokio` + `hyper` 1.x reverse proxy (TCP loopback + Unix domain socket) injecting real upstream API keys strictly on outbound hops; 256-bit `rb_*` tokens, route/model allowlists, `Notify`-driven immediate revocation |
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
