# Runbay — Architecture & Design (v0.2)

> Status: v0.2 design working draft paired with the core Rust workspace crates (`runbay-mvcc`, `runbay-sandbox`, `runbay-gateway`, `runbay-scheduler`, `runbayd`) and `beta-bench`.

## 1. Overview

Runbay is a **local-first runtime daemon** (`runbayd`) for spawning, supervising, isolating, and scheduling coding-agent sessions on a developer workstation or dedicated single-node host.

Runbay is intentionally consumer-agnostic — it owns **execution mechanism** (process lifecycle, OS sandboxing, copy-on-write workspace versioning, loopback credential proxying, and scheduling) and leaves domain policy (prompts, personas, external memory stores, UI) to callers over a local HTTP + SSE contract.

### Why a standalone daemon

1. **Single security & credential boundary.** OS sandbox enforcement, host-side secretless API credential injection, and append-only telemetry live in one process.
2. **Shared workspaces & pooled proxies.** Read-only content-addressed layers (L0), versioned derived indexes (L2), and provider rate-limit buckets are pooled once instead of duplicated *N* times across concurrent agent runs.
3. **Lifecycle independence.** Managed agent sessions, warm workspaces, and event streams survive caller restarts.

### Non-goals (V1)

- Multi-tenant cloud cluster orchestration (Runbay focuses on single-host, zero-container-rebuild local and workstation execution).
- Domain-specific knowledge retrieval or conversation prompt assembly — callers or external MCP tools own domain context.

---

## 2. Core Model: Durable Logical Sessions & 3-Tier Pool

An **agent** from Runbay's perspective is:

```text
agent_id        opaque caller-assigned identifier
runtime         adapter name ("codex" | "claude" | "gemini" | custom)
workspace_dir   base repository or directory
home_dir        isolated per-agent runtime home
session_id      runtime-native session/thread id persisted by Runbay
```

**Logical sessions are durable; OS processes are ephemeral.** Runbay manages active runtimes across three resource tiers driven by **RSS memory watermarks, LRU eviction, and upstream prompt-cache TTL**:

| Tier | Resident State | Resume Cost | Eviction Trigger |
| :--- | :--- | :--- | :--- |
| **Hot** | Live OS process, warm MCP connections, CoW workspace | Sub-10 ms | Soft RSS watermark exceeded or idle past cache TTL |
| **Warm** | Process reaped; `session_id`, CoW workspace, and versioned local state preserved | Native resume + incremental delta sync | Disk/slot cap or extended inactivity |
| **Cold** | Workspace pruned; branch ref + session metadata retained | CoW workspace clone + native resume | Explicit delete |

```text
         spawn ok                 watermark / idle reap
warm/cold ────────► hot (running) ────────────────────► warm (process reaped, session+workspace kept)
   │                   │                                   │
   │ spawn fail        │ runtime error                     │ long idle / prune
   ▼                   ▼                                   ▼
backoff (exp.) ──► terminal_failure (manual reset)        cold (metadata + ref only)
```

---

## 3. Transport, Auth & Runtime Signals

- **Loopback HTTP** on `127.0.0.1:<port>` with per-caller bearer tokens (`rb_*`, 256-bit OS CSPRNG). No non-loopback binding in V1.
- **SSE** streams for live output, tool events, lifecycle transitions, and gateway cost/cache telemetry.
- **Exported runtime signals**: `state`, `context_fill_ratio`, `cache_warm_until`, `workspace_rev`, `cost_usd`, `cache_read_tokens`, `ttft_ms`, and structured `last_error` (`rate_limit | auth | runtime_crash | stale_rev`).

### Control API (Implemented vs Planned)

**Implemented in `runbayd serve` today:**

```text
GET    /health                      unauthenticated liveness check
GET    /v1/status                   authenticated daemon status, adapters, sandbox & live queue metrics
POST   /agents                      register agent {agent_id, runtime, workspace_dir, config?}
GET    /agents                      list agents + capability tier + MVCC lease counts
GET    /agents/:id                  agent status and active run pointer
DELETE /agents/:id                  cancel active runs and delete isolated agent state
POST   /agents/:id/run              submit a one-shot run {prompt, priority?, required_rev?, timeout_secs?, allowed_model?, sandbox_mode?}
GET    /agents/:id/events           SSE stream scoped to agent
GET    /runs                        list recent runs (?agent_id=...)
GET    /runs/:run_id                run state (queued|running|succeeded|failed|cancelled), exit code, sandbox/gateway metadata
POST   /runs/:run_id/cancel         cancel queued/running turn, kill+reap POSIX process group, revoke gateway token, release MVCC lease
GET    /runs/:run_id/events         bounded SSE stream with Last-Event-ID / ?after_seq= replay (disconnect-safe)
```

**Planned (not yet exposed over HTTP):**

```text
POST   /agents/:id/wake             content-free wake notification
POST   /runs/:run_id/steer          inject a steer message into a duplex/hot turn
POST   /workspaces/:id/fork         O(1) CoW fork of workspace snapshot -> branch_id
POST   /workspaces/:id/commit       atomically commit winning branch & abort siblings
POST   /workspaces/:id/abort        discard speculative branch workspace
```

---

## 4. Runtime Adapters & Honest Capability Tiers

Current execution implements the one-shot tier for Codex, Claude Code, and Pi.
Claude/Pi use isolated invocation configuration and an Anthropic-compatible gateway;
macOS supports their TCP transport, while Linux remains fail-closed. Native JSON
completion outcomes supplement exit codes, including Pi errors that exit with zero.
See the README for the tested CLI versions, tool restrictions, and setup.

The richer session interface below is a future design, not the current Rust trait:

Adapters advertise what they genuinely support instead of hiding protocol differences:

```rust
trait RuntimeAdapter {
    fn capabilities(&self) -> Capabilities; // duplex | resumable | one_shot
    async fn ensure_session(&self, agent: &AgentConfig) -> Result<SessionHandle>;
    async fn run_turn(&self, s: &SessionHandle, input: TurnInput, events: EventSink) -> Result<TurnResult>;
    async fn reap(&self, s: &SessionHandle) -> Result<()>;
}
```

- **Tier A (`duplex`)**: persistent JSON-RPC process servers benefiting directly from the Hot tier.
- **Tier B (`resumable`)**: per-turn CLI processes with native session resume benefiting from isolated slim `home_dir` and shared read-only proxies.
- **Tier C (`one_shot`)**: stateless headless invocations where callers supply turn context.

---

## 5. Three-Layer MVCC / CoW Workspaces (`runbay-mvcc`)

1. **L0 Content-Addressed Store**: immutable `0o444` blobs indexed by SHA-256 and materialized via hardlinks so *N* concurrent workspaces share a single physical inode and OS page-cache copy.
2. **L1 Copy-on-Write Snapshots & Speculative Branching**:
   - Uses APFS `clonefile(2)` on macOS and `ioctl(FICLONE)` / `--reflink` on Linux (with clean recursive copy fallback on filesystems without reflinks).
   - Enforces canonical relative path normalization and root containment (rejecting traversal, absolute paths, and escaping symlinks).
   - RAII `SnapshotLease` guarantees pinned readers (`rev = k`) never observe concurrent mutations (`rev = k+1, k+2`) and automatically releases leases on drop.
   - Speculative `fork_branch_group` + First-Valid-Commit-Wins `try_atomic_commit_branch` atomically publishes the winning branch via CAS (`base_rev == current_rev`) and fires sibling cancellation hooks outside the store lock.
3. **L2 Shared Versioned Derived Index**:
   - Out-of-sandbox SQLite FTS5 + dense projection index updated incrementally (`O(delta)`).
   - Uses SQLite's online backup API (`rusqlite::backup::Backup`) + atomic rename to create WAL-complete immutable historical snapshots scoped by proxy generation ID.
   - Readers query finalized snapshots via thread-local `mode=ro&immutable=1` + `mmap_size` connections with `SQLITE_CONFIG_MEMSTATUS=0` for lock-free multi-core throughput.

---

## 6. Cross-Platform OS Sandbox & Secretless Gateway

### OS Subprocess Sandbox (`runbay-sandbox`)
- **macOS (`MacOSSeatbeltSandbox`)**: global `(deny file-write*)` write baseline allowing writes solely to `allowed_workspace`, isolated per-agent `home_dir`, and `/dev/null`; explicit read denial on sibling pool slots and protected secret paths (with metadata-only ancestor `lstat` so `realpath(3)` succeeds on the agent's own workspace/home); L0 read-only allowlists; `(deny network*)` with optional loopback port or Unix-socket exception for the gateway.
- **Linux (`LinuxBubblewrapSandbox`)**: unprivileged `bwrap` namespaces with `--ro-bind / /` (zero arbitrary host writes), `--tmpfs` masking of pool and secret paths, `--ro-bind` for L0, `--bind` for `allowed_workspace` and isolated `home_dir`, and unconditional `--unshare-net` when `allow_network=false`. Gateway access under `--unshare-net` uses a bound Unix domain socket (`--bind <sock> <sock>`); requesting `allow_network=false` with a TCP-only loopback port fails closed rather than silently dropping `--unshare-net`.
- **Process-Group Supervision (`SandboxedChild`)**: every async sandboxed run starts in its own POSIX process group (`setpgid(0, 0)`) so cancellation and timeouts cleanly signal and reap the entire child/grandchild tree.
- **Quarantine (`QuarantineRegistry`)**: permanently retires worker slots whose post-run cleanup fails.

### Secretless Loopback Model Gateway (`runbay-gateway`)
- Binds `127.0.0.1:0` (and optional Unix domain sockets via `bind_unix_socket` for Linux `--unshare-net`) and injects real upstream API keys strictly on outbound hops; sandboxed processes receive only ephemeral 256-bit CSPRNG `rb_*` tokens.
- Enforces an explicit route allowlist (`/v1/chat/completions`, `/v1/responses`, `/v1/embeddings`, `/v1/messages`), verifies unchanged trusted upstream origin, disables redirects, and enforces optional per-token `allowed_model` restrictions (`403 Forbidden`). Anthropic requests accept ephemeral Bearer or `x-api-key` authentication and replace credentials on the host-side upstream hop.
- Passively parses OpenAI SSE usage telemetry and tool names to invoke prewarm callbacks (surfacing callback errors explicitly), and wakes pending or stalled streams immediately upon token revocation via `tokio::sync::Notify`. Anthropic forwarding preserves native events but does not yet populate protocol-specific gateway usage/prewarm metrics.

---

## 7. Cancellation Guard, Live Queue & Policy Simulator (`runbay-scheduler`)

- **Live in `runbayd`**: atomic `CancelToken` + `GuardedRunSink` guaranteeing zero late-event delivery after abort, paired with a bounded live concurrency/queue gate (`--max-concurrent-runs` / `--max-queued-runs` -> `429 Too Many Requests`).
- **Simulated in `beta-bench`**: `simulate_scheduler_policy` compares `fifo`, `priority_aging`, `warm_env_affinity`, and `dual_cache_ttl_aware` in a discrete-event simulator (planned for live daemon integration in Issue #5).
