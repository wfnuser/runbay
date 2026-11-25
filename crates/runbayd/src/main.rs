mod adapters;
mod auth;
mod fixture_worker;
mod models;
mod server;
mod store;
mod supervisor;

use anyhow::{bail, Result};
use auth::generate_csprng_token;
use runbay_gateway::{MockUpstreamServer, SecretlessLoopbackGateway};
use runbay_mvcc::{MVCCWorkspaceStore, SharedOutOfSandboxDerivedProxy};
use runbay_sandbox::{run_os_sandbox_verification_suite, LocalOsSandbox, SandboxLimits};
use runbay_scheduler::{CancelToken, GuardedRunSink};
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use supervisor::DaemonConfig;
use tempfile::TempDir;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(|s| s.as_str()) {
        Some("__internal-fixture-worker") => fixture_worker::run_fixture_worker().await,
        Some("self-check") => run_self_check().await,
        Some("serve") => {
            let config = parse_serve_args(&args[1..])?;
            server::run_loopback_server(config).await
        }
        Some("--version") | Some("-V") => {
            println!("runbayd {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        _ => {
            println!(
                "runbayd {} — local runtime daemon\n\n\
                 Usage:\n\
                   runbayd self-check          Verify OS sandbox, MVCC/CoW branching, gateway, and cancel guard\n\
                   runbayd serve [OPTIONS]     Start authenticated loopback HTTP + SSE control daemon\n\n\
                 Serve options:\n\
                   --port <PORT>                      Loopback TCP port (default 7310; 0 for ephemeral)\n\
                   --state-dir <DIR>                  Durable state directory (default ~/.runbay)\n\
                   --token <TOKEN>                    Control bearer token (or env RUNBAY_CONTROL_TOKEN; auto-generated if omitted)\n\
                   --allow-workspace-root <DIR>       Allowed base root for agent workspaces (repeatable)\n\
                   --max-concurrent-runs <N>          Max simultaneous live runs (default 4)\n\
                   --max-queued-runs <N>              Max queued runs before 429 (default 32)\n\
                   --default-timeout-secs <N>         Default per-run timeout seconds (default 300)\n\
                   --sandbox-cgroup-root <DIR>        Delegated Linux cgroup v2 directory for per-run limits\n\
                   --sandbox-cpu-percent <N>          Per-run ceiling, 100 = one CPU (default 100)\n\
                   --sandbox-memory-mib <N>           Per-run memory ceiling in MiB (default 256)\n\
                   --sandbox-max-pids <N>             Per-run process/thread ceiling (default 64)\n\
                   --upstream-url <URL>               Upstream OpenAI-compatible URL for secretless gateway (uses env RUNBAY_UPSTREAM_API_KEY)\n\
                   --codex-bin <PATH>                 Explicit codex executable path\n\
                   --enable-fixture-adapter           Opt-in: enable built-in deterministic protocol fixture adapter (for tests/CI)",
                env!("CARGO_PKG_VERSION")
            );
            Ok(())
        }
    }
}

fn parse_serve_args(args: &[String]) -> Result<DaemonConfig> {
    let mut port: u16 = 7310;
    let mut state_dir: Option<PathBuf> = None;
    let mut control_token: Option<String> = std::env::var("RUNBAY_CONTROL_TOKEN")
        .ok()
        .filter(|s| !s.trim().is_empty());
    let mut max_concurrent_runs: usize = 4;
    let mut max_queued_runs: usize = 32;
    let mut default_timeout_secs: u64 = 300;
    let mut cgroup_root = std::env::var_os("RUNBAY_CGROUP_ROOT")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from);
    let mut sandbox_limits = SandboxLimits::default();
    let mut limits_specified = false;
    let mut allowed_workspace_roots: Vec<PathBuf> = Vec::new();
    let mut upstream_url: Option<String> = std::env::var("RUNBAY_UPSTREAM_URL")
        .ok()
        .filter(|s| !s.trim().is_empty());
    let upstream_api_key: Option<String> = std::env::var("RUNBAY_UPSTREAM_API_KEY")
        .ok()
        .filter(|s| !s.trim().is_empty());
    let mut enable_fixture_adapter = false;
    let mut codex_bin: Option<PathBuf> = None;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--port" => {
                i += 1;
                let v = args
                    .get(i)
                    .ok_or_else(|| anyhow::anyhow!("--port requires value"))?;
                port = v.parse()?;
            }
            "--state-dir" => {
                i += 1;
                let v = args
                    .get(i)
                    .ok_or_else(|| anyhow::anyhow!("--state-dir requires value"))?;
                state_dir = Some(PathBuf::from(v));
            }
            "--token" => {
                i += 1;
                let v = args
                    .get(i)
                    .ok_or_else(|| anyhow::anyhow!("--token requires value"))?;
                if v.trim().is_empty() {
                    bail!("--token cannot be empty");
                }
                control_token = Some(v.clone());
            }
            "--allow-workspace-root" => {
                i += 1;
                let v = args
                    .get(i)
                    .ok_or_else(|| anyhow::anyhow!("--allow-workspace-root requires path"))?;
                allowed_workspace_roots.push(PathBuf::from(v));
            }
            "--max-concurrent-runs" => {
                i += 1;
                let v = args
                    .get(i)
                    .ok_or_else(|| anyhow::anyhow!("--max-concurrent-runs requires value"))?;
                max_concurrent_runs = v.parse::<usize>()?.max(1);
            }
            "--max-queued-runs" => {
                i += 1;
                let v = args
                    .get(i)
                    .ok_or_else(|| anyhow::anyhow!("--max-queued-runs requires value"))?;
                max_queued_runs = v.parse::<usize>()?.max(1);
            }
            "--default-timeout-secs" => {
                i += 1;
                let v = args
                    .get(i)
                    .ok_or_else(|| anyhow::anyhow!("--default-timeout-secs requires value"))?;
                default_timeout_secs = v.parse::<u64>()?.clamp(1, auth::MAX_TIMEOUT_SECS);
            }
            "--upstream-url" => {
                i += 1;
                let v = args
                    .get(i)
                    .ok_or_else(|| anyhow::anyhow!("--upstream-url requires URL"))?;
                upstream_url = Some(v.clone());
            }
            "--sandbox-cgroup-root" => {
                i += 1;
                cgroup_root = Some(PathBuf::from(args.get(i).ok_or_else(|| {
                    anyhow::anyhow!("--sandbox-cgroup-root requires a directory")
                })?));
            }
            flag @ ("--sandbox-cpu-percent" | "--sandbox-memory-mib" | "--sandbox-max-pids") => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| anyhow::anyhow!("{flag} requires a positive integer"))?
                    .parse()?;
                match flag {
                    "--sandbox-cpu-percent" => sandbox_limits.cpu_percent = value,
                    "--sandbox-memory-mib" => sandbox_limits.memory_mib = value,
                    _ => sandbox_limits.max_pids = value,
                }
                limits_specified = true;
            }
            "--codex-bin" => {
                i += 1;
                let v = args
                    .get(i)
                    .ok_or_else(|| anyhow::anyhow!("--codex-bin requires path"))?;
                codex_bin = Some(PathBuf::from(v));
            }
            "--enable-fixture-adapter" => {
                enable_fixture_adapter = true;
            }
            other => bail!("unknown serve flag: {other}"),
        }
        i += 1;
    }

    if limits_specified && cgroup_root.is_none() {
        bail!("sandbox limits require --sandbox-cgroup-root");
    }
    let state_dir = state_dir.unwrap_or_else(|| {
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".runbay")
    });
    let control_token = control_token.unwrap_or_else(|| generate_csprng_token("rb_ctl_"));

    Ok(DaemonConfig {
        port,
        state_dir,
        control_token,
        max_concurrent_runs,
        max_queued_runs,
        default_timeout_secs,
        cgroup_root,
        sandbox_limits,
        allowed_workspace_roots,
        upstream_url,
        upstream_api_key,
        enable_fixture_adapter,
        codex_bin,
    })
}

async fn run_self_check() -> Result<()> {
    let tmp = TempDir::new()?;
    let src = tmp.path().join("src");
    fs::create_dir_all(&src)?;
    fs::write(
        src.join("doc_0.md"),
        "# Core\n\nRunbay MVCC self-check canary.\n",
    )?;

    let store = MVCCWorkspaceStore::new(tmp.path().join("mvcc"), &src)?;
    let proxy =
        SharedOutOfSandboxDerivedProxy::new(tmp.path().join("proxy"), Arc::clone(&store), 32, 256)?;
    let lease0 = store.pin_snapshot(Some(0))?;
    let mut up = BTreeMap::new();
    up.insert("doc_1.md".into(), "# Delta\n\nNew rev1 content.\n".into());
    store.commit_delta(&up, &[], Some(0))?;
    let rev0_future = proxy.query_at_revision(0, "rev1 content", 3)?;
    let rev1_new = proxy.query_at_revision(1, "rev1 content", 3)?;
    drop(lease0);

    let eq = proxy.verify_incremental_equals_full_rebuild(&["canary", "rev1 content"])?;

    let tok = CancelToken::new();
    let sink = GuardedRunSink::new(tok.clone());
    sink.emit(serde_json::json!({"ok": true}));
    tok.cancel("self_check");
    sink.emit(serde_json::json!({"late": true}));

    let mock = MockUpstreamServer::start().await?;
    let gw = SecretlessLoopbackGateway::start(mock.base_url(), "self-check-host-key", None).await?;
    let run_tok = gw.mint_run_token("self-check", None);
    let resp = reqwest::Client::new()
        .post(format!("{}/chat/completions", gw.base_url()))
        .header("Authorization", format!("Bearer {run_tok}"))
        .json(&serde_json::json!({"model": "self-check", "stream": false}))
        .send()
        .await?;
    let gw_ok = resp.status().is_success();

    let probe_sb = LocalOsSandbox::new(tmp.path(), &[]);
    let sb_ok = match probe_sb.check_available() {
        Ok(()) => run_os_sandbox_verification_suite(&tmp.path().join("sb"))?.all_invariants_passed,
        Err(error) => {
            eprintln!("{error:#}");
            false
        }
    };

    let mvcc_ok = rev0_future.hits.is_empty() && !rev1_new.hits.is_empty();
    let cancel_ok = sink.accepted_count() == 1 && sink.rejected_late_count() == 1;
    let summary = serde_json::json!({
        "runbayd_version": env!("CARGO_PKG_VERSION"),
        "mvcc_isolation_ok": mvcc_ok,
        "incremental_equals_full": eq.exact_equivalence,
        "cancel_guard_ok": cancel_ok,
        "secretless_gateway_ok": gw_ok,
        "sandbox_mechanism": probe_sb.mechanism_name(),
        "os_sandbox_ok": sb_ok
    });
    println!("{}", serde_json::to_string_pretty(&summary)?);
    anyhow::ensure!(
        sb_ok && gw_ok && eq.exact_equivalence && mvcc_ok && cancel_ok,
        "runbayd self-check failed"
    );
    Ok(())
}
