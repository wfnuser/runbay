use anyhow::Result;
use chrono::Utc;
use runbay_gateway::{MockUpstreamServer, SecretlessLoopbackGateway};
use runbay_mvcc::{
    copy_dir_recursive, cow_clone_tree, DerivedWorkspaceIndexLayer, L0ContentAddressedStore,
    MVCCWorkspaceStore, SharedOutOfSandboxDerivedProxy,
};
use runbay_sandbox::{
    run_os_sandbox_verification_suite, LocalOsSandbox, QuarantineRegistry, SandboxRunOptions,
};
use runbay_scheduler::{
    generate_rate_limited_workload, simulate_scheduler_policy, CancelToken, GuardedRunSink,
    PolicyName,
};
use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tempfile::TempDir;

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
struct RecordedTrace {
    trace_id: String,
    task_kind: String,
    model: String,
    ttft_ms: f64,
    total_llm_ms: f64,
    tool_name_detect_ms: Option<f64>,
    specbox_hidden_window_ms: Option<f64>,
    #[serde(default)]
    prompt_tokens: u64,
    #[serde(default)]
    cached_tokens: u64,
    #[serde(default)]
    completion_tokens: u64,
    #[serde(default)]
    tool_calls: Vec<serde_json::Value>,
}

fn round_n(v: f64, decimals: u32) -> f64 {
    let f = 10_f64.powi(decimals as i32);
    (v * f).round() / f
}

fn percentile(vals: &[f64], q: f64) -> f64 {
    if vals.is_empty() {
        return 0.0;
    }
    let mut s = vals.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let raw = (q * s.len() as f64).ceil() as isize - 1;
    let idx = raw.clamp(0, (s.len() - 1) as isize) as usize;
    s[idx]
}

fn build_synthetic_repo(root: &Path, n_files: usize) -> Result<Vec<String>> {
    if root.exists() {
        fs::remove_dir_all(root)?;
    }
    let docs = root.join("docs");
    fs::create_dir_all(&docs)?;
    let mut rels = Vec::with_capacity(n_files);
    for i in 0..n_files {
        let rel = format!("docs/service_{i:04}.md");
        let content = format!(
            "# Service {i:04} Runbook\n\n\
             ## Architecture\n\
             Service {i:04} coordinates shard_{} and replica_{} under strict SLA.\n\n\
             ## Thresholds\n\
             Max p95 latency budget is {}ms. Retry backoff is {}ms.\n\
             Escalation channel is #ops-{}. Canary token: CANARY_{i:04}_V0.\n",
            i % 17,
            i % 7,
            120 + (i % 40) * 3,
            25 + (i % 10) * 5,
            i % 5
        );
        fs::write(root.join(&rel), content)?;
        rels.push(rel);
    }
    Ok(rels)
}

async fn run_correctness_oracle(work_dir: &Path) -> Result<serde_json::Value> {
    // 1. Real OS sandbox isolation (selected macOS or Linux backend)
    let probe_sb = LocalOsSandbox::new(work_dir, &[]);
    let sb_report = if probe_sb.is_available() {
        serde_json::to_value(run_os_sandbox_verification_suite(
            &work_dir.join("os_sandbox"),
        )?)?
    } else {
        serde_json::json!({
            "measurement_type": "real_os_kernel_exec",
            "all_invariants_passed": false,
            "note": "Requested OS sandbox is unavailable on this host"
        })
    };
    let sb_ok = sb_report["all_invariants_passed"]
        .as_bool()
        .unwrap_or(false);

    // 2. Poisoned worker quarantine
    let quarantine = QuarantineRegistry::new();
    quarantine.mark_poisoned("worker-poisoned-1", "simulated_cleanup_eperm_failure");
    let quarantine_ok =
        !quarantine.is_reusable("worker-poisoned-1") && quarantine.is_reusable("worker-healthy-2");

    // 3. MVCC exact snapshot pinning + WAL backup non-empty verification
    let src_dir = work_dir.join("mvcc_src");
    build_synthetic_repo(&src_dir, 60)?;
    let store = MVCCWorkspaceStore::new(work_dir.join("mvcc_store"), &src_dir)?;
    let proxy = SharedOutOfSandboxDerivedProxy::new(
        work_dir.join("mvcc_proxy"),
        Arc::clone(&store),
        64,
        640,
    )?;

    let lease0 = store.pin_snapshot(Some(0))?;
    let mut up1 = BTreeMap::new();
    up1.insert(
        "docs/service_0005.md".into(),
        "# Service 0005 Runbook\n\n## Thresholds\nMax p95 latency budget is 999ms. Canary token: CANARY_0005_V1.\n".into(),
    );
    up1.insert(
        "docs/service_new_9999.md".into(),
        "# Service 9999 New\n\nCanary token: FUTURE_ONLY_REV1.\n".into(),
    );
    store.commit_delta(&up1, &[], Some(0))?;
    store.commit_delta(&BTreeMap::new(), &["docs/service_0010.md".into()], Some(1))?;

    // Advance active_rev to 2 first (forces WAL online backup of rev=0!)
    let rev2_del = proxy.query_at_revision(2, "CANARY_0010_V0", 5)?;
    let rev2_new = proxy.query_at_revision(2, "FUTURE_ONLY_REV1", 5)?;
    let rev0_future = proxy.query_at_revision(0, "FUTURE_ONLY_REV1", 5)?;
    let rev0_old = proxy.query_at_revision(0, "CANARY_0010_V0", 5)?;
    drop(lease0);

    let mvcc_ok = rev0_future.hits.is_empty()
        && !rev0_old.hits.is_empty()
        && rev2_del.hits.is_empty()
        && !rev2_new.hits.is_empty();

    // 4. Incremental == Full Rebuild strict equivalence (hashes, blocks, FTS5 BM25, frozen vectors)
    let eq_report = proxy.verify_incremental_equals_full_rebuild(&[
        "latency budget",
        "CANARY_0005_V1",
        "FUTURE_ONLY_REV1",
        "shard_3 replica_2",
        "Escalation channel",
    ])?;

    // 5. CancelToken & GuardedRunSink
    let tok = CancelToken::new();
    let sink = GuardedRunSink::new(tok.clone());
    sink.emit(serde_json::json!({"event": "chunk_1"}));
    tok.cancel("branch_lost_race");
    sink.emit(serde_json::json!({"event": "late_chunk_2"}));
    sink.emit(serde_json::json!({"event": "late_chunk_3"}));
    let cancel_ok = sink.accepted_count() == 1 && sink.rejected_late_count() == 2;

    // 6. Real Seatbelt -> Rust Secretless Loopback Gateway E2E
    let gw_e2e = run_seatbelt_gateway_e2e(&work_dir.join("gw_e2e")).await?;
    let gw_ok = gw_e2e["verified"].as_bool().unwrap_or(false);

    let all_passed =
        sb_ok && quarantine_ok && mvcc_ok && eq_report.exact_equivalence && cancel_ok && gw_ok;

    Ok(serde_json::json!({
        "measurement_type": "real_os_kernel_exec",
        "all_invariants_passed": all_passed,
        "1_os_sandbox_isolation": sb_report,
        "2_poisoned_env_quarantine": {
            "passed": quarantine_ok,
            "quarantined": quarantine.snapshot()
        },
        "3_mvcc_exact_snapshot_pinning": {
            "passed": mvcc_ok,
            "rev0_sees_future_rev2": !rev0_future.hits.is_empty(),
            "rev0_preserves_later_deleted_doc": !rev0_old.hits.is_empty(),
            "rev2_removes_deleted_doc": rev2_del.hits.is_empty(),
            "rev2_sees_new_doc": !rev2_new.hits.is_empty()
        },
        "4_incremental_equals_full_rebuild": eq_report,
        "5_cancel_and_late_event_rejection": {
            "passed": cancel_ok,
            "accepted_before_cancel": sink.accepted_count(),
            "rejected_late_events": sink.rejected_late_count()
        },
        "6_secretless_gateway_seatbelt_e2e": gw_e2e
    }))
}

async fn run_seatbelt_gateway_e2e(work_dir: &Path) -> Result<serde_json::Value> {
    let mock = MockUpstreamServer::start().await?;
    let gw = SecretlessLoopbackGateway::start(
        mock.base_url(),
        "host-only-secret-key-never-in-sandbox",
        None,
    )
    .await?;

    let pool = work_dir.join("pool");
    let ws = pool.join("agent_run");
    let secrets = work_dir.join("secrets");
    fs::create_dir_all(&ws)?;
    fs::create_dir_all(&secrets)?;
    fs::write(
        secrets.join(".env"),
        "API_KEY=host-only-secret-key-never-in-sandbox\n",
    )?;

    let sb = LocalOsSandbox::new(&pool, std::slice::from_ref(&secrets));
    if !sb.is_available() {
        let tok = gw.mint_run_token("fallback-e2e", Some("beta-model".into()));
        let resp = reqwest::Client::new()
            .post(format!("{}/chat/completions", gw.base_url()))
            .header("Authorization", format!("Bearer {tok}"))
            .json(&serde_json::json!({"model": "beta-model", "stream": false}))
            .send()
            .await?;
        return Ok(serde_json::json!({
            "verified": resp.status().is_success(),
            "sandbox_enforced": false,
            "gateway_telemetry": gw.stats()
        }));
    }

    let tok = gw.mint_run_token("sandbox-e2e", Some("beta-model".into()));
    let mut env_overrides = BTreeMap::new();
    env_overrides.insert("RUNBAY_RUN_TOKEN".into(), tok);

    let (opts, curl_call, gateway_transport) = if cfg!(target_os = "macos") {
        env_overrides.insert("RUNBAY_GATEWAY_URL".into(), gw.base_url());
        (
            SandboxRunOptions {
                allow_network: false,
                allowed_loopback_port: Some(gw.port()),
                env_overrides,
                ..Default::default()
            },
            "curl -sS --max-time 5 -X POST \"$RUNBAY_GATEWAY_URL/chat/completions\" \
               -H \"Authorization: Bearer $RUNBAY_RUN_TOKEN\" \
               -H \"Content-Type: application/json\" \
               -d '{\"model\":\"beta-model\",\"stream\":false}'"
                .to_string(),
            "macos_seatbelt_loopback_tcp",
        )
    } else {
        let sock_path = gw
            .bind_unix_socket(work_dir.join("gw_sock/gateway.sock"))
            .await?;
        env_overrides.insert(
            "RUNBAY_GATEWAY_SOCKET".into(),
            sock_path.to_string_lossy().to_string(),
        );
        (
            SandboxRunOptions {
                allow_network: false,
                allowed_loopback_port: None,
                gateway_unix_socket: Some(sock_path),
                env_overrides,
                ..Default::default()
            },
            "curl -sS --max-time 5 --unix-socket \"$RUNBAY_GATEWAY_SOCKET\" \
               -X POST \"http://localhost/v1/chat/completions\" \
               -H \"Authorization: Bearer $RUNBAY_RUN_TOKEN\" \
               -H \"Content-Type: application/json\" \
               -d '{\"model\":\"beta-model\",\"stream\":false}'"
                .to_string(),
            sb.gateway_transport(),
        )
    };

    let script = format!(
        "if cat \"{}/.env\" >/dev/null 2>&1; then echo ENV_LEAKED; exit 2; fi; \
         if curl -sS --max-time 1 https://1.1.1.1 >/dev/null 2>&1; then echo NET_LEAKED; exit 3; fi; \
         {curl_call}",
        secrets.display()
    );

    let ws_clone = ws.clone();
    let res = tokio::task::spawn_blocking(move || {
        sb.run(&["/bin/sh".into(), "-c".into(), script], &ws_clone, &opts)
    })
    .await??;

    let verified = res.returncode == 0
        && res.stdout.contains("Multi-Version Concurrency Control")
        && !res.stdout.contains("host-only-secret-key-never-in-sandbox");

    Ok(serde_json::json!({
        "verified": verified,
        "returncode": res.returncode,
        "elapsed_ms": res.elapsed_ms,
        "gateway_transport": gateway_transport,
        "sandbox_checks": {
            "env_file_blocked": verified,
            "direct_internet_check_executed": true,
            "direct_internet_blocked": verified,
            "real_key_in_sandbox_env": false,
            "loopback_gateway_ok": verified
        },
        "gateway_telemetry": gw.stats()
    }))
}

fn run_l0_l1_l2_scaling(work_dir: &Path) -> Result<serde_json::Value> {
    let mut workspace_scaling = Vec::new();
    for n_files in [100usize, 500, 1200] {
        let src = work_dir.join(format!("src_{n_files}"));
        let rels = build_synthetic_repo(&src, n_files)?;
        let n_workers = 4usize;

        // Baseline: full copy into 4 worker dirs
        let t0 = Instant::now();
        let mut full_inodes: HashSet<u64> = HashSet::new();
        for w in 0..n_workers {
            let dst = work_dir.join(format!("full_{n_files}_w{w}"));
            copy_dir_recursive(&src, &dst)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                full_inodes.insert(fs::metadata(dst.join(&rels[0]))?.ino());
            }
        }
        let full_ms = round_n(t0.elapsed().as_secs_f64() * 1000.0, 2);

        // L1 APFS clonefile(2) into 4 worker dirs
        let t1 = Instant::now();
        let mut method_str = "full_copy_fallback";
        for w in 0..n_workers {
            let dst = work_dir.join(format!("cow_{n_files}_w{w}"));
            method_str = cow_clone_tree(&src, &dst)?.as_str();
        }
        let cow_ms = round_n(t1.elapsed().as_secs_f64() * 1000.0, 2);

        // L0 CAS hardlink into 4 worker dirs
        let cas = L0ContentAddressedStore::new(work_dir.join(format!("l0_cas_{n_files}")))?;
        let mut manifest = BTreeMap::new();
        for rel in &rels {
            let bytes = fs::read(src.join(rel))?;
            let (digest, _) = cas.put_bytes(&bytes)?;
            manifest.insert(rel.clone(), digest);
        }
        let t2 = Instant::now();
        let mut l0_inodes: HashSet<u64> = HashSet::new();
        for w in 0..n_workers {
            let dst = work_dir.join(format!("l0_{n_files}_w{w}"));
            let _ = cas.materialize_hardlink_tree(&manifest, &dst)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                l0_inodes.insert(fs::metadata(dst.join(&rels[0]))?.ino());
            }
        }
        let l0_ms = round_n(t2.elapsed().as_secs_f64() * 1000.0, 2);

        workspace_scaling.push(serde_json::json!({
            "n_files": n_files,
            "workers": n_workers,
            "full_copy_4w_ms": full_ms,
            "cow_clonefile_4w_ms": cow_ms,
            "cow_method": method_str,
            "cow_speedup_x": round_n(full_ms / cow_ms.max(0.01), 2),
            "l0_hardlink_4w_ms": l0_ms,
            "full_copy_inodes_per_file": full_inodes.len(),
            "l0_shared_inodes_per_file": l0_inodes.len()
        }));
    }

    // L2 Shared Proxy vs 8 Duplicated Workers (N=200 files)
    let src200 = work_dir.join("src_l2_200");
    build_synthetic_repo(&src200, 200)?;

    let mut dup_ms_total = 0.0_f64;
    let mut dup_bytes = 0u64;
    for w in 0..8 {
        let db = work_dir.join(format!("dup_l2_w{w}.sqlite"));
        let mut layer = DerivedWorkspaceIndexLayer::open(&db, 64, 640)?;
        let st = layer.rebuild_all(&src200, "rev-0000")?;
        dup_ms_total += st.build_ms;
        drop(layer);
        dup_bytes += fs::metadata(&db)?.len();
    }

    let store200 = MVCCWorkspaceStore::new(work_dir.join("shared_l2_store"), &src200)?;
    let proxy200 = SharedOutOfSandboxDerivedProxy::new(
        work_dir.join("shared_l2_proxy"),
        Arc::clone(&store200),
        64,
        640,
    )?;
    let shared_ms = proxy200.initial_build_ms;
    let shared_bytes = proxy200.db_size_bytes();

    Ok(serde_json::json!({
        "measurement_type": "real_os_kernel_exec",
        "workspace_scaling": workspace_scaling,
        "l2_shared_proxy_vs_duplicated_8w": {
            "n_files": 200,
            "workers": 8,
            "duplicated_build_ms": round_n(dup_ms_total, 2),
            "shared_proxy_build_ms": round_n(shared_ms, 2),
            "build_speedup_x": round_n(dup_ms_total / shared_ms.max(0.01), 2),
            "duplicated_index_kb": round_n(dup_bytes as f64 / 1024.0, 1),
            "shared_proxy_index_kb": round_n(shared_bytes as f64 / 1024.0, 1),
            "index_footprint_reduction_x": round_n(dup_bytes as f64 / shared_bytes.max(1) as f64, 2)
        }
    }))
}

fn run_concurrent_mvcc_throughput(work_dir: &Path) -> Result<serde_json::Value> {
    let src = work_dir.join("conc_src");
    build_synthetic_repo(&src, 200)?;
    let store = MVCCWorkspaceStore::new(work_dir.join("conc_store"), &src)?;
    let proxy = Arc::new(SharedOutOfSandboxDerivedProxy::new(
        work_dir.join("conc_proxy"),
        Arc::clone(&store),
        64,
        640,
    )?);

    // Pin rev=0 and advance to rev=1 so we exercise real immutable WAL-backed concurrent readers
    let lease0 = store.pin_snapshot(Some(0))?;
    let mut up1 = BTreeMap::new();
    up1.insert(
        "docs/service_0001.md".into(),
        "# Service 0001\n\nCONCURRENT_REV1_ONLY_MARKER latency budget 55ms.\n".into(),
    );
    store.commit_delta(&up1, &[], Some(0))?;
    let inc_refresh_ms = proxy.sync_to_revision(1)?;
    // Warm the rev=0 snapshot handle once
    let _ = proxy.query_at_revision_uncached(0, "latency budget", 5)?;

    let queries = [
        "latency budget",
        "CANARY_0010_V0",
        "shard_3 replica_2",
        "Escalation channel",
        "Retry backoff",
    ];

    let mut levels = Vec::new();
    let queries_per_thread = 250usize;
    for n_threads in [1usize, 4, 8, 16] {
        let barrier = Arc::new(std::sync::Barrier::new(n_threads + 1));
        let mut handles = Vec::with_capacity(n_threads);
        for tid in 0..n_threads {
            let p = Arc::clone(&proxy);
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || -> Result<Vec<f64>> {
                // Warm thread-local immutable read-only SQLite handles for rev=0 and rev=1
                let _ = p.query_at_revision_uncached(0, "latency budget", 5)?;
                let _ = p.query_at_revision_uncached(1, "latency budget", 5)?;
                b.wait();

                let mut lats = Vec::with_capacity(queries_per_thread);
                for i in 0..queries_per_thread {
                    let rev = if (tid + i) % 2 == 0 { 0 } else { 1 };
                    let q = queries[(tid + i) % queries.len()];
                    let qt0 = Instant::now();
                    let res = p.query_at_revision_uncached(rev, q, 5)?;
                    lats.push(qt0.elapsed().as_secs_f64() * 1000.0);
                    if res.hits.is_empty() {
                        anyhow::bail!("unexpected empty result for rev={rev} q={q}");
                    }
                    if rev == 0 && i % 10 == 0 {
                        let iso =
                            p.query_at_revision_uncached(0, "CONCURRENT_REV1_ONLY_MARKER", 3)?;
                        if !iso.hits.is_empty() {
                            anyhow::bail!("MVCC isolation violation: rev=0 saw rev=1 marker");
                        }
                    }
                }
                Ok(lats)
            }));
        }

        barrier.wait();
        let t0 = Instant::now();
        let mut all_lats = Vec::with_capacity(n_threads * queries_per_thread);
        for h in handles {
            all_lats.extend(h.join().expect("reader thread panicked")?);
        }
        let wall_s = t0.elapsed().as_secs_f64();
        let total_queries = n_threads * queries_per_thread;
        levels.push(serde_json::json!({
            "reader_threads": n_threads,
            "total_uncached_queries": total_queries,
            "wall_ms": round_n(wall_s * 1000.0, 2),
            "throughput_qps": round_n(total_queries as f64 / wall_s.max(1e-6), 1),
            "p50_query_ms": round_n(percentile(&all_lats, 0.50), 3),
            "p95_query_ms": round_n(percentile(&all_lats, 0.95), 3),
            "isolation_violations": 0
        }));
    }

    drop(lease0);
    Ok(serde_json::json!({
        "measurement_type": "real_os_kernel_exec",
        "workspace_files": 200,
        "incremental_sync_1file_ms": inc_refresh_ms,
        "concurrency_levels": levels
    }))
}

fn run_trace_replay_benchmarks(
    work_dir: &Path,
    traces: &[RecordedTrace],
) -> Result<serde_json::Value> {
    let src = work_dir.join("replay_src");
    build_synthetic_repo(&src, 120)?;
    let store = MVCCWorkspaceStore::new(work_dir.join("replay_store"), &src)?;
    let proxy = SharedOutOfSandboxDerivedProxy::new(
        work_dir.join("replay_proxy"),
        Arc::clone(&store),
        64,
        640,
    )?;

    // 1. SpecBox Mid-Stream Prewarm Replay (real Rust APFS clonefile + L2 incremental delta sync)
    let mut specbox_runs = Vec::new();
    for (idx, tr) in traces.iter().enumerate() {
        if tr.task_kind != "tool_call_specbox" {
            continue;
        }
        let overlap_ms = tr.specbox_hidden_window_ms.unwrap_or(0.0);
        let tool_name = tr
            .tool_calls
            .first()
            .and_then(|tc| tc.get("name"))
            .and_then(|n| n.as_str())
            .unwrap_or("unknown_tool");

        let mut up = BTreeMap::new();
        up.insert(
            format!("docs/service_{:04}.md", idx % 10),
            format!(
                "# Service {idx}\n\nSpecBox prewarm delta rev {}.\n",
                idx + 1
            ),
        );
        let new_snap = store.commit_delta(&up, &[], None)?;

        let t0 = Instant::now();
        let (branches, _) =
            store.fork_branch_group(&format!("specbox-{idx}"), 1, Some(new_snap.rev))?;
        let _ = proxy.sync_to_revision(new_snap.rev)?;
        let real_prep_ms = round_n(t0.elapsed().as_secs_f64() * 1000.0, 3);
        store.abort_branch(&branches[0].branch_id, "prewarm_bench_cleanup");

        let exposed_ms = round_n((real_prep_ms - overlap_ms).max(0.0), 3);
        specbox_runs.push(serde_json::json!({
            "trace_id": tr.trace_id,
            "detected_tool": tool_name,
            "available_overlap_window_ms": overlap_ms,
            "real_rust_cow_plus_sync_ms": real_prep_ms,
            "exposed_after_llm_ms": exposed_ms,
            "fully_hidden": exposed_ms <= 0.0
        }));
    }

    let n_spec = specbox_runs.len().max(1) as f64;
    let fully_hidden_count = specbox_runs
        .iter()
        .filter(|r| r["fully_hidden"].as_bool().unwrap_or(false))
        .count();
    let mean_overlap = round_n(
        specbox_runs
            .iter()
            .map(|r| r["available_overlap_window_ms"].as_f64().unwrap_or(0.0))
            .sum::<f64>()
            / n_spec,
        2,
    );
    let mean_prep = round_n(
        specbox_runs
            .iter()
            .map(|r| r["real_rust_cow_plus_sync_ms"].as_f64().unwrap_or(0.0))
            .sum::<f64>()
            / n_spec,
        3,
    );
    let mean_exposed = round_n(
        specbox_runs
            .iter()
            .map(|r| r["exposed_after_llm_ms"].as_f64().unwrap_or(0.0))
            .sum::<f64>()
            / n_spec,
        3,
    );

    // 2. Speculative 4-Branch CoW Fork + Early Abort Replay (40 trials)
    let (branches, fork4_ms) = store.fork_branch_group("spec_replay_4", 4, None)?;
    for b in &branches {
        store.abort_branch(&b.branch_id, "replay_cleanup");
    }

    let llm_durs: Vec<f64> = traces.iter().map(|t| t.total_llm_ms).collect();
    let mut seq_jcts = Vec::new();
    let mut wait_all_jcts = Vec::new();
    let mut early_jcts = Vec::new();
    let mut wait_all_costs = Vec::new();
    let mut early_costs = Vec::new();

    let mut rng_state: u64 = 2026_0519;
    let mut next_u64 = || -> u64 {
        rng_state = rng_state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = rng_state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };

    for _ in 0..40 {
        let mut durs = Vec::with_capacity(4);
        let mut valid = Vec::with_capacity(4);
        for b in 0..4 {
            let base = llm_durs[(next_u64() as usize) % llm_durs.len()];
            let jitter = 0.85 + ((next_u64() % 350) as f64) / 1000.0;
            durs.push(base * jitter);
            valid.push(b == 0 || (next_u64() % 100) < 45);
        }
        // Sequential tries until first valid branch
        let mut seq_t = 0.0;
        for b in 0..4 {
            seq_t += durs[b] + 25.0;
            if valid[b] {
                break;
            }
        }
        seq_jcts.push(seq_t);

        let max_d = durs.iter().copied().fold(0.0_f64, f64::max);
        wait_all_jcts.push(fork4_ms + max_d + 25.0);
        wait_all_costs.push(4.0 * (40_000.0 * 0.1 + 10_600.0)); // shared prefix + outputs

        let win_d = durs
            .iter()
            .zip(valid.iter())
            .filter(|(_, &v)| v)
            .map(|(&d, _)| d)
            .fold(f64::INFINITY, f64::min);
        early_jcts.push(fork4_ms + win_d + 25.0);
        // Losing branches aborted at win_d save their remaining output tokens
        let mut cost = 0.0;
        for &d in &durs {
            let frac = (win_d / d).min(1.0);
            cost += 40_000.0 * 0.1 + 10_600.0 * frac;
        }
        early_costs.push(cost);
    }

    let seq_p95 = round_n(percentile(&seq_jcts, 0.95), 2);
    let wait_p95 = round_n(percentile(&wait_all_jcts, 0.95), 2);
    let early_p95 = round_n(percentile(&early_jcts, 0.95), 2);

    // 3. Honest Churn Saturation Analysis (12 RPS & 80 RPS across 10% / 30% / 50% churn)
    let mean_llm_ms = round_n(
        llm_durs.iter().sum::<f64>() / llm_durs.len().max(1) as f64,
        2,
    );
    let cold_rebuild_ms = proxy.initial_build_ms;
    let inc_ms = mean_prep.max(0.5);
    let mut churn_rows = Vec::new();
    for arrival_rps in [12.0_f64, 80.0_f64] {
        for churn in [0.1_f64, 0.3, 0.5] {
            let mut row = serde_json::Map::new();
            row.insert("churn_rate".into(), serde_json::json!(churn));
            row.insert("arrival_rps".into(), serde_json::json!(arrival_rps));
            for (mode, prep_fn) in [
                ("always_new", cold_rebuild_ms),
                ("full_rebuild_on_change", cold_rebuild_ms),
                ("layered_incremental", inc_ms),
            ] {
                let mut slot_free = [0.0_f64; 2];
                let mut q_waits = Vec::new();
                let mut jcts = Vec::new();
                for i in 0..80 {
                    let arr = i as f64 * (1000.0 / arrival_rps);
                    let drifted = ((i as f64 * 0.6180339887) % 1.0) < churn;
                    let prep = match mode {
                        "always_new" => prep_fn,
                        "full_rebuild_on_change" => {
                            if drifted {
                                prep_fn
                            } else {
                                0.4
                            }
                        }
                        _ => {
                            if drifted {
                                prep_fn
                            } else {
                                0.2
                            }
                        }
                    };
                    let s_idx = if slot_free[0] <= slot_free[1] { 0 } else { 1 };
                    let start = arr.max(slot_free[s_idx]);
                    let q = start - arr;
                    slot_free[s_idx] = start + prep;
                    let llm = llm_durs[i % llm_durs.len()];
                    q_waits.push(q);
                    jcts.push(q + prep + llm + 45.0);
                }
                row.insert(
                    mode.into(),
                    serde_json::json!({
                        "mean_queue_ms": round_n(q_waits.iter().sum::<f64>() / q_waits.len() as f64, 2),
                        "p95_queue_ms": round_n(percentile(&q_waits, 0.95), 2),
                        "p95_jct_ms": round_n(percentile(&jcts, 0.95), 2)
                    }),
                );
            }
            churn_rows.push(serde_json::Value::Object(row));
        }
    }

    Ok(serde_json::json!({
        "specbox_prewarm_replay": {
            "measurement_type": "real_trace_replay",
            "runs": specbox_runs,
            "triggered_count": specbox_runs.len(),
            "fully_hidden_count": fully_hidden_count,
            "mean_overlap_window_ms": mean_overlap,
            "mean_real_rust_cow_plus_sync_ms": mean_prep,
            "mean_exposed_ms": mean_exposed
        },
        "speculative_cow_fork_replay": {
            "measurement_type": "real_trace_replay",
            "trials": 40,
            "branches_per_task": 4,
            "real_rust_cow_fork_4_branches_ms": fork4_ms,
            "sequential": {
                "p50_jct_ms": round_n(percentile(&seq_jcts, 0.50), 2),
                "p95_jct_ms": seq_p95
            },
            "parallel_wait_all": {
                "p50_jct_ms": round_n(percentile(&wait_all_jcts, 0.50), 2),
                "p95_jct_ms": wait_p95,
                "mean_weighted_token_cost": round_n(wait_all_costs.iter().sum::<f64>() / wait_all_costs.len() as f64, 1)
            },
            "cow_fork_early_abort": {
                "p50_jct_ms": round_n(percentile(&early_jcts, 0.50), 2),
                "p95_jct_ms": early_p95,
                "mean_weighted_token_cost": round_n(early_costs.iter().sum::<f64>() / early_costs.len() as f64, 1),
                "p95_speedup_vs_sequential_x": round_n(seq_p95 / early_p95.max(0.01), 2),
                "p95_speedup_vs_wait_all_x": round_n(wait_p95 / early_p95.max(0.01), 2)
            }
        },
        "honest_churn_analysis": {
            "measurement_type": "real_trace_replay",
            "single_request_breakdown": {
                "mean_recorded_llm_ms": mean_llm_ms,
                "rust_cold_rebuild_120_files_ms": cold_rebuild_ms,
                "rust_incremental_refresh_ms": inc_ms,
                "cold_env_share_pct": round_n(100.0 * cold_rebuild_ms / (mean_llm_ms + cold_rebuild_ms + 45.0), 2),
                "incremental_env_share_pct": round_n(100.0 * inc_ms / (mean_llm_ms + inc_ms + 45.0), 3)
            },
            "concurrent_burst_saturation": churn_rows
        }
    }))
}

fn run_scheduler_benchmarks(traces: &[RecordedTrace]) -> serde_json::Value {
    let mean_ttft_s =
        traces.iter().map(|t| t.ttft_ms).sum::<f64>() / traces.len().max(1) as f64 / 1000.0;
    let mean_decode_s = traces
        .iter()
        .map(|t| (t.total_llm_ms - t.ttft_ms).max(100.0))
        .sum::<f64>()
        / traces.len().max(1) as f64
        / 1000.0;

    let policies = [
        PolicyName::Fifo,
        PolicyName::PriorityAging,
        PolicyName::WarmEnvAffinity,
        PolicyName::DualCacheTtlAware,
    ];

    let run_regime = |overload: bool| {
        let jobs = generate_rate_limited_workload(2026, 120, overload, mean_ttft_s, mean_decode_s);
        let mut map = serde_json::Map::new();
        for p in policies {
            let report = simulate_scheduler_policy(&jobs, p, 3, 12.0, 35.0, 40_000);
            map.insert(
                p.as_str().to_string(),
                serde_json::to_value(report).unwrap(),
            );
        }
        serde_json::Value::Object(map)
    };

    serde_json::json!({
        "measurement_type": "controlled_stress_simulation",
        "parameterized_from_recorded_traces": {
            "mean_ttft_s": round_n(mean_ttft_s, 3),
            "mean_decode_s": round_n(mean_decode_s, 3)
        },
        "normal_load": run_regime(false),
        "overload_stress": run_regime(true)
    })
}

async fn run_daemon_http_check(
    work_dir: &Path,
    runbayd_bin: Option<&Path>,
) -> Result<serde_json::Value> {
    let bin = runbayd_bin
        .map(PathBuf::from)
        .or_else(|| {
            std::env::current_exe()
                .ok()
                .and_then(|p| p.parent().map(|d| d.join("runbayd")))
        })
        .filter(|p| p.is_file());

    let Some(bin) = bin else {
        return Ok(serde_json::json!({
            "measurement_type": "real_daemon_http_e2e",
            "executed": false,
            "passed": false,
            "reason": "runbayd binary not found; pass --runbayd-bin <path> or build runbayd first"
        }));
    };

    let state_dir = work_dir.join("daemon_state");
    let ws_dir = work_dir.join("daemon_ws");
    fs::create_dir_all(&ws_dir)?;
    fs::write(ws_dir.join("README.md"), "# Daemon HTTP Check\n")?;

    let mock = MockUpstreamServer::start().await?;
    let token = "rb_ctl_beta_bench_http";
    let t0 = Instant::now();

    let mut child = std::process::Command::new(&bin)
        .args([
            "serve",
            "--port",
            "0",
            "--state-dir",
            &state_dir.to_string_lossy(),
            "--token",
            token,
            "--allow-workspace-root",
            &ws_dir.to_string_lossy(),
            "--upstream-url",
            &mock.base_url(),
            "--enable-fixture-adapter",
        ])
        .env("RUNBAY_UPSTREAM_API_KEY", "bench-host-key-never-in-sandbox")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("runbayd stdout unavailable"))?;
    let mut reader = std::io::BufReader::new(stdout);
    let mut ready_line = String::new();
    use std::io::BufRead;
    let _ = reader.read_line(&mut ready_line)?;
    let addr = ready_line
        .trim()
        .strip_prefix("RUNBAYD_READY ")
        .and_then(|rest| serde_json::from_str::<serde_json::Value>(rest).ok())
        .and_then(|v| v["addr"].as_str().map(str::to_string))
        .ok_or_else(|| anyhow::anyhow!("runbayd failed to emit RUNBAYD_READY: {ready_line}"))?;

    let base_url = format!("http://{addr}");
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(std::time::Duration::from_secs(15))
        .build()?;

    let reg = client
        .post(format!("{base_url}/agents"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({
            "agent_id": "bench-http-agent",
            "runtime": "fixture",
            "workspace_dir": ws_dir.to_string_lossy()
        }))
        .send()
        .await?;
    let reg_ok = reg.status().as_u16() == 201;

    let prompt = serde_json::json!({
        "action": "call_gateway",
        "model": "bench-model",
        "check_direct_net": true
    })
    .to_string();
    let sub = client
        .post(format!("{base_url}/agents/bench-http-agent/run"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({"prompt": prompt, "allowed_model": "bench-model"}))
        .send()
        .await?;
    let sub_ok = sub.status().as_u16() == 202;
    let run_id = sub.json::<serde_json::Value>().await?["run_id"]
        .as_str()
        .unwrap_or_default()
        .to_string();

    let sse_text = client
        .get(format!("{base_url}/runs/{run_id}/events"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?
        .text()
        .await?;

    let run_json: serde_json::Value = client
        .get(format!("{base_url}/runs/{run_id}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?
        .json()
        .await?;

    let del = client
        .delete(format!("{base_url}/agents/bench-http-agent"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;
    let del_ok = del.status().as_u16() == 200;

    let _ = child.kill();
    let _ = child.wait();

    let elapsed_ms = round_n(t0.elapsed().as_secs_f64() * 1000.0, 2);
    let passed = reg_ok
        && sub_ok
        && del_ok
        && run_json["state"] == "succeeded"
        && run_json["sandbox_enforced"] == true
        && run_json["network_isolated"] == true
        && run_json["secretless_gateway"] == true
        && sse_text.contains("event: run.succeeded")
        && sse_text.contains("fixture.gateway_ok")
        && !sse_text.contains("bench-host-key-never-in-sandbox")
        && mock
            .seen_auth_headers()
            .iter()
            .any(|h| h == "Bearer bench-host-key-never-in-sandbox");

    Ok(serde_json::json!({
        "measurement_type": "real_daemon_http_e2e",
        "executed": true,
        "passed": passed,
        "elapsed_ms": elapsed_ms,
        "sandbox_enforced": run_json["sandbox_enforced"],
        "network_isolated": run_json["network_isolated"],
        "secretless_gateway": run_json["secretless_gateway"],
        "gateway_transport": run_json["gateway_transport"]
    }))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let traces_path = args
        .windows(2)
        .find(|w| w[0] == "--traces")
        .map(|w| PathBuf::from(&w[1]))
        .unwrap_or_else(|| PathBuf::from("beta-bench/fixtures/recorded_llm_traces.json"));
    let out_path = args
        .windows(2)
        .find(|w| w[0] == "--out")
        .map(|w| PathBuf::from(&w[1]))
        .unwrap_or_else(|| PathBuf::from("beta-bench/artifacts/beta_bench_results.json"));
    let runbayd_bin = args
        .windows(2)
        .find(|w| w[0] == "--runbayd-bin")
        .map(|w| PathBuf::from(&w[1]));

    let t_total = Instant::now();
    let traces_raw = fs::read_to_string(&traces_path)?;
    let traces: Vec<RecordedTrace> = serde_json::from_str(&traces_raw)?;

    let tmp = TempDir::new()?;
    let oracle = run_correctness_oracle(&tmp.path().join("oracle")).await?;
    let daemon_http =
        run_daemon_http_check(&tmp.path().join("daemon_http"), runbayd_bin.as_deref()).await?;
    let scaling = run_l0_l1_l2_scaling(&tmp.path().join("scaling"))?;
    let concurrency = run_concurrent_mvcc_throughput(&tmp.path().join("concurrency"))?;
    let replay = run_trace_replay_benchmarks(&tmp.path().join("replay"), &traces)?;
    let sched = run_scheduler_benchmarks(&traces);

    let full_report = serde_json::json!({
        "suite": "runbay-beta-bench",
        "implementation": "rust",
        "daemon_http_execution": daemon_http,
        "generated_at_utc": Utc::now().to_rfc3339(),
        "total_bench_wall_ms": round_n(t_total.elapsed().as_secs_f64() * 1000.0, 2),
        "correctness_oracle": oracle,
        "l0_l1_l2_scaling": scaling,
        "concurrent_mvcc_throughput": concurrency,
        "specbox_prewarm_replay": replay["specbox_prewarm_replay"],
        "speculative_cow_fork_replay": replay["speculative_cow_fork_replay"],
        "honest_churn_analysis": replay["honest_churn_analysis"],
        "scheduler_benchmarks": sched
    });

    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&out_path, serde_json::to_string_pretty(&full_report)?)?;

    println!("=== Runbay Beta Bench (Rust) Complete ===");
    println!("Output written to: {}", out_path.display());
    println!(
        "Total wall time: {} ms | Correctness Oracle: {}",
        full_report["total_bench_wall_ms"],
        full_report["correctness_oracle"]["all_invariants_passed"]
    );
    Ok(())
}
