//! `DecisionLedger` — append-only JSONL decision/actual log (P3a, design §4.3).
//!
//! Two row kinds:
//! - `decision`: what the shadow (later active) scheduler *would* do at submit
//!   — decision, score, `rem_ttl_pred`, `est_wait_pred`, `est_service`, reason.
//! - `actual`: run-outcome actuals — queue wait, gateway hit stats, termination
//!   attribution (`exit | timeout | cancelled | failed | oom_killed`).
//!
//! Purposes: exact scheduling replay, `est_service` calibration (P5), real
//! rescue/hit attribution, TTL-drift detection (predicted vs actual hit gap).
//!
//! Tenant hygiene (design §4.6 / review C4): rows carry a tenant id for
//! offline analysis, but live `/metrics` must only export global aggregates.

use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionRow {
    pub kind: &'static str, // "decision"
    /// Unix seconds.
    pub ts: f64,
    pub run_id: String,
    pub tenant: String,
    /// Cache-cluster id (Space/doc prefix group) — recon groups per-cluster
    /// inter-arrival gaps on this field (L2 reconciliation).
    #[serde(default)]
    pub cluster: String,
    /// "admit" | "defer" | "reject"
    pub decision: String,
    pub score: f64,
    pub rem_ttl_pred: Option<f64>,
    pub est_wait_pred: f64,
    pub est_service: f64,
    /// "budget_reject" | "rescue_window" | "queue_full" | "normal"
    pub reason: String,
    pub cap: f64,
    pub in_flight: usize,
    pub queued: usize,
    pub deferred: usize,
    pub defer_until: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActualRow {
    pub kind: &'static str, // "actual"
    pub ts: f64,
    pub run_id: String,
    pub tenant: String,
    #[serde(default)]
    pub cluster: String,
    pub queue_wait_actual: f64,
    pub defer_wait_actual: Option<f64>,
    /// Run wall-clock service time (start→end, seconds) for `est_service`
    /// residual calibration (P5). 0 when start time is unknown.
    #[serde(default)]
    pub service_actual: f64,
    /// Sum of gateway-call total durations (ms) over the run's calls; 0 if no
    /// calls. The gateway does not expose TTFT, so this is total call time,
    /// not time-to-first-token (L1).
    pub total_ms_sum: f64,
    pub cached_tokens_sum: u64,
    pub prompt_tokens_sum: u64,
    /// True if any call of the run hit (`cached_tokens > 0`).
    pub actual_hit: bool,
    /// "exit" | "timeout" | "cancelled" | "failed" | "oom_killed"
    pub termination: String,
}

pub struct DecisionLedger {
    path: PathBuf,
    /// Serializes the open+write+flush critical section. `register_submit`,
    /// `log_actual` and dispatch logging are all `&self` methods reachable from
    /// multiple tokio tasks, so a bare `writeln!` here raced (two `write`
    /// syscalls — JSON body then `\n` — interleaved, producing a corrupt
    /// `{row}{row}` line) and silently dropped decision rows under concurrent
    /// submits. The lock + single `write_all` make each row atomic.
    write_lock: Mutex<()>,
}

impl DecisionLedger {
    pub fn open(state_dir: &Path) -> std::io::Result<Self> {
        let dir = state_dir.join("scheduler");
        fs::create_dir_all(&dir)?;
        Ok(Self {
            path: dir.join("decisions.jsonl"),
            write_lock: Mutex::new(()),
        })
    }

    /// In-memory-only ledger path (tests).
    pub fn at(path: PathBuf) -> Self {
        Self {
            path,
            write_lock: Mutex::new(()),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn log_decision(&self, row: &DecisionRow) {
        self.append(row);
    }

    pub fn log_actual(&self, row: &ActualRow) {
        self.append(row);
    }

    fn append<T: Serialize>(&self, row: &T) {
        // Serialize outside the lock so a slow serialize can't hold it; the
        // lock only guards the open+write+flush critical section.
        let Ok(mut line) = serde_json::to_string(row) else {
            return;
        };
        line.push('\n');
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
        {
            Ok(mut f) => {
                // Single write_all (O_APPEND is atomic for one syscall) plus the
                // lock above guarantees one row per line even under concurrent
                // submits — a split JSON-then-newline write was corrupting rows.
                if f.write_all(line.as_bytes()).is_err() || f.flush().is_err() {
                    // L2: surface (don't swallow) ledger write failures so a
                    // full disk / permission error can't silently drop
                    // decision/actual rows.
                    eprintln!(
                        "runbayd: DecisionLedger write failed for {}",
                        self.path.display()
                    );
                }
            }
            Err(e) => {
                eprintln!(
                    "runbayd: DecisionLedger open failed for {}: {e}",
                    self.path.display()
                );
            }
        }
    }
}

/// Map a run's terminal state/error to the termination attribution bucket
/// (design §4.3 / review B6): OOM and sandbox failures are *infra*, never
/// scheduling starvation.
pub fn termination_bucket(state: &str, error_code: Option<&str>) -> String {
    let ec = error_code.unwrap_or("");
    if ec.contains("oom") {
        return "oom_killed".into();
    }
    match state {
        "succeeded" => "exit".into(),
        "cancelled" => "cancelled".into(),
        "failed" => {
            if ec.contains("timeout") {
                "timeout".into()
            } else {
                "failed".into()
            }
        }
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decision_and_actual_rows_roundtrip() {
        let dir = std::env::temp_dir().join(format!("rb_ledger_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let ledger = DecisionLedger::open(&dir).unwrap();

        ledger.log_decision(&DecisionRow {
            kind: "decision",
            ts: 1.0,
            run_id: "run_a".into(),
            tenant: "t1".into(),
            cluster: "c1".into(),
            decision: "defer".into(),
            score: 540.0,
            rem_ttl_pred: Some(28.0),
            est_wait_pred: 3.0,
            est_service: 4.0,
            reason: "rescue_window".into(),
            cap: 3.0,
            in_flight: 2,
            queued: 1,
            deferred: 0,
            defer_until: Some(25.0),
        });
        ledger.log_actual(&ActualRow {
            kind: "actual",
            ts: 30.0,
            run_id: "run_a".into(),
            tenant: "t1".into(),
            cluster: "c1".into(),
            queue_wait_actual: 2.5,
            defer_wait_actual: Some(20.0),
            service_actual: 4.2,
            total_ms_sum: 2200.0,
            cached_tokens_sum: 38000,
            prompt_tokens_sum: 40000,
            actual_hit: true,
            termination: "exit".into(),
        });

        let raw = fs::read_to_string(ledger.path()).unwrap();
        let lines: Vec<&str> = raw.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"decision\":\"defer\""));
        assert!(lines[0].contains("\"cluster\":\"c1\""));
        assert!(lines[1].contains("\"termination\":\"exit\""));
        assert!(lines[1].contains("\"service_actual\":4.2"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn termination_buckets() {
        assert_eq!(termination_bucket("succeeded", None), "exit");
        assert_eq!(
            termination_bucket("cancelled", Some("cancelled_while_queued")),
            "cancelled"
        );
        assert_eq!(
            termination_bucket("failed", Some("timeout_exceeded")),
            "timeout"
        );
        assert_eq!(termination_bucket("failed", Some("spawn_failed")), "failed");
        assert_eq!(
            termination_bucket("failed", Some("sandbox_oom_killed")),
            "oom_killed"
        );
    }

    // Regression: concurrent `log_decision`/`log_actual` must never interleave a
    // JSON body with the trailing newline (which produced a corrupt `{row}{row}`
    // line and silently dropped rows under concurrent submits).
    #[test]
    fn concurrent_appends_never_corrupt_lines() {
        let dir = std::env::temp_dir().join(format!("rb_ledger_conc_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let ledger = std::sync::Arc::new(DecisionLedger::open(&dir).unwrap());

        let n_threads = 8;
        let n_rows = 50;
        let mut handles = Vec::new();
        for t in 0..n_threads {
            let ledger = std::sync::Arc::clone(&ledger);
            handles.push(std::thread::spawn(move || {
                for i in 0..n_rows {
                    ledger.log_decision(&DecisionRow {
                        kind: "decision",
                        ts: 0.0,
                        run_id: format!("run_t{t}_i{i}"),
                        tenant: "t".into(),
                        cluster: "c".into(),
                        decision: "admit".into(),
                        score: 100.0,
                        rem_ttl_pred: None,
                        est_wait_pred: 0.0,
                        est_service: 4.0,
                        reason: "normal".into(),
                        cap: 4.0,
                        in_flight: 0,
                        queued: 0,
                        deferred: 0,
                        defer_until: None,
                    });
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let raw = fs::read_to_string(ledger.path()).unwrap();
        let mut parsed = 0usize;
        for line in raw.lines() {
            if line.is_empty() {
                continue;
            }
            // Every non-empty line must be a complete, valid JSON row — a
            // split/interleaved write would yield `{...}{...}` and fail here.
            let v: serde_json::Value = serde_json::from_str(line)
                .unwrap_or_else(|e| panic!("corrupt ledger line: {e}\nline: {line}"));
            assert_eq!(v["kind"], "decision");
            parsed += 1;
        }
        assert_eq!(parsed, n_threads * n_rows);
        let _ = fs::remove_dir_all(&dir);
    }
}
