//! `bench_recon` — shadow-ledger reconciliation (P3a exit criterion, design §7).
//!
//! Reads `<state_dir>/scheduler/decisions.jsonl` (decision+actual dual rows),
//! joins by `run_id`, and reports:
//!
//! 1. Four-quadrant table (predicted warm/cold × actual hit/miss);
//! 2. **Overestimation share + Wilson upper bound** — the A1 kill criterion
//!    ("no systematic overestimation" of cache hits);
//! 3. Predicted-vs-actual hit-rate diff with 95% CI (underestimation is
//!    reported but not penalized — TTL-pessimism is a calibration issue);
//! 4. Hit rate bucketed by per-cluster inter-arrival gap (TTL drift readout);
//! 5. `est_service` / `est_wait` residual distributions (P5 calibration input).
//!
//! Verdict gates (B4 four-way accounting; sample floor ≥30 per prediction
//! side):
//! - `PASS`  : overestimation share ≤ 5% and Wilson upper ≤ 15% and
//!             hit-rate diff CI upper ≤ 5pp;
//! - `WARN`  : share ≤ 10% and diff CI upper ≤ 10pp;
//! - `FAIL`  : otherwise — do NOT enable P3b Active.
//!
//! Wilson bounds shrink with n: zero overestimation at n=30 gives ~11.4%
//! upper, at n=73 gives ~5%. The report prints both so larger L2 runs get
//! statistically stronger PASSes for free.
//!
//! Usage: `bench_recon <state_dir | path/to/decisions.jsonl>`
//! Writes `recon_report.json` next to the JSONL and prints a markdown report.

use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
struct DecisionRec {
    ts: f64,
    cluster: String,
    est_wait_pred: f64,
    est_service: f64,
    rem_ttl_pred: Option<f64>,
}

#[derive(Debug, Clone)]
struct ActualRec {
    queue_wait_actual: f64,
    service_actual: f64,
    cached_tokens: u64,
    prompt_tokens: u64,
    hit: bool,
    termination: String,
}

#[derive(Debug)]
pub struct ReconReport {
    pub n_joined: usize,
    pub n_orphan_decision: usize,
    pub n_orphan_actual: usize,
    /// Joined runs excluded from the A1 overestimation signal because their
    /// termination was not a normal completion (M2).
    pub n_excluded_non_exit: usize,
    pub q_hit_hit: usize,
    pub q_hit_miss: usize,
    pub q_miss_hit: usize,
    pub q_miss_miss: usize,
    pub n_pred_warm: usize,
    pub n_pred_miss: usize,
    pub over_share: f64,
    pub over_wilson_upper: f64,
    pub hit_rate_diff: f64,
    pub diff_ci: (f64, f64),
    pub token_ratio_warm_p50: f64,
    pub gap_buckets: Vec<(String, usize, f64, f64)>,
    pub est_service_residual: (f64, f64, f64),
    pub est_wait_residual: (f64, f64, f64),
    pub terminations: Vec<(String, usize)>,
    pub verdict: String,
}

fn pred_warm(rem: Option<f64>) -> bool {
    matches!(rem, Some(r) if r > 0.0)
}

/// Wilson score interval upper bound (two-sided z, default 1.96).
fn wilson_upper(k: usize, n: usize, z: f64) -> f64 {
    if n == 0 {
        return 1.0;
    }
    let p = k as f64 / n as f64;
    let z2 = z * z;
    let denom = 1.0 + z2 / n as f64;
    let center = p + z2 / (2.0 * n as f64);
    let margin = z * ((p * (1.0 - p) / n as f64) + z2 / (4.0 * n as f64 * n as f64)).sqrt();
    ((center + margin) / denom).min(1.0)
}

fn quantiles(mut v: Vec<f64>) -> (f64, f64, f64) {
    if v.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mean = v.iter().sum::<f64>() / v.len() as f64;
    let pick = |q: f64| v[(((v.len() - 1) as f64) * q).round() as usize];
    (mean, pick(0.5), pick(0.9))
}

fn f64_field(v: &Value, key: &str) -> f64 {
    v.get(key).and_then(|x| x.as_f64()).unwrap_or(0.0)
}

fn opt_f64_field(v: &Value, key: &str) -> Option<f64> {
    v.get(key).and_then(|x| x.as_f64())
}

pub fn run_recon(jsonl: &Path) -> std::io::Result<ReconReport> {
    let raw = std::fs::read_to_string(jsonl)?;
    let mut decisions: HashMap<String, DecisionRec> = HashMap::new();
    let mut actuals: HashMap<String, ActualRec> = HashMap::new();

    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(v) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        match v.get("kind").and_then(|k| k.as_str()) {
            Some("decision") => {
                let run_id = v.get("run_id").and_then(|x| x.as_str()).unwrap_or("").to_string();
                if run_id.is_empty() {
                    continue;
                }
                decisions.insert(
                    run_id,
                    DecisionRec {
                        ts: f64_field(&v, "ts"),
                        cluster: v
                            .get("cluster")
                            .and_then(|x| x.as_str())
                            .unwrap_or("")
                            .to_string(),
                        est_wait_pred: f64_field(&v, "est_wait_pred"),
                        est_service: f64_field(&v, "est_service"),
                        rem_ttl_pred: opt_f64_field(&v, "rem_ttl_pred"),
                    },
                );
            }
            Some("actual") => {
                let run_id = v.get("run_id").and_then(|x| x.as_str()).unwrap_or("").to_string();
                if run_id.is_empty() {
                    continue;
                }
                actuals.insert(
                    run_id,
                    ActualRec {
                        queue_wait_actual: f64_field(&v, "queue_wait_actual"),
                        service_actual: f64_field(&v, "service_actual"),
                        cached_tokens: v.get("cached_tokens_sum").and_then(|x| x.as_u64()).unwrap_or(0),
                        prompt_tokens: v.get("prompt_tokens_sum").and_then(|x| x.as_u64()).unwrap_or(0),
                        hit: v.get("actual_hit").and_then(|x| x.as_bool()).unwrap_or(false),
                        termination: v
                            .get("termination")
                            .and_then(|x| x.as_str())
                            .unwrap_or("unknown")
                            .to_string(),
                    },
                );
            }
            _ => {}
        }
    }

    let mut n_joined = 0usize;
    let mut n_excluded_non_exit = 0usize;
    let (mut q_hh, mut q_hm, mut q_mh, mut q_mm) = (0usize, 0usize, 0usize, 0usize);
    let (mut n_warm, mut n_miss) = (0usize, 0usize);
    let mut diffs: Vec<f64> = Vec::new();
    let mut warm_token_ratios: Vec<f64> = Vec::new();
    let mut service_res: Vec<f64> = Vec::new();
    let mut wait_res: Vec<f64> = Vec::new();
    let mut term_counts: HashMap<String, usize> = HashMap::new();
    // Per-cluster ordered rows for gap buckets: (cluster, ts, pred_warm, hit).
    let mut per_cluster: HashMap<String, Vec<(f64, bool, bool)>> = HashMap::new();

    for (run_id, d) in decisions.iter() {
        let Some(a) = actuals.get(run_id) else {
            continue;
        };
        n_joined += 1;
        *term_counts.entry(a.termination.clone()).or_insert(0) += 1;

        // M2: the A1 overestimation signal must only count runs that actually
        // completed a gateway call (`termination == "exit"`). Cancelled / failed
        // / oom / timeout runs never (or only partially) reached the gateway,
        // so their `actual_hit == false` is not a scheduling overestimate — it
        // would otherwise inflate the overestimation share and trigger a false
        // A1 FAIL in production reconciliation.
        let a1_eligible = a.termination == "exit";
        let warm = pred_warm(d.rem_ttl_pred);
        if a1_eligible {
            if warm {
                n_warm += 1;
            } else {
                n_miss += 1;
            }
            match (warm, a.hit) {
                (true, true) => q_hh += 1,
                (true, false) => q_hm += 1,
                (false, true) => q_mh += 1,
                (false, false) => q_mm += 1,
            }
            diffs.push(warm as i8 as f64 - a.hit as i8 as f64);
            if warm && a.prompt_tokens > 0 {
                warm_token_ratios.push(a.cached_tokens as f64 / a.prompt_tokens as f64);
            }
        } else {
            n_excluded_non_exit += 1;
        }
        if a.service_actual > 0.0 {
            service_res.push(a.service_actual - d.est_service);
            wait_res.push(a.queue_wait_actual - d.est_wait_pred);
        }
        if a1_eligible {
            let key = if d.cluster.is_empty() {
                format!("__run_{run_id}")
            } else {
                d.cluster.clone()
            };
            per_cluster.entry(key).or_default().push((d.ts, warm, a.hit));
        }
    }
    let n_orphan_decision = decisions.len() - n_joined;
    let n_orphan_actual = actuals.len().saturating_sub(n_joined);

    // Hit-rate diff (paired): mean ± 1.96·se.
    let (hit_rate_diff, diff_ci) = if diffs.is_empty() {
        (0.0, (0.0, 0.0))
    } else {
        let n = diffs.len() as f64;
        let mean = diffs.iter().sum::<f64>() / n;
        let var = if diffs.len() > 1 {
            diffs.iter().map(|d| (d - mean).powi(2)).sum::<f64>() / (n - 1.0)
        } else {
            0.0
        };
        let se = (var / n).sqrt();
        (mean, (mean - 1.96 * se, mean + 1.96 * se))
    };

    let over_share = if n_warm > 0 { q_hm as f64 / n_warm as f64 } else { 0.0 };
    let over_wilson_upper = wilson_upper(q_hm, n_warm, 1.96);
    let token_ratio_warm_p50 = quantiles(warm_token_ratios.clone()).1;

    // Gap buckets: within-cluster inter-arrival.
    let mut buckets: Vec<(String, usize, f64, f64)> = vec![
        ("first_seen".into(), 0, 0.0, 0.0),
        ("<60s".into(), 0, 0.0, 0.0),
        ("60-600s".into(), 0, 0.0, 0.0),
        ("600-3600s".into(), 0, 0.0, 0.0),
        (">3600s".into(), 0, 0.0, 0.0),
    ];
    {
        let mut hit_sum = vec![0.0f64; buckets.len()];
        let mut warm_sum = vec![0.0f64; buckets.len()];
        for rows in per_cluster.values_mut() {
            rows.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            let mut prev: Option<f64> = None;
            for (ts, warm, hit) in rows.iter() {
                let idx = match prev {
                    None => 0,
                    Some(p) => {
                        let gap = ts - p;
                        if gap < 60.0 {
                            1
                        } else if gap < 600.0 {
                            2
                        } else if gap < 3600.0 {
                            3
                        } else {
                            4
                        }
                    }
                };
                buckets[idx].1 += 1;
                hit_sum[idx] += *hit as i8 as f64;
                warm_sum[idx] += *warm as i8 as f64;
                prev = Some(*ts);
            }
        }
        for i in 0..buckets.len() {
            let n = buckets[i].1;
            if n > 0 {
                buckets[i].2 = hit_sum[i] / n as f64;
                buckets[i].3 = warm_sum[i] / n as f64;
            }
        }
    }

    let est_service_residual = quantiles(service_res);
    let est_wait_residual = quantiles(wait_res);
    let mut terminations: Vec<(String, usize)> = term_counts.into_iter().collect();
    terminations.sort_by(|a, b| b.1.cmp(&a.1));

    let verdict = if n_warm < 30 || n_miss < 30 {
        "INSUFFICIENT_DATA — need >=30 predicted-warm and >=30 predicted-miss completed (exit) samples"
            .to_string()
    } else if over_share <= 0.05 && over_wilson_upper <= 0.15 && diff_ci.1 <= 0.05 {
        "PASS — no systematic overestimation (A1 clear)".to_string()
    } else if over_share <= 0.10 && over_wilson_upper <= 0.25 && diff_ci.1 <= 0.10 {
        "WARN — mild overestimation signal; inspect drift buckets before P3b".to_string()
    } else {
        "FAIL — systematic overestimation suspected (A1); do NOT enable P3b Active".to_string()
    };

    Ok(ReconReport {
        n_joined,
        n_orphan_decision,
        n_orphan_actual,
        n_excluded_non_exit,
        q_hit_hit: q_hh,
        q_hit_miss: q_hm,
        q_miss_hit: q_mh,
        q_miss_miss: q_mm,
        n_pred_warm: n_warm,
        n_pred_miss: n_miss,
        over_share,
        over_wilson_upper,
        hit_rate_diff,
        diff_ci,
        token_ratio_warm_p50,
        gap_buckets: buckets,
        est_service_residual,
        est_wait_residual,
        terminations,
        verdict,
    })
}

pub fn render_markdown(r: &ReconReport) -> String {
    let pct = |x: f64| format!("{:.1}%", x * 100.0);
    let mut md = String::new();
    md.push_str("# L2 Shadow-Ledger Reconciliation Report (P3a exit)\n\n");
    md.push_str(&format!("**Verdict: {}**\n\n", r.verdict));
    md.push_str(&format!(
        "Joined runs: {} (orphan decisions: {}, orphan actuals: {})\n",
        r.n_joined, r.n_orphan_decision, r.n_orphan_actual
    ));
    if r.n_excluded_non_exit > 0 {
        md.push_str(&format!(
            "- Excluded from the A1 overestimation signal (non-`exit` termination): {}\n",
            r.n_excluded_non_exit
        ));
    }
    md.push('\n');

    md.push_str("## 1. Four-quadrant table (predicted × actual)\n\n");
    md.push_str("|  | actual hit | actual miss |\n|---|---|---|\n");
    md.push_str(&format!(
        "| **predicted warm** | {} | {} (overestimate) |\n",
        r.q_hit_hit, r.q_hit_miss
    ));
    md.push_str(&format!(
        "| **predicted cold/expired** | {} (underestimate) | {} |\n\n",
        r.q_miss_hit, r.q_miss_miss
    ));
    md.push_str(&format!(
        "- Overestimation share: {} (Wilson upper {:.1}%)\n- Hit-rate diff (pred−act): {:+.4}  95% CI [{:+.4}, {:+.4}]\n- Median cached/prompt on predicted-warm runs: {}\n\n",
        pct(r.over_share),
        r.over_wilson_upper * 100.0,
        r.hit_rate_diff,
        r.diff_ci.0,
        r.diff_ci.1,
        pct(r.token_ratio_warm_p50)
    ));

    md.push_str("## 2. Per-cluster gap buckets (TTL drift readout)\n\n");
    md.push_str("| gap to prev run | n | actual hit rate | predicted-warm rate |\n|---|---|---|---|\n");
    for (label, n, hit_rate, warm_rate) in &r.gap_buckets {
        md.push_str(&format!(
            "| {label} | {n} | {} | {} |\n",
            pct(*hit_rate),
            pct(*warm_rate)
        ));
    }
    md.push_str(
        "\nUnderestimation on long gaps = our TTL is pessimistic vs provider (calibration input,\nnot an A1 failure). Overestimation on short gaps = the A1 red flag.\n\n",
    );

    md.push_str("## 3. Estimator residuals (P5 calibration input)\n\n");
    let fmt3 = |t: (f64, f64, f64)| format!("mean {:+.2}s, p50 {:+.2}s, p90 {:+.2}s", t.0, t.1, t.2);
    md.push_str(&format!("- `est_service` residual (actual−pred): {}\n", fmt3(r.est_service_residual)));
    md.push_str(&format!("- `est_wait` residual (actual−pred): {}\n\n", fmt3(r.est_wait_residual)));

    md.push_str("## 4. Termination attribution\n\n");
    for (t, n) in &r.terminations {
        md.push_str(&format!("- {t}: {n}\n"));
    }
    md
}

fn report_json(r: &ReconReport) -> Value {
    serde_json::json!({
        "verdict": r.verdict,
        "n_joined": r.n_joined,
        "n_orphan_decision": r.n_orphan_decision,
        "n_orphan_actual": r.n_orphan_actual,
        "n_excluded_non_exit": r.n_excluded_non_exit,
        "quadrants": {
            "hit_hit": r.q_hit_hit,
            "hit_miss_overestimate": r.q_hit_miss,
            "miss_hit_underestimate": r.q_miss_hit,
            "miss_miss": r.q_miss_miss,
        },
        "n_pred_warm": r.n_pred_warm,
        "n_pred_miss": r.n_pred_miss,
        "over_share": r.over_share,
        "over_wilson_upper": r.over_wilson_upper,
        "hit_rate_diff": r.hit_rate_diff,
        "diff_ci": [r.diff_ci.0, r.diff_ci.1],
        "token_ratio_warm_p50": r.token_ratio_warm_p50,
        "gap_buckets": r.gap_buckets.iter().map(|(l, n, h, w)| {
            serde_json::json!({"bucket": l, "n": n, "actual_hit_rate": h, "pred_warm_rate": w})
        }).collect::<Vec<_>>(),
        "est_service_residual": {
            "mean": r.est_service_residual.0, "p50": r.est_service_residual.1, "p90": r.est_service_residual.2
        },
        "est_wait_residual": {
            "mean": r.est_wait_residual.0, "p50": r.est_wait_residual.1, "p90": r.est_wait_residual.2
        },
        "terminations": r.terminations.iter().map(|(t, n)| {
            serde_json::json!({"bucket": t, "n": n})
        }).collect::<Vec<_>>(),
    })
}

fn main() {
    let arg = std::env::args().nth(1).unwrap_or_else(|| ".".to_string());
    let p = PathBuf::from(&arg);
    let jsonl = if p.is_dir() {
        p.join("scheduler").join("decisions.jsonl")
    } else {
        p
    };
    if !jsonl.exists() {
        eprintln!(
            "bench_recon: ledger not found at {}\n\
             hint: run `runbayd serve --state-dir <dir>` and drive traffic first, then pass <dir>",
            jsonl.display()
        );
        std::process::exit(2);
    }
    match run_recon(&jsonl) {
        Ok(report) => {
            let md = render_markdown(&report);
            println!("{md}");
            let out = jsonl.with_file_name("recon_report.json");
            let _ = std::fs::write(&out, serde_json::to_string_pretty(&report_json(&report)).unwrap());
            eprintln!("bench_recon: JSON report -> {}", out.display());
        }
        Err(e) => {
            eprintln!("bench_recon: failed to read {}: {e}", jsonl.display());
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_rows(dir: &Path, rows: &[String]) {
        let file = dir.join("scheduler").join("decisions.jsonl");
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, rows.join("\n")).unwrap();
    }

    fn row(kind: &str, run: &str, cluster: &str, warm: Option<f64>, hit: bool, tokens: u64) -> String {
        if kind == "decision" {
            serde_json::json!({
                "kind": "decision", "ts": 1000.0, "run_id": run, "tenant": "t", "cluster": cluster,
                "decision": "admit", "score": 100.0, "rem_ttl_pred": warm,
                "est_wait_pred": 1.0, "est_service": 4.0, "reason": "normal",
                "cap": 2.0, "in_flight": 0, "queued": 0, "deferred": 0, "defer_until": null
            })
            .to_string()
        } else {
            serde_json::json!({
                "kind": "actual", "ts": 1010.0, "run_id": run, "tenant": "t", "cluster": cluster,
                "queue_wait_actual": 1.0, "defer_wait_actual": null, "service_actual": 4.5,
                "total_ms_sum": 900.0, "cached_tokens_sum": if hit { tokens } else { 0 },
                "prompt_tokens_sum": tokens, "actual_hit": hit, "termination": "exit"
            })
            .to_string()
        }
    }

    #[test]
    fn recon_pass_on_overestimate_free_ledger() {
        let dir = std::env::temp_dir().join(format!("rb_recon_pass_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut rows = Vec::new();
        for i in 0..35 {
            rows.push(row("decision", &format!("r{i}"), "c1", Some(20.0), true, 4000));
            rows.push(row("actual", &format!("r{i}"), "c1", Some(20.0), true, 4000));
        }
        for i in 35..70 {
            rows.push(row("decision", &format!("r{i}"), "c1", None, true, 4000));
            rows.push(row("actual", &format!("r{i}"), "c1", None, true, 4000));
        }
        write_rows(&dir, &rows);
        let r = run_recon(&dir.join("scheduler").join("decisions.jsonl")).unwrap();
        assert_eq!(r.n_joined, 70);
        assert_eq!(r.q_hit_hit, 35);
        assert_eq!(r.q_hit_miss, 0);
        assert_eq!(r.q_miss_hit, 35);
        assert_eq!(r.q_miss_miss, 0);
        assert!(r.verdict.starts_with("PASS"), "verdict = {}", r.verdict);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn recon_fail_on_systematic_overestimation() {
        let dir = std::env::temp_dir().join(format!("rb_recon_fail_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut rows = Vec::new();
        for i in 0..35 {
            let hit = i % 7 != 0; // 30 hits, 5 misses among predicted-warm
            rows.push(row("decision", &format!("r{i}"), "c1", Some(20.0), hit, 4000));
            rows.push(row("actual", &format!("r{i}"), "c1", Some(20.0), hit, 4000));
        }
        for i in 35..70 {
            rows.push(row("decision", &format!("r{i}"), "c1", None, false, 4000));
            rows.push(row("actual", &format!("r{i}"), "c1", None, false, 4000));
        }
        write_rows(&dir, &rows);
        let r = run_recon(&dir.join("scheduler").join("decisions.jsonl")).unwrap();
        assert_eq!(r.q_hit_miss, 5);
        assert!(r.verdict.starts_with("FAIL"), "verdict = {}", r.verdict);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn recon_insufficient_on_small_sample() {
        let dir = std::env::temp_dir().join(format!("rb_recon_small_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let rows = vec![
            row("decision", "r0", "c1", Some(20.0), true, 4000),
            row("actual", "r0", "c1", Some(20.0), true, 4000),
            row("decision", "r1", "c1", None, false, 4000),
            row("actual", "r1", "c1", None, false, 4000),
        ];
        write_rows(&dir, &rows);
        let r = run_recon(&dir.join("scheduler").join("decisions.jsonl")).unwrap();
        assert!(r.verdict.starts_with("INSUFFICIENT"), "verdict = {}", r.verdict);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn recon_excludes_non_exit_terminations_from_a1() {
        // M2 regression: cancelled/failed runs must not count as "predicted warm
        // but missed" overestimation — they never hit the gateway.
        let dir = std::env::temp_dir().join(format!("rb_recon_non_exit_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut rows = Vec::new();
        for i in 0..35 {
            let cancelled = i >= 30;
            rows.push(row("decision", &format!("r{i}"), "c1", Some(20.0), !cancelled, 4000));
            if cancelled {
                rows.push(serde_json::json!({
                    "kind": "actual", "ts": 1010.0, "run_id": format!("r{i}"), "tenant": "t", "cluster": "c1",
                    "queue_wait_actual": 0.0, "defer_wait_actual": null, "service_actual": 0.0,
                    "total_ms_sum": 0.0, "cached_tokens_sum": 0, "prompt_tokens_sum": 4000,
                    "actual_hit": false, "termination": "cancelled"
                }).to_string());
            } else {
                rows.push(row("actual", &format!("r{i}"), "c1", Some(20.0), true, 4000));
            }
        }
        for i in 35..70 {
            rows.push(row("decision", &format!("r{i}"), "c1", None, false, 4000));
            rows.push(row("actual", &format!("r{i}"), "c1", None, false, 4000));
        }
        write_rows(&dir, &rows);
        let r = run_recon(&dir.join("scheduler").join("decisions.jsonl")).unwrap();
        assert_eq!(r.n_joined, 70);
        assert_eq!(r.n_excluded_non_exit, 5);
        assert_eq!(r.q_hit_miss, 0, "cancelled runs must not count as overestimation");
        assert_eq!(r.n_pred_warm, 30, "only exit warm runs count toward A1");
        assert!(r.verdict.starts_with("PASS"), "verdict = {}", r.verdict);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
