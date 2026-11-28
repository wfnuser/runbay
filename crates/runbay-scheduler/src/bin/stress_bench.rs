//! Stress benchmark for the issue-#5 scheduling policies.
//!
//! Pushes the five policies (fifo / priority_aging / warm_env_affinity /
//! dual_cache_ttl_aware / session_guarded_dual) through a matrix of pressure
//! scenarios and reports where each one breaks:
//!
//!   S0a/S0b  baseline cross-check      (issue-#5 generator, normal/overload)
//!   S1a-f    load sweep                (offered rate 0.5x .. 2x capacity)
//!   S2       burst storm               (clustered arrivals at matched rate)
//!   S3       hot-expiry storm          (60% of fg jobs are expiring-hot)
//!   S4       background flood          (55% background)
//!   S5       short-TTL hell            (TTL 60s, rescue window 12s)
//!   S6a/S6b  worker scarcity / scale   (2 workers / 6 workers)
//!   S7       tenant skew               (70% of load on one tenant)
//!
//! All cells run under sliding-TTL renewal semantics (the corrected model);
//! `session_guarded_dual` additionally gets its guardrails.
//!
//! Usage:
//!   cargo run --release -p runbay-scheduler --bin stress_bench --
//!       [--seeds 20] [--out beta-bench/artifacts/stress_bench.json]
//!       [--md beta-bench/artifacts/stress_bench.md]

use runbay_scheduler::workload::{generate_stress_workload, WorkloadSpec};
use runbay_scheduler::{
    generate_rate_limited_workload, simulate_with_config, PolicyName, SchedulerPolicyReport,
    SimConfig,
};
use std::collections::BTreeMap;
use std::fs;

const TRACE_TTFT_S: f64 = 2.937;
const TRACE_DECODE_S: f64 = 0.602;
const AGING_CAP_S: f64 = 12.0;
const MIN_RESCUE_TOKENS: usize = 40_000;

const POLICIES: [PolicyName; 5] = [
    PolicyName::Fifo,
    PolicyName::PriorityAging,
    PolicyName::WarmEnvAffinity,
    PolicyName::DualCacheTtlAware,
    PolicyName::SessionGuardedDual,
];

fn semantics_for(p: PolicyName) -> SimConfig {
    if p == PolicyName::SessionGuardedDual {
        SimConfig::renewal_guarded()
    } else {
        SimConfig::renewal()
    }
}

// ---------- statistics ----------

fn mean(v: &[f64]) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.iter().sum::<f64>() / v.len() as f64
}

fn std_sample(v: &[f64]) -> f64 {
    if v.len() < 2 {
        return 0.0;
    }
    let m = mean(v);
    (v.iter().map(|x| (x - m) * (x - m)).sum::<f64>() / (v.len() - 1) as f64).sqrt()
}

fn t_crit_95(n: usize) -> f64 {
    match n {
        0..=1 => 0.0,
        2..=4 => 4.303,
        5..=9 => 2.776,
        10..=14 => 2.228,
        15..=19 => 2.131,
        20..=24 => 2.093,
        25..=29 => 2.064,
        30..=39 => 2.045,
        _ => 2.023,
    }
}

fn ci95(v: &[f64]) -> f64 {
    if v.len() < 2 {
        return 0.0;
    }
    t_crit_95(v.len()) * std_sample(v) / (v.len() as f64).sqrt()
}

// ---------- scenario definition ----------

#[derive(Clone)]
enum Workload {
    /// The original issue-#5 trace generator (for baseline cross-check).
    Baseline { overload: bool },
    /// Parametric stress generator.
    Parametric(WorkloadSpec),
}

struct Scenario {
    id: String,
    story: String,
    knobs: String,
    workload: Workload,
    n_workers: usize,
    ttl_rescue_window_s: f64,
    /// Load-sweep cells carry their target offered rate (jobs/min).
    sweep_rate: Option<f64>,
}

fn sc_spec(mut spec: WorkloadSpec) -> Workload {
    spec.real_ttft_hit_s = TRACE_TTFT_S;
    spec.real_decode_s = TRACE_DECODE_S;
    Workload::Parametric(spec)
}

fn build_scenarios(n_jobs_stress: usize) -> Vec<Scenario> {
    let base = WorkloadSpec {
        n_jobs: n_jobs_stress,
        ..Default::default()
    };
    let mut v = Vec::new();

    // S0: baseline cross-check (issue-#5 generator, 120 jobs, 3 workers).
    for (suffix, overload) in [("normal", false), ("overload", true)] {
        v.push(Scenario {
            id: format!("S0_{}", suffix),
            story: "issue-#5 baseline generator, cross-check vs sched_eval".into(),
            knobs: format!("baseline trace, overload={}", overload),
            workload: Workload::Baseline { overload },
            n_workers: 3,
            ttl_rescue_window_s: 35.0,
            sweep_rate: None,
        });
    }

    // S1: load sweep — offered rate 22..86 jobs/min vs ~43/min capacity (3 workers).
    for (suffix, gap) in [
        ("0.5x", 2.73),
        ("0.75x", 1.88),
        ("1.0x", 1.40),
        ("1.25x", 1.11),
        ("1.5x", 0.92),
        ("2.0x", 0.70),
    ] {
        let spec = WorkloadSpec {
            mean_gap_s: gap,
            ..base.clone()
        };
        let rate = spec.nominal_rate_per_min();
        v.push(Scenario {
            id: format!("S1_load_{}", suffix),
            story: "arrival-rate sweep -> saturation curve".into(),
            knobs: format!("mean_gap {:.2}s (~{:.0} jobs/min offered)", gap, rate),
            workload: sc_spec(spec),
            n_workers: 3,
            ttl_rescue_window_s: 35.0,
            sweep_rate: Some(rate),
        });
    }

    // S2: burst storm — same ~43 jobs/min average, clustered arrivals.
    {
        let spec = WorkloadSpec {
            mean_gap_s: 1.40,
            burst: Some((3, 8)),
            ..base.clone()
        };
        v.push(Scenario {
            id: "S2_burst_storm".into(),
            story: "clustered bursts (one user action fans out) at matched mean rate".into(),
            knobs: "mean_gap 1.40s, bursts of 3-8 jobs".into(),
            workload: sc_spec(spec),
            n_workers: 3,
            ttl_rescue_window_s: 35.0,
            sweep_rate: None,
        });
    }

    // S3: hot-expiry storm — 60% of fg jobs are expiring-hot.
    {
        let spec = WorkloadSpec {
            mean_gap_s: 1.40,
            hot_share: 0.60,
            ..base.clone()
        };
        v.push(Scenario {
            id: "S3_hot_expiry_storm".into(),
            story: "rescue-pressure extreme: most fg prefixes near expiry".into(),
            knobs: "mean_gap 1.40s, hot_share 0.60".into(),
            workload: sc_spec(spec),
            n_workers: 3,
            ttl_rescue_window_s: 35.0,
            sweep_rate: None,
        });
    }

    // S4: background flood — 55% background.
    {
        let spec = WorkloadSpec {
            mean_gap_s: 1.40,
            bg_share: 0.55,
            ..base.clone()
        };
        v.push(Scenario {
            id: "S4_bg_flood".into(),
            story: "batch ingestion floods the pool, fairness/guardrail under test".into(),
            knobs: "mean_gap 1.40s, bg_share 0.55".into(),
            workload: sc_spec(spec),
            n_workers: 3,
            ttl_rescue_window_s: 35.0,
            sweep_rate: None,
        });
    }

    // S5: short-TTL hell — TTL 60s, rescue window scaled to 12s.
    {
        let spec = WorkloadSpec {
            mean_gap_s: 1.40,
            cache_ttl_s: 60.0,
            ..base.clone()
        };
        v.push(Scenario {
            id: "S5_short_ttl_60".into(),
            story: "TTL sensitivity: rescue window compressed, renewal races tighten".into(),
            knobs: "mean_gap 1.40s, ttl 60s, rescue_window 12s".into(),
            workload: sc_spec(spec),
            n_workers: 3,
            ttl_rescue_window_s: 12.0,
            sweep_rate: None,
        });
    }

    // S6: worker scarcity / scale.
    {
        let spec = WorkloadSpec {
            mean_gap_s: 1.88,
            ..base.clone()
        };
        v.push(Scenario {
            id: "S6a_worker_scarcity_2".into(),
            story: "capacity shrinks below demand".into(),
            knobs: "mean_gap 1.88s (~32/min), workers=2".into(),
            workload: sc_spec(spec),
            n_workers: 2,
            ttl_rescue_window_s: 35.0,
            sweep_rate: None,
        });
        let spec = WorkloadSpec {
            mean_gap_s: 0.70,
            ..base.clone()
        };
        v.push(Scenario {
            id: "S6b_worker_scale_6".into(),
            story: "capacity doubles, pressure scales with it".into(),
            knobs: "mean_gap 0.70s (~86/min), workers=6".into(),
            workload: sc_spec(spec),
            n_workers: 6,
            ttl_rescue_window_s: 35.0,
            sweep_rate: None,
        });
    }

    // S7: tenant skew — 70% of load on tenant-0.
    {
        let spec = WorkloadSpec {
            mean_gap_s: 1.40,
            tenant_skew: 0.70,
            ..base.clone()
        };
        v.push(Scenario {
            id: "S7_tenant_skew".into(),
            story: "multi-tenant fairness: hot Space vs cold Spaces".into(),
            knobs: "mean_gap 1.40s, tenant_skew 0.70".into(),
            workload: sc_spec(spec),
            n_workers: 3,
            ttl_rescue_window_s: 35.0,
            sweep_rate: None,
        });
    }

    v
}

// ---------- aggregation ----------

#[derive(Default, Clone)]
struct Series {
    rescue: Vec<f64>,
    starved: Vec<f64>,
    fg_p95: Vec<f64>,
    fg_p99: Vec<f64>,
    bg_max_wait: Vec<f64>,
    hit_rate: Vec<f64>,
    cost: Vec<f64>,
    warm_reuse: Vec<f64>,
    throughput: Vec<f64>,
    ceiling_forced: Vec<f64>,
}

impl Series {
    fn push(&mut self, r: &SchedulerPolicyReport) {
        self.rescue.push(r.prompt_cache.expiring_hot_hit_rate);
        self.starved.push(r.background.starved_jobs_over_45s as f64);
        self.fg_p95.push(r.foreground.p95_jct_s);
        self.fg_p99.push(r.foreground.p99_jct_s);
        self.bg_max_wait.push(r.background.max_queue_wait_s);
        self.hit_rate.push(r.prompt_cache.overall_hit_rate);
        self.cost
            .push(r.prompt_cache.total_effective_input_cost_units);
        self.warm_reuse.push(r.warm_env_reuse_rate);
        self.throughput.push(r.throughput_jobs_per_min);
        self.ceiling_forced
            .push(r.guardrail.ceiling_forced_dispatches as f64);
    }

    fn starved_max(&self) -> f64 {
        self.starved.iter().copied().fold(0.0_f64, f64::max)
    }

    /// Issue-#5 acceptance double-constraint on mean values + max starvation.
    fn dual_ok(&self) -> bool {
        mean(&self.rescue) >= 0.95 && self.starved_max() <= 0.0
    }
}

fn mc(v: &[f64], scale: f64) -> String {
    format!("{:.3} ± {:.3}", mean(v) * scale, ci95(v) * scale)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let get = |flag: &str| -> Option<String> {
        args.windows(2).find(|w| w[0] == flag).map(|w| w[1].clone())
    };
    let n_seeds: usize = get("--seeds").and_then(|s| s.parse().ok()).unwrap_or(20);
    let n_jobs: usize = get("--jobs").and_then(|s| s.parse().ok()).unwrap_or(240);
    let out_path = get("--out").unwrap_or_else(|| "beta-bench/artifacts/stress_bench.json".into());
    let md_path = get("--md").unwrap_or_else(|| "beta-bench/artifacts/stress_bench.md".into());
    let base_seed = 7700u64;

    let scenarios = build_scenarios(n_jobs);

    // results[scenario_id][policy] = Series
    let mut results: BTreeMap<String, BTreeMap<String, Series>> = BTreeMap::new();
    // Measured offered rate per scenario (jobs/min, from actual arrival span).
    let mut offered: BTreeMap<String, f64> = BTreeMap::new();
    // Tenant breakdown snapshot for S7 (last seed, guarded + dual).
    type TenantSnap = BTreeMap<String, Vec<(String, usize, f64, f64, usize)>>;
    let mut tenant_snap: TenantSnap = BTreeMap::new();

    for sc in &scenarios {
        let pol_entry = results.entry(sc.id.clone()).or_default();
        for &p in &POLICIES {
            let s = pol_entry.entry(p.as_str().to_string()).or_default();
            for i in 0..n_seeds {
                let jobs = match &sc.workload {
                    Workload::Baseline { overload } => generate_rate_limited_workload(
                        base_seed + i as u64,
                        120,
                        *overload,
                        TRACE_TTFT_S,
                        TRACE_DECODE_S,
                    ),
                    Workload::Parametric(spec) => {
                        generate_stress_workload(spec, base_seed + i as u64)
                    }
                };
                if i == 0 {
                    let min_a = jobs.first().map(|j| j.arrival_s).unwrap_or(0.0);
                    let max_a = jobs.last().map(|j| j.arrival_s).unwrap_or(1.0);
                    let span = (max_a - min_a).max(0.001);
                    offered.insert(sc.id.clone(), jobs.len() as f64 / (span / 60.0));
                }
                let r = simulate_with_config(
                    &jobs,
                    p,
                    sc.n_workers,
                    AGING_CAP_S,
                    sc.ttl_rescue_window_s,
                    MIN_RESCUE_TOKENS,
                    &semantics_for(p),
                );
                if sc.id == "S7_tenant_skew" && i == n_seeds - 1 {
                    let rows = r
                        .tenant_breakdown
                        .iter()
                        .map(|t| {
                            (
                                t.tenant_id.clone(),
                                t.jobs,
                                t.fg_p95_jct_s,
                                t.bg_max_wait_s,
                                t.starved_jobs_over_45s,
                            )
                        })
                        .collect();
                    tenant_snap.insert(format!("{}/{}", sc.id, p.as_str()), rows);
                }
                s.push(&r);
            }
        }
    }

    // ---- verdicts: max sustainable offered load per policy (S1 sweep) ----
    let sweep_cells: Vec<&Scenario> = scenarios
        .iter()
        .filter(|s| s.sweep_rate.is_some())
        .collect();
    let mut verdicts: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    for &p in &POLICIES {
        let mut best: Option<(f64, String)> = None;
        let mut knee: Option<(f64, String)> = None;
        for sc in &sweep_cells {
            let rate = sc.sweep_rate.unwrap();
            let s = &results[&sc.id][p.as_str()];
            if s.dual_ok() {
                if best.as_ref().map(|(r, _)| rate > *r).unwrap_or(true) {
                    best = Some((rate, sc.id.clone()));
                }
            } else if knee.as_ref().map(|(r, _)| rate < *r).unwrap_or(true) {
                knee = Some((rate, sc.id.clone()));
            }
        }
        verdicts.insert(
            p.as_str().to_string(),
            serde_json::json!({
                "max_sustainable_rate_per_min": best.as_ref().map(|(r, _)| *r),
                "max_sustainable_cell": best.as_ref().map(|(_, id)| id.clone()),
                "first_violation_rate_per_min": knee.as_ref().map(|(r, _)| *r),
                "first_violation_cell": knee.as_ref().map(|(_, id)| id.clone()),
            }),
        );
    }

    // ---- JSON ----
    let mut json = serde_json::Map::new();
    json.insert("suite".into(), serde_json::json!("stress_bench"));
    json.insert(
        "params".into(),
        serde_json::json!({
            "seeds": n_seeds,
            "base_seed": base_seed,
            "jobs_stress_cells": n_jobs,
            "jobs_baseline_cells": 120,
            "aging_cap_s": AGING_CAP_S,
            "min_rescue_tokens": MIN_RESCUE_TOKENS,
            "trace_mean_ttft_s": TRACE_TTFT_S,
            "trace_mean_decode_s": TRACE_DECODE_S,
            "semantics": "renewal (session_guarded_dual gets guardrails)",
        }),
    );
    let mut sc_map = serde_json::Map::new();
    for sc in &scenarios {
        let pols = &results[&sc.id];
        let mut pol_map = serde_json::Map::new();
        for (pol, s) in pols {
            pol_map.insert(
                pol.clone(),
                serde_json::json!({
                    "rescue_rate": {"mean": mean(&s.rescue), "ci95": ci95(&s.rescue)},
                    "starved_over_45s": {"mean": mean(&s.starved), "ci95": ci95(&s.starved), "max": s.starved_max()},
                    "fg_p95_jct_s": {"mean": mean(&s.fg_p95), "ci95": ci95(&s.fg_p95)},
                    "fg_p99_jct_s": {"mean": mean(&s.fg_p99), "ci95": ci95(&s.fg_p99)},
                    "bg_max_wait_s": {"mean": mean(&s.bg_max_wait), "ci95": ci95(&s.bg_max_wait)},
                    "hit_rate": {"mean": mean(&s.hit_rate), "ci95": ci95(&s.hit_rate)},
                    "cost_units": {"mean": mean(&s.cost), "ci95": ci95(&s.cost)},
                    "warm_reuse": {"mean": mean(&s.warm_reuse), "ci95": ci95(&s.warm_reuse)},
                    "throughput_per_min": {"mean": mean(&s.throughput), "ci95": ci95(&s.throughput)},
                    "ceiling_forced": {"mean": mean(&s.ceiling_forced), "ci95": ci95(&s.ceiling_forced)},
                    "dual_constraint_ok": s.dual_ok(),
                }),
            );
        }
        sc_map.insert(
            sc.id.clone(),
            serde_json::json!({
                "story": sc.story,
                "knobs": sc.knobs,
                "workers": sc.n_workers,
                "rescue_window_s": sc.ttl_rescue_window_s,
                "offered_rate_per_min_measured": offered.get(&sc.id),
                "policies": serde_json::Value::Object(pol_map),
            }),
        );
    }
    json.insert("scenarios".into(), serde_json::Value::Object(sc_map));
    json.insert("verdicts".into(), serde_json::to_value(&verdicts)?);
    json.insert(
        "tenant_snapshot_S7".into(),
        serde_json::to_value(&tenant_snap)?,
    );

    if let Some(parent) = std::path::Path::new(&out_path).parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&out_path, serde_json::to_string_pretty(&json)?)?;

    // ---- Markdown ----
    let mut md = String::new();
    md.push_str("# Scheduler stress benchmark (issue #5)\n\n");
    md.push_str(&format!(
        "- seeds/cell: {} (base {}), jobs: {} stress / 120 baseline, workers per cell\n",
        n_seeds, base_seed, n_jobs
    ));
    md.push_str(&format!(
        "- trace: ttft {}s, decode {}s | aging_cap {}s, min_rescue {} tokens\n",
        TRACE_TTFT_S, TRACE_DECODE_S, AGING_CAP_S, MIN_RESCUE_TOKENS
    ));
    md.push_str(
        "- semantics: sliding-TTL renewal for all policies; session_guarded_dual + guardrails\n",
    );
    md.push_str("- acceptance dual-constraint: rescue rate >= 95% AND starved(>45s) == 0\n\n");

    md.push_str("## Scenario matrix\n\n");
    md.push_str(
        "| id | story | knobs | workers | offered/min (measured) |\n|---|---|---|---|---|\n",
    );
    for sc in &scenarios {
        md.push_str(&format!(
            "| {} | {} | {} | {} | {:.1} |\n",
            sc.id,
            sc.story,
            sc.knobs,
            sc.n_workers,
            offered.get(&sc.id).copied().unwrap_or(0.0)
        ));
    }
    md.push('\n');

    // S1 sweep tables (one per headline metric).
    md.push_str("## S1 load sweep — saturation curves\n\n");
    let sweep_ids: Vec<&str> = sweep_cells.iter().map(|s| s.id.as_str()).collect();
    for (title, key) in [
        ("starved jobs (>45s), mean", "starved"),
        ("rescue rate %", "rescue"),
        ("FG p95 JCT s", "fg_p95"),
        ("cost units", "cost"),
    ] {
        md.push_str(&format!("### {}\n\n", title));
        md.push_str("| load cell |");
        for p in &POLICIES {
            md.push_str(&format!(" {} |", p.as_str()));
        }
        md.push_str("\n|---|");
        for _ in &POLICIES {
            md.push_str("---|");
        }
        md.push('\n');
        for id in &sweep_ids {
            let rate = offered.get(*id).copied().unwrap_or(0.0);
            md.push_str(&format!("| {} (~{:.0}/min) |", id, rate));
            for p in &POLICIES {
                let s = &results[*id][p.as_str()];
                let cell = match key {
                    "starved" => format!("{:.1}", mean(&s.starved)),
                    "rescue" => format!("{:.1}", mean(&s.rescue) * 100.0),
                    "fg_p95" => format!("{:.1}", mean(&s.fg_p95)),
                    _ => format!("{:.0}", mean(&s.cost)),
                };
                md.push_str(&format!(" {} |", cell));
            }
            md.push('\n');
        }
        md.push('\n');
    }

    // Per-scenario scorecards.
    md.push_str("## Per-scenario scorecards\n\n");
    for sc in &scenarios {
        if sc.sweep_rate.is_some() {
            continue; // covered by the sweep tables
        }
        md.push_str(&format!(
            "### {} — {}\n\n(knobs: {}, workers: {})\n\n",
            sc.id, sc.story, sc.knobs, sc.n_workers
        ));
        md.push_str("| policy | rescue % | starved | FG p95 s | FG p99 s | BG max wait s | hit % | cost | warm % | dual-ok |\n");
        md.push_str("|---|---|---|---|---|---|---|---|---|---|\n");
        for p in &POLICIES {
            let s = &results[&sc.id][p.as_str()];
            md.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
                p.as_str(),
                mc(&s.rescue, 100.0),
                mc(&s.starved, 1.0),
                mc(&s.fg_p95, 1.0),
                mc(&s.fg_p99, 1.0),
                mc(&s.bg_max_wait, 1.0),
                mc(&s.hit_rate, 100.0),
                mc(&s.cost, 1.0),
                mc(&s.warm_reuse, 100.0),
                if s.dual_ok() { "PASS" } else { "FAIL" },
            ));
        }
        md.push('\n');
    }

    // Verdicts.
    md.push_str(
        "## Verdicts — max sustainable load (dual-constraint: rescue >= 95% and starved = 0)\n\n",
    );
    md.push_str("| policy | max sustainable (jobs/min) | cell | first violation (jobs/min) | cell |\n|---|---|---|---|---|\n");
    for p in &POLICIES {
        let v = &verdicts[p.as_str()];
        let fmt = |k: &str| -> String {
            v[k].as_f64()
                .map(|x| format!("{:.0}", x))
                .unwrap_or_else(|| "—".into())
        };
        let fmtc = |k: &str| -> String {
            v[k].as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| "—".into())
        };
        md.push_str(&format!(
            "| {} | {} | {} | {} | {} |\n",
            p.as_str(),
            fmt("max_sustainable_rate_per_min"),
            fmtc("max_sustainable_cell"),
            fmt("first_violation_rate_per_min"),
            fmtc("first_violation_cell"),
        ));
    }
    md.push('\n');

    // Tenant fairness snapshot (S7).
    if !tenant_snap.is_empty() {
        md.push_str("## S7 tenant-skew fairness snapshot (last seed)\n\n");
        md.push_str("| run | tenant | jobs | FG p95 JCT s | BG max wait s | starved |\n|---|---|---|---|---|---|\n");
        for (run, rows) in &tenant_snap {
            for (tid, jobs, fg95, bgw, st) in rows {
                md.push_str(&format!(
                    "| {} | {} | {} | {:.1} | {:.1} | {} |\n",
                    run, tid, jobs, fg95, bgw, st
                ));
            }
        }
        md.push('\n');
    }

    if let Some(parent) = std::path::Path::new(&md_path).parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&md_path, &md)?;

    // ---- stdout summary ----
    println!("=== stress_bench complete ===");
    println!(
        "cells={} seeds={} | json: {} | md: {}",
        scenarios.len(),
        n_seeds,
        out_path,
        md_path
    );
    println!("\n-- verdicts (max sustainable jobs/min, dual-constraint) --");
    for p in &POLICIES {
        let v = &verdicts[p.as_str()];
        let best = v["max_sustainable_rate_per_min"]
            .as_f64()
            .map(|x| format!("{:.0}", x))
            .unwrap_or_else(|| "none".into());
        println!("  {:22} {}", p.as_str(), best);
    }
    println!("\n-- key cells --");
    for id in [
        "S0_overload",
        "S3_hot_expiry_storm",
        "S5_short_ttl_60",
        "S4_bg_flood",
    ] {
        let pols = &results[id];
        for p in &POLICIES {
            let s = &pols[p.as_str()];
            println!(
                "  [{id}] {:22} rescue {:.0}% starved {:.1} fg_p95 {:.0}s cost {:.0}",
                p.as_str(),
                mean(&s.rescue) * 100.0,
                mean(&s.starved),
                mean(&s.fg_p95),
                mean(&s.cost),
            );
        }
    }
    Ok(())
}
