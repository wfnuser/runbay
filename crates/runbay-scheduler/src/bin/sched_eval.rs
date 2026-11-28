//! Multi-seed scheduler evaluation for Runbay issue #5.
//!
//! Grid: {semantics} x {policy} x {regime} x {seeds}, reporting mean +/- 95% CI
//! and checking the acceptance double-constraint:
//!   rescue rate >= 95% AND background starvation (>45s) == 0 under overload.
//!
//! Usage:
//!   cargo run -p runbay-scheduler --bin sched_eval -- [--seeds 20] [--jobs 120]
//!       [--out beta-bench/artifacts/sched_eval.json] [--md beta-bench/artifacts/sched_eval.md]

use runbay_scheduler::{
    generate_rate_limited_workload, simulate_with_config, PolicyName, SchedulerPolicyReport,
    SimConfig,
};
use std::collections::BTreeMap;
use std::fs;

const TRACE_TTFT_S: f64 = 2.937;
const TRACE_DECODE_S: f64 = 0.602;
const N_WORKERS: usize = 3;
const AGING_CAP_S: f64 = 12.0;
const RESCUE_WINDOW_S: f64 = 35.0;
const MIN_RESCUE_TOKENS: usize = 40_000;

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

/// Two-sided 95% critical value (Student-t), small lookup with normal fallback.
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

#[derive(Default, Clone)]
struct MetricSeries {
    rescue_rate: Vec<f64>,
    starved: Vec<f64>,
    fg_p95_jct: Vec<f64>,
    bg_max_wait: Vec<f64>,
    hit_rate: Vec<f64>,
    cost: Vec<f64>,
    warm_reuse: Vec<f64>,
    throughput: Vec<f64>,
    ceiling_forced: Vec<f64>,
}

impl MetricSeries {
    fn push(&mut self, r: &SchedulerPolicyReport) {
        self.rescue_rate.push(r.prompt_cache.expiring_hot_hit_rate);
        self.starved.push(r.background.starved_jobs_over_45s as f64);
        self.fg_p95_jct.push(r.foreground.p95_jct_s);
        self.bg_max_wait.push(r.background.max_queue_wait_s);
        self.hit_rate.push(r.prompt_cache.overall_hit_rate);
        self.cost
            .push(r.prompt_cache.total_effective_input_cost_units);
        self.warm_reuse.push(r.warm_env_reuse_rate);
        self.throughput.push(r.throughput_jobs_per_min);
        self.ceiling_forced
            .push(r.guardrail.ceiling_forced_dispatches as f64);
    }
}

fn fmt_mc(v: &[f64], scale: f64) -> String {
    format!("{:.3} ± {:.3}", mean(v) * scale, ci95(v) * scale)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let get = |flag: &str| -> Option<String> {
        args.windows(2).find(|w| w[0] == flag).map(|w| w[1].clone())
    };
    let n_seeds: usize = get("--seeds").and_then(|s| s.parse().ok()).unwrap_or(20);
    let n_jobs: usize = get("--jobs").and_then(|s| s.parse().ok()).unwrap_or(120);
    let out_path = get("--out").unwrap_or_else(|| "beta-bench/artifacts/sched_eval.json".into());
    let md_path = get("--md").unwrap_or_else(|| "beta-bench/artifacts/sched_eval.md".into());

    let base_seed = 2026u64;
    let regimes = [("normal", false), ("overload", true)];

    // (label, semantics config, policies to run under it)
    let dual = PolicyName::DualCacheTtlAware;
    let guarded = PolicyName::SessionGuardedDual;
    let all_legacy = [
        PolicyName::Fifo,
        PolicyName::PriorityAging,
        PolicyName::WarmEnvAffinity,
        dual,
    ];
    let all_renewal = [
        PolicyName::Fifo,
        PolicyName::PriorityAging,
        PolicyName::WarmEnvAffinity,
        dual,
        guarded,
    ];

    // results[regime][semantics][policy] = MetricSeries
    let mut results: BTreeMap<&str, BTreeMap<String, BTreeMap<String, MetricSeries>>> =
        BTreeMap::new();
    // Per-seed starved counts for acceptance (guarded, overload).
    let mut acceptance_starved: Vec<f64> = Vec::new();
    let mut acceptance_rescue: Vec<f64> = Vec::new();

    for (regime_name, overload) in regimes {
        let reg_entry = results.entry(regime_name).or_default();

        let mut run =
            |semantics: &str,
             cfg: SimConfig,
             policies: &[PolicyName],
             series_sink: &mut BTreeMap<String, BTreeMap<String, MetricSeries>>| {
                let sem_entry = series_sink.entry(semantics.to_string()).or_default();
                for &p in policies {
                    let s = sem_entry.entry(p.as_str().to_string()).or_default();
                    for i in 0..n_seeds {
                        let jobs = generate_rate_limited_workload(
                            base_seed + i as u64,
                            n_jobs,
                            overload,
                            TRACE_TTFT_S,
                            TRACE_DECODE_S,
                        );
                        let r = simulate_with_config(
                            &jobs,
                            p,
                            N_WORKERS,
                            AGING_CAP_S,
                            RESCUE_WINDOW_S,
                            MIN_RESCUE_TOKENS,
                            &cfg,
                        );
                        if regime_name == "overload"
                            && p == guarded
                            && semantics == "renewal_guarded"
                        {
                            acceptance_rescue.push(r.prompt_cache.expiring_hot_hit_rate);
                            acceptance_starved.push(r.background.starved_jobs_over_45s as f64);
                        }
                        s.push(&r);
                    }
                }
            };

        run("legacy", SimConfig::legacy(), &all_legacy, reg_entry);
        run(
            "renewal",
            SimConfig::renewal(),
            &all_renewal[..4],
            reg_entry,
        );
        run(
            "renewal_guarded",
            SimConfig::renewal_guarded(),
            &all_renewal[4..],
            reg_entry,
        );
        // Guardrails on top of legacy semantics: do they fix the legacy deadlock too?
        run(
            "legacy_guarded",
            SimConfig {
                renew_ttl_on_hit: false,
                guardrails: Some(Default::default()),
            },
            &all_renewal[4..],
            reg_entry,
        );
    }

    // ---- acceptance check ----
    let rescue_mean = mean(&acceptance_rescue);
    let starved_max = acceptance_starved.iter().copied().fold(0.0_f64, f64::max);
    let acceptance_pass = rescue_mean >= 0.95 && starved_max <= 0.0;

    // ---- build JSON ----
    let mut json = serde_json::Map::new();
    json.insert("suite".into(), serde_json::json!("sched_eval"));
    json.insert(
        "params".into(),
        serde_json::json!({
            "seeds": n_seeds,
            "base_seed": base_seed,
            "jobs": n_jobs,
            "workers": N_WORKERS,
            "aging_cap_s": AGING_CAP_S,
            "rescue_window_s": RESCUE_WINDOW_S,
            "min_rescue_tokens": MIN_RESCUE_TOKENS,
            "trace_mean_ttft_s": TRACE_TTFT_S,
            "trace_mean_decode_s": TRACE_DECODE_S,
        }),
    );
    for (regime, sems) in &results {
        let mut reg_map = serde_json::Map::new();
        for (sem, pols) in sems {
            let mut pol_map = serde_json::Map::new();
            for (pol, s) in pols {
                pol_map.insert(
                    pol.clone(),
                    serde_json::json!({
                        "rescue_rate": {"mean": mean(&s.rescue_rate), "ci95": ci95(&s.rescue_rate)},
                        "starved_over_45s": {"mean": mean(&s.starved), "ci95": ci95(&s.starved), "max": s.starved.iter().copied().fold(0.0, f64::max)},
                        "fg_p95_jct_s": {"mean": mean(&s.fg_p95_jct), "ci95": ci95(&s.fg_p95_jct)},
                        "bg_max_wait_s": {"mean": mean(&s.bg_max_wait), "ci95": ci95(&s.bg_max_wait)},
                        "hit_rate": {"mean": mean(&s.hit_rate), "ci95": ci95(&s.hit_rate)},
                        "cost_units": {"mean": mean(&s.cost), "ci95": ci95(&s.cost)},
                        "warm_reuse": {"mean": mean(&s.warm_reuse), "ci95": ci95(&s.warm_reuse)},
                        "throughput_per_min": {"mean": mean(&s.throughput), "ci95": ci95(&s.throughput)},
                        "ceiling_forced": {"mean": mean(&s.ceiling_forced), "ci95": ci95(&s.ceiling_forced)},
                    }),
                );
            }
            reg_map.insert(sem.clone(), serde_json::Value::Object(pol_map));
        }
        json.insert(regime.to_string(), serde_json::Value::Object(reg_map));
    }
    json.insert(
        "acceptance".into(),
        serde_json::json!({
            "policy": "session_guarded_dual",
            "semantics": "renewal_guarded",
            "regime": "overload",
            "rescue_rate_mean": rescue_mean,
            "rescue_rate_ci95": ci95(&acceptance_rescue),
            "starved_max_across_seeds": starved_max,
            "rescue_ge_95pct": rescue_mean >= 0.95,
            "zero_starvation": starved_max <= 0.0,
            "pass": acceptance_pass,
        }),
    );

    if let Some(parent) = std::path::Path::new(&out_path).parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&out_path, serde_json::to_string_pretty(&json)?)?;

    // ---- build Markdown ----
    let mut md = String::new();
    md.push_str("# Scheduler multi-seed evaluation (issue #5)\n\n");
    md.push_str(&format!(
        "- seeds: {} (base {}), jobs/seed: {}, workers: {}\n",
        n_seeds, base_seed, n_jobs, N_WORKERS
    ));
    md.push_str(&format!(
        "- trace params: ttft {}s, decode {}s | aging_cap {}s, rescue_window {}s, min_rescue {} tokens\n\n",
        TRACE_TTFT_S, TRACE_DECODE_S, AGING_CAP_S, RESCUE_WINDOW_S, MIN_RESCUE_TOKENS
    ));
    md.push_str("**semantics**: `legacy` = issue-#5 baseline (no TTL renewal); ");
    md.push_str("`renewal` = sliding-TTL write-back on every run; ");
    md.push_str("`renewal_guarded` = renewal + guardrails (only session_guarded_dual).\n\n");

    for (regime, sems) in &results {
        md.push_str(&format!("## Regime: {}\n\n", regime));
        for (sem, pols) in sems {
            md.push_str(&format!("### semantics: {}\n\n", sem));
            md.push_str("| policy | rescue rate % | starved (>45s) | FG p95 JCT s | BG max wait s | hit rate % | cost units | warm reuse % | throughput/min |\n");
            md.push_str("|---|---|---|---|---|---|---|---|---|\n");
            for (pol, s) in pols {
                md.push_str(&format!(
                    "| {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
                    pol,
                    fmt_mc(&s.rescue_rate, 100.0),
                    fmt_mc(&s.starved, 1.0),
                    fmt_mc(&s.fg_p95_jct, 1.0),
                    fmt_mc(&s.bg_max_wait, 1.0),
                    fmt_mc(&s.hit_rate, 100.0),
                    fmt_mc(&s.cost, 1.0),
                    fmt_mc(&s.warm_reuse, 100.0),
                    fmt_mc(&s.throughput, 1.0),
                ));
            }
            md.push('\n');
        }
    }

    md.push_str("## Acceptance (session_guarded_dual, renewal_guarded, overload)\n\n");
    md.push_str(&format!(
        "- rescue rate: {:.3} ± {:.3} (need >= 95%): {}\n",
        rescue_mean * 100.0,
        ci95(&acceptance_rescue) * 100.0,
        if rescue_mean >= 0.95 { "PASS" } else { "FAIL" }
    ));
    md.push_str(&format!(
        "- starved max across seeds: {:.0} (need = 0): {}\n",
        starved_max,
        if starved_max <= 0.0 { "PASS" } else { "FAIL" }
    ));
    md.push_str(&format!(
        "- **overall: {}**\n",
        if acceptance_pass { "PASS" } else { "FAIL" }
    ));

    if let Some(parent) = std::path::Path::new(&md_path).parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&md_path, &md)?;

    // ---- stdout summary ----
    println!("=== sched_eval complete ===");
    println!(
        "seeds={} jobs={} | json: {} | md: {}",
        n_seeds, n_jobs, out_path, md_path
    );
    println!(
        "acceptance: rescue {:.1}% (>=95%) | starved max {:.0} (=0) => {}",
        rescue_mean * 100.0,
        starved_max,
        if acceptance_pass { "PASS" } else { "FAIL" }
    );
    // quick overload comparison line
    if let Some(sems) = results.get("overload") {
        for (sem, pols) in sems {
            for (pol, s) in pols {
                println!(
                    "  [overload/{sem}] {pol}: rescue {:.1}% starved {:.1} fg_p95 {:.1}s cost {:.0}",
                    mean(&s.rescue_rate) * 100.0,
                    mean(&s.starved),
                    mean(&s.fg_p95_jct),
                    mean(&s.cost),
                );
            }
        }
    }
    Ok(())
}
