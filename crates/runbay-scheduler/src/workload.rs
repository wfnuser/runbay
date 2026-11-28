//! Parametric stress-workload generator for the issue-#5 stress benchmark.
//!
//! Unlike `generate_rate_limited_workload` (the fixed issue-#5 baseline trace),
//! every pressure knob is explicit: arrival rate, burstiness, fg/bg mix,
//! expiring-hot share, tenant skew, TTL window and prefix sizes.

use crate::{JobPriority, ScheduledJob, SplitMix64};

/// Fully parametric workload description. `Default` approximates the
/// issue-#5 baseline flavour (3 tenants, ~1/3 background, ~1/3 hot-expiring
/// foreground, 300s TTL) but with a clean Poisson arrival process whose rate
/// is exactly `60 / mean_gap_s` jobs/min.
#[derive(Debug, Clone)]
pub struct WorkloadSpec {
    pub n_jobs: usize,
    /// Mean inter-arrival gap in seconds — the load knob.
    /// Offered rate = 60 / mean_gap_s jobs/min.
    pub mean_gap_s: f64,
    /// Clustered arrivals `(lo, hi)` burst sizes; inter-burst gaps are scaled
    /// so the long-run offered rate matches `mean_gap_s` (S2 burst storm).
    pub burst: Option<(usize, usize)>,
    /// Probability a job is background (batch / ingestion style).
    pub bg_share: f64,
    /// Probability a *foreground* job carries an expiring-hot prefix
    /// (large prefix, remaining TTL < 10% of the window).
    pub hot_share: f64,
    /// 0.0 = uniform over 3 tenants; e.g. 0.7 = 70% of jobs land on tenant-0.
    pub tenant_skew: f64,
    pub cache_ttl_s: f64,
    /// Prefix token range for warm / background jobs.
    pub prefix_small: (usize, usize),
    /// Prefix token range for expiring-hot jobs.
    pub prefix_large: (usize, usize),
    pub real_ttft_hit_s: f64,
    pub real_decode_s: f64,
}

impl Default for WorkloadSpec {
    fn default() -> Self {
        Self {
            n_jobs: 120,
            mean_gap_s: 1.0,
            burst: None,
            bg_share: 1.0 / 3.0,
            hot_share: 1.0 / 3.0,
            tenant_skew: 0.0,
            cache_ttl_s: 300.0,
            prefix_small: (25_000, 70_000),
            prefix_large: (80_000, 160_000),
            real_ttft_hit_s: 2.937,
            real_decode_s: 0.602,
        }
    }
}

impl WorkloadSpec {
    /// Nominal offered rate in jobs/min (ground truth is measured from the
    /// generated arrival span and reported by `stress_bench`).
    pub fn nominal_rate_per_min(&self) -> f64 {
        60.0 / self.mean_gap_s
    }
}

/// Generate a parametric stress workload.
pub fn generate_stress_workload(spec: &WorkloadSpec, seed: u64) -> Vec<ScheduledJob> {
    let mut rng = SplitMix64::new(seed);
    let mut jobs = Vec::with_capacity(spec.n_jobs);
    let mut t = 0.0_f64;
    let mut burst_left = 0usize;
    let ttl = spec.cache_ttl_s;

    for i in 0..spec.n_jobs {
        // ---- arrival process ----
        if burst_left == 0 {
            if let Some((lo, hi)) = spec.burst {
                let k = rng.randint(lo, hi) as f64;
                burst_left = k as usize - 1;
                // Scale the inter-burst gap so the long-run rate matches the
                // target (intra-burst gaps add ~0.075s * (k-1) per burst).
                let intra = 0.075 * (k - 1.0);
                let gap_mean = (spec.mean_gap_s * k - intra).max(0.05);
                t += rng.expovariate(1.0 / gap_mean);
            } else {
                t += rng.expovariate(1.0 / spec.mean_gap_s.max(0.01));
            }
        } else {
            t += rng.uniform(0.03, 0.12);
            burst_left -= 1;
        }

        // ---- identity: tenant (skew) + session namespaced by tenant ----
        let tenant_id = if spec.tenant_skew > 0.0 && rng.uniform_01() < spec.tenant_skew {
            "tenant-0".to_string()
        } else if spec.tenant_skew > 0.0 {
            format!("tenant-{}", rng.randint(1, 2))
        } else {
            format!("tenant-{}", rng.randint(0, 2))
        };

        // ---- job type ----
        let is_bg = rng.uniform_01() < spec.bg_share;
        let (priority, session_id, prefix_tokens, last_hit, tool_lo, tool_hi) = if is_bg {
            (
                JobPriority::Background,
                format!("{tenant_id}-bg-sess-{}", i % 10),
                rng.randint(3_000, 12_000),
                None,
                0.08,
                0.25,
            )
        } else {
            let is_hot = rng.uniform_01() < spec.hot_share;
            if is_hot {
                // Expiring-hot: remaining TTL is 2%~9.5% of the window
                // (baseline: rem in [3.5, 22]s of a 300s TTL).
                let rem_ttl = rng.uniform(0.02 * ttl, 0.095 * ttl);
                (
                    JobPriority::Interactive,
                    format!("{tenant_id}-fg-sess-{}", i % 14),
                    rng.randint(spec.prefix_large.0, spec.prefix_large.1),
                    Some(round_n(t - (ttl - rem_ttl), 4)),
                    0.05,
                    0.20,
                )
            } else {
                let elapsed = rng.uniform(20.0, (0.4 * ttl).min(120.0));
                let lh = if rng.uniform_01() < 0.8 {
                    Some(round_n(t - elapsed, 4))
                } else {
                    None
                };
                (
                    JobPriority::Interactive,
                    format!("{tenant_id}-fg-sess-{}", i % 14),
                    rng.randint(spec.prefix_small.0, spec.prefix_small.1),
                    lh,
                    0.05,
                    0.20,
                )
            }
        };

        let miss_penalty = (prefix_tokens as f64 / 100_000.0) * 2.8;
        jobs.push(ScheduledJob {
            job_id: format!("job-{i:04}"),
            session_id,
            tenant_id,
            priority,
            arrival_s: round_n(t, 4),
            required_rev: i / 6,
            prefix_tokens,
            last_cache_hit_s: last_hit,
            cache_ttl_s: ttl,
            base_ttft_hit_s: spec.real_ttft_hit_s,
            base_ttft_miss_s: spec.real_ttft_hit_s + miss_penalty,
            decode_s: spec.real_decode_s * rng.uniform(0.75, 1.25),
            tool_s: rng.uniform(tool_lo, tool_hi),
            workspace_rebuild_s: 0.30,
            workspace_inc_sync_s: 0.002,
        });
    }

    jobs.sort_by(|a, b| {
        a.arrival_s
            .partial_cmp(&b.arrival_s)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    jobs
}

fn round_n(v: f64, decimals: u32) -> f64 {
    let f = 10_f64.powi(decimals as i32);
    (v * f).round() / f
}
