//! `RuntimePoolScheduler` (P3a **shadow** mode, design v1.1).
//!
//! Full admission machinery — three-state admit/defer/reject, `score_job_v2`
//! scoring, rescue-window detection, rolling `est_service` bookkeeping — but in
//! shadow mode it only *logs* what it would do. Dispatch order stays untouched
//! (plain FIFO semaphore), so there is zero behavioral regression while the
//! decision ledger accumulates calibration data (P3a exit criteria in design §7).
//!
//! The same object hosts the **TelemetryBridge**: gateway call records are
//! mapped (token → run → cluster) into the `CacheStateTable` sliding-TTL ledger
//! using the v1.1 renewal rule (request-start anchored; hit AND miss renew;
//! aborted / below-floor only bump activity).

use parking_lot::Mutex;
use runbay_scheduler::cache_table::{CacheStateTable, CallEvent};
use runbay_scheduler::ledger::{
    termination_bucket, ActualRow, DecisionLedger, DecisionRow,
};
use runbay_scheduler::{score_job_v2, GuardrailConfig, GuardrailMetrics, JobPriority, PolicyName, ScheduledJob};
use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Shadow scheduler configuration (design §6 subset relevant to P3a).
#[derive(Debug, Clone)]
pub struct ShadowConfig {
    pub fg_deadline_budget_s: Option<f64>,
    pub bg_deadline_budget_s: Option<f64>,
    /// A3: rescue window = `clamp(ratio × ttl, min, max)` — 0.12 is the
    /// empirically validated 35/300 ratio; the 0.2 variant is unvalidated.
    pub rescue_window_ratio: f64,
    pub rescue_window_min_s: f64,
    pub rescue_window_max_s: f64,
    /// Rescue floor (estimated prompt tokens): only prefixes at/above this are
    /// rescuable. The online estimate is `prompt.len()/4` (bytes/4), and
    /// `supervisor::MAX_PROMPT_BYTES = 64 KiB` caps it at ~16k tokens — so any
    /// floor above 16k is unreachable in the live path (the sim's 40k is a
    /// *true* token count, not a bytes/4 estimate). 10k keeps the "long prefix
    /// only" semantics while staying reachable; recalibrate against cowiki's
    /// real prompt-length distribution in P4.
    pub min_rescue_prefix_tokens: usize,
    pub cache_ttl_s: f64,
    pub combined_queue_cap: usize,
    /// Constant service model (sim constants + sandbox boot); calibrated by
    /// the ledger in P3a and replaced with rolling estimates in P5.
    pub est_service_s: f64,
    pub aging_starvation_cap_s: f64,
    // ---- P3b (Active) additions: AIMD gate + guardrail knobs (design §4.2/§4.4) ----
    /// Reserved background workers for the bg slice (design §4.2).
    pub n_reserved_bg_workers: usize,
    /// AIMD control period (design §4.4: T = 1s).
    pub aimd_period_s: f64,
    /// AIMD smoothing window (design §4.4: 10s sliding window).
    pub aimd_window_s: f64,
    /// AIMD additive increase α.
    pub aimd_alpha: f64,
    /// AIMD multiplicative decrease β.
    pub aimd_beta: f64,
    /// Aequitas admission-probability floor per class (design §4.1).
    pub admit_floor_prob: f64,
    /// Physical concurrency ceiling — also the AIMD cap maximum (§4.4:
    /// semaphore sits at this value; the logical gate lives below it).
    pub max_concurrent_runs: usize,
}

impl Default for ShadowConfig {
    fn default() -> Self {
        Self {
            fg_deadline_budget_s: Some(120.0),
            bg_deadline_budget_s: Some(600.0),
            rescue_window_ratio: 0.12,
            rescue_window_min_s: 5.0,
            rescue_window_max_s: 35.0,
            min_rescue_prefix_tokens: 10_000,
            cache_ttl_s: 300.0,
            combined_queue_cap: 128,
            est_service_s: 4.0,
            aging_starvation_cap_s: 12.0,
            n_reserved_bg_workers: 1,
            aimd_period_s: 1.0,
            aimd_window_s: 10.0,
            aimd_alpha: 1.0,
            aimd_beta: 0.5,
            admit_floor_prob: 0.05,
            max_concurrent_runs: 8,
        }
    }
}

impl ShadowConfig {
    pub fn rescue_window_s(&self) -> f64 {
        (self.cache_ttl_s * self.rescue_window_ratio)
            .clamp(self.rescue_window_min_s, self.rescue_window_max_s)
    }

    pub fn budget_for(&self, priority: &str) -> Option<f64> {
        match priority {
            "background" => self.bg_deadline_budget_s,
            _ => self.fg_deadline_budget_s,
        }
    }

    /// AIMD `min_cap = n_reserved_bg_workers + 1` (design §4.4: the slice and
    /// arbitration always keep at least one slot).
    pub fn aimd_min_cap(&self) -> f64 {
        (self.n_reserved_bg_workers as f64 + 1.0).max(1.0)
    }
}

/// Submit-time metadata retained until the run finalizes.
#[derive(Debug, Clone)]
pub struct SubmitMeta {
    pub run_id: String,
    pub cluster_id: String,
    pub tenant: String,
    pub rev: u64,
    pub priority: String,
    /// Rough token estimate (`prompt.len() / 4`) for scoring.
    pub prompt_tokens_est: usize,
    pub submitted_at: f64,
    pub deadline_budget_s: Option<f64>,
    /// True when this submit fell inside the rescue window (rem_ttl ∈ (0,35s]
    /// and prefix ≥ `min_rescue_prefix_tokens`) — feeds the live rescue-rate
    /// metric (P3c /metrics: candidates vs hits).
    pub rescuable: bool,
}

/// Gateway token → run/cluster attribution for the telemetry bridge.
#[derive(Debug, Clone)]
struct TokenCtx {
    run_id: String,
    cluster_id: String,
    tenant: String,
    rev: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct QueueDepth {
    pub in_flight: usize,
    pub queued: usize,
    pub deferred: usize,
}

/// Worker-slot affinity context for online scoring (design §4.2: worker's
/// `ActiveRunEntry`/recent-completion record).
#[derive(Debug, Clone, Default)]
pub struct SlotCtx {
    pub session: Option<String>,
    pub tenant: Option<String>,
    pub rev: Option<u64>,
}

/// One pickable job as seen by the dispatcher (design §3: unified arbitration
/// queue — deferred jobs carry a deadline and escalate to forced priority).
#[derive(Debug, Clone)]
pub struct DispatchCandidate {
    pub meta: SubmitMeta,
    pub enqueued_at: f64,
    /// `Some` while the entry came from the DeferPool path.
    pub defer_until: Option<f64>,
    /// Set once `defer_until` has passed — forced 1000-score arbitration
    /// (same sequence as the ceiling, per B3).
    pub escalated: bool,
    pub from_defer: bool,
}

/// Deterministic Aequitas admission floor (design §4.1): when the AIMD gate is
/// closed, each class keeps ≥ `floor` admission probability so it can never be
/// squeezed to zero forever. The coin is a hash of `run_id` — deterministic,
/// so ledger replay reproduces the same decision (§8.2).
pub fn admit_floor_pass(run_id: &str, floor: f64) -> bool {
    if floor <= 0.0 {
        return false;
    }
    hash01(run_id) < floor
}

/// FNV-1a folded to [0, 1) — deterministic per key (replay-stable).
pub fn hash01(key: &str) -> f64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in key.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (h % 10_000) as f64 / 10_000.0
}

/// Reject `retry-after` policy (design §4.1 C revision):
/// generic rejects use `max(aimd_period_s, 2s)`; `queue_full` uses 2× the
/// queue-waterline estimate (capped 60s). ±20% deterministic jitter so
/// synchronous client retries cannot re-pulse together.
pub fn retry_after_s(run_id: &str, reason: &str, est_wait_pred: f64, aimd_period_s: f64) -> f64 {
    let base = match reason {
        "queue_full" => (2.0 * est_wait_pred).clamp(2.0, 60.0),
        _ => aimd_period_s.max(2.0),
    };
    base * (0.8 + 0.4 * hash01(run_id))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AimdMode {
    Normal,
    Congested,
}

/// AIMD watermark controller (design §4.4: smoothed signals + hysteresis).
///
/// `cap` is the *logical* admission gate; the tokio `Semaphore` stays at the
/// physical maximum (there is no shrink API, by design).
pub struct AimdController {
    cap: f64,
    min_cap: f64,
    max_cap: f64,
    alpha: f64,
    beta: f64,
    window_s: f64,
    period_s: f64,
    mode: AimdMode,
    recovered_since: Option<f64>,
    last_tick: f64,
    /// Sliding-window event log: (ts, kind) — 0=submit, 1=reject, 2=ceiling,
    /// 3=bg_submit.
    events: VecDeque<(f64, u8)>,
    /// Queue-depth samples (ts, depth) for the slope signal.
    queue_samples: VecDeque<(f64, f64)>,
}

impl AimdController {
    pub fn new(cfg: &ShadowConfig, max_cap: f64, now: f64) -> Self {
        Self {
            cap: max_cap,
            min_cap: cfg.aimd_min_cap().min(max_cap),
            max_cap,
            alpha: cfg.aimd_alpha,
            beta: cfg.aimd_beta,
            window_s: cfg.aimd_window_s,
            period_s: cfg.aimd_period_s.max(0.001),
            mode: AimdMode::Normal,
            recovered_since: None,
            last_tick: now,
            events: VecDeque::new(),
            queue_samples: VecDeque::new(),
        }
    }

    pub fn cap(&self) -> f64 {
        self.cap
    }

    /// AIMD mode accessor (used by tests / future /metrics export).
    #[allow(dead_code)]
    pub fn mode(&self) -> AimdMode {
        self.mode
    }

    pub fn note_submit(&mut self, bg: bool, now: f64) {
        self.events.push_back((now, 0));
        if bg {
            self.events.push_back((now, 3));
        }
        self.trim(now);
    }

    pub fn note_reject(&mut self, now: f64) {
        self.events.push_back((now, 1));
        self.trim(now);
    }

    /// A ceiling-forced dispatch is the starvation signal (design §4.4:
    /// `starve_rate = ceiling triggers / bg jobs`).
    pub fn note_ceiling(&mut self, now: f64) {
        self.events.push_back((now, 2));
        self.trim(now);
    }

    pub fn note_queue(&mut self, depth: f64, now: f64) {
        self.queue_samples.push_back((now, depth));
        self.trim(now);
    }

    fn trim(&mut self, now: f64) {
        let cut = now - self.window_s;
        while let Some((ts, _)) = self.events.front() {
            if *ts < cut {
                self.events.pop_front();
            } else {
                break;
            }
        }
        while let Some((ts, _)) = self.queue_samples.front() {
            if *ts < cut {
                self.queue_samples.pop_front();
            } else {
                break;
            }
        }
    }

    fn signals(&self) -> (f64, f64, f64) {
        let mut submits: f64 = 0.0;
        let mut bg_submits: f64 = 0.0;
        let mut rejects: f64 = 0.0;
        let mut ceilings: f64 = 0.0;
        for (_, k) in &self.events {
            match k {
                0 => submits += 1.0,
                1 => rejects += 1.0,
                2 => ceilings += 1.0,
                3 => bg_submits += 1.0,
                _ => {}
            }
        }
        let starve_rate = ceilings / bg_submits.max(1.0);
        let reject_rate = rejects / submits.max(1.0);
        let queue_slope = self.slope();
        (starve_rate, reject_rate, queue_slope)
    }

    /// Least-squares slope of the queue-depth samples (depth per second).
    fn slope(&self) -> f64 {
        let n = self.queue_samples.len();
        if n < 2 {
            return 0.0;
        }
        let mean_t: f64 = self.queue_samples.iter().map(|(t, _)| *t).sum::<f64>() / n as f64;
        let mean_d: f64 = self.queue_samples.iter().map(|(_, d)| *d).sum::<f64>() / n as f64;
        let mut num = 0.0;
        let mut den = 0.0;
        for (t, d) in &self.queue_samples {
            num += (t - mean_t) * (d - mean_d);
            den += (t - mean_t) * (t - mean_t);
        }
        if den <= f64::EPSILON {
            0.0
        } else {
            num / den
        }
    }

    /// Advance one control period. Returns the (possibly new) logical cap.
    /// `now` may advance in arbitrary increments; whole `period_s` steps are
    /// accumulated internally, so a fast tick cadence still reaches the §4.4
    /// schedule.
    pub fn tick(&mut self, now: f64) -> f64 {
        self.trim(now);
        let (starve_rate, reject_rate, queue_slope) = self.signals();

        // Hysteresis double-threshold (design §4.4).
        let congested = starve_rate > 0.0 || reject_rate > 0.10 || queue_slope > 0.0;
        let recovered =
            starve_rate == 0.0 && reject_rate < 0.02 && queue_slope <= 0.0;

        if congested {
            self.mode = AimdMode::Congested;
            self.recovered_since = None;
        } else if recovered {
            match self.recovered_since {
                None => self.recovered_since = Some(now),
                Some(t0) if now - t0 >= 5.0 => self.mode = AimdMode::Normal,
                Some(_) => {}
            }
        } else {
            // Neither congested nor recovered: hold state, reset the recovery
            // streak so flapping cannot sneak into Normal.
            self.recovered_since = None;
        }

        // Period accumulation (H1 fix): only advance by whole periods, so a
        // fast call cadence (the 200ms dispatch tick) accumulates to a full
        // control period before the gate moves. `last_tick` is a *watermark* —
        // it advances by whole periods, NOT reset on every call.
        let mut periods: f64 = 0.0;
        if now > self.last_tick {
            let elapsed = now - self.last_tick;
            periods = (elapsed / self.period_s).floor();
            if periods >= 1.0 {
                self.last_tick += periods * self.period_s;
            }
        } else if now < self.last_tick {
            // Clock went backward (NTP / restart) — resync and hold the gate.
            self.last_tick = now;
        }
        if periods >= 1.0 {
            match self.mode {
                AimdMode::Congested => {
                    // Multiplicative decrease, applied per elapsed period
                    // (capped so a long stall can't slam the gate to the floor
                    // in one go).
                    for _ in 0..(periods as usize).min(10) {
                        self.cap = (self.cap * self.beta).floor().max(self.min_cap);
                    }
                }
                AimdMode::Normal => {
                    if self.recovered_since.is_some()
                        && now - self.recovered_since.unwrap_or(now) >= 5.0
                    {
                        for _ in 0..(periods as usize).min(10) {
                            self.cap = (self.cap + self.alpha).min(self.max_cap);
                        }
                    }
                }
            }
        }
        self.cap
    }
}

/// What the scheduler *would* do — logged, never enforced in P3a.
#[derive(Debug, Clone, Copy)]
pub struct ShadowDecision {
    /// "admit" | "defer" | "reject"
    pub decision: &'static str,
    /// "normal" | "rescue_window" | "budget_reject" | "queue_full"
    pub reason: &'static str,
    pub score: f64,
    pub rem_ttl_pred: Option<f64>,
    pub est_wait_pred: f64,
    pub est_service: f64,
    pub defer_until: Option<f64>,
}

struct RunCallStats {
    total_ms_sum: f64,
    cached_tokens_sum: u64,
    prompt_tokens_sum: u64,
    hit_any: bool,
    calls: usize,
}

pub struct RuntimePoolScheduler {
    cache: Mutex<CacheStateTable>,
    ledger: DecisionLedger,
    cfg: ShadowConfig,
    guardrails: GuardrailConfig,
    submits: Mutex<HashMap<String, SubmitMeta>>,
    tokens: Mutex<HashMap<String, TokenCtx>>,
    run_calls: Mutex<HashMap<String, RunCallStats>>,
    inflight_by_cluster: Mutex<HashMap<String, usize>>,
    recent_service_s: Mutex<VecDeque<f64>>,
    /// P3b: logical admission watermark (§4.4). Physical cap stays on the
    /// supervisor's semaphore.
    aimd: Mutex<AimdController>,
    /// Guardrail activation counters (exported via /metrics in P3c).
    guardrail_metrics: Mutex<GuardrailMetrics>,
    /// run_id → defer decision time, for defer-wait accounting (§4.3).
    deferred_at: Mutex<HashMap<String, f64>>,
    /// P3c live metrics: rescue candidates (non-rejected, in-window submits)
    /// and rescue hits (candidates whose gateway call actually cached).
    rescue_candidates: AtomicU64,
    rescue_hits: AtomicU64,
    /// Rolling defer-wait samples for the p50/p95/max distribution (P3c).
    defer_wait_samples: Mutex<VecDeque<f64>>,
}

impl RuntimePoolScheduler {
    pub fn open(state_dir: &Path, cfg: ShadowConfig) -> std::io::Result<Arc<Self>> {
        let cache = CacheStateTable::open(state_dir, Default::default())?;
        let ledger = DecisionLedger::open(state_dir)?;
        // Seed the AIMD watermark with the real wall clock (H1): a `0.0` seed
        // makes the first tick see a ~1.7e9s elapsed gap and the controller
        // misbehaves at startup.
        let now = unix_now();
        let aimd = AimdController::new(&cfg, cfg.max_concurrent_runs.max(1) as f64, now);
        Ok(Arc::new(Self {
            cache: Mutex::new(cache),
            ledger,
            guardrails: GuardrailConfig::default(),
            cfg,
            submits: Mutex::new(HashMap::new()),
            tokens: Mutex::new(HashMap::new()),
            run_calls: Mutex::new(HashMap::new()),
            inflight_by_cluster: Mutex::new(HashMap::new()),
            recent_service_s: Mutex::new(VecDeque::with_capacity(64)),
            aimd: Mutex::new(aimd),
            guardrail_metrics: Mutex::new(GuardrailMetrics::default()),
            deferred_at: Mutex::new(HashMap::new()),
            rescue_candidates: AtomicU64::new(0),
            rescue_hits: AtomicU64::new(0),
            defer_wait_samples: Mutex::new(VecDeque::with_capacity(256)),
        }))
    }

    pub fn config(&self) -> &ShadowConfig {
        &self.cfg
    }

    /// Current AIMD logical cap (§4.4).
    pub fn aimd_cap(&self) -> f64 {
        self.aimd.lock().cap()
    }

    /// Feed one AIMD control tick; returns the updated logical cap.
    pub fn aimd_tick(&self, now: f64) -> f64 {
        self.aimd.lock().tick(now)
    }

    /// Record queue depth for the AIMD slope signal (§4.4).
    pub fn note_queue_depth(&self, depth: f64, now: f64) {
        self.aimd.lock().note_queue(depth, now);
    }

    /// Record a submit; returns the shadow decision (which is also logged).
    pub fn register_submit(
        &self,
        mut meta: SubmitMeta,
        depth: QueueDepth,
        cap: f64,
        now: f64,
    ) -> ShadowDecision {
        self.cache.lock().note_activity(&meta.cluster_id, now);
        *self
            .inflight_by_cluster
            .lock()
            .entry(meta.cluster_id.clone())
            .or_insert(0) += 1;

        let decision = self.evaluate(&meta, depth, cap, now);
        // Rescue-window flag mirrors `evaluate`'s `rescuable` predicate so the
        // live rescue-rate metric (P3c) counts every in-window *submitted* run
        // (admit or defer), exactly matching §8.4's candidate definition.
        let rescuable = matches!(decision.rem_ttl_pred, Some(r) if r > 0.0
            && r <= self.cfg.rescue_window_s()
            && meta.prompt_tokens_est >= self.cfg.min_rescue_prefix_tokens);
        meta.rescuable = rescuable;
        {
            let mut aimd = self.aimd.lock();
            aimd.note_submit(meta.priority == "background", now);
            if decision.decision == "reject" {
                aimd.note_reject(now);
            }
        }
        if decision.decision == "defer" {
            self.deferred_at.lock().insert(meta.run_id.clone(), now);
        }
        if decision.decision == "reject" {
            // No run will ever finalize — drop attribution state eagerly so a
            // burst of rejections cannot leak map entries.
            self.cache.lock().note_activity(&meta.cluster_id, now);
            if let Some(c) = self.inflight_by_cluster.lock().get_mut(&meta.cluster_id) {
                *c = c.saturating_sub(1);
            }
        }
        self.ledger.log_decision(&DecisionRow {
            kind: "decision",
            ts: now,
            run_id: meta.run_id.clone(),
            tenant: meta.tenant.clone(),
            cluster: meta.cluster_id.clone(),
            decision: decision.decision.to_string(),
            score: decision.score,
            rem_ttl_pred: decision.rem_ttl_pred,
            est_wait_pred: decision.est_wait_pred,
            est_service: decision.est_service,
            reason: decision.reason.to_string(),
            cap,
            in_flight: depth.in_flight,
            queued: depth.queued,
            deferred: depth.deferred,
            defer_until: decision.defer_until,
        });
        if decision.decision != "reject" {
            if rescuable {
                self.rescue_candidates.fetch_add(1, Ordering::Relaxed);
            }
            self.submits.lock().insert(meta.run_id.clone(), meta);
        }
        decision
    }

    /// Pure shadow evaluation (design §4.1) — testable without the ledger.
    pub fn evaluate(
        &self,
        meta: &SubmitMeta,
        depth: QueueDepth,
        cap: f64,
        now: f64,
    ) -> ShadowDecision {
        let est_service = self.est_service_now();
        let est_wait_pred = self.estimate_wait(depth);
        let rem_ttl_pred = self.cache.lock().rem_ttl(&meta.cluster_id, now);
        let window = self.cfg.rescue_window_s();
        let rescuable = matches!(rem_ttl_pred, Some(r) if r > 0.0
            && r <= window
            && meta.prompt_tokens_est >= self.cfg.min_rescue_prefix_tokens);
        let budget = meta
            .deadline_budget_s
            .or_else(|| self.cfg.budget_for(&meta.priority));

        let score = self.score(meta, now, rem_ttl_pred);

        if let Some(b) = budget {
            if est_wait_pred + est_service > b {
                return ShadowDecision {
                    decision: "reject",
                    reason: "budget_reject",
                    score,
                    rem_ttl_pred,
                    est_wait_pred,
                    est_service,
                    defer_until: None,
                };
            }
        }
        if (depth.in_flight as f64) < cap {
            return ShadowDecision {
                decision: "admit",
                reason: "normal",
                score,
                rem_ttl_pred,
                est_wait_pred,
                est_service,
                defer_until: None,
            };
        }
        // Aequitas floor (design §4.1): when the AIMD gate is closed, each
        // class keeps a nonzero admission probability so it can never be
        // squeezed to zero. Deterministic per run_id — replay-stable.
        if admit_floor_pass(&meta.run_id, self.cfg.admit_floor_prob) {
            return ShadowDecision {
                decision: "admit",
                reason: "aimd_floor",
                score,
                rem_ttl_pred,
                est_wait_pred,
                est_service,
                defer_until: None,
            };
        }
        if rescuable {
            let defer_until = now + rem_ttl_pred.unwrap_or(0.0) - est_service;
            return ShadowDecision {
                decision: "defer",
                reason: "rescue_window",
                score,
                rem_ttl_pred,
                est_wait_pred,
                est_service,
                defer_until: Some(defer_until),
            };
        }
        if depth.queued + depth.deferred >= self.cfg.combined_queue_cap {
            return ShadowDecision {
                decision: "reject",
                reason: "queue_full",
                score,
                rem_ttl_pred,
                est_wait_pred,
                est_service,
                defer_until: None,
            };
        }
        ShadowDecision {
            decision: "admit",
            reason: "normal",
            score,
            rem_ttl_pred,
            est_wait_pred,
            est_service,
            defer_until: None,
        }
    }

    /// `score_job_v2` with a neutral worker (no affinity at submit time).
    fn score(&self, meta: &SubmitMeta, now: f64, rem_ttl: Option<f64>) -> f64 {
        self.score_with(meta, now, rem_ttl, None)
    }

    /// Online scoring against a concrete worker-slot affinity context
    /// (design §4.2: worker's last tenant/rev/session).
    fn score_with(
        &self,
        meta: &SubmitMeta,
        now: f64,
        rem_ttl: Option<f64>,
        slot: Option<&SlotCtx>,
    ) -> f64 {
        let job = ScheduledJob {
            job_id: meta.run_id.clone(),
            session_id: meta.cluster_id.clone(),
            tenant_id: meta.tenant.clone(),
            priority: if meta.priority == "background" {
                JobPriority::Background
            } else {
                JobPriority::Interactive
            },
            arrival_s: now,
            required_rev: meta.rev as usize,
            prefix_tokens: meta.prompt_tokens_est,
            last_cache_hit_s: None,
            cache_ttl_s: self.cfg.cache_ttl_s,
            base_ttft_hit_s: 2.937,
            base_ttft_miss_s: 2.937,
            decode_s: 0.602,
            tool_s: 0.0,
            workspace_rebuild_s: 0.3,
            workspace_inc_sync_s: 0.002,
        };
        let last_hit = rem_ttl.map(|r| now + r - self.cfg.cache_ttl_s);
        // `inflight_by_cluster` counts this very run (registered before
        // scoring), but `expected_reuse` means *siblings* that will share the
        // cache window — exclude self for sim parity (L4).
        let expected_reuse = self
            .inflight_by_cluster
            .lock()
            .get(&meta.cluster_id)
            .copied()
            .unwrap_or(0)
            .saturating_sub(1) as f64;
        score_job_v2(
            &job,
            now,
            slot.and_then(|s| s.session.as_deref()),
            slot.and_then(|s| s.tenant.as_deref()),
            slot.and_then(|s| s.rev.map(|r| r as usize)),
            PolicyName::SessionGuardedDual,
            self.cfg.aging_starvation_cap_s,
            self.cfg.rescue_window_s(),
            self.cfg.min_rescue_prefix_tokens,
            last_hit,
            expected_reuse,
            &self.guardrails,
        )
    }

    /// Dispatcher pick (design §4.2): bg reserved slice + unified arbitration
    /// (escalated/ceiling at forced 1000) + `score_job_v2` ordering.
    ///
    /// Returns `(index, dispatch_reason)` with reason ∈
    /// `{"expiry_escalation","ceiling","bg_reserved","rescue_window","normal"}`.
    pub fn pick_best(
        &self,
        cands: &[DispatchCandidate],
        now: f64,
        slot: Option<&SlotCtx>,
        reserved_slot: bool,
    ) -> Option<(usize, &'static str, f64)> {
        if cands.is_empty() {
            return None;
        }
        let is_bg = |c: &DispatchCandidate| c.meta.priority == "background";

        // Reserved background slice (M1): a *reserved* worker slot must serve
        // background jobs when any are pending (work-conserving: it borrows
        // shared work only when no bg is pending). This mirrors the simulator's
        // per-worker `WorkerSlot::reserved_bg` semantics, so
        // `n_reserved_bg_workers > 1` reserves that many slots — not a global
        // "force one bg" priority.
        let bg_pending = cands.iter().any(is_bg);
        let restrict_to_bg = reserved_slot && bg_pending;

        let window = self.cfg.rescue_window_s();
        let mut best: Option<(f64, usize, &'static str)> = None;
        for (i, c) in cands.iter().enumerate() {
            if restrict_to_bg && !is_bg(c) {
                continue;
            }
            let wait = (now - c.enqueued_at).max(0.0);
            let rem = self.cache.lock().rem_ttl(&c.meta.cluster_id, now);
            let rescuable = matches!(rem, Some(r) if r > 0.0
                && r <= window
                && c.meta.prompt_tokens_est >= self.cfg.min_rescue_prefix_tokens);

            // Defensive: treat a passed deadline as escalated even if the
            // dispatcher tick has not marked it yet.
            let forced = c.escalated || matches!(c.defer_until, Some(d) if now >= d);

            // Unified arbitration (B3): escalation and ceiling share one forced
            // tier — no second queue-jumping mechanism.
            let (score, reason) = if forced {
                (1000.0, "expiry_escalation")
            } else if is_bg(c) && wait >= self.guardrails.starvation_ceiling_s {
                (1000.0, "ceiling")
            } else if restrict_to_bg {
                (self.score_with(&c.meta, now, rem, slot), "bg_reserved")
            } else if rescuable {
                (self.score_with(&c.meta, now, rem, slot), "rescue_window")
            } else {
                let _ = c.from_defer; // deferred-but-early picks score normally
                (self.score_with(&c.meta, now, rem, slot), "normal")
            };

            let better = match &best {
                None => true,
                Some((bs, bi, _)) => {
                    score > *bs || (score == *bs && i < *bi) // stable: earlier wins ties
                }
            };
            if better {
                best = Some((score, i, reason));
            }
        }
        let (score, idx, reason) = best?;

        // Guardrail + AIMD telemetry at dispatch time.
        if reason == "ceiling" {
            self.aimd.lock().note_ceiling(now);
        }
        {
            let mut g = self.guardrail_metrics.lock();
            match reason {
                "ceiling" => g.ceiling_forced_dispatches += 1,
                "bg_reserved" => g.bg_reserved_dispatches += 1,
                _ => {}
            }
        }
        Some((idx, reason, score))
    }

    /// Log a dispatch-time decision row (`kind:"dispatch"`) — the submit-time
    /// row keeps `kind:"decision"` so decision→actual joins stay 1:1.
    pub fn log_dispatch(
        &self,
        run_id: &str,
        tenant: &str,
        cluster: &str,
        decision: &str,
        reason: &str,
        score: f64,
        now: f64,
    ) {
        self.ledger.log_decision(&DecisionRow {
            kind: "dispatch",
            ts: now,
            run_id: run_id.to_string(),
            tenant: tenant.to_string(),
            cluster: cluster.to_string(),
            decision: decision.to_string(),
            score,
            rem_ttl_pred: None,
            est_wait_pred: 0.0,
            est_service: self.cfg.est_service_s,
            reason: reason.to_string(),
            cap: self.aimd_cap(),
            in_flight: 0,
            queued: 0,
            deferred: 0,
            defer_until: None,
        });
    }

    /// Bridge: gateway run token minted — remember attribution.
    pub fn register_run_token(&self, token: &str, run_id: &str) {
        let ctx = self.submits.lock().get(run_id).map(|m| TokenCtx {
            run_id: m.run_id.clone(),
            cluster_id: m.cluster_id.clone(),
            tenant: m.tenant.clone(),
            rev: m.rev,
        });
        if let Some(ctx) = ctx {
            let prefix: String = token.chars().take(16).collect();
            self.tokens.lock().insert(prefix, ctx);
        }
    }

    /// Bridge: one gateway call record → cache ledger renewal + per-run stats.
    /// `now` must be the moment the record is received (≈ request end);
    /// the renewal anchor is derived as `now − total_ms` (request start).
    pub fn on_gateway_call(
        &self,
        run_token_prefix: &str,
        prompt_tokens: u64,
        cached_tokens: u64,
        total_ms: f64,
        aborted: bool,
        now: f64,
    ) {
        // Records carry the token's first 16 chars; normalize defensively so a
        // full token string also resolves.
        let key: String = run_token_prefix.chars().take(16).collect();
        let ctx = self.tokens.lock().get(&key).cloned();
        let Some(ctx) = ctx else { return };
        let request_start = now - total_ms / 1000.0;

        {
            let mut cache = self.cache.lock();
            let prefix_hint = cache
                .entry(&ctx.cluster_id)
                .map(|e| e.prefix_tokens)
                .unwrap_or(0);
            let ev = CallEvent {
                request_start,
                prompt_tokens,
                cached_tokens,
                aborted,
                cluster_prefix_tokens: prefix_hint,
            };
            cache.on_call(&ctx.cluster_id, &ctx.tenant, ctx.rev, &ev, now);
        }

        let mut calls = self.run_calls.lock();
        let stats = calls.entry(ctx.run_id.clone()).or_insert(RunCallStats {
            total_ms_sum: 0.0,
            cached_tokens_sum: 0,
            prompt_tokens_sum: 0,
            hit_any: false,
            calls: 0,
        });
        stats.total_ms_sum += total_ms; // gateway exposes only total duration (no TTFT)
        stats.cached_tokens_sum += cached_tokens;
        stats.prompt_tokens_sum += prompt_tokens;
        stats.hit_any |= cached_tokens > 0;
        stats.calls += 1;
    }

    /// Run ended: log the actuals row and release per-cluster accounting.
    pub fn finalize_run(
        &self,
        run_id: &str,
        started_at_unix: Option<f64>,
        state: &str,
        error_code: Option<&str>,
        now: f64,
    ) {
        let meta = self.submits.lock().remove(run_id);
        let Some(meta) = meta else { return };
        if let Some(c) = self.inflight_by_cluster.lock().get_mut(&meta.cluster_id) {
            *c = c.saturating_sub(1);
        }
        self.tokens.lock().retain(|_, ctx| ctx.run_id != run_id);
        let calls = self.run_calls.lock().remove(run_id);

        // P3c rescue-rate metric: a rescue candidate that actually hit counts.
        if meta.rescuable && calls.as_ref().map(|c| c.hit_any).unwrap_or(false) {
            self.rescue_hits.fetch_add(1, Ordering::Relaxed);
        }

        let queue_wait_actual = started_at_unix
            .map(|s| (s - meta.submitted_at).max(0.0))
            .unwrap_or(0.0);
        let service_actual = started_at_unix.map(|s| (now - s).max(0.0)).unwrap_or(0.0);
        // Defer-wait accounting (§4.3): time from defer decision until start
        // (or until cancel/failure for never-started runs).
        let defer_wait_actual = self.deferred_at.lock().remove(run_id).map(|t0| {
            started_at_unix
                .map(|s| (s - t0).max(0.0))
                .unwrap_or((now - t0).max(0.0))
        });
        // P3c: retain defer-wait samples for the p50/p95/max distribution.
        if let Some(w) = defer_wait_actual {
            let mut q = self.defer_wait_samples.lock();
            q.push_back(w);
            if q.len() > 256 {
                q.pop_front();
            }
        }
        // Rolling *service* estimate (start→end), used by `estimate_wait`.
        if started_at_unix.is_some() {
            let mut q = self.recent_service_s.lock();
            q.push_back(service_actual);
            if q.len() > 64 {
                q.pop_front();
            }
        }

        self.ledger.log_actual(&ActualRow {
            kind: "actual",
            ts: now,
            run_id: run_id.to_string(),
            tenant: meta.tenant.clone(),
            cluster: meta.cluster_id.clone(),
            queue_wait_actual,
            defer_wait_actual,
            service_actual,
            total_ms_sum: calls.as_ref().map(|c| c.total_ms_sum).unwrap_or(0.0),
            cached_tokens_sum: calls.as_ref().map(|c| c.cached_tokens_sum).unwrap_or(0),
            prompt_tokens_sum: calls.as_ref().map(|c| c.prompt_tokens_sum).unwrap_or(0),
            actual_hit: calls.as_ref().map(|c| c.hit_any).unwrap_or(false),
            termination: termination_bucket(state, error_code),
        });
    }

    /// L6: backfill `actual` rows for runs that were queued/deferred/running at
    /// the previous shutdown. Their `decision` rows were already written at
    /// submit; the process died before `finalize_run` could write the matching
    /// `actual`, leaving orphan decisions in the reconciliation ledger. These
    /// are infra interruptions (not scheduling starvation), so termination is
    /// `failed` (== `termination_bucket("failed", Some("daemon_restarted"))`)
    /// and bench_recon's A1 signal excludes them via the non-`exit` filter.
    pub fn backfill_restart_actuals(&self, run_ids: &[String]) {
        if run_ids.is_empty() {
            return;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        for run_id in run_ids {
            self.ledger.log_actual(&ActualRow {
                kind: "actual",
                ts: now,
                run_id: run_id.clone(),
                tenant: String::new(),
                cluster: String::new(),
                queue_wait_actual: 0.0,
                defer_wait_actual: None,
                service_actual: 0.0,
                total_ms_sum: 0.0,
                cached_tokens_sum: 0,
                prompt_tokens_sum: 0,
                actual_hit: false,
                termination: "failed".into(),
            });
        }
    }

    /// Rolling service-time estimate (P4): mean of recent completed runs
    /// (start→end), falling back to the configured constant when no samples
    /// have been observed yet. Replaces the constant `est_service_s` in the
    /// budget check and defer-deadline arithmetic once enough runs finish.
    fn est_service_now(&self) -> f64 {
        let q = self.recent_service_s.lock();
        if q.is_empty() {
            self.cfg.est_service_s
        } else {
            q.iter().sum::<f64>() / q.len() as f64
        }
    }

    fn estimate_wait(&self, depth: QueueDepth) -> f64 {
        self.est_service_now() * depth.queued as f64
    }

    /// P3c live metrics snapshot — **global aggregates only** (§4.6: a
    /// per-tenant breakdown would be a timing side-channel and is forbidden).
    pub fn metrics(&self) -> serde_json::Value {
        let (cap, aimd_mode) = {
            let aimd = self.aimd.lock();
            (
                aimd.cap(),
                match aimd.mode() {
                    AimdMode::Normal => "normal",
                    AimdMode::Congested => "congested",
                },
            )
        };
        let guardrail = *self.guardrail_metrics.lock();
        let rescue_candidates = self.rescue_candidates.load(Ordering::Relaxed);
        let rescue_hits = self.rescue_hits.load(Ordering::Relaxed);
        // `None` (→ null) when no candidates yet: never report a misleading 0/1.
        let rescue_rate =
            (rescue_candidates > 0).then(|| rescue_hits as f64 / rescue_candidates as f64);

        // Defer-wait distribution (p50/p95/max) over the rolling sample window.
        let (defer_samples, defer_p50, defer_p95, defer_max) = {
            let q = self.defer_wait_samples.lock();
            let mut v: Vec<f64> = q.iter().copied().collect();
            v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let pct = |v: &[f64], p: f64| -> Option<f64> {
                if v.is_empty() {
                    None
                } else {
                    let idx = ((v.len() - 1) as f64 * p).round() as usize;
                    v.get(idx).copied()
                }
            };
            (v.len(), pct(&v, 0.50), pct(&v, 0.95), v.last().copied())
        };

        let (cache_entries, cache_warm) = {
            let cache = self.cache.lock();
            let now = unix_now();
            (cache.len(), cache.warm_count(now))
        };

        serde_json::json!({
            "cache": { "entries": cache_entries, "warm_entries": cache_warm },
            "rescue": {
                "candidates": rescue_candidates,
                "hits": rescue_hits,
                "hit_rate": rescue_rate,
            },
            "defer": {
                "samples": defer_samples,
                "wait_p50_s": defer_p50,
                "wait_p95_s": defer_p95,
                "wait_max_s": defer_max,
            },
            "aimd": {
                "cap": cap,
                "min_cap": self.cfg.aimd_min_cap(),
                "max_cap": self.cfg.max_concurrent_runs as f64,
                "mode": aimd_mode,
            },
            "guardrail": {
                "ceiling_forced_dispatches": guardrail.ceiling_forced_dispatches,
                "bg_reserved_dispatches": guardrail.bg_reserved_dispatches,
                "borrowed_dispatches": guardrail.borrowed_dispatches,
            },
        })
    }
}

/// Fallback cluster key when the caller supplies none (design §3.1).
pub fn cluster_key(explicit: Option<&str>, prompt: &str) -> String {
    explicit
        .map(str::to_string)
        .unwrap_or_else(|| runbay_scheduler::cache_table::fingerprint_prompt(prompt))
}

/// Current wall-clock time in unix seconds.
fn unix_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(run: &str, cluster: &str, priority: &str, tokens: usize) -> SubmitMeta {
        SubmitMeta {
            run_id: run.into(),
            cluster_id: cluster.into(),
            tenant: "t1".into(),
            rev: 1,
            priority: priority.into(),
            prompt_tokens_est: tokens,
            submitted_at: 1000.0,
            deadline_budget_s: None,
            rescuable: false,
        }
    }

    fn pool() -> Arc<RuntimePoolScheduler> {
        let dir = std::env::temp_dir().join(format!("rb_pool_test_{}", std::process::id()));
        RuntimePoolScheduler::open(&dir, ShadowConfig::default()).unwrap()
    }

    #[test]
    fn expiring_hot_submit_shadows_defer() {
        let p = pool();
        let m = meta("run_1", "c_hot", "interactive", 40_000);
        p.submits.lock().insert("run_1".into(), m.clone());
        p.register_run_token("tok_prefix_aaaaaaaa", "run_1");
        // miss call: request start = 720 - 2s = 718 → warm_until 1018
        p.on_gateway_call("tok_prefix_aaaaaaaa", 40_000, 38_000, 2000.0, false, 720.0);
        // at now=1000: rem_ttl = 18s → inside 35s rescue window ✓
        let d = p.register_submit(
            m,
            QueueDepth {
                in_flight: 3,
                queued: 5,
                deferred: 0,
            },
            3.0,
            1000.0,
        );
        assert_eq!(d.decision, "defer");
        assert_eq!(d.reason, "rescue_window");
        assert!(d.defer_until.is_some());
    }

    #[test]
    fn budget_reject_triggers_with_tight_budget() {
        let p = pool();
        let mut m = meta("run_2", "c_cold", "interactive", 40_000);
        m.deadline_budget_s = Some(1.0); // est_wait + est_service >> 1s
        let d = p.register_submit(
            m,
            QueueDepth {
                in_flight: 3,
                queued: 5,
                deferred: 0,
            },
            3.0,
            1000.0,
        );
        assert_eq!(d.decision, "reject");
        assert_eq!(d.reason, "budget_reject");
    }

    #[test]
    fn in_flight_slot_admits_normally() {
        let p = pool();
        let m = meta("run_3", "c_x", "interactive", 40_000);
        let d = p.register_submit(
            m,
            QueueDepth {
                in_flight: 0,
                queued: 0,
                deferred: 0,
            },
            3.0,
            1000.0,
        );
        assert_eq!(d.decision, "admit");
    }

    #[test]
    fn bridge_miss_renews_abort_does_not() {
        let p = pool();
        let m = meta("run_4", "c_bridge", "interactive", 40_000);
        p.submits.lock().insert("run_4".into(), m);
        p.register_run_token("tok_abcdefgh1234567", "run_4");

        // miss call, request started at 100 (received 110, total 10s)
        p.on_gateway_call("tok_abcdefgh1234567", 40_000, 0, 10_000.0, false, 110.0);
        let rem = p.cache.lock().rem_ttl("c_bridge", 110.0).unwrap();
        assert!((rem - 290.0).abs() < 0.5, "miss must renew: rem={rem}");

        // abort call at request start 200 → no renewal, rem = 400 - 205 = 195
        p.on_gateway_call("tok_abcdefgh1234567", 40_000, 0, 5_000.0, true, 205.0);
        let rem2 = p.cache.lock().rem_ttl("c_bridge", 205.0).unwrap();
        assert!((rem2 - 195.0).abs() < 0.5, "abort must not renew: rem={rem2}");
    }

    #[test]
    fn finalize_logs_actual_and_frees_cluster() {
        let p = pool();
        let m = meta("run_5", "c_fin", "background", 40_000);
        p.register_submit(
            m,
            QueueDepth {
                in_flight: 1,
                queued: 0,
                deferred: 0,
            },
            3.0,
            1000.0,
        );
        p.finalize_run("run_5", Some(1002.0), "failed", Some("sandbox_oom_killed"), 1030.0);
        assert!(p.submits.lock().is_empty());
        assert_eq!(
            *p.inflight_by_cluster.lock().get("c_fin").unwrap_or(&0),
            0
        );
    }

    // ---- P3b (design §8.1) ----

    fn cand(run: &str, priority: &str, enq: f64, escalated: bool, defer_until: Option<f64>) -> DispatchCandidate {
        DispatchCandidate {
            meta: meta(run, &format!("c_{run}"), priority, 40_000),
            enqueued_at: enq,
            defer_until,
            escalated,
            from_defer: defer_until.is_some(),
        }
    }

    #[test]
    fn budget_three_level_takes_explicit_over_class_default() {
        let p = pool();
        // Level 1: explicit request budget wins (tight → reject).
        let mut m = meta("run_b1", "c_b", "interactive", 40_000);
        m.deadline_budget_s = Some(1.0);
        let d = p.evaluate(
            &m,
            QueueDepth { in_flight: 3, queued: 5, deferred: 0 },
            3.0,
            1000.0,
        );
        assert_eq!((d.decision, d.reason), ("reject", "budget_reject"));

        // Level 2: no explicit budget → class default (fg 120s admits here).
        let m2 = meta("run_b2", "c_b", "interactive", 40_000);
        let d2 = p.evaluate(
            &m2,
            QueueDepth { in_flight: 3, queued: 5, deferred: 0 },
            3.0,
            1000.0,
        );
        assert_ne!(d2.reason, "budget_reject");

        // Level 3: class default = null → never budget-rejected.
        let dir = std::env::temp_dir().join(format!("rb_pool_budget_{}", std::process::id()));
        let cfg = ShadowConfig { fg_deadline_budget_s: None, ..Default::default() };
        let p2 = RuntimePoolScheduler::open(&dir, cfg).unwrap();
        let mut m3 = meta("run_b3", "c_b", "interactive", 40_000);
        m3.deadline_budget_s = None;
        let d3 = p2.evaluate(
            &m3,
            QueueDepth { in_flight: 3, queued: 5, deferred: 0 },
            3.0,
            1000.0,
        );
        assert_ne!(d3.reason, "budget_reject", "null budget class must never reject");
    }

    #[test]
    fn pick_enforces_ceiling_slice_and_expiry_escalation() {
        let p = pool();
        let now = 5000.0;

        // bg slice: reserved slot + bg pending → bg must win even vs fg.
        let cands = vec![
            cand("fg_1", "interactive", now - 5.0, false, None),
            cand("bg_1", "background", now - 5.0, false, None),
        ];
        let (i, reason, _) = p.pick_best(&cands, now, None, true).unwrap();
        assert_eq!(i, 1);
        assert_eq!(reason, "bg_reserved");

        // non-reserved slot: no bg restriction → score order (fg wins).
        let (i2, _, _) = p.pick_best(&cands, now, None, false).unwrap();
        assert_eq!(i2, 0);

        // ceiling: bg waiting ≥40s force-jumps even on a non-reserved slot.
        let cands2 = vec![
            cand("fg_2", "interactive", now - 50.0, false, None),
            cand("bg_2", "background", now - 41.0, false, None),
        ];
        let (i3, reason3, _) = p.pick_best(&cands2, now, None, false).unwrap();
        assert_eq!(i3, 1);
        assert_eq!(reason3, "ceiling");

        // expiry escalation shares the forced tier (B3).
        let cands3 = vec![
            cand("fg_3", "interactive", now - 50.0, false, None),
            cand("bg_3", "background", now - 1.0, true, Some(now - 1.0)),
        ];
        let (i4, reason4, _) = p.pick_best(&cands3, now, None, false).unwrap();
        assert_eq!(i4, 1);
        assert_eq!(reason4, "expiry_escalation");
    }

    #[test]
    fn aimd_decreases_under_congestion_and_recovers_without_oscillation() {
        let cfg = ShadowConfig {
            max_concurrent_runs: 8,
            n_reserved_bg_workers: 1,
            ..Default::default()
        };
        let mut a = AimdController::new(&cfg, 8.0, 0.0);
        assert_eq!(a.cap(), 8.0);

        // Congestion: every submit rejected + rising queue → congested.
        let mut prev_cap = a.cap();
        for i in 0..6 {
            let t = i as f64;
            a.note_submit(false, t);
            a.note_reject(t);
            a.note_queue(i as f64, t);
            a.tick(t + 1.0);
            let c = a.cap();
            assert!(c <= prev_cap, "cap must never grow during congestion");
            prev_cap = c;
        }
        assert_eq!(a.cap(), cfg.aimd_min_cap(), "cap floors at min_cap = reserved+1");
        assert_eq!(a.mode(), AimdMode::Congested);

        // Recovery: silence + flat queue. Old events age out of the 10s window,
        // then the 5s sustained-recovery streak must hold before growth.
        let floor_cap = a.cap();
        let mut t = 7.0;
        let mut saw_growth = false;
        let mut prev = a.cap();
        for _ in 0..30 {
            a.note_queue(0.0, t);
            a.tick(t);
            let c = a.cap();
            assert!(c >= prev, "no flap: cap must not decrease while quiet");
            if c > prev {
                saw_growth = true;
            }
            prev = c;
            t += 1.0;
        }
        assert_eq!(a.mode(), AimdMode::Normal);
        assert!(saw_growth, "cap must grow after sustained recovery");
        assert!(a.cap() > floor_cap && a.cap() <= 8.0);
    }

    #[test]
    fn aimd_accumulates_fractional_ticks() {
        // Regression for H1: the dispatcher drives `tick` at 200ms, so a
        // per-call reset of `last_tick` made `periods` stay 0 forever and the
        // gate never moved. With accumulation, 30 × 0.2s ticks (≈6s) must
        // floor the cap under sustained rejection.
        let cfg = ShadowConfig {
            max_concurrent_runs: 8,
            n_reserved_bg_workers: 1,
            ..Default::default()
        };
        let mut a = AimdController::new(&cfg, 8.0, 1000.0);
        for i in 0..30 {
            let t = 1000.0 + i as f64 * 0.2;
            a.note_submit(false, t);
            a.note_reject(t);
            a.note_queue(i as f64, t);
            a.tick(t);
        }
        assert_eq!(a.mode(), AimdMode::Congested);
        assert_eq!(
            a.cap(),
            cfg.aimd_min_cap(),
            "cap must floor after ~6s of 200ms ticks (H1 regression)"
        );
    }

    #[test]
    fn evaluate_is_deterministic_for_replay() {
        let p = pool();
        let m = meta("run_rp", "c_rp", "interactive", 40_000);
        let depth = QueueDepth { in_flight: 3, queued: 5, deferred: 1 };
        let d1 = p.evaluate(&m, depth, 3.0, 1000.0);
        let d2 = p.evaluate(&m, depth, 3.0, 1000.0);
        assert_eq!(d1.decision, d2.decision);
        assert_eq!(d1.reason, d2.reason);
        assert_eq!(d1.score, d2.score);
        assert_eq!(d1.defer_until, d2.defer_until);

        // Gate-closed path (in_flight ≥ cap): the Aequitas floor coin is a
        // pure function of run_id — same id, same admit/reject outcome.
        let m2 = meta("run_rp2", "c_rp", "interactive", 40_000);
        let full = QueueDepth { in_flight: 8, queued: 0, deferred: 0 };
        let a1 = p.evaluate(&m2, full, 3.0, 1000.0);
        let a2 = p.evaluate(&m2, full, 3.0, 1000.0);
        assert_eq!(a1.decision, a2.decision);
        assert_eq!(a1.reason, a2.reason);
    }

    #[test]
    fn reject_cleans_state_and_defer_tracks_wait() {
        let p = pool();
        // Reject under a tight budget → no submit state leaks.
        let mut m = meta("run_rj", "c_rj", "interactive", 40_000);
        m.deadline_budget_s = Some(1.0);
        p.register_submit(
            m,
            QueueDepth { in_flight: 3, queued: 5, deferred: 0 },
            3.0,
            1000.0,
        );
        assert!(p.submits.lock().is_empty(), "reject must not leak submit state");
        assert_eq!(
            *p.inflight_by_cluster.lock().get("c_rj").unwrap_or(&0),
            0,
            "reject must release cluster accounting"
        );

        // Defer → finalize carries defer_wait_actual.
        let dir = std::env::temp_dir().join(format!("rb_pool_dw_{}", std::process::id()));
        let p2 = RuntimePoolScheduler::open(&dir, ShadowConfig::default()).unwrap();
        let m2 = meta("run_dw", "c_dw", "interactive", 40_000);
        p2.submits.lock().insert("run_dw".into(), m2.clone());
        p2.register_run_token("tok_dw_abcdefgh", "run_dw");
        p2.on_gateway_call("tok_dw_abcdefgh", 40_000, 38_000, 2000.0, false, 720.0);
        // now=1000: rem_ttl=18 → rescue window → defer (gate closed).
        let d = p2.register_submit(
            m2,
            QueueDepth { in_flight: 3, queued: 5, deferred: 0 },
            3.0,
            1000.0,
        );
        assert_eq!(d.decision, "defer");
        p2.finalize_run("run_dw", Some(1012.0), "succeeded", None, 1030.0);
        // defer_wait = start(1012) − defer_decision(1000) = 12
        // (verified via the ledger row written by finalize_run).
        let raw = std::fs::read_to_string(p2.ledger.path()).unwrap();
        assert!(
            raw.contains("\"defer_wait_actual\":12"),
            "defer wait must be recorded: {raw}"
        );
    }

    #[test]
    fn metrics_tracks_rescue_rate() {
        let p = pool();
        let m = meta("run_m1", "c_m", "interactive", 40_000);
        p.submits.lock().insert("run_m1".into(), m.clone());
        p.register_run_token("tok_m1_abcdefgh", "run_m1");
        p.on_gateway_call("tok_m1_abcdefgh", 40_000, 38_000, 2000.0, false, 720.0);
        // now=1000: rem_ttl=18 → rescuable regardless of admit/defer.
        let _d = p.register_submit(
            m,
            QueueDepth { in_flight: 3, queued: 5, deferred: 0 },
            3.0,
            1000.0,
        );
        // Candidate counted at submit; hit counted at finalize.
        assert_eq!(p.rescue_candidates.load(Ordering::Relaxed), 1);
        assert_eq!(p.rescue_hits.load(Ordering::Relaxed), 0);
        p.finalize_run("run_m1", Some(1012.0), "succeeded", None, 1030.0);
        let metrics = p.metrics();
        assert_eq!(metrics["rescue"]["candidates"], 1);
        assert_eq!(metrics["rescue"]["hits"], 1);
        assert_eq!(metrics["rescue"]["hit_rate"], 1.0);
        assert_eq!(metrics["aimd"]["mode"], "normal");
        assert!(metrics["cache"]["entries"].is_number());
    }

    #[test]
    fn metrics_computes_defer_percentiles() {
        let p = pool();
        {
            let mut q = p.defer_wait_samples.lock();
            q.extend([1.0, 2.0, 3.0, 4.0, 5.0]);
        }
        let metrics = p.metrics();
        assert_eq!(metrics["defer"]["samples"], 5);
        // round((5-1)*0.50)=2 → v[2]=3.0; round(4*0.95)=4 → v[4]=5.0.
        assert_eq!(metrics["defer"]["wait_p50_s"], 3.0);
        assert_eq!(metrics["defer"]["wait_p95_s"], 5.0);
        assert_eq!(metrics["defer"]["wait_max_s"], 5.0);
        // AIMD + guardrail fields are always present in the snapshot.
        assert!(metrics["aimd"]["cap"].is_number());
        assert!(metrics["aimd"]["min_cap"].is_number());
        assert!(metrics["guardrail"]["ceiling_forced_dispatches"].is_number());
    }
}
