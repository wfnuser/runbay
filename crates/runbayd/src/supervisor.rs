use crate::adapters::{AdapterLaunchContext, AdapterRegistry};
use crate::auth::{
    generate_csprng_token, validate_identifier, validate_workspace_dir, MAX_PROMPT_BYTES,
    MAX_TIMEOUT_SECS,
};
use crate::models::{
    AgentRecord, AgentStatusView, CapabilityTier, DaemonStatusResponse, RegisterAgentRequest,
    RunEvent, RunRecord, RunState, SubmitRunRequest,
};
use crate::dispatcher::{DispatchEntry, DispatchQueue, DispatchTicket, SlotGuard};
use crate::runtime_pool::{cluster_key, QueueDepth, RuntimePoolScheduler, ShadowConfig, SubmitMeta};
use crate::store::DaemonStore;
use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use parking_lot::Mutex;
use runbay_gateway::SecretlessLoopbackGateway;
use runbay_mvcc::MVCCWorkspaceStore;
use runbay_sandbox::{
    clean_interrupted_cgroup, validate_cgroup_parent, LocalOsSandbox, SandboxCgroup, SandboxLimits,
};
use runbay_scheduler::{CancelToken, GuardedRunSink};
use std::collections::HashMap;
use std::fs;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{broadcast, oneshot, Notify, Semaphore};

const MAX_EVENT_LINE_BYTES: usize = 8192;
const BROADCAST_CAPACITY: usize = 256;

/// P3b scheduler mode (design §7): `Shadow` logs decisions only (P3a
/// behavior); `Active` enforces three-state admission + dispatcher ordering.
/// Flippable at runtime — one-switch fallback to Shadow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulerMode {
    Shadow,
    Active,
}

impl SchedulerMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            SchedulerMode::Shadow => "shadow",
            SchedulerMode::Active => "active",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "shadow" => Some(SchedulerMode::Shadow),
            "active" => Some(SchedulerMode::Active),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DaemonConfig {
    pub port: u16,
    pub state_dir: PathBuf,
    pub control_token: String,
    pub max_concurrent_runs: usize,
    pub max_queued_runs: usize,
    pub default_timeout_secs: u64,
    pub cgroup_root: Option<PathBuf>,
    pub sandbox_limits: SandboxLimits,
    pub allowed_workspace_roots: Vec<PathBuf>,
    pub upstream_url: Option<String>,
    pub upstream_api_key: Option<String>,
    pub enable_fixture_adapter: bool,
    pub codex_bin: Option<PathBuf>,
    /// P3b: shadow (log-only) or active (enforced) admission control.
    pub scheduler_mode: SchedulerMode,
    /// P3b guardrail knobs (design §4.2 / §6).
    pub n_reserved_bg_workers: usize,
    /// Rescue floor in estimated prompt tokens (bytes/4). CLI-overridable so
    /// P4 can recalibrate against cowiki's real prompt-length distribution.
    pub min_rescue_prefix_tokens: usize,
}

struct ActiveRunEntry {
    agent_id: String,
    cancel_token: CancelToken,
    cancel_notify: Arc<Notify>,
    broadcaster: broadcast::Sender<RunEvent>,
    done_notify: Arc<Notify>,
}

/// RAII guard revoking ephemeral gateway tokens on normal return, error, cancel, or panic.
struct GatewayTokenGuard {
    gateway: Arc<SecretlessLoopbackGateway>,
    token: String,
}

impl Drop for GatewayTokenGuard {
    fn drop(&mut self) {
        self.gateway.revoke_run_token(&self.token);
    }
}

/// RAII guard decrementing the live queued counter when a queued task starts or aborts.
struct QueueCounterGuard {
    counter: Arc<AtomicUsize>,
    active: bool,
}

impl QueueCounterGuard {
    fn release(&mut self) {
        if self.active {
            self.active = false;
            self.counter.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl Drop for QueueCounterGuard {
    fn drop(&mut self) {
        self.release();
    }
}

pub struct DaemonSupervisor {
    _state_lock: fs::File,
    pub config: DaemonConfig,
    pub store: DaemonStore,
    pub adapters: AdapterRegistry,
    pub gateway: Option<Arc<SecretlessLoopbackGateway>>,
    pub gateway_unix_socket: Option<PathBuf>,
    mvcc_stores: Mutex<HashMap<String, Arc<MVCCWorkspaceStore>>>,
    active_runs: Mutex<HashMap<String, Arc<ActiveRunEntry>>>,
    agent_broadcasters: Mutex<HashMap<String, broadcast::Sender<RunEvent>>>,
    run_semaphore: Arc<Semaphore>,
    queued_runs: Arc<AtomicUsize>,
    running_runs: Arc<AtomicUsize>,
    shutdown_notify: Arc<Notify>,
    /// P3a shadow admission controller + telemetry bridge (design v1.1).
    pub runtime_pool: Arc<RuntimePoolScheduler>,
    /// P3b unified arbitration queue + slot ring (design §3/§4.5).
    dispatch: Arc<Mutex<DispatchQueue>>,
    dispatch_notify: Arc<Notify>,
    /// 0 = shadow, 1 = active (runtime-flippable, one-switch fallback).
    scheduler_mode: AtomicU8,
}

impl DaemonSupervisor {
    pub async fn new(config: DaemonConfig) -> Result<Arc<Self>> {
        fs::create_dir_all(&config.state_dir)?;
        let state_dir = fs::canonicalize(&config.state_dir)?;
        // Recovery may terminate interrupted cgroups. Only the current state owner may do it.
        let control_dir = state_dir.join("control");
        fs::create_dir_all(&control_dir)?;
        let state_lock = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(control_dir.join("daemon.lock"))?;
        // SAFETY: state_lock retains the open descriptor for the supervisor's lifetime.
        if unsafe { libc::flock(state_lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(std::io::Error::last_os_error())
                .context("cannot lock daemon state directory");
        }
        fs::create_dir_all(state_dir.join("agents"))?;

        let mut cfg = config;
        cfg.state_dir = state_dir.clone();

        let store = DaemonStore::open(&state_dir.join("runbayd.sqlite"))?;
        if let Some(parent) = &mut cfg.cgroup_root {
            *parent = fs::canonicalize(&*parent)?;
            validate_cgroup_parent(parent)?;
        }
        let recovered = store.recover_interrupted_runs(|id| {
            if let Some(parent) = &cfg.cgroup_root {
                clean_interrupted_cgroup(parent, id)?;
            }
            Ok(())
        })?;

        // P3b: scheduler pool config derived from daemon config (design §6).
        let pool_cfg = ShadowConfig {
            max_concurrent_runs: cfg.max_concurrent_runs.max(1),
            combined_queue_cap: cfg.max_queued_runs.max(1),
            n_reserved_bg_workers: cfg.n_reserved_bg_workers,
            min_rescue_prefix_tokens: cfg.min_rescue_prefix_tokens,
            ..Default::default()
        };
        let runtime_pool = RuntimePoolScheduler::open(&state_dir, pool_cfg)?;
        // L6: backfill `actual` rows for runs interrupted by the previous
        // shutdown so the decision ledger has no orphan decisions.
        runtime_pool.backfill_restart_actuals(&recovered);

        let adapters = AdapterRegistry::build(cfg.enable_fixture_adapter, cfg.codex_bin.clone())?;

        let (gateway, gateway_unix_socket) =
            if let (Some(url), Some(key)) = (&cfg.upstream_url, &cfg.upstream_api_key) {
                let gw = Arc::new(SecretlessLoopbackGateway::start(url, key, None).await?);
                let unix_sock = if !cfg!(target_os = "macos") {
                    #[cfg(unix)]
                    {
                        let sock_path = state_dir.join("gateway/gateway.sock");
                        Some(gw.bind_unix_socket(&sock_path).await?)
                    }
                    #[cfg(not(unix))]
                    {
                        None
                    }
                } else {
                    None
                };
                (Some(gw), unix_sock)
            } else {
                (None, None)
            };

        // P3a telemetry bridge: gateway call records feed the cache ledger live.
        if let Some(gw) = &gateway {
            let pool = Arc::clone(&runtime_pool);
            gw.set_record_listener(move |rec| {
                let now = Utc::now().timestamp_millis() as f64 / 1000.0;
                pool.on_gateway_call(
                    &rec.run_token_prefix,
                    rec.prompt_tokens,
                    rec.cached_tokens,
                    rec.total_ms,
                    rec.aborted,
                    now,
                );
            });
        }

        let max_conc = cfg.max_concurrent_runs.max(1);
        let n_reserved_bg = cfg.n_reserved_bg_workers;
        let mode_init = match cfg.scheduler_mode {
            SchedulerMode::Shadow => 0u8,
            SchedulerMode::Active => 1u8,
        };
        let sup = Arc::new(Self {
            _state_lock: state_lock,
            config: cfg,
            store,
            adapters,
            gateway,
            gateway_unix_socket,
            mvcc_stores: Mutex::new(HashMap::new()),
            active_runs: Mutex::new(HashMap::new()),
            agent_broadcasters: Mutex::new(HashMap::new()),
            run_semaphore: Arc::new(Semaphore::new(max_conc)),
            queued_runs: Arc::new(AtomicUsize::new(0)),
            running_runs: Arc::new(AtomicUsize::new(0)),
            shutdown_notify: Arc::new(Notify::new()),
            runtime_pool,
            dispatch: Arc::new(Mutex::new(DispatchQueue::new(max_conc, n_reserved_bg))),
            dispatch_notify: Arc::new(Notify::new()),
            scheduler_mode: AtomicU8::new(mode_init),
        });
        // P3b dispatcher loop: ordering + defer escalation + AIMD ticks.
        {
            let s = Arc::clone(&sup);
            tokio::spawn(async move {
                s.dispatch_loop().await;
            });
        }
        Ok(sup)
    }

    /// Current scheduler mode (shadow = log-only, active = enforced).
    pub fn scheduler_mode(&self) -> SchedulerMode {
        if self.scheduler_mode.load(Ordering::SeqCst) == 1 {
            SchedulerMode::Active
        } else {
            SchedulerMode::Shadow
        }
    }

    /// Runtime switch (design §7: one-click fallback to Shadow). In-flight
    /// runs are unaffected; only new submits and dispatcher ordering change.
    pub fn set_scheduler_mode(&self, mode: SchedulerMode) {
        self.scheduler_mode.store(
            match mode {
                SchedulerMode::Shadow => 0,
                SchedulerMode::Active => 1,
            },
            Ordering::SeqCst,
        );
        self.dispatch_notify.notify_one();
    }

    pub fn status(&self) -> Result<DaemonStatusResponse> {
        let probe_sb = LocalOsSandbox::new(&self.config.state_dir, &[]);
        let agents = self.store.list_agents()?;
        let available = probe_sb.is_available();
        let gw_transport = (self.gateway.is_some()
            && available
            && (cfg!(target_os = "macos") || self.gateway_unix_socket.is_some()))
        .then(|| probe_sb.gateway_transport().to_owned());
        Ok(DaemonStatusResponse {
            service: "runbayd".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            platform: format!("{} ({})", std::env::consts::OS, std::env::consts::ARCH),
            sandbox_mechanism: probe_sb.mechanism_name().to_string(),
            sandbox_available: available,
            sandbox_resource_limits: self.config.cgroup_root.as_ref().map(|_| self.config.sandbox_limits),
            gateway_enabled: self.gateway.is_some(),
            gateway_transport: gw_transport,
            max_concurrent_runs: self.config.max_concurrent_runs,
            max_queued_runs: self.config.max_queued_runs,
            active_runs: self.running_runs.load(Ordering::SeqCst),
            queued_runs: self.queued_runs.load(Ordering::SeqCst),
            registered_agents: agents.len(),
            components: vec![
                "runbay-mvcc".to_string(),
                "runbay-sandbox".to_string(),
                "runbay-gateway".to_string(),
                "runbay-scheduler".to_string(),
            ],
            adapters: self.adapters.capabilities_map(),
            restart_semantics: "Durable agent registry and run history persist in SQLite; running child processes do not survive daemon restarts and are marked failed (daemon_restarted) on startup.".to_string(),
        })
    }

    /// P3c `/metrics` payload: global scheduler aggregates (§4.6 — no
    /// per-tenant breakdown, which would be a timing side-channel). Combines
    /// the live dispatch queue depth with the `RuntimePoolScheduler` snapshot
    /// (AIMD / rescue / defer / cache / guardrail).
    pub fn scheduler_metrics(&self) -> serde_json::Value {
        let (queued, deferred, total) = {
            let q = self.dispatch.lock();
            (q.queued_count(), q.deferred_count(), q.len())
        };
        let pool = self.runtime_pool.metrics();
        serde_json::json!({
            "scheduler": { "mode": self.scheduler_mode().as_str() },
            "queue": {
                "in_flight": self.running_runs.load(Ordering::SeqCst),
                "queued": queued,
                "deferred": deferred,
                "total": total,
            },
            "pool": pool,
        })
    }

    fn ensure_mvcc_store(&self, agent: &AgentRecord) -> Result<Arc<MVCCWorkspaceStore>> {
        if let Some(existing) = self.mvcc_stores.lock().get(&agent.agent_id).cloned() {
            return Ok(existing);
        }
        let store = MVCCWorkspaceStore::new(&agent.mvcc_dir, &agent.workspace_dir)?;
        let mut map = self.mvcc_stores.lock();
        let entry = map
            .entry(agent.agent_id.clone())
            .or_insert_with(|| Arc::clone(&store));
        Ok(Arc::clone(entry))
    }

    fn agent_broadcaster(&self, agent_id: &str) -> broadcast::Sender<RunEvent> {
        let mut map = self.agent_broadcasters.lock();
        map.entry(agent_id.to_string())
            .or_insert_with(|| broadcast::channel(BROADCAST_CAPACITY).0)
            .clone()
    }

    pub fn register_agent(&self, req: RegisterAgentRequest) -> Result<AgentStatusView> {
        validate_identifier(&req.agent_id, "agent_id")?;
        if self.store.get_agent(&req.agent_id)?.is_some() {
            bail!(
                "AGENT_EXISTS: agent '{}' is already registered",
                req.agent_id
            );
        }
        let Some(adapter) = self.adapters.get(&req.runtime) else {
            bail!(
                "UNKNOWN_RUNTIME: runtime '{}' is not enabled on this daemon",
                req.runtime
            );
        };
        let caps = adapter.capabilities();
        if !caps.available {
            bail!(
                "ADAPTER_UNAVAILABLE: runtime '{}' is unavailable ({})",
                req.runtime,
                caps.notes
            );
        }

        let canonical_ws = validate_workspace_dir(
            &req.workspace_dir,
            &self.config.state_dir,
            &self.config.allowed_workspace_roots,
        )?;

        let agent_root = self.config.state_dir.join("agents").join(&req.agent_id);
        let home_dir = agent_root.join("home");
        let mvcc_dir = agent_root.join("mvcc");
        fs::create_dir_all(&home_dir)?;

        let record = AgentRecord {
            agent_id: req.agent_id.clone(),
            runtime: req.runtime,
            workspace_dir: canonical_ws.to_string_lossy().to_string(),
            home_dir: fs::canonicalize(&home_dir)?.to_string_lossy().to_string(),
            mvcc_dir: mvcc_dir.to_string_lossy().to_string(),
            config: if req.config.is_null() {
                serde_json::json!({})
            } else {
                req.config
            },
            created_at: Utc::now().to_rfc3339(),
        };

        let mvcc = self.ensure_mvcc_store(&record)?;
        self.store.insert_agent(&record)?;

        Ok(self.build_agent_view(&record, Some(&mvcc)))
    }

    pub fn get_agent_view(&self, agent_id: &str) -> Result<Option<AgentStatusView>> {
        validate_identifier(agent_id, "agent_id")?;
        let Some(record) = self.store.get_agent(agent_id)? else {
            return Ok(None);
        };
        let mvcc = self.mvcc_stores.lock().get(agent_id).cloned();
        Ok(Some(self.build_agent_view(&record, mvcc.as_deref())))
    }

    pub fn list_agent_views(&self) -> Result<Vec<AgentStatusView>> {
        let records = self.store.list_agents()?;
        let stores = self.mvcc_stores.lock().clone();
        Ok(records
            .iter()
            .map(|r| {
                let mvcc = stores.get(&r.agent_id).map(|a| a.as_ref());
                self.build_agent_view(r, mvcc)
            })
            .collect())
    }

    fn build_agent_view(
        &self,
        record: &AgentRecord,
        mvcc: Option<&MVCCWorkspaceStore>,
    ) -> AgentStatusView {
        let caps = self
            .adapters
            .get(&record.runtime)
            .map(|a| a.capabilities())
            .unwrap_or_else(|| crate::models::AdapterCapabilities {
                runtime: record.runtime.clone(),
                tier: CapabilityTier::OneShot,
                supports_streaming: false,
                supports_sandbox: false,
                supports_gateway: false,
                supports_native_resume: false,
                supports_hot_process: false,
                available: false,
                binary_path: None,
                notes: "adapter disabled in current daemon configuration".to_string(),
            });
        let workspace_rev = mvcc.map(|s| s.current_rev()).unwrap_or(0);
        let active_leases = mvcc
            .map(|s| s.active_leases_for(workspace_rev))
            .unwrap_or(0);
        let active_run_id = self
            .active_runs
            .lock()
            .iter()
            .find(|(_, entry)| entry.agent_id == record.agent_id)
            .map(|(id, _)| id.clone());
        AgentStatusView {
            agent_id: record.agent_id.clone(),
            runtime: record.runtime.clone(),
            workspace_dir: record.workspace_dir.clone(),
            home_dir: record.home_dir.clone(),
            config: record.config.clone(),
            capabilities: caps,
            workspace_rev,
            active_leases,
            active_run_id,
            created_at: record.created_at.clone(),
        }
    }

    pub async fn delete_agent(&self, agent_id: &str) -> Result<bool> {
        validate_identifier(agent_id, "agent_id")?;
        if self.store.get_agent(agent_id)?.is_none() {
            return Ok(false);
        }

        // Cancel and await termination of any active runs belonging to this agent.
        let matching_runs: Vec<String> = self
            .active_runs
            .lock()
            .iter()
            .filter(|(_, entry)| entry.agent_id == agent_id)
            .map(|(rid, _)| rid.clone())
            .collect();
        for rid in matching_runs {
            let _ = self.cancel_run(&rid, "agent_deleted").await;
        }

        self.mvcc_stores.lock().remove(agent_id);
        self.agent_broadcasters.lock().remove(agent_id);
        let deleted = self.store.delete_agent(agent_id)?;
        let agent_root = self.config.state_dir.join("agents").join(agent_id);
        if agent_root.exists() {
            let _ = fs::remove_dir_all(&agent_root);
        }
        Ok(deleted)
    }

    pub fn submit_run(
        self: &Arc<Self>,
        agent_id: &str,
        req: SubmitRunRequest,
    ) -> Result<RunRecord> {
        validate_identifier(agent_id, "agent_id")?;
        if req.prompt.trim().is_empty() {
            bail!("INVALID_REQUEST: prompt must be non-empty");
        }
        if req.prompt.len() > MAX_PROMPT_BYTES {
            bail!(
                "PAYLOAD_TOO_LARGE: prompt exceeds {} bytes",
                MAX_PROMPT_BYTES
            );
        }
        if !matches!(req.priority.as_str(), "interactive" | "background") {
            bail!("INVALID_REQUEST: priority must be 'interactive' or 'background'");
        }
        let timeout_secs = req.timeout_secs.unwrap_or(self.config.default_timeout_secs);
        if timeout_secs == 0 || timeout_secs > MAX_TIMEOUT_SECS {
            bail!(
                "INVALID_REQUEST: timeout_secs must be in 1..={}",
                MAX_TIMEOUT_SECS
            );
        }

        let Some(agent) = self.store.get_agent(agent_id)? else {
            bail!("NOT_FOUND: agent '{agent_id}' not found");
        };
        if self.adapters.get(&agent.runtime).is_none() {
            bail!(
                "ADAPTER_UNAVAILABLE: runtime '{}' is not enabled",
                agent.runtime
            );
        }

        if let Some(limits) = req.resource_limits {
            if self.config.cgroup_root.is_none() {
                bail!("INVALID_REQUEST: per-run resource limits require a configured Linux cgroup parent");
            }
            if !limits.fits_within(self.config.sandbox_limits) {
                bail!("INVALID_REQUEST: resource_limits exceed the daemon's per-run ceilings");
            }
        }

        // Bounded live queue admission check
        let prev_queued = self.queued_runs.fetch_add(1, Ordering::SeqCst);
        if prev_queued >= self.config.max_queued_runs {
            self.queued_runs.fetch_sub(1, Ordering::SeqCst);
            bail!(
                "QUEUE_FULL: live queue limit ({}) reached",
                self.config.max_queued_runs
            );
        }
        let queue_guard = QueueCounterGuard {
            counter: Arc::clone(&self.queued_runs),
            active: true,
        };

        let mvcc = self.ensure_mvcc_store(&agent)?;
        let workspace_rev = req.required_rev.unwrap_or_else(|| mvcc.current_rev());
        if mvcc.get_snapshot(workspace_rev).is_none() {
            drop(queue_guard);
            bail!("INVALID_REQUEST: unknown required_rev {workspace_rev} for agent '{agent_id}'");
        }

        let run_id = format!("run_{}", &generate_csprng_token("")[..20]);

        // P3b admission: three-state decision (always logged; enforced only in
        // Active mode — Shadow keeps P3a log-only semantics).
        let meta = SubmitMeta {
            run_id: run_id.clone(),
            cluster_id: cluster_key(req.cluster_id.as_deref(), &req.prompt),
            tenant: req
                .tenant_id
                .clone()
                .unwrap_or_else(|| agent.agent_id.clone()),
            rev: workspace_rev,
            priority: req.priority.clone(),
            prompt_tokens_est: req.prompt.len() / 4, // bytes/4 heuristic (no tokenizer at submit; calibrate in live validation, L3)
            submitted_at: Utc::now().timestamp_millis() as f64 / 1000.0,
            deadline_budget_s: req.deadline_budget_s,
            // Placeholder — `register_submit` recomputes this from the live
            // rescue-window predicate before the meta is stored.
            rescuable: false,
        };
        let now = Utc::now().timestamp_millis() as f64 / 1000.0;
        let depth = {
            let q = self.dispatch.lock();
            QueueDepth {
                in_flight: self.running_runs.load(Ordering::SeqCst),
                queued: q.queued_count(),
                deferred: q.deferred_count(),
            }
        };
        let active_mode = self.scheduler_mode() == SchedulerMode::Active;
        let cap = if active_mode {
            self.runtime_pool.aimd_cap()
        } else {
            self.config.max_concurrent_runs as f64
        };
        let decision = self.runtime_pool.register_submit(meta.clone(), depth, cap, now);

        if active_mode && decision.decision == "reject" {
            drop(queue_guard);
            let retry_after = crate::runtime_pool::retry_after_s(
                &run_id,
                decision.reason,
                decision.est_wait_pred,
                self.runtime_pool.config().aimd_period_s,
            );
            bail!(
                "ADMISSION_REJECTED: reason={} retry_after_s={:.1}",
                decision.reason,
                retry_after
            );
        }

        let is_deferred = active_mode && decision.decision == "defer";
        let record = RunRecord {
            run_id: run_id.clone(),
            agent_id: agent.agent_id.clone(),
            runtime: agent.runtime.clone(),
            prompt: req.prompt.clone(),
            state: if is_deferred {
                RunState::Deferred
            } else {
                RunState::Queued
            },
            priority: req.priority.clone(),
            capability_tier: CapabilityTier::OneShot,
            workspace_rev,
            branch_workspace: None,
            exit_code: None,
            error_code: None,
            error_message: None,
            sandbox_enforced: false,
            network_isolated: false,
            secretless_gateway: false,
            gateway_transport: None,
            events_count: 0,
            events_dropped: 0,
            late_events_rejected: 0,
            created_at: Utc::now().to_rfc3339(),
            started_at: None,
            finished_at: None,
        };
        self.store.insert_run(&record)?;

        let (tx, _) = broadcast::channel::<RunEvent>(BROADCAST_CAPACITY);
        let active = Arc::new(ActiveRunEntry {
            agent_id: agent.agent_id.clone(),
            cancel_token: CancelToken::new(),
            cancel_notify: Arc::new(Notify::new()),
            broadcaster: tx,
            done_notify: Arc::new(Notify::new()),
        });
        self.active_runs
            .lock()
            .insert(run_id.clone(), Arc::clone(&active));

        // P3c: defer emits a dedicated `run.deferred` lifecycle event (design
        // §4.5) carrying the deadline + reason, so the submitter can observe
        // the three-state decision. Admitted runs keep `run.queued`.
        let lifecycle = if is_deferred {
            (
                "run.deferred",
                serde_json::json!({
                    "state": "deferred",
                    "runtime": agent.runtime,
                    "workspace_rev": workspace_rev,
                    "reason": decision.reason,
                    "defer_until": decision.defer_until,
                    "est_wait_pred": decision.est_wait_pred,
                }),
            )
        } else {
            (
                "run.queued",
                serde_json::json!({
                    "state": record.state.as_str(),
                    "runtime": agent.runtime,
                    "workspace_rev": workspace_rev
                }),
            )
        };
        self.emit_lifecycle_event(&active, &run_id, &agent.agent_id, lifecycle.0, lifecycle.1);

        // Enter the unified arbitration queue (P3b). Defer entries carry the
        // deadline from the admission decision (design §4.1).
        let (ticket_tx, ticket_rx) = oneshot::channel::<DispatchTicket>();
        {
            let mut q = self.dispatch.lock();
            q.push(DispatchEntry {
                meta,
                enqueued_at: now,
                defer_until: if is_deferred {
                    decision.defer_until
                } else {
                    None
                },
                escalated: false,
                from_defer: is_deferred,
                ticket: ticket_tx,
            });
        }
        self.dispatch_notify.notify_one();

        let sup = Arc::clone(self);
        let run_clone = record.clone();
        let active_clone = Arc::clone(&active);
        tokio::spawn(async move {
            sup.execute_run_task(
                agent,
                mvcc,
                run_clone,
                req,
                timeout_secs,
                active_clone,
                queue_guard,
                ticket_rx,
            )
            .await;
        });

        Ok(self.store.get_run(&run_id)?.unwrap_or(record))
    }

    pub fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>> {
        validate_identifier(run_id, "run_id")?;
        self.store.get_run(run_id)
    }

    pub fn list_runs(&self, agent_id: Option<&str>) -> Result<Vec<RunRecord>> {
        if let Some(aid) = agent_id {
            validate_identifier(aid, "agent_id")?;
        }
        self.store.list_runs(agent_id)
    }

    pub fn subscribe_run(
        &self,
        run_id: &str,
    ) -> Option<(broadcast::Receiver<RunEvent>, Arc<Notify>)> {
        self.active_runs.lock().get(run_id).map(|entry| {
            (
                entry.broadcaster.subscribe(),
                Arc::clone(&entry.done_notify),
            )
        })
    }

    pub fn subscribe_agent(&self, agent_id: &str) -> broadcast::Receiver<RunEvent> {
        self.agent_broadcaster(agent_id).subscribe()
    }

    pub async fn cancel_run(&self, run_id: &str, reason: &str) -> Result<Option<RunRecord>> {
        validate_identifier(run_id, "run_id")?;
        let Some(existing) = self.store.get_run(run_id)? else {
            return Ok(None);
        };
        if existing.state.is_terminal() {
            return Ok(Some(existing));
        }

        let active_opt = self.active_runs.lock().get(run_id).cloned();
        if let Some(active) = active_opt {
            active.cancel_token.cancel(reason);
            active.cancel_notify.notify_one();
            // Wait up to 3 seconds for process-group termination & terminal state persistence.
            let _ =
                tokio::time::timeout(Duration::from_secs(3), active.done_notify.notified()).await;
        }
        self.store.get_run(run_id)
    }

    pub async fn shutdown_all(&self) {
        self.shutdown_notify.notify_waiters();
        let entries: Vec<(String, Arc<ActiveRunEntry>)> = self
            .active_runs
            .lock()
            .iter()
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect();
        for (_rid, entry) in &entries {
            entry.cancel_token.cancel("daemon_shutdown");
            entry.cancel_notify.notify_one();
        }
        for (_rid, entry) in entries {
            let _ =
                tokio::time::timeout(Duration::from_secs(3), entry.done_notify.notified()).await;
        }
    }

    fn emit_lifecycle_event(
        &self,
        active: &ActiveRunEntry,
        run_id: &str,
        agent_id: &str,
        event_type: &str,
        data: serde_json::Value,
    ) {
        if let Ok(Some(ev)) = self.store.append_event(run_id, agent_id, event_type, data) {
            let _ = active.broadcaster.send(ev.clone());
            let _ = self.agent_broadcaster(agent_id).send(ev);
        }
    }

    fn emit_guarded_output(
        &self,
        sink: &GuardedRunSink,
        active: &ActiveRunEntry,
        run_id: &str,
        agent_id: &str,
        event_type: &str,
        data: serde_json::Value,
    ) {
        if !sink.emit(data.clone()) {
            return;
        }
        if let Ok(Some(ev)) = self.store.append_event(run_id, agent_id, event_type, data) {
            let _ = active.broadcaster.send(ev.clone());
            let _ = self.agent_broadcaster(agent_id).send(ev);
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_run_task(
        self: Arc<Self>,
        agent: AgentRecord,
        mvcc: Arc<MVCCWorkspaceStore>,
        mut run: RunRecord,
        req: SubmitRunRequest,
        timeout_secs: u64,
        active: Arc<ActiveRunEntry>,
        mut queue_guard: QueueCounterGuard,
        ticket_rx: oneshot::Receiver<DispatchTicket>,
    ) {
        let sink = Arc::new(GuardedRunSink::new(active.cancel_token.clone()));

        // 1. Wait for the dispatcher ticket (or abort cleanly if cancelled
        //    while queued). The ticket carries the concurrency permit + slot.
        let ticket = tokio::select! {
            biased;
            _ = active.cancel_notify.notified() => {
                queue_guard.release();
                self.dispatch.lock().remove(&run.run_id);
                self.finalize_run(
                    &mut run,
                    &active,
                    &sink,
                    RunState::Cancelled,
                    None,
                    Some("cancelled_while_queued"),
                    active.cancel_token.reason().as_deref().or(Some("cancelled")),
                );
                return;
            }
            _ = self.shutdown_notify.notified() => {
                queue_guard.release();
                self.dispatch.lock().remove(&run.run_id);
                self.finalize_run(
                    &mut run,
                    &active,
                    &sink,
                    RunState::Cancelled,
                    None,
                    Some("daemon_shutdown"),
                    Some("Daemon shutting down"),
                );
                return;
            }
            dispatched = ticket_rx => {
                queue_guard.release();
                match dispatched {
                    Ok(t) => t,
                    Err(_) => {
                        self.finalize_run(
                            &mut run,
                            &active,
                            &sink,
                            RunState::Failed,
                            None,
                            Some("scheduler_closed"),
                            Some("Dispatch queue closed"),
                        );
                        return;
                    }
                }
            }
        };
        let permit = ticket.permit;
        // Any early return from here on frees the slot (affinity memory kept).
        let _slot_guard = SlotGuard::new(Arc::clone(&self.dispatch), ticket.slot_id);

        if active.cancel_token.is_cancelled() {
            drop(permit);
            self.finalize_run(
                &mut run,
                &active,
                &sink,
                RunState::Cancelled,
                None,
                Some("cancelled_while_queued"),
                active
                    .cancel_token
                    .reason()
                    .as_deref()
                    .or(Some("cancelled")),
            );
            return;
        }

        self.running_runs.fetch_add(1, Ordering::SeqCst);
        let running_counter = Arc::clone(&self.running_runs);
        struct RunningGuard(Arc<AtomicUsize>);
        impl Drop for RunningGuard {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        let _running_guard = RunningGuard(running_counter);

        // 2. Pin MVCC snapshot lease and fork isolated CoW workspace branch for this run.
        let lease = match mvcc.pin_snapshot(Some(run.workspace_rev)) {
            Ok(l) => l,
            Err(e) => {
                drop(permit);
                self.finalize_run(
                    &mut run,
                    &active,
                    &sink,
                    RunState::Failed,
                    None,
                    Some("mvcc_lease_error"),
                    Some(&e.to_string()),
                );
                return;
            }
        };
        let branch = match mvcc.fork_branch_group(&run.run_id, 1, Some(lease.rev)) {
            Ok((mut handles, _elapsed)) if !handles.is_empty() => handles.remove(0),
            Ok(_) => {
                drop(lease);
                drop(permit);
                self.finalize_run(
                    &mut run,
                    &active,
                    &sink,
                    RunState::Failed,
                    None,
                    Some("mvcc_fork_error"),
                    Some("MVCC branch fork returned empty handle list"),
                );
                return;
            }
            Err(e) => {
                drop(lease);
                drop(permit);
                self.finalize_run(
                    &mut run,
                    &active,
                    &sink,
                    RunState::Failed,
                    None,
                    Some("mvcc_fork_error"),
                    Some(&e.to_string()),
                );
                return;
            }
        };

        // 3. Mint ephemeral gateway token (RAII-revoked on exit/cancel/timeout).
        let gw_guard = self.gateway.as_ref().map(|gw| {
            let tok = gw.mint_run_token(&run.run_id, req.allowed_model.clone());
            self.runtime_pool.register_run_token(&tok, &run.run_id);
            GatewayTokenGuard {
                gateway: Arc::clone(gw),
                token: tok,
            }
        });

        let launch_ctx = AdapterLaunchContext {
            prompt: req.prompt.clone(),
            workspace_dir: branch.workspace_dir.clone(),
            agent_home_dir: PathBuf::from(&agent.home_dir),
            allowed_model: req.allowed_model.clone(),
            sandbox_mode: req.sandbox_mode.clone(),
            gateway_base_url: self.gateway.as_ref().map(|g| g.base_url()),
            gateway_port: self.gateway.as_ref().map(|g| g.port()),
            gateway_unix_socket: self.gateway_unix_socket.clone(),
            gateway_run_token: gw_guard.as_ref().map(|g| g.token.clone()),
        };

        let mut prepared = match self
            .adapters
            .get(&agent.runtime)
            .ok_or_else(|| anyhow!("adapter '{}' unavailable", agent.runtime))
            .and_then(|adapter| adapter.prepare_launch(&launch_ctx))
        {
            Ok(p) => p,
            Err(e) => {
                mvcc.abort_branch(&branch.branch_id, "launch_prepare_failed");
                drop(gw_guard);
                drop(lease);
                drop(permit);
                self.finalize_run(
                    &mut run,
                    &active,
                    &sink,
                    RunState::Failed,
                    None,
                    Some("launch_prepare_failed"),
                    Some(&e.to_string()),
                );
                return;
            }
        };

        // 4. Spawn real OS-sandboxed process in its own POSIX process group.
        let pool_root = self.config.state_dir.join("agents");
        let control_token_dir = self.config.state_dir.join("control");
        let limits = req.resource_limits.unwrap_or(self.config.sandbox_limits);
        prepared.sandbox_opts.cgroup =
            self.config
                .cgroup_root
                .as_ref()
                .map(|parent| SandboxCgroup {
                    parent: parent.clone(),
                    run_id: run.run_id.clone(),
                    limits,
                });
        let workspace = branch.workspace_dir.clone();
        let requested_limits = prepared
            .sandbox_opts
            .cgroup
            .as_ref()
            .map(|group| group.limits);
        let launch = tokio::task::spawn_blocking(move || {
            let sandbox = LocalOsSandbox::new(pool_root, &[control_token_dir]);
            sandbox
                .check_available()
                .map_err(|e| ("sandbox_unavailable", e))?;
            let child = sandbox
                .spawn_async(&prepared.command, &workspace, &prepared.sandbox_opts)
                .map_err(|e| ("spawn_failed", e))?;
            Ok((child, sandbox.gateway_transport()))
        })
        .await
        .unwrap_or_else(|e| Err(("spawn_failed", e.into())));
        let (mut sandboxed, transport) = match launch {
            Ok(value) => value,
            Err((code, error)) => {
                mvcc.abort_branch(&branch.branch_id, code);
                drop(gw_guard);
                drop(lease);
                drop(permit);
                self.finalize_run(
                    &mut run,
                    &active,
                    &sink,
                    RunState::Failed,
                    None,
                    Some(code),
                    Some(&format!("{error:#}")),
                );
                return;
            }
        };

        run.state = RunState::Running;
        run.started_at = Some(Utc::now().to_rfc3339());
        run.branch_workspace = Some(branch.workspace_dir.to_string_lossy().to_string());
        run.sandbox_enforced = true;
        run.network_isolated = prepared.network_isolated;
        run.secretless_gateway = prepared.secretless_gateway;
        run.gateway_transport = prepared.secretless_gateway.then(|| transport.to_owned());
        let _ = self.store.update_run(&run);

        self.emit_lifecycle_event(
            &active,
            &run.run_id,
            &run.agent_id,
            "run.started",
            serde_json::json!({
                "state": "running",
                "pid": sandboxed.pid(),
                "pgid": sandboxed.pgid(),
                "workspace_rev": run.workspace_rev,
                "resource_limits": requested_limits,
                "sandbox_enforced": true,
                "network_isolated": run.network_isolated,
                "secretless_gateway": run.secretless_gateway,
                "gateway_transport": run.gateway_transport
            }),
        );

        // 5. Stream stdout & stderr concurrently through GuardedRunSink.
        let stdout_handle = sandboxed.child.stdout.take().map(|stdout| {
            let sup = Arc::clone(&self);
            let sink_c = Arc::clone(&sink);
            let active_c = Arc::clone(&active);
            let rid = run.run_id.clone();
            let aid = run.agent_id.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stdout).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    let bounded: String = line.chars().take(MAX_EVENT_LINE_BYTES).collect();
                    let (ev_type, payload) =
                        match serde_json::from_str::<serde_json::Value>(&bounded) {
                            Ok(parsed) if parsed.is_object() => ("agent.event", parsed),
                            _ => (
                                "run.output",
                                serde_json::json!({"stream": "stdout", "line": bounded}),
                            ),
                        };
                    sup.emit_guarded_output(&sink_c, &active_c, &rid, &aid, ev_type, payload);
                }
            })
        });

        let stderr_handle = sandboxed.child.stderr.take().map(|stderr| {
            let sup = Arc::clone(&self);
            let sink_c = Arc::clone(&sink);
            let active_c = Arc::clone(&active);
            let rid = run.run_id.clone();
            let aid = run.agent_id.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    let bounded: String = line.chars().take(MAX_EVENT_LINE_BYTES).collect();
                    sup.emit_guarded_output(
                        &sink_c,
                        &active_c,
                        &rid,
                        &aid,
                        "run.stderr",
                        serde_json::json!({"stream": "stderr", "line": bounded}),
                    );
                }
            })
        });

        // 6. Supervise child exit vs cancel vs timeout vs daemon shutdown.
        let timeout_sleep = tokio::time::sleep(Duration::from_secs(timeout_secs));
        tokio::pin!(timeout_sleep);

        enum WaitOutcome {
            Exited(std::io::Result<std::process::ExitStatus>),
            Cancelled(String),
            TimedOut,
        }

        let outcome = tokio::select! {
            biased;
            _ = active.cancel_notify.notified() => {
                WaitOutcome::Cancelled(active.cancel_token.reason().unwrap_or_else(|| "cancelled".to_string()))
            }
            _ = self.shutdown_notify.notified() => {
                active.cancel_token.cancel("daemon_shutdown");
                WaitOutcome::Cancelled("daemon_shutdown".to_string())
            }
            _ = &mut timeout_sleep => {
                active.cancel_token.cancel("timeout");
                WaitOutcome::TimedOut
            }
            res = sandboxed.child.wait() => WaitOutcome::Exited(res),
        };

        // Release the upstream credential as soon as the process exits or cancellation wins.
        drop(gw_guard);
        let drain_timeout = if matches!(&outcome, WaitOutcome::Exited(Ok(_))) {
            Duration::from_secs(2)
        } else {
            let _ = sandboxed
                .terminate_process_group(Duration::from_millis(400))
                .await;
            Duration::from_millis(300)
        };
        let cleanup = sandboxed.finish_resources_with_oom().await;
        for handle in [stdout_handle, stderr_handle].into_iter().flatten() {
            let _ = tokio::time::timeout(drain_timeout, handle).await;
        }

        let (final_state, exit_code, err_code, err_msg) = match (cleanup, outcome) {
            (Err(error), _) => {
                mvcc.abort_branch(&branch.branch_id, "resource_cleanup_failed");
                (
                    RunState::Failed,
                    None,
                    Some("resource_cleanup_failed"),
                    Some(format!("{error:#}")),
                )
            }
            (Ok(true), WaitOutcome::Exited(Ok(status))) => {
                mvcc.abort_branch(&branch.branch_id, "oom_killed");
                (
                    RunState::Failed,
                    status.code(),
                    Some("oom_killed"),
                    Some("The run exceeded its cgroup memory limit".to_owned()),
                )
            }
            (Ok(_), WaitOutcome::Exited(Ok(status))) if status.success() => {
                (RunState::Succeeded, status.code(), None, None)
            }
            (Ok(_), WaitOutcome::Exited(Ok(status))) => {
                mvcc.abort_branch(&branch.branch_id, "nonzero_exit");
                let code = status.code();
                (
                    RunState::Failed,
                    code,
                    Some("nonzero_exit"),
                    Some(format!("Process exited with code {}", code.unwrap_or(-1))),
                )
            }
            (Ok(_), WaitOutcome::Exited(Err(error))) => {
                mvcc.abort_branch(&branch.branch_id, "wait_error");
                (
                    RunState::Failed,
                    None,
                    Some("wait_error"),
                    Some(error.to_string()),
                )
            }
            (Ok(_), WaitOutcome::Cancelled(reason)) => {
                mvcc.abort_branch(&branch.branch_id, &reason);
                (RunState::Cancelled, None, Some("cancelled"), Some(reason))
            }
            (Ok(_), WaitOutcome::TimedOut) => {
                mvcc.abort_branch(&branch.branch_id, "timeout");
                (
                    RunState::Failed,
                    None,
                    Some("timeout"),
                    Some(format!("Run timed out after {timeout_secs}s")),
                )
            }
        };

        drop(lease);
        drop(permit);

        self.finalize_run(
            &mut run,
            &active,
            &sink,
            final_state,
            exit_code,
            err_code,
            err_msg.as_deref(),
        );
    }

    /// P3b dispatcher loop (design §3): on slot release, score the unified
    /// arbitration queue and hand out tickets. Also escalates expired defer
    /// entries and drives AIMD control ticks (§4.4).
    async fn dispatch_loop(self: Arc<Self>) {
        let mut tick = tokio::time::interval(Duration::from_millis(200));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = self.shutdown_notify.notified() => break,
                _ = self.dispatch_notify.notified() => {}
                _ = tick.tick() => {}
            }
            self.drain_dispatch();
        }
    }

    /// One dispatch drain: AIMD tick → defer escalation → fill free slots.
    fn drain_dispatch(&self) {
        let now = Utc::now().timestamp_millis() as f64 / 1000.0;

        let active_mode = self.scheduler_mode() == SchedulerMode::Active;

        // AIMD: queue-depth sample + control tick (§4.4) — active only. In
        // shadow mode the controller stays frozen at max_cap so "shadow"
        // truly records without intervening; switching to Active starts from
        // a clean max_cap gate.
        if active_mode {
            let depth_total = self.dispatch.lock().len();
            self.runtime_pool.note_queue_depth(depth_total as f64, now);
            self.runtime_pool.aimd_tick(now);
        }

        // Defer expiry → forced tier (unified arbitration with ceiling, B3).
        let escalated = self.dispatch.lock().escalate_due(now);
        for rid in &escalated {
            if let Ok(Some(mut rec)) = self.store.get_run(rid) {
                if rec.state == RunState::Deferred {
                    rec.state = RunState::Queued;
                    let _ = self.store.update_run(&rec);
                }
            }
        }

        // Fill free slots while permits are available.
        loop {
            // Take a slot first so slot ring and semaphore stay in lockstep.
            let slot_id = {
                let mut q = self.dispatch.lock();
                if q.is_empty() {
                    return;
                }
                match q.pop_free_slot() {
                    Some(s) => s,
                    None => return,
                }
            };
            let Ok(permit) = self.run_semaphore.clone().try_acquire_owned() else {
                self.dispatch.lock().release(slot_id);
                return;
            };
            let (entry, reason, score) = {
                let mut q = self.dispatch.lock();
                let cands = q.candidates();
                if cands.is_empty() {
                    q.release(slot_id);
                    drop(permit);
                    return;
                }
                let slot_ctx = q.slot_ctx(slot_id);
                let reserved_slot = q.slot_reserved_bg(slot_id);
                let picked = if active_mode {
                    self.runtime_pool
                        .pick_best(&cands, now, Some(&slot_ctx), reserved_slot)
                } else {
                    // Shadow fallback: FIFO (oldest first), no guardrails.
                    Some((0, "normal", 0.0))
                };
                let Some((idx, reason, score)) = picked else {
                    q.release(slot_id);
                    drop(permit);
                    return;
                };
                let entry = q.take(idx);
                q.assign(slot_id, &entry);
                (entry, reason, score)
            };

            // Dispatch-time ledger row (`kind:"dispatch"`; joins stay 1:1).
            self.runtime_pool.log_dispatch(
                &entry.meta.run_id,
                &entry.meta.tenant,
                &entry.meta.cluster_id,
                if entry.escalated { "forced" } else { "admit" },
                reason,
                score,
                now,
            );

            if entry
                .ticket
                .send(DispatchTicket { permit, slot_id })
                .is_err()
            {
                // Receiver gone (cancelled while queued): free the slot.
                self.dispatch.lock().release(slot_id);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn finalize_run(
        &self,
        run: &mut RunRecord,
        active: &ActiveRunEntry,
        sink: &GuardedRunSink,
        state: RunState,
        exit_code: Option<i32>,
        error_code: Option<&str>,
        error_message: Option<&str>,
    ) {
        run.state = state;
        run.exit_code = exit_code;
        run.error_code = error_code.map(str::to_string);
        run.error_message = error_message.map(str::to_string);
        run.late_events_rejected = sink.rejected_late_count() as u64;
        run.finished_at = Some(Utc::now().to_rfc3339());
        let _ = self.store.update_run(run);

        // P3a shadow ledger: run-outcome actuals + termination attribution.
        let started_unix = run.started_at.as_deref().and_then(|s| {
            chrono::DateTime::parse_from_rfc3339(s)
                .ok()
                .map(|dt| dt.timestamp_millis() as f64 / 1000.0)
        });
        self.runtime_pool.finalize_run(
            &run.run_id,
            started_unix,
            state.as_str(),
            error_code,
            Utc::now().timestamp_millis() as f64 / 1000.0,
        );

        let ev_type = match state {
            RunState::Succeeded => "run.succeeded",
            RunState::Cancelled => "run.cancelled",
            _ => "run.failed",
        };
        self.emit_lifecycle_event(
            active,
            &run.run_id,
            &run.agent_id,
            ev_type,
            serde_json::json!({
                "state": state.as_str(),
                "exit_code": exit_code,
                "error_code": error_code,
                "error_message": error_message,
                "late_events_rejected": run.late_events_rejected
            }),
        );

        self.active_runs.lock().remove(&run.run_id);
        active.done_notify.notify_waiters();
    }
}

pub fn write_control_token_file(state_dir: &Path, token: &str) -> Result<PathBuf> {
    let dir = state_dir.join("control");
    fs::create_dir_all(&dir)?;
    let path = dir.join("control.token");
    fs::write(&path, format!("{token}\n"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(path)
}
