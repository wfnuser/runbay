use crate::adapters::{AdapterLaunchContext, AdapterRegistry};
use crate::auth::{
    generate_csprng_token, validate_identifier, validate_workspace_dir, MAX_PROMPT_BYTES,
    MAX_TIMEOUT_SECS,
};
use crate::models::{
    AgentRecord, AgentStatusView, CapabilityTier, DaemonStatusResponse, RegisterAgentRequest,
    RunEvent, RunRecord, RunState, SubmitRunRequest,
};
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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{broadcast, Notify, Semaphore};

const MAX_EVENT_LINE_BYTES: usize = 8192;
const BROADCAST_CAPACITY: usize = 256;

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
        store.recover_interrupted_runs(|id| {
            if let Some(parent) = &cfg.cgroup_root {
                clean_interrupted_cgroup(parent, id)?;
            }
            Ok(())
        })?;

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

        let max_conc = cfg.max_concurrent_runs.max(1);
        Ok(Arc::new(Self {
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
        }))
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
        let record = RunRecord {
            run_id: run_id.clone(),
            agent_id: agent.agent_id.clone(),
            runtime: agent.runtime.clone(),
            prompt: req.prompt.clone(),
            state: RunState::Queued,
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

        self.emit_lifecycle_event(
            &active,
            &run_id,
            &agent.agent_id,
            "run.queued",
            serde_json::json!({
                "state": "queued",
                "runtime": agent.runtime,
                "workspace_rev": workspace_rev
            }),
        );

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
    ) {
        let sink = Arc::new(GuardedRunSink::new(active.cancel_token.clone()));

        // 1. Wait for concurrency permit (or abort cleanly if cancelled while queued)
        let permit = tokio::select! {
            biased;
            _ = active.cancel_notify.notified() => {
                queue_guard.release();
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
            acquired = self.run_semaphore.clone().acquire_owned() => {
                queue_guard.release();
                match acquired {
                    Ok(p) => p,
                    Err(_) => {
                        self.finalize_run(
                            &mut run,
                            &active,
                            &sink,
                            RunState::Failed,
                            None,
                            Some("scheduler_closed"),
                            Some("Live run queue semaphore closed"),
                        );
                        return;
                    }
                }
            }
        };

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
