use crate::models::{AdapterCapabilities, CapabilityTier};
use anyhow::{bail, Result};
use runbay_sandbox::SandboxRunOptions;
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct AdapterLaunchContext {
    pub prompt: String,
    pub workspace_dir: PathBuf,
    pub agent_home_dir: PathBuf,
    pub allowed_model: Option<String>,
    pub sandbox_mode: Option<String>,
    pub gateway_base_url: Option<String>,
    pub gateway_port: Option<u16>,
    pub gateway_unix_socket: Option<PathBuf>,
    pub gateway_run_token: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PreparedLaunch {
    pub command: Vec<String>,
    pub sandbox_opts: SandboxRunOptions,
    pub network_isolated: bool,
    pub secretless_gateway: bool,
}

pub trait RuntimeAdapter: Send + Sync {
    fn name(&self) -> &'static str;
    fn capabilities(&self) -> AdapterCapabilities;
    fn prepare_launch(&self, ctx: &AdapterLaunchContext) -> Result<PreparedLaunch>;
}

/// Real one-shot Codex CLI runtime adapter (`codex exec --json --ephemeral ...`).
/// Honestly advertises `CapabilityTier::OneShot` (no native hot process or duplex resume).
/// Strictly uses Runbay's `SecretlessLoopbackGateway` — never copies host credentials into sandboxes.
pub struct CodexOneShotAdapter {
    binary_path: Option<PathBuf>,
}

impl CodexOneShotAdapter {
    pub fn discover(explicit_bin: Option<PathBuf>) -> Self {
        let binary_path = explicit_bin
            .filter(|p| p.is_file())
            .or_else(find_codex_binary);
        Self { binary_path }
    }
}

fn find_codex_binary() -> Option<PathBuf> {
    if let Ok(path_var) = std::env::var("PATH") {
        for dir in std::env::split_paths(&path_var) {
            let candidate = dir.join("codex");
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    for fallback in [
        "/opt/homebrew/bin/codex",
        "/usr/local/bin/codex",
        "/usr/bin/codex",
    ] {
        let p = PathBuf::from(fallback);
        if p.is_file() {
            return Some(p);
        }
    }
    None
}

impl RuntimeAdapter for CodexOneShotAdapter {
    fn name(&self) -> &'static str {
        "codex"
    }

    fn capabilities(&self) -> AdapterCapabilities {
        let available = self.binary_path.is_some();
        let notes = if !available {
            "codex binary not found on host PATH".to_string()
        } else {
            "One-shot `codex exec --json --ephemeral`. Uses Runbay secretless loopback gateway (--upstream-url + RUNBAY_UPSTREAM_API_KEY) on macOS Seatbelt with host credentials kept in the gateway. On Linux the isolated gateway uses a Unix socket and this version of Codex requires TCP loopback.".to_string()
        };
        AdapterCapabilities {
            runtime: "codex".to_string(),
            tier: CapabilityTier::OneShot,
            supports_streaming: true,
            supports_sandbox: true,
            supports_gateway: cfg!(target_os = "macos"),
            supports_native_resume: false,
            supports_hot_process: false,
            available,
            binary_path: self
                .binary_path
                .as_ref()
                .map(|p| p.to_string_lossy().to_string()),
            notes,
        }
    }

    fn prepare_launch(&self, ctx: &AdapterLaunchContext) -> Result<PreparedLaunch> {
        let Some(ref bin) = self.binary_path else {
            bail!("ADAPTER_UNAVAILABLE: codex binary is not installed on this host");
        };

        let tmp_dir = ctx.agent_home_dir.join("tmp");
        std::fs::create_dir_all(&tmp_dir)?;

        let codex_sandbox_mode = match ctx.sandbox_mode.as_deref().unwrap_or("read-only") {
            "read-only" => "read-only",
            "workspace-write" => "workspace-write",
            other => bail!(
                "INVALID_SANDBOX_MODE: unsupported codex sandbox_mode '{other}'; allowed: read-only, workspace-write"
            ),
        };

        let mut command = vec![
            bin.to_string_lossy().to_string(),
            "exec".to_string(),
            "--json".to_string(),
            "--ephemeral".to_string(),
            "--ignore-user-config".to_string(),
            "--skip-git-repo-check".to_string(),
            "-s".to_string(),
            codex_sandbox_mode.to_string(),
            "-C".to_string(),
            ctx.workspace_dir.to_string_lossy().to_string(),
        ];

        if let Some(ref model) = ctx.allowed_model {
            command.push("-m".to_string());
            command.push(model.clone());
        }

        let mut env_overrides = BTreeMap::new();
        env_overrides.insert(
            "HOME".to_string(),
            ctx.agent_home_dir.to_string_lossy().to_string(),
        );
        env_overrides.insert("TMPDIR".to_string(), tmp_dir.to_string_lossy().to_string());

        let extra_writable_paths = vec![ctx.agent_home_dir.clone()];

        if let (Some(gw_url), Some(gw_port), Some(run_tok)) = (
            &ctx.gateway_base_url,
            ctx.gateway_port,
            &ctx.gateway_run_token,
        ) {
            if !cfg!(target_os = "macos") {
                bail!(
                    "FAIL_CLOSED: codex gateway mode requires TCP loopback access, which the isolated Linux gateway does not expose"
                );
            }
            command.push("-c".to_string());
            command.push("model_provider=\"runbay_gateway\"".to_string());
            command.push("-c".to_string());
            command.push("model_providers.runbay_gateway.name=\"Runbay Gateway\"".to_string());
            command.push("-c".to_string());
            command.push(format!(
                "model_providers.runbay_gateway.base_url=\"{gw_url}\""
            ));
            command.push("-c".to_string());
            command.push("model_providers.runbay_gateway.env_key=\"RUNBAY_RUN_TOKEN\"".to_string());
            command.push("-c".to_string());
            command.push("model_providers.runbay_gateway.wire_api=\"responses\"".to_string());
            command.push("-c".to_string());
            command.push("model_providers.runbay_gateway.supports_websockets=false".to_string());
            command.push("--".to_string());
            command.push(ctx.prompt.clone());

            env_overrides.insert(
                "CODEX_HOME".to_string(),
                ctx.agent_home_dir.to_string_lossy().to_string(),
            );
            env_overrides.insert("RUNBAY_RUN_TOKEN".to_string(), run_tok.clone());
            env_overrides.insert("NO_PROXY".to_string(), "127.0.0.1,localhost".to_string());
            env_overrides.insert("no_proxy".to_string(), "127.0.0.1,localhost".to_string());

            return Ok(PreparedLaunch {
                command,
                sandbox_opts: SandboxRunOptions {
                    extra_writable_paths,
                    allow_network: false,
                    allowed_loopback_port: Some(gw_port),
                    env_overrides,
                    ..Default::default()
                },
                network_isolated: true,
                secretless_gateway: true,
            });
        }

        bail!(
            "AUTH_UNAVAILABLE: codex adapter requires Runbay secretless gateway configuration (--upstream-url + RUNBAY_UPSTREAM_API_KEY); host CLI credentials are never copied into sandboxes"
        )
    }
}

/// Deterministic protocol-fixture adapter (`runtime = "fixture"`), opt-in via
/// `--enable-fixture-adapter`. Invokes `runbayd __internal-fixture-worker` inside the real
/// OS sandbox (macOS Seatbelt or a private Linux network namespace) — cannot execute arbitrary commands.
pub struct FixtureProtocolAdapter {
    self_binary: PathBuf,
}

impl FixtureProtocolAdapter {
    pub fn new() -> Result<Self> {
        let self_binary = std::env::current_exe()?;
        Ok(Self { self_binary })
    }
}

impl RuntimeAdapter for FixtureProtocolAdapter {
    fn name(&self) -> &'static str {
        "fixture"
    }

    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities {
            runtime: "fixture".to_string(),
            tier: CapabilityTier::OneShot,
            supports_streaming: true,
            supports_sandbox: true,
            supports_gateway: true,
            supports_native_resume: false,
            supports_hot_process: false,
            available: true,
            binary_path: Some(self.self_binary.to_string_lossy().to_string()),
            notes: "Deterministic built-in protocol fixture (`runbayd __internal-fixture-worker`) for CI and black-box tests; explicitly opt-in and never executes arbitrary shell commands.".to_string(),
        }
    }

    fn prepare_launch(&self, ctx: &AdapterLaunchContext) -> Result<PreparedLaunch> {
        let mut env_overrides = BTreeMap::new();
        env_overrides.insert("RUNBAY_FIXTURE_DIRECTIVE".to_string(), ctx.prompt.clone());
        env_overrides.insert(
            "HOME".to_string(),
            ctx.agent_home_dir.to_string_lossy().to_string(),
        );

        let extra_writable_paths = vec![ctx.agent_home_dir.clone()];
        let mut allowed_loopback_port = None;
        let mut gateway_unix_socket = None;
        let mut secretless_gateway = false;

        if let Some(ref tok) = ctx.gateway_run_token {
            env_overrides.insert("OPENAI_API_KEY".to_string(), tok.clone());
            env_overrides.insert("RUNBAY_RUN_TOKEN".to_string(), tok.clone());

            if cfg!(target_os = "macos") {
                if let (Some(url), Some(port)) = (&ctx.gateway_base_url, ctx.gateway_port) {
                    env_overrides.insert("OPENAI_BASE_URL".to_string(), url.clone());
                    env_overrides.insert("RUNBAY_GATEWAY_URL".to_string(), url.clone());
                    env_overrides.insert("NO_PROXY".to_string(), "127.0.0.1,localhost".to_string());
                    env_overrides.insert("no_proxy".to_string(), "127.0.0.1,localhost".to_string());
                    allowed_loopback_port = Some(port);
                    secretless_gateway = true;
                }
            } else if let Some(ref sock) = ctx.gateway_unix_socket {
                env_overrides.insert(
                    "RUNBAY_GATEWAY_SOCKET".to_string(),
                    sock.to_string_lossy().to_string(),
                );
                gateway_unix_socket = Some(sock.clone());
                secretless_gateway = true;
            } else if ctx.gateway_port.is_some() {
                bail!(
                    "FAIL_CLOSED: Linux gateway requires a bound gateway_unix_socket under --unshare-net"
                );
            }
        }

        Ok(PreparedLaunch {
            command: vec![
                self.self_binary.to_string_lossy().to_string(),
                "__internal-fixture-worker".to_string(),
            ],
            sandbox_opts: SandboxRunOptions {
                extra_readonly_paths: vec![self.self_binary.clone()],
                extra_writable_paths,
                allow_network: false,
                allowed_loopback_port,
                gateway_unix_socket,
                env_overrides,
                ..Default::default()
            },
            network_isolated: true,
            secretless_gateway,
        })
    }
}

pub struct AdapterRegistry {
    adapters: BTreeMap<String, Box<dyn RuntimeAdapter>>,
}

impl AdapterRegistry {
    pub fn build(enable_fixture: bool, codex_bin: Option<PathBuf>) -> Result<Self> {
        let mut adapters: BTreeMap<String, Box<dyn RuntimeAdapter>> = BTreeMap::new();
        let codex = CodexOneShotAdapter::discover(codex_bin);
        adapters.insert(codex.name().to_string(), Box::new(codex));
        if enable_fixture {
            let fixture = FixtureProtocolAdapter::new()?;
            adapters.insert(fixture.name().to_string(), Box::new(fixture));
        }
        Ok(Self { adapters })
    }

    pub fn get(&self, runtime: &str) -> Option<&dyn RuntimeAdapter> {
        self.adapters.get(runtime).map(|b| b.as_ref())
    }

    pub fn capabilities_map(&self) -> BTreeMap<String, AdapterCapabilities> {
        self.adapters
            .iter()
            .map(|(k, v)| (k.clone(), v.capabilities()))
            .collect()
    }
}
