use super::{AdapterLaunchContext, NativeOutcome, PreparedLaunch, RuntimeAdapter};
use crate::auth::generate_csprng_token;
use crate::models::{AdapterCapabilities, CapabilityTier};
use anyhow::{bail, Context, Result};
use runbay_sandbox::SandboxRunOptions;
use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy)]
pub enum CliKind {
    Claude,
    Pi,
}

impl CliKind {
    fn name(self) -> &'static str {
        match self {
            Self::Claude => "claude",
            Self::Pi => "pi",
        }
    }
}

pub struct AnthropicCliAdapter {
    kind: CliKind,
    binary: Option<PathBuf>,
}

fn find_binary(name: &str) -> Option<PathBuf> {
    std::env::split_paths(&std::env::var_os("PATH")?)
        .map(|dir| dir.join(name))
        .find(|p| p.is_file())
        .and_then(|p| fs::canonicalize(p).ok())
}

// npm CLIs use /usr/bin/env node, but the sandbox intentionally clears host PATH.
fn executable_command(binary: &Path) -> Result<Vec<String>> {
    let binary = fs::canonicalize(binary)?;
    let mut prefix = [0u8; 128];
    let len = fs::File::open(&binary)?.read(&mut prefix)?;
    let first_line = String::from_utf8_lossy(&prefix[..len]);
    let mut command = Vec::new();
    if first_line.starts_with("#!") && first_line.lines().next().unwrap_or("").contains("node") {
        let node = find_binary("node")
            .context("ADAPTER_UNAVAILABLE: npm CLI requires node on host PATH")?;
        command.push(node.to_string_lossy().into_owned());
    }
    command.push(binary.to_string_lossy().into_owned());
    Ok(command)
}

impl AnthropicCliAdapter {
    pub fn discover(kind: CliKind, explicit: Option<PathBuf>) -> Self {
        // An explicit invalid override must not silently launch a different installation.
        let binary = match explicit {
            Some(p) => fs::canonicalize(p).ok().filter(|p| p.is_file()),
            None => find_binary(kind.name()),
        };
        Self { kind, binary }
    }
}

impl RuntimeAdapter for AnthropicCliAdapter {
    fn name(&self) -> &'static str {
        self.kind.name()
    }

    fn requires_completion_event(&self) -> bool {
        true
    }

    fn output_outcome(&self, event: &serde_json::Value) -> Option<NativeOutcome> {
        match self.kind {
            CliKind::Claude if event["type"] == "result" => Some(
                if event["is_error"] == false && event["subtype"] == "success" {
                    NativeOutcome::Succeeded
                } else {
                    NativeOutcome::Failed("Claude reported a failed result".into())
                },
            ),
            CliKind::Pi
                if event["type"] == "message_end" && event["message"]["role"] == "assistant" =>
            {
                // Replace prior outcomes, so an automatic retry can recover from an earlier error.
                Some(match event["message"]["stopReason"].as_str() {
                    Some("stop" | "toolUse") => NativeOutcome::Succeeded,
                    Some(reason @ ("error" | "aborted" | "length")) => {
                        NativeOutcome::Failed(format!("Pi reported {reason}"))
                    }
                    _ => NativeOutcome::Failed("Pi returned an unknown completion reason".into()),
                })
            }
            _ => None,
        }
    }

    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities {
            runtime: self.name().into(),
            tier: CapabilityTier::OneShot,
            supports_streaming: true,
            supports_sandbox: true,
            supports_gateway: cfg!(target_os = "macos"),
            supports_native_resume: false,
            supports_hot_process: false,
            available: self.binary.is_some(),
            binary_path: self.binary.as_ref().map(|p| p.to_string_lossy().into_owned()),
            notes: "One-shot JSON CLI through an Anthropic-compatible secretless gateway. Requires allowed_model and macOS; Linux fails closed. Default read-only disables tools; workspace-write enables file tools only. No host login, extensions, or session resume.".into(),
        }
    }

    fn prepare_launch(&self, ctx: &AdapterLaunchContext) -> Result<PreparedLaunch> {
        let binary = self
            .binary
            .as_ref()
            .context("ADAPTER_UNAVAILABLE: requested CLI is not installed")?;
        let write = match ctx.sandbox_mode.as_deref().unwrap_or("read-only") {
            "read-only" => false,
            "workspace-write" => true,
            _ => bail!("INVALID_SANDBOX_MODE: allowed modes are read-only and workspace-write"),
        };
        let model = ctx
            .allowed_model
            .as_deref()
            .filter(|m| !m.trim().is_empty())
            .context("INVALID_REQUEST: claude/pi require an explicit allowed_model")?;
        let (Some(gateway), Some(port), Some(token)) = (
            &ctx.gateway_base_url,
            ctx.gateway_port,
            &ctx.gateway_run_token,
        ) else {
            bail!("AUTH_UNAVAILABLE: configure --upstream-url and RUNBAY_UPSTREAM_API_KEY; host login credentials are never copied");
        };
        if !cfg!(target_os = "macos") {
            bail!(
                "FAIL_CLOSED: claude/pi require TCP loopback; Linux --unshare-net remains enforced"
            );
        }
        let gateway_root = gateway
            .strip_suffix("/v1")
            .context("INVALID_REQUEST: expected gateway /v1 base URL")?;
        let mut command = executable_command(binary)?;
        // Isolate config per invocation: concurrent one-shot runs must not overwrite models.json.
        let runtime_dir = ctx.agent_home_dir.join(generate_csprng_token("cli_"));
        let tmp = runtime_dir.join("tmp");
        fs::create_dir_all(&tmp)?;
        let mut env = BTreeMap::from([
            ("HOME".into(), runtime_dir.to_string_lossy().into_owned()),
            ("TMPDIR".into(), tmp.to_string_lossy().into_owned()),
            ("RUNBAY_RUN_TOKEN".into(), token.clone()),
            ("NO_PROXY".into(), "127.0.0.1,localhost".into()),
            ("no_proxy".into(), "127.0.0.1,localhost".into()),
        ]);
        match self.kind {
            CliKind::Claude => {
                command.extend(
                    [
                        "--bare",
                        "--print",
                        "--verbose",
                        "--output-format",
                        "stream-json",
                        "--include-partial-messages",
                        "--no-session-persistence",
                        "--strict-mcp-config",
                        "--mcp-config",
                        "{\"mcpServers\":{}}",
                        "--permission-mode",
                        if write { "acceptEdits" } else { "dontAsk" },
                        "--tools",
                        if write {
                            "Read,Glob,Grep,Edit,Write"
                        } else {
                            ""
                        },
                        "--model",
                        model,
                        "--",
                        &ctx.prompt,
                    ]
                    .into_iter()
                    .map(str::to_string),
                );
                env.insert("ANTHROPIC_BASE_URL".into(), gateway_root.into());
                env.insert("ANTHROPIC_API_KEY".into(), token.clone());
                env.insert(
                    "CLAUDE_CONFIG_DIR".into(),
                    runtime_dir.to_string_lossy().into_owned(),
                );
                env.insert(
                    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC".into(),
                    "1".into(),
                );
            }
            CliKind::Pi => {
                fs::write(
                    runtime_dir.join("models.json"),
                    serde_json::to_vec_pretty(&serde_json::json!({
                        "providers": {"runbay": {
                            "baseUrl": gateway_root, "api": "anthropic-messages", "apiKey": "$RUNBAY_RUN_TOKEN",
                            "models": [{"id": model, "reasoning": false, "input": ["text"], "contextWindow": 131072, "maxTokens": 1024}]
                        }}
                    }))?,
                )?;
                env.insert(
                    "PI_CODING_AGENT_DIR".into(),
                    runtime_dir.to_string_lossy().into_owned(),
                );
                env.insert("PI_OFFLINE".into(), "1".into());
                env.insert("PI_TELEMETRY".into(), "0".into());
                command.extend(
                    [
                        "--print",
                        "--mode",
                        "json",
                        "--no-session",
                        "--no-extensions",
                        "--no-mcp",
                        "--no-skills",
                        "--no-prompt-templates",
                        "--no-themes",
                        "--no-context-files",
                        "--no-approve",
                        "--offline",
                        "--provider",
                        "runbay",
                        "--model",
                        model,
                        "--thinking",
                        "off",
                    ]
                    .into_iter()
                    .map(str::to_string),
                );
                if write {
                    command.extend(
                        ["--tools", "read,write,edit"]
                            .into_iter()
                            .map(str::to_string),
                    );
                } else {
                    command.push("--no-tools".into());
                }
                // Pi treats standalone @arguments as file attachments even after --.
                command.extend(["--".into(), format!("User task:\n{}", ctx.prompt)]);
            }
        }
        let extra_forbidden_reads = std::env::var_os("HOME")
            .map(PathBuf::from)
            .map(|home| {
                [
                    ".claude",
                    ".claude.json",
                    ".pi",
                    ".codex",
                    ".ssh",
                    ".aws",
                    ".config",
                ]
                .into_iter()
                .map(|p| home.join(p))
                .collect()
            })
            .unwrap_or_default();
        Ok(PreparedLaunch {
            command,
            sandbox_opts: SandboxRunOptions {
                extra_writable_paths: vec![runtime_dir],
                extra_forbidden_reads,
                allow_network: false,
                allowed_loopback_port: Some(port),
                env_overrides: env,
                ..Default::default()
            },
            network_isolated: true,
            secretless_gateway: true,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context(root: &Path) -> AdapterLaunchContext {
        AdapterLaunchContext {
            prompt: "@/private/secret\n--dangerously-skip-permissions".into(),
            workspace_dir: root.join("workspace"),
            agent_home_dir: root.join("agent-home"),
            allowed_model: Some("test-model".into()),
            sandbox_mode: None,
            gateway_base_url: Some("http://127.0.0.1:12345/v1".into()),
            gateway_port: Some(12345),
            gateway_unix_socket: None,
            gateway_run_token: Some("rb_ephemeral_test".into()),
        }
    }

    #[test]
    fn invalid_explicit_binary_does_not_fall_back_to_host_install() {
        let adapter = AnthropicCliAdapter::discover(
            CliKind::Pi,
            Some(PathBuf::from("/nonexistent-runbay-cli")),
        );
        assert!(!adapter.capabilities().available);
    }

    #[test]
    fn missing_gateway_or_model_and_unsafe_modes_fail_closed() {
        let tmp = tempfile::tempdir().unwrap();
        for kind in [CliKind::Claude, CliKind::Pi] {
            let adapter = AnthropicCliAdapter::discover(kind, Some(PathBuf::from("/bin/echo")));
            let mut ctx = context(tmp.path());
            ctx.gateway_run_token = None;
            assert!(adapter
                .prepare_launch(&ctx)
                .unwrap_err()
                .to_string()
                .starts_with("AUTH_UNAVAILABLE"));
            ctx = context(tmp.path());
            ctx.allowed_model = None;
            assert!(adapter
                .prepare_launch(&ctx)
                .unwrap_err()
                .to_string()
                .starts_with("INVALID_REQUEST"));
            ctx = context(tmp.path());
            ctx.sandbox_mode = Some("danger-full-access".into());
            assert!(adapter
                .prepare_launch(&ctx)
                .unwrap_err()
                .to_string()
                .starts_with("INVALID_SANDBOX_MODE"));
            #[cfg(not(target_os = "macos"))]
            assert!(adapter
                .prepare_launch(&context(tmp.path()))
                .unwrap_err()
                .to_string()
                .starts_with("FAIL_CLOSED"));
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn cli_launches_isolate_config_and_disable_implicit_execution() {
        let tmp = tempfile::tempdir().unwrap();
        let ctx = context(tmp.path());
        for kind in [CliKind::Claude, CliKind::Pi] {
            let adapter = AnthropicCliAdapter::discover(kind, Some(PathBuf::from("/bin/echo")));
            let first = adapter.prepare_launch(&ctx).unwrap();
            let second = adapter.prepare_launch(&ctx).unwrap();
            assert_ne!(
                first.sandbox_opts.env_overrides["HOME"],
                second.sandbox_opts.env_overrides["HOME"]
            );
            assert!(!first.sandbox_opts.allow_network);
            assert_eq!(first.sandbox_opts.allowed_loopback_port, Some(12345));
            assert!(!first.command.iter().any(|arg| arg == "rb_ephemeral_test"));
            match kind {
                CliKind::Claude => {
                    assert!(first.command.windows(2).any(|pair| pair == ["--tools", ""]));
                    assert!(first
                        .command
                        .windows(2)
                        .any(|pair| pair == ["--", ctx.prompt.as_str()]));
                    assert!(first.command.contains(&"--bare".into()));
                }
                CliKind::Pi => {
                    assert!(first.command.contains(&"--no-tools".into()));
                    assert!(first.command.last().unwrap().starts_with("User task:\n@"));
                    let config_dir = &first.sandbox_opts.env_overrides["PI_CODING_AGENT_DIR"];
                    let body =
                        fs::read_to_string(Path::new(config_dir).join("models.json")).unwrap();
                    assert!(!body.contains("rb_ephemeral_test"));
                    let config: serde_json::Value = serde_json::from_str(&body).unwrap();
                    assert_eq!(config["providers"]["runbay"]["apiKey"], "$RUNBAY_RUN_TOKEN");
                    assert_eq!(
                        config["providers"]["runbay"]["models"][0]["id"],
                        "test-model"
                    );
                }
            }
            let mut writable = ctx.clone();
            writable.sandbox_mode = Some("workspace-write".into());
            let launch = adapter.prepare_launch(&writable).unwrap();
            assert!(launch
                .command
                .windows(2)
                .any(|pair| pair[0] == "--tools" && pair[1].to_lowercase().contains("write")));
            assert!(!launch
                .command
                .iter()
                .any(|arg| arg.contains("bypassPermissions")
                    || arg == "--dangerously-skip-permissions"));
        }
    }
}
