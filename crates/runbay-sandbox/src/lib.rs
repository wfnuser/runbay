mod cgroup;
#[cfg(runbay_native_linux)]
mod native;
#[cfg(target_os = "linux")]
mod writable;
use anyhow::{bail, Context, Result};
use cgroup::RunCgroup;
pub use cgroup::{clean_interrupted_cgroup, validate_cgroup_parent, SandboxCgroup, SandboxLimits};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxExecResult {
    pub command: Vec<String>,
    pub returncode: i32,
    pub stdout: String,
    pub stderr: String,
    pub elapsed_ms: f64,
    pub sandbox_enforced: bool,
    pub kernel_denied: bool,
}

#[derive(Debug, Clone, Default)]
pub struct SandboxRunOptions {
    pub extra_readonly_paths: Vec<PathBuf>,
    pub extra_writable_paths: Vec<PathBuf>,
    pub extra_forbidden_reads: Vec<PathBuf>,
    pub allow_network: bool,
    pub allowed_loopback_port: Option<u16>,
    /// Unix domain socket path bound into the sandbox for secretless gateway access
    /// under `--unshare-net` on Linux (and allowed as a literal outbound socket on macOS).
    pub gateway_unix_socket: Option<PathBuf>,
    pub cgroup: Option<SandboxCgroup>,
    pub env_overrides: BTreeMap<String, String>,
}

/// Async child process running inside an OS sandbox and its own POSIX process group.
pub struct SandboxedChild {
    pub child: tokio::process::Child,
    pgid: Option<i32>,
    _profile_guard: Option<NamedTempFile>,
    cgroup: Option<RunCgroup>,
}

impl SandboxedChild {
    /// Release the cgroup before publishing the terminal run event.
    pub async fn finish_resources(&mut self) -> Result<()> {
        self.finish_resources_with_oom().await.map(|_| ())
    }

    /// Return whether the kernel recorded a memory-cgroup OOM kill before removing the leaf.
    pub async fn finish_resources_with_oom(&mut self) -> Result<bool> {
        match self.cgroup.take() {
            Some(group) => tokio::task::spawn_blocking(move || group.close_with_oom()).await?,
            None => Ok(false),
        }
    }

    pub fn pid(&self) -> Option<u32> {
        self.child.id()
    }

    pub fn pgid(&self) -> Option<i32> {
        self.pgid
    }

    /// Terminate the entire POSIX process group (`SIGTERM` -> grace -> `SIGKILL`) and reap the child.
    pub async fn terminate_process_group(&mut self, grace: Duration) -> Result<ExitStatus> {
        if let Some(status) = self.child.try_wait()? {
            return Ok(status);
        }

        #[cfg(unix)]
        if let Some(pgid) = self.pgid {
            if pgid > 0 {
                unsafe {
                    libc::kill(-pgid, libc::SIGTERM);
                }
            }
        }

        let deadline = Instant::now() + grace;
        while Instant::now() < deadline {
            if let Some(status) = self.child.try_wait()? {
                return Ok(status);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        #[cfg(unix)]
        if let Some(pgid) = self.pgid {
            if pgid > 0 {
                unsafe {
                    libc::kill(-pgid, libc::SIGKILL);
                }
            }
        }
        let _ = self.child.start_kill();
        let status = self.child.wait().await?;
        Ok(status)
    }
}

/// Owns the launcher and the resources that must survive until the worker starts.
struct SandboxCommand<'a> {
    command: Command,
    argv: &'a [String],
    cgroup_spec: Option<&'a SandboxCgroup>,
    profile: Option<NamedTempFile>,
    #[cfg(runbay_native_linux)]
    handshake: Option<native::Handshake>,
}

impl<'a> SandboxCommand<'a> {
    fn new(
        binary: &Path,
        argv: &'a [String],
        workspace: &Path,
        search_path: &str,
        opts: &'a SandboxRunOptions,
    ) -> Self {
        let mut command = Command::new(binary);
        command
            .env_clear()
            .env("PATH", search_path)
            .env("HOME", workspace)
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .envs(&opts.env_overrides)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        Self {
            command,
            argv,
            cgroup_spec: opts.cgroup.as_ref(),
            profile: None,
            #[cfg(runbay_native_linux)]
            handshake: None,
        }
    }

    fn attach_cgroup(&mut self) -> Result<Option<RunCgroup>> {
        self.cgroup_spec
            .map(|spec| RunCgroup::prepare(&mut self.command, spec))
            .transpose()
    }

    fn spawn_async(mut self) -> Result<SandboxedChild> {
        self.command.process_group(0);
        let cgroup = self.attach_cgroup()?;
        #[allow(unused_mut)]
        let mut child = tokio::process::Command::from(self.command)
            .spawn()
            .with_context(|| format!("failed to spawn sandboxed process {:?}", self.argv))?;
        #[cfg(runbay_native_linux)]
        if let Some(channel) = self.handshake {
            if let Err(error) = channel.wait() {
                if let Some(pid) = child.id() {
                    kill_launch_group(pid);
                }
                let _ = child.start_kill();
                // This synchronous entry point runs on the daemon's blocking pool.
                while child
                    .try_wait()
                    .context("reap failed sandbox launch")?
                    .is_none()
                {
                    std::thread::sleep(Duration::from_millis(2));
                }
                return Err(error);
            }
        }
        Ok(SandboxedChild {
            pgid: child.id().map(|id| id as i32),
            child,
            _profile_guard: self.profile,
            cgroup,
        })
    }

    fn run(mut self) -> Result<SandboxExecResult> {
        #[cfg(runbay_native_linux)]
        if self.handshake.is_some() {
            self.command.process_group(0);
        }
        let start = Instant::now();
        let cgroup = self.attach_cgroup()?;
        #[allow(unused_mut)]
        let mut child = self
            .command
            .spawn()
            .with_context(|| format!("failed to spawn sandboxed process {:?}", self.argv))?;
        #[cfg(runbay_native_linux)]
        if let Some(channel) = self.handshake {
            if let Err(error) = channel.wait() {
                kill_launch_group(child.id());
                let _ = child.kill();
                child.wait().context("reap failed sandbox launch")?;
                return Err(error);
            }
        }
        let output = child.wait_with_output()?;
        if let Some(group) = cgroup {
            group.close()?;
        }
        let elapsed_ms = (start.elapsed().as_secs_f64() * 1_000_000.0).round() / 1000.0;
        let returncode = output.status.code().unwrap_or(-1);
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        Ok(SandboxExecResult {
            command: self.argv.to_vec(),
            returncode,
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            kernel_denied: detect_kernel_denial(returncode, &stderr),
            stderr,
            elapsed_ms,
            sandbox_enforced: true,
        })
    }
}

#[cfg(runbay_native_linux)]
fn kill_launch_group(pid: u32) {
    // The group leader has not been reaped, so its PID cannot be reused here.
    unsafe {
        libc::kill(-(pid as i32), libc::SIGKILL);
    }
}

fn canonical_or_original(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_owned())
}

fn sb_escape(path: &Path) -> String {
    canonical_or_original(path)
        .to_string_lossy()
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
}

fn detect_kernel_denial(returncode: i32, stderr: &str) -> bool {
    returncode != 0
        && (stderr.contains("Operation not permitted")
            || stderr.contains("Read-only file system")
            || stderr.contains("Permission denied")
            || stderr.contains("No such file or directory")
            || stderr.contains("Directory nonexistent")
            || stderr.contains("PermissionError")
            || stderr.contains("EPERM")
            || stderr.contains("EROFS")
            || stderr.contains("Errno 1")
            || stderr.contains("Errno 30"))
}

/// Generates per-run macOS Seatbelt (`.sb`) profiles and executes real OS child processes.
///
/// Write policy: **default-deny globally** (`(deny file-write*)`), allowing writes ONLY inside
/// `allowed_workspace` and `/dev/null`.
/// Read policy: explicitly denies `pool_root` (sibling workspaces) and `forbidden_read_paths`,
/// while allowing system libraries/binaries, `extra_readonly_paths` (L0), and `allowed_workspace`.
pub struct MacOSSeatbeltSandbox {
    pool_root: PathBuf,
    forbidden_read_paths: Vec<PathBuf>,
    sandbox_exec_bin: Option<PathBuf>,
}

impl MacOSSeatbeltSandbox {
    pub fn new(pool_root: impl AsRef<Path>, forbidden_read_paths: &[PathBuf]) -> Self {
        let pool_root = canonical_or_original(pool_root.as_ref());
        let forbidden = forbidden_read_paths
            .iter()
            .map(|p| canonical_or_original(p))
            .collect();
        let bin = if cfg!(target_os = "macos") && Path::new("/usr/bin/sandbox-exec").exists() {
            Some(PathBuf::from("/usr/bin/sandbox-exec"))
        } else {
            None
        };
        Self {
            pool_root,
            forbidden_read_paths: forbidden,
            sandbox_exec_bin: bin,
        }
    }

    pub fn is_available(&self) -> bool {
        self.sandbox_exec_bin.is_some()
    }

    pub fn build_profile(&self, allowed_workspace: &Path, opts: &SandboxRunOptions) -> String {
        let ws = sb_escape(allowed_workspace);
        let pool = sb_escape(&self.pool_root);

        let mut lines = vec![
            "(version 1)".to_string(),
            "(allow default)".to_string(),
            // Default-deny ALL host filesystem writes globally!
            "(deny file-write*)".to_string(),
            // Deny reads across pool root (isolates sibling worker runs)
            format!("(deny file-read* (subpath \"{pool}\"))"),
        ];

        for fpath in self
            .forbidden_read_paths
            .iter()
            .chain(opts.extra_forbidden_reads.iter())
        {
            let esc = sb_escape(fpath);
            lines.push(format!("(deny file-read* (subpath \"{esc}\"))"));
        }

        // Allow metadata-only stat on exact ancestor directories leading from `pool_root`
        // to this run's own allowed workspace / writable dirs so `realpath(3)` / `fs::canonicalize`
        // succeeds without exposing directory listing (`file-read-data`) or sibling slots.
        let mut metadata_ancestors: Vec<PathBuf> = Vec::new();
        for target in std::iter::once(allowed_workspace)
            .chain(opts.extra_writable_paths.iter().map(PathBuf::as_path))
        {
            let target_real = canonical_or_original(target);
            if target_real.starts_with(&self.pool_root) {
                let mut cur = target_real.parent();
                while let Some(anc) = cur {
                    if !anc.starts_with(&self.pool_root) {
                        break;
                    }
                    if !metadata_ancestors.iter().any(|p| p == anc) {
                        metadata_ancestors.push(anc.to_path_buf());
                    }
                    if anc == self.pool_root {
                        break;
                    }
                    cur = anc.parent();
                }
            }
        }
        for anc in &metadata_ancestors {
            let esc = sb_escape(anc);
            lines.push(format!("(allow file-read-metadata (literal \"{esc}\"))"));
        }

        // Allow shared read-only L0 layers
        for ro in &opts.extra_readonly_paths {
            let esc = sb_escape(ro);
            lines.push(format!("(allow file-read* (subpath \"{esc}\"))"));
        }

        // Allow explicitly scoped per-agent writable paths (e.g., isolated agent home)
        for rw in &opts.extra_writable_paths {
            let esc = sb_escape(rw);
            lines.push(format!("(allow file-read* (subpath \"{esc}\"))"));
            lines.push(format!("(allow file-write* (subpath \"{esc}\"))"));
        }

        // Allow read + write strictly inside this run's isolated workspace (+ /dev/null)
        lines.push(format!("(allow file-read* (subpath \"{ws}\"))"));
        lines.push(format!("(allow file-write* (subpath \"{ws}\"))"));
        lines.push("(allow file-write* (literal \"/dev/null\"))".to_string());

        if !opts.allow_network {
            lines.push("(deny network*)".to_string());
            if let Some(port) = opts.allowed_loopback_port {
                lines.push(format!(
                    "(allow network-outbound (remote ip \"localhost:{port}\"))"
                ));
            }
            if let Some(ref sock) = opts.gateway_unix_socket {
                let esc = sb_escape(sock);
                lines.push(format!("(allow network-outbound (literal \"{esc}\"))"));
                lines.push(format!("(allow file-read* (literal \"{esc}\"))"));
                lines.push(format!("(allow file-write* (literal \"{esc}\"))"));
            }
        }

        lines.join("\n") + "\n"
    }

    fn prepare<'a>(
        &self,
        cmd: &'a [String],
        workspace: &Path,
        opts: &'a SandboxRunOptions,
        search_path: &str,
    ) -> Result<SandboxCommand<'a>> {
        let binary = self
            .sandbox_exec_bin
            .as_ref()
            .context("macOS sandbox-exec is unavailable on this host")?;
        if cmd.is_empty() {
            bail!("sandbox command cannot be empty");
        }
        if opts.cgroup.is_some() {
            bail!("sandbox cgroup limits require Linux");
        }
        let mut profile = tempfile::Builder::new().suffix(".sb").tempfile()?;
        profile.write_all(self.build_profile(workspace, opts).as_bytes())?;
        let workspace = canonical_or_original(workspace);
        let mut launch = SandboxCommand::new(binary, cmd, &workspace, search_path, opts);
        launch
            .command
            .arg("-f")
            .arg(profile.path())
            .args(cmd)
            .current_dir(workspace);
        launch.profile = Some(profile);
        Ok(launch)
    }

    pub fn spawn_async(
        &self,
        cmd: &[String],
        allowed_workspace: &Path,
        opts: &SandboxRunOptions,
    ) -> Result<SandboxedChild> {
        self.prepare(
            cmd,
            allowed_workspace,
            opts,
            "/usr/bin:/bin:/opt/homebrew/bin:/usr/local/bin",
        )?
        .spawn_async()
    }

    pub fn run(
        &self,
        cmd: &[String],
        allowed_workspace: &Path,
        opts: &SandboxRunOptions,
    ) -> Result<SandboxExecResult> {
        self.prepare(
            cmd,
            allowed_workspace,
            opts,
            "/usr/bin:/bin:/opt/homebrew/bin",
        )?
        .run()
    }
}

/// Linux namespace sandbox with bubblewrap and native Landlock + seccomp launchers.
///
/// Enforces:
/// - Global read-only root (`--ro-bind / /`): zero writes anywhere on host outside `allowed_workspace`;
/// - Sibling pool & forbidden directory masking (`--tmpfs <dir>`);
/// - Shared L0 read-only bind (`--ro-bind <l0> <l0>`);
/// - Isolated workspace read-write bind (`--bind <ws> <ws>`);
/// - Network namespace isolation (`--unshare-net`) when network is disabled.
pub struct LinuxSandbox {
    pool_root: PathBuf,
    forbidden_read_paths: Vec<PathBuf>,
    binary: Option<PathBuf>,
    backend: LinuxBackend,
}

#[derive(Clone, Copy, PartialEq)]
enum LinuxBackend {
    Bubblewrap,
    Landlock,
    Invalid,
}

/// Retained for callers which explicitly request bubblewrap.
pub type LinuxBubblewrapSandbox = LinuxSandbox;

impl LinuxSandbox {
    pub fn new(pool_root: impl AsRef<Path>, forbidden_read_paths: &[PathBuf]) -> Self {
        let pool_root = canonical_or_original(pool_root.as_ref());
        let forbidden = forbidden_read_paths
            .iter()
            .map(|p| canonical_or_original(p))
            .collect();
        let bin = if cfg!(target_os = "linux") {
            ["/usr/bin/bwrap", "/bin/bwrap", "/usr/local/bin/bwrap"]
                .iter()
                .map(PathBuf::from)
                .find(|p| p.exists())
        } else {
            None
        };
        Self {
            pool_root,
            forbidden_read_paths: forbidden,
            binary: bin,
            backend: LinuxBackend::Bubblewrap,
        }
    }

    pub fn is_available(&self) -> bool {
        self.check_available().is_ok()
    }

    fn check_available(&self) -> Result<()> {
        let binary = self
            .binary
            .as_deref()
            .context("requested Linux sandbox backend unavailable")?;
        #[cfg(runbay_native_linux)]
        if self.backend == LinuxBackend::Landlock {
            return native::check_available(binary);
        }
        anyhow::ensure!(
            Self::bwrap_works(binary),
            "bubblewrap isolation probe failed"
        );
        Ok(())
    }

    fn bwrap_works(binary: &Path) -> bool {
        static AVAILABLE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *AVAILABLE.get_or_init(|| {
            Command::new(binary)
                .env_clear()
                .args([
                    "--unshare-net",
                    "--unshare-pid",
                    "--ro-bind",
                    "/",
                    "/",
                    "--",
                    "/bin/true",
                ])
                .output()
                .is_ok_and(|output| output.status.success())
        })
    }

    fn selected(pool: impl AsRef<Path>, forbidden: &[PathBuf]) -> Self {
        let mut sb = Self::new(pool, forbidden);
        let preference = std::env::var("RUNBAY_SANDBOX_BACKEND").unwrap_or_else(|_| "auto".into());
        match preference.as_str() {
            "bwrap" => return sb,
            "auto" if sb.is_available() => return sb,
            "landlock" | "auto" => {
                sb.backend = LinuxBackend::Landlock;
                sb.binary = None;
                #[cfg(runbay_native_linux)]
                {
                    sb.binary = native::launcher();
                }
            }
            _ => {
                sb.backend = LinuxBackend::Invalid;
                sb.binary = None;
            }
        }
        sb
    }

    fn prepare<'a>(
        &self,
        cmd: &'a [String],
        ws: &Path,
        opts: &'a SandboxRunOptions,
    ) -> Result<SandboxCommand<'a>> {
        let binary = self
            .binary
            .as_ref()
            .context("requested Linux sandbox backend unavailable")?;
        if cmd.is_empty() {
            bail!("sandbox command cannot be empty");
        }
        let workspace = fs::canonicalize(ws).context("resolve isolated workspace")?;
        let ws = workspace.as_path();
        #[cfg(target_os = "linux")]
        writable::check_links(ws, &opts.extra_writable_paths)?;

        let mut launch = SandboxCommand::new(binary, cmd, ws, "/usr/local/bin:/usr/bin:/bin", opts);
        #[cfg(runbay_native_linux)]
        if self.backend == LinuxBackend::Landlock {
            native::check_available(binary)?;
            launch.handshake = Some(native::Handshake::install(&mut launch.command)?);
            launch.command.args(native::arguments(
                cmd,
                ws,
                &self.pool_root,
                &self.forbidden_read_paths,
                opts,
            )?);
            return Ok(launch);
        }
        launch.command.args(self.build_bwrap_args(cmd, ws, opts)?);
        Ok(launch)
    }

    /// Build the exact `bwrap` argument list (testable on all platforms!).
    ///
    /// Fails closed if `!allow_network` is combined with `allowed_loopback_port` without
    /// a `gateway_unix_socket`: `bwrap` cannot allow a single host TCP loopback port without
    /// disabling `--unshare-net`, which would silently expose host networking.
    pub fn build_bwrap_args(
        &self,
        cmd: &[String],
        allowed_workspace: &Path,
        opts: &SandboxRunOptions,
    ) -> Result<Vec<String>> {
        if !opts.allow_network
            && opts.allowed_loopback_port.is_some()
            && opts.gateway_unix_socket.is_none()
        {
            bail!(
                "FAIL_CLOSED: Linux bwrap cannot restrict network access to a single host TCP \
                 loopback port without disabling --unshare-net; bind a gateway_unix_socket \
                 (under --unshare-net) or explicitly set allow_network=true"
            );
        }

        let path_arg = |path: &Path| canonical_or_original(path).to_string_lossy().into_owned();
        let ws = path_arg(allowed_workspace);
        let pool_s = self.pool_root.to_string_lossy().to_string();

        let mut args: Vec<String> = vec![
            "--die-with-parent".into(),
            "--unshare-ipc".into(),
            "--unshare-pid".into(),
            "--unshare-uts".into(),
            // Mount entire host filesystem read-only by default (zero arbitrary host writes!)
            "--ro-bind".into(),
            "/".into(),
            "/".into(),
            "--dev".into(),
            "/dev".into(),
            "--proc".into(),
            "/proc".into(),
            // Mask pool_root with an empty tmpfs so sibling worker slots are invisible
            "--tmpfs".into(),
            pool_s,
        ];

        // Mask explicit forbidden secret paths with empty tmpfs
        for fpath in self
            .forbidden_read_paths
            .iter()
            .chain(opts.extra_forbidden_reads.iter())
        {
            args.extend(["--tmpfs".into(), path_arg(fpath)]);
        }

        // Restore the read-only layers first, then the writable paths, single socket and workspace.
        let mut bind =
            |flag: &str, path: String| args.extend([flag.to_owned(), path.clone(), path]);
        for path in &opts.extra_readonly_paths {
            bind("--ro-bind", path_arg(path));
        }
        for path in opts
            .extra_writable_paths
            .iter()
            .chain(&opts.gateway_unix_socket)
        {
            bind("--bind", path_arg(path));
        }
        bind("--bind", ws.clone());
        args.extend(["--chdir".into(), ws]);

        if !opts.allow_network {
            args.push("--unshare-net".into());
        }

        args.push("--".into());
        args.extend(cmd.iter().cloned());
        Ok(args)
    }

    pub fn spawn_async(
        &self,
        cmd: &[String],
        allowed_workspace: &Path,
        opts: &SandboxRunOptions,
    ) -> Result<SandboxedChild> {
        self.prepare(cmd, allowed_workspace, opts)?.spawn_async()
    }

    pub fn run(
        &self,
        cmd: &[String],
        allowed_workspace: &Path,
        opts: &SandboxRunOptions,
    ) -> Result<SandboxExecResult> {
        self.prepare(cmd, allowed_workspace, opts)?.run()
    }
}

/// Cross-platform OS sandbox facade selecting Seatbelt, bubblewrap, or Landlock + seccomp.
pub enum LocalOsSandbox {
    MacOS(MacOSSeatbeltSandbox),
    Linux(LinuxBubblewrapSandbox),
}

impl LocalOsSandbox {
    pub fn new(pool_root: impl AsRef<Path>, forbidden_read_paths: &[PathBuf]) -> Self {
        if cfg!(target_os = "macos") {
            Self::MacOS(MacOSSeatbeltSandbox::new(pool_root, forbidden_read_paths))
        } else {
            Self::Linux(LinuxSandbox::selected(pool_root, forbidden_read_paths))
        }
    }

    pub fn is_available(&self) -> bool {
        self.check_available().is_ok()
    }

    pub fn check_available(&self) -> Result<()> {
        match self {
            Self::MacOS(s) => {
                anyhow::ensure!(s.is_available(), "macOS Seatbelt launcher unavailable");
                Ok(())
            }
            Self::Linux(s) => s.check_available(),
        }
    }

    pub fn gateway_transport(&self) -> &'static str {
        match self {
            Self::MacOS(_) => "macos_seatbelt_loopback_tcp",
            Self::Linux(s) if s.backend == LinuxBackend::Landlock => {
                "linux_landlock_seccomp_unix_socket"
            }
            Self::Linux(s) if s.backend == LinuxBackend::Bubblewrap => {
                "linux_bwrap_unshare_net_unix_socket"
            }
            Self::Linux(_) => "unavailable",
        }
    }

    pub fn mechanism_name(&self) -> &'static str {
        match self {
            Self::MacOS(_) => "macOS Seatbelt (sandbox-exec) global deny file-write* + pool/secret read-deny + workspace allowlist",
            Self::Linux(s) if s.backend == LinuxBackend::Landlock => "Linux Landlock + seccomp-BPF, private mount/PID/network namespaces",
            Self::Linux(s) if s.backend == LinuxBackend::Invalid => "unknown RUNBAY_SANDBOX_BACKEND",
            Self::Linux(_) => "Linux bubblewrap (bwrap) global --ro-bind / + tmpfs pool/secret masking + workspace --bind + --unshare-net",
        }
    }

    pub fn run(
        &self,
        cmd: &[String],
        allowed_workspace: &Path,
        opts: &SandboxRunOptions,
    ) -> Result<SandboxExecResult> {
        match self {
            Self::MacOS(s) => s.run(cmd, allowed_workspace, opts),
            Self::Linux(s) => s.run(cmd, allowed_workspace, opts),
        }
    }

    /// Returns a Tokio child after synchronous isolation setup. Call from a blocking task
    /// when directory walks, the launcher handshake or cgroup cleanup may take time.
    pub fn spawn_async(
        &self,
        cmd: &[String],
        allowed_workspace: &Path,
        opts: &SandboxRunOptions,
    ) -> Result<SandboxedChild> {
        match self {
            Self::MacOS(s) => s.spawn_async(cmd, allowed_workspace, opts),
            Self::Linux(s) => s.spawn_async(cmd, allowed_workspace, opts),
        }
    }
}

/// Tracks worker slots whose cleanup failed so poisoned environments are never reused.
#[derive(Debug, Default)]
pub struct QuarantineRegistry {
    quarantined: RwLock<BTreeMap<String, String>>,
}

impl QuarantineRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn mark_poisoned(&self, worker_id: &str, reason: &str) {
        self.quarantined
            .write()
            .insert(worker_id.to_string(), reason.to_string());
    }

    pub fn is_reusable(&self, worker_id: &str) -> bool {
        !self.quarantined.read().contains_key(worker_id)
    }

    pub fn snapshot(&self) -> BTreeMap<String, String> {
        self.quarantined.read().clone()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxCheckOutcome {
    pub passed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stderr: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxOverheadReport {
    pub bare_exec_mean_ms: f64,
    pub sandbox_exec_mean_ms: f64,
    pub added_sandbox_overhead_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxVerificationReport {
    pub measurement_type: String,
    pub platform: String,
    pub sandbox_mechanism: String,
    pub all_invariants_passed: bool,
    pub checks: BTreeMap<String, SandboxCheckOutcome>,
    pub overhead_benchmark: SandboxOverheadReport,
}

/// Execute real OS child processes under the selected host sandbox
/// to prove all 7 kernel isolation invariants, including arbitrary outside-workspace host writes.
pub fn run_os_sandbox_verification_suite(base_dir: &Path) -> Result<SandboxVerificationReport> {
    if base_dir.exists() {
        fs::remove_dir_all(base_dir)?;
    }
    fs::create_dir_all(base_dir)?;
    let base_real = fs::canonicalize(base_dir)?;

    let pool_root = base_real.join("pool");
    let secret_dir = base_real.join("host_secrets");
    let unrelated_host_dir = base_real.join("unrelated_host_writable");
    let l0_ro = pool_root.join("l0_shared_ro");
    let run1_ws = pool_root.join("worker_0_run1");
    let run2_ws = pool_root.join("worker_0_run2");
    for d in [&secret_dir, &unrelated_host_dir, &l0_ro, &run1_ws, &run2_ws] {
        fs::create_dir_all(d)?;
    }

    fs::write(secret_dir.join("host_canary.txt"), "HOST_SECRET_CANARY_999")?;
    fs::write(l0_ro.join("shared_lib.txt"), "SHARED_READONLY_L0_DATA")?;
    fs::write(
        run1_ws.join("private_canary.txt"),
        "RUN1_PRIVATE_CANARY_ABC",
    )?;
    fs::write(run2_ws.join("task.txt"), "RUN2_ALLOWED_TASK_INPUT")?;

    let escape_link = run2_ws.join("symlink_to_secret.txt");
    #[cfg(unix)]
    std::os::unix::fs::symlink(secret_dir.join("host_canary.txt"), &escape_link)?;

    let sb = LocalOsSandbox::new(&pool_root, std::slice::from_ref(&secret_dir));
    if !sb.is_available() {
        bail!("No supported OS sandbox binary available on this platform");
    }
    let ro_opts = SandboxRunOptions {
        extra_readonly_paths: vec![l0_ro.clone()],
        ..Default::default()
    };

    let read_file = |path: &Path| {
        sb.run(
            &["/bin/cat".into(), path.to_string_lossy().into_owned()],
            &run2_ws,
            &ro_opts,
        )
    };
    let run_shell = |script: &str, files: &[&Path]| {
        let mut cmd = vec![
            "/bin/sh".into(),
            "-c".into(),
            script.into(),
            "runbay-check".into(),
        ];
        cmd.extend(files.iter().map(|path| path.to_string_lossy().into_owned()));
        sb.run(&cmd, &run2_ws, &ro_opts)
    };

    let task_path = run2_ws.join("task.txt");
    let tamper_target = l0_ro.join("tamper.txt");
    let outside_target = unrelated_host_dir.join("arbitrary_escape_write.txt");
    let r_allow = run_shell(
        r#"cat "$1" && cat "$2" && echo ok > "$3""#,
        &[
            &task_path,
            &l0_ro.join("shared_lib.txt"),
            &run2_ws.join("out.txt"),
        ],
    )?;
    let r_host_secret = read_file(&secret_dir.join("host_canary.txt"))?;
    let r_prev_canary = read_file(&run1_ws.join("private_canary.txt"))?;
    let r_symlink = read_file(&escape_link)?;
    let r_l0_write = run_shell(r#"echo pwned > "$1""#, &[&tamper_target])?;
    let r_arbitrary_write = run_shell(r#"echo escaped > "$1""#, &[&outside_target])?;
    let r_net = run_shell("curl -sS --max-time 2 https://1.1.1.1", &[])?;

    // Bare vs sandboxed subprocess overhead (10 trials)
    let mut bare_ms = Vec::with_capacity(10);
    let mut sb_ms = Vec::with_capacity(10);
    for _ in 0..10 {
        let t0 = Instant::now();
        let _ = Command::new("/bin/cat").arg(&task_path).output()?;
        bare_ms.push(t0.elapsed().as_secs_f64() * 1000.0);

        let res = read_file(&task_path)?;
        sb_ms.push(res.elapsed_ms);
    }

    let bare_mean = ((bare_ms.iter().sum::<f64>() / bare_ms.len() as f64) * 100.0).round() / 100.0;
    let sb_mean = ((sb_ms.iter().sum::<f64>() / sb_ms.len() as f64) * 100.0).round() / 100.0;
    let added = ((sb_mean - bare_mean) * 100.0).round() / 100.0;

    let base_prefix = base_real.to_string_lossy().to_string();
    let redact = |s: &str| s.trim().replace(&base_prefix, "<SANDBOX_TMP>");

    let denied = |result: &SandboxExecResult, canary_protected: bool| SandboxCheckOutcome {
        passed: result.kernel_denied && canary_protected,
        elapsed_ms: None,
        stderr: Some(redact(&result.stderr)),
    };

    let mut checks = BTreeMap::new();
    checks.insert(
        "allowed_workspace_and_l0_ro".into(),
        SandboxCheckOutcome {
            passed: r_allow.returncode == 0
                && r_allow.stdout.contains("RUN2_ALLOWED_TASK_INPUT")
                && r_allow.stdout.contains("SHARED_READONLY_L0_DATA"),
            elapsed_ms: Some(r_allow.elapsed_ms),
            stderr: None,
        },
    );
    checks.insert(
        "block_host_secret_read".into(),
        denied(
            &r_host_secret,
            !r_host_secret.stdout.contains("HOST_SECRET_CANARY_999"),
        ),
    );
    checks.insert(
        "block_prev_run_private_canary".into(),
        denied(
            &r_prev_canary,
            !r_prev_canary.stdout.contains("RUN1_PRIVATE_CANARY_ABC"),
        ),
    );
    checks.insert(
        "block_symlink_escape".into(),
        denied(
            &r_symlink,
            !r_symlink.stdout.contains("HOST_SECRET_CANARY_999"),
        ),
    );
    checks.insert(
        "block_l0_tamper_write".into(),
        denied(&r_l0_write, !tamper_target.exists()),
    );
    checks.insert(
        "block_arbitrary_outside_host_write".into(),
        denied(&r_arbitrary_write, !outside_target.exists()),
    );
    checks.insert(
        "block_outbound_network".into(),
        SandboxCheckOutcome {
            passed: r_net.returncode != 0,
            elapsed_ms: None,
            stderr: Some(redact(&r_net.stderr).chars().take(160).collect()),
        },
    );

    Ok(SandboxVerificationReport {
        measurement_type: "real_os_kernel_exec".into(),
        platform: format!("{} ({})", std::env::consts::OS, std::env::consts::ARCH),
        sandbox_mechanism: sb.mechanism_name().into(),
        all_invariants_passed: checks.values().all(|check| check.passed),
        checks,
        overhead_benchmark: SandboxOverheadReport {
            bare_exec_mean_ms: bare_mean,
            sandbox_exec_mean_ms: sb_mean,
            added_sandbox_overhead_ms: added,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_quarantine_registry() {
        let reg = QuarantineRegistry::new();
        assert!(reg.is_reusable("worker-0"));
        reg.mark_poisoned("worker-0", "cleanup_eperm");
        assert!(!reg.is_reusable("worker-0"));
        assert_eq!(
            reg.snapshot().get("worker-0").map(|s| s.as_str()),
            Some("cleanup_eperm")
        );
    }

    #[test]
    fn test_macos_profile_uses_global_write_deny() {
        let tmp = TempDir::new().unwrap();
        let sb = MacOSSeatbeltSandbox::new(tmp.path().join("pool"), &[tmp.path().join("sec")]);
        let profile = sb.build_profile(&tmp.path().join("pool/ws"), &SandboxRunOptions::default());
        assert!(profile.contains("(deny file-write*)"));
        assert!(profile.contains("(allow file-write* (literal \"/dev/null\"))"));
    }

    #[test]
    fn test_linux_bwrap_args_enforce_ro_root_and_net_isolation() {
        let tmp = TempDir::new().unwrap();
        let pool = tmp.path().join("pool");
        let ws = pool.join("ws");
        let sec = tmp.path().join("sec");
        let sock = tmp.path().join("gateway.sock");
        fs::create_dir_all(&ws).unwrap();
        fs::create_dir_all(&sec).unwrap();
        fs::write(&sock, b"").unwrap();

        let sb = LinuxBubblewrapSandbox::new(&pool, &[sec]);
        let args = sb
            .build_bwrap_args(
                &["/bin/true".into()],
                &ws,
                &SandboxRunOptions {
                    allow_network: false,
                    allowed_loopback_port: None,
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(args.windows(3).any(|w| w == ["--ro-bind", "/", "/"]));
        assert!(args.contains(&"--unshare-net".to_string()));
        assert!(args.contains(&"--tmpfs".to_string()));

        // FAIL CLOSED: allow_network=false + TCP loopback port without unix socket MUST error
        let err = sb
            .build_bwrap_args(
                &["/bin/true".into()],
                &ws,
                &SandboxRunOptions {
                    allow_network: false,
                    allowed_loopback_port: Some(7310),
                    gateway_unix_socket: None,
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert!(
            err.to_string().contains("FAIL_CLOSED"),
            "unexpected error: {err}"
        );

        // Safe Linux gateway mode: Unix socket bound WHILE keeping --unshare-net enabled
        let sock_args = sb
            .build_bwrap_args(
                &["/bin/true".into()],
                &ws,
                &SandboxRunOptions {
                    allow_network: false,
                    allowed_loopback_port: None,
                    gateway_unix_socket: Some(sock.clone()),
                    ..Default::default()
                },
            )
            .unwrap();
        let sock_real = fs::canonicalize(&sock)
            .unwrap()
            .to_string_lossy()
            .to_string();
        assert!(sock_args.contains(&"--unshare-net".to_string()));
        assert!(
            sock_args
                .windows(3)
                .any(|w| w[0] == "--bind" && w[1] == sock_real && w[2] == sock_real),
            "expected gateway unix socket bind mount in args: {sock_args:?}"
        );
    }

    #[tokio::test]
    async fn test_worker_environment_and_output_on_both_entry_points() {
        let pool = TempDir::new().unwrap();
        let real = pool.path().join("workspace");
        fs::create_dir(&real).unwrap();
        let ws = pool.path().join("workspace-link");
        std::os::unix::fs::symlink(&real, &ws).unwrap();
        let sandbox = LocalOsSandbox::new(pool.path(), &[]);
        if !sandbox.is_available() {
            return;
        }
        let opts = SandboxRunOptions {
            env_overrides: [("MARKER".into(), "worker".into())].into(),
            ..Default::default()
        };
        let cmd = [
            "/bin/sh".into(),
            "-c".into(),
            r#"test "$PWD" = "$HOME" && printf %s "$PATH" && printf %s "$MARKER" >&2"#.into(),
        ];
        let direct = sandbox.run(&cmd, &ws, &opts).unwrap();
        let child = sandbox.spawn_async(&cmd, &ws, &opts).unwrap();
        let output = child.child.wait_with_output().await.unwrap();
        let (direct_path, async_path) = if cfg!(target_os = "macos") {
            (
                "/usr/bin:/bin:/opt/homebrew/bin",
                "/usr/bin:/bin:/opt/homebrew/bin:/usr/local/bin",
            )
        } else {
            let linux_path = "/usr/local/bin:/usr/bin:/bin";
            (linux_path, linux_path)
        };
        assert_eq!(direct.returncode, 0, "{}", direct.stderr);
        assert_eq!(direct.stdout, direct_path);
        assert_eq!(direct.stderr, "worker");
        assert!(output.status.success());
        assert_eq!(output.stdout, async_path.as_bytes());
        assert_eq!(output.stderr, b"worker");
    }

    #[cfg(runbay_native_linux)]
    #[tokio::test]
    async fn failed_handshake_kills_the_process_group_and_reaps_its_leader() {
        let tmp = tempfile::tempdir().unwrap();
        let marker = tmp.path().join("pids");
        let worker_marker = marker.clone();
        tokio::task::spawn_blocking(move || {
            let opts = SandboxRunOptions::default();
            let mut launch = SandboxCommand::new(
                Path::new("/usr/bin/python3"),
                &[],
                tmp.path(),
                "/usr/bin:/bin",
                &opts,
            );
            launch
                .command
                .args([
                    "-c",
                    r#"
import os, subprocess, sys, time
child = subprocess.Popen(["/bin/sleep", "5"])
with open(sys.argv[1], "w") as marker:
    print(os.getpid(), child.pid, file=marker)
fd = int(sys.argv[-1])
os.write(fd, b"setup failed")
os.close(fd)
time.sleep(5)
"#,
                ])
                .arg(worker_marker);
            launch.handshake = Some(native::Handshake::install(&mut launch.command).unwrap());
            assert!(launch
                .spawn_async()
                .err()
                .unwrap()
                .to_string()
                .contains("setup failed"));
            let pids = fs::read_to_string(&marker).unwrap();
            let mut pids = pids.split_whitespace().map(|p| p.parse::<i32>().unwrap());
            let leader = pids.next().unwrap();
            let descendant = pids.next().unwrap();
            assert_eq!(
                unsafe { libc::waitpid(leader, std::ptr::null_mut(), libc::WNOHANG) },
                -1
            );
            assert_eq!(
                std::io::Error::last_os_error().raw_os_error(),
                Some(libc::ECHILD)
            );
            let deadline = Instant::now() + Duration::from_secs(1);
            while let Ok(stat) = fs::read_to_string(format!("/proc/{descendant}/stat")) {
                if stat.contains(") Z ") {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "descendant survived failed launch"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_async_process_group_termination_reaps_grandchild() {
        let tmp = TempDir::new().unwrap();
        let pool = tmp.path().join("pool");
        let ws = pool.join("ws");
        fs::create_dir_all(&ws).unwrap();
        let sb = LocalOsSandbox::new(&pool, &[]);
        if !sb.is_available() {
            return;
        }

        let pid_file = ws.join("grandchild.pid");
        let script = format!("sleep 60 & echo $! > \"{}\" ; wait", pid_file.display());
        let mut child = sb
            .spawn_async(
                &["/bin/sh".into(), "-c".into(), script],
                &ws,
                &SandboxRunOptions::default(),
            )
            .unwrap();

        let pid = child.pid().unwrap() as i32;
        // The host must be able to address the entire sandbox by the launcher's PID.
        assert_eq!(unsafe { libc::getpgid(pid) }, pid);
        assert_eq!(child.pgid(), Some(pid));

        // Wait until grandchild PID is written
        let mut gpid: Option<i32> = None;
        for _ in 0..50 {
            if let Ok(raw) = fs::read_to_string(&pid_file) {
                if let Ok(parsed) = raw.trim().parse::<i32>() {
                    gpid = Some(parsed);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let gpid = gpid.expect("grandchild pid should be written");

        let _status = child
            .terminate_process_group(Duration::from_millis(300))
            .await
            .unwrap();

        // Confirm grandchild is dead (ESRCH) on macOS where PID namespace is shared;
        // on Linux the private PID namespace is torn down with its init process.
        #[cfg(target_os = "macos")]
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let alive = unsafe { libc::kill(gpid, 0) == 0 };
            assert!(
                !alive,
                "grandchild pid {gpid} survived process group termination"
            );
        }
        #[cfg(not(target_os = "macos"))]
        {
            let _ = gpid;
        }
    }

    #[test]
    fn test_real_os_sandbox_all_7_invariants() {
        let tmp = TempDir::new().unwrap();
        let sb = LocalOsSandbox::new(tmp.path(), &[]);
        if !sb.is_available() {
            return;
        }
        let report =
            run_os_sandbox_verification_suite(&tmp.path().join(r#"suite with "quotes""#)).unwrap();
        assert!(
            report.all_invariants_passed,
            "OS sandbox suite failed: {:?}",
            report.checks
        );
        assert!(report.checks["block_arbitrary_outside_host_write"].passed);
    }
}
