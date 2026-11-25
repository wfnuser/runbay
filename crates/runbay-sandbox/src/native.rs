use super::SandboxRunOptions;
use anyhow::{bail, ensure, Context, Result};
use std::fs;
use std::io::Read;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileTypeExt;
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;

pub(super) fn launcher() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("RUNBAY_LANDLOCK_LAUNCHER") {
        return Some(path.into());
    }
    let installed = Path::new("/usr/local/libexec/runbay-landlock");
    let restricted = fs::read_to_string("/proc/sys/kernel/apparmor_restrict_unprivileged_userns")
        .is_ok_and(|value| value.trim() == "1");
    // Ubuntu attaches the user namespace profile to the root-owned installed path.
    let built = option_env!("RUNBAY_BUILT_LAUNCHER").map(PathBuf::from);
    [
        restricted.then(|| installed.to_owned()),
        built,
        std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().map(|d| d.join("runbay-landlock"))),
        Some(installed.to_owned()),
    ]
    .into_iter()
    .flatten()
    .find(|p| p.is_file())
}

pub(super) fn arguments(
    cmd: &[String],
    workspace: &Path,
    pool: &Path,
    forbidden: &[PathBuf],
    opts: &SandboxRunOptions,
) -> Result<Vec<String>> {
    ensure!(!cmd.is_empty(), "sandbox command cannot be empty");
    ensure!(
        opts.allow_network
            || opts.allowed_loopback_port.is_none()
            || opts.gateway_unix_socket.is_some(),
        "FAIL_CLOSED: use a Unix socket for isolated Linux gateway access"
    );
    let mut exposures: Vec<(&str, PathBuf)> = Vec::new();
    let mut forbid = forbidden.to_vec();
    for path in &opts.extra_forbidden_reads {
        forbid.push(fs::canonicalize(path).unwrap_or_else(|_| path.clone()));
    }
    // These are the runtime, loader and small device files. No host home, /run or /etc tree.
    for name in [
        "/usr",
        "/bin",
        "/sbin",
        "/lib",
        "/lib64",
        "/etc/ld.so.cache",
    ] {
        if Path::new(name).exists() {
            exposures.push(("--ro", name.into()));
        }
    }
    if opts.allow_network {
        for name in [
            "/etc/hosts",
            "/etc/resolv.conf",
            "/etc/nsswitch.conf",
            "/etc/ssl/certs",
        ] {
            if Path::new(name).exists() {
                exposures.push(("--ro", name.into()));
            }
        }
    }
    for name in ["/dev/null", "/dev/zero", "/dev/urandom", "/dev/random"] {
        if Path::new(name).exists() {
            exposures.push(("--device", name.into()));
        }
    }
    let ws = workspace.to_owned();
    exposures.push(("--workspace", ws.clone()));
    for (mode, paths) in [
        ("--ro", &opts.extra_readonly_paths),
        ("--rw", &opts.extra_writable_paths),
    ] {
        for path in paths {
            exposures.push((
                mode,
                fs::canonicalize(path).with_context(|| format!("resolve {}", path.display()))?,
            ));
        }
    }
    let binary = Path::new(&cmd[0]);
    let executable = if binary.is_absolute() {
        binary.to_owned()
    } else if binary.components().count() > 1 {
        ws.join(binary)
    } else {
        ["/usr/local/bin", "/usr/bin", "/bin"]
            .iter()
            .map(|d| Path::new(d).join(binary))
            .find(|p| p.is_file())
            .context("resolve worker executable")?
    };
    let executable = fs::canonicalize(executable).context("resolve worker executable")?;
    if !exposures.iter().any(|(_, p)| executable.starts_with(p)) {
        exposures.push(("--ro", executable.clone()));
    }
    let gateway = opts
        .gateway_unix_socket
        .as_ref()
        .map(fs::canonicalize)
        .transpose()?;
    if let Some(sock) = &gateway {
        ensure!(
            fs::metadata(sock)?.file_type().is_socket(),
            "gateway endpoint must be a Unix socket"
        );
        exposures.push(("--ro", sock.clone()));
    }
    exposures.sort_by(|(lm, left), (rm, right)| {
        left.cmp(right)
            .then_with(|| (*rm == "--workspace").cmp(&(*lm == "--workspace")))
    });
    let mut args = vec![
        "--expect-policy".into(),
        env!("RUNBAY_POLICY_ID").into(),
        "--argv0".into(),
        cmd[0].clone(),
    ];
    if opts.allow_network {
        args.push("--network".to_string());
    }
    'exposure: for (i, (mode, path)) in exposures.iter().enumerate() {
        let canonical = fs::canonicalize(path)?;
        ensure!(
            path != Path::new("/")
                && !canonical.starts_with("/proc")
                && !canonical.starts_with("/sys"),
            "invalid sandbox exposure {}",
            path.display()
        );
        let gateway_exception = gateway.as_ref() == Some(path) && *mode == "--ro";
        ensure!(
            gateway_exception
                || !forbid
                    .iter()
                    .any(|f| canonical.starts_with(f) || f.starts_with(&canonical)),
            "sandbox exposure overlaps a forbidden path: {}",
            path.display()
        );
        ensure!(
            canonical != pool || *mode == "--workspace",
            "cannot expose the entire worker pool"
        );
        ensure!(
            !pool.starts_with(&canonical) || canonical == pool,
            "cannot expose a parent of the worker pool"
        );
        for (other_mode, other) in &exposures[..i] {
            if path.starts_with(other)
                && (*mode == *other_mode || (*mode == "--rw" && *other_mode == "--workspace"))
            {
                continue 'exposure;
            }
            if path.starts_with(other) || other.starts_with(path) {
                bail!(
                    "overlapping sandbox exposures: {other_mode} {} and {mode} {}",
                    other.display(),
                    path.display()
                );
            }
        }
        args.push((*mode).to_string());
        args.push(path.to_str().context("non-UTF8 sandbox path")?.to_string());
    }
    args.push("--".into());
    args.push(executable.to_str().context("non-UTF8 worker path")?.into());
    args.extend_from_slice(&cmd[1..]);
    Ok(args)
}

pub(super) fn check_available(binary: &Path) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    use std::sync::Mutex;
    // AppArmor attaches by path. Replacement or in-place changes invalidate a successful probe.
    struct Verified {
        path: PathBuf,
        inode: (u64, u64, i64, i64),
    }
    static VERIFIED: Mutex<Option<Verified>> = Mutex::new(None);
    let metadata = fs::metadata(binary)
        .with_context(|| format!("open Landlock launcher {}", binary.display()))?;
    let inode = (
        metadata.dev(),
        metadata.ino(),
        metadata.ctime(),
        metadata.ctime_nsec(),
    );
    let mut verified = VERIFIED.lock().unwrap();
    if verified
        .as_ref()
        .is_some_and(|v| v.path == binary && v.inode == inode)
    {
        return Ok(());
    }
    let version = Command::new(binary)
        .env_clear()
        .arg("--print-policy-id")
        .output()?;
    ensure!(version.status.success() && version.stdout.strip_suffix(b"\n") == Some(env!("RUNBAY_POLICY_ID").as_bytes()),
        "Landlock launcher policy mismatch at {}. Reinstall with scripts/install-linux-launcher.sh or set RUNBAY_LANDLOCK_LAUNCHER to the matching binary", binary.display());
    let tmp = tempfile::tempdir()?;
    let workspace = fs::canonicalize(tmp.path())?;
    let args = arguments(
        &["/bin/true".into()],
        &workspace,
        &workspace,
        &[],
        &Default::default(),
    )?;
    let probe = Command::new(binary).env_clear().args(args).output()?;
    ensure!(
        probe.status.success(),
        "Landlock launcher {} failed its isolation probe: {}",
        binary.display(),
        String::from_utf8_lossy(&probe.stderr).trim()
    );
    *verified = Some(Verified {
        path: binary.to_owned(),
        inode,
    });
    Ok(())
}

pub(super) struct Handshake {
    read: UnixStream,
    write: UnixStream,
}

impl Handshake {
    pub(super) fn install(command: &mut Command) -> Result<Self> {
        let (read, write) = UnixStream::pair()?;
        read.set_read_timeout(Some(std::time::Duration::from_secs(10)))?;
        let fd = write.as_raw_fd();
        command.args(["--ready", &fd.to_string()]);
        unsafe {
            command.pre_exec(move || {
                if libc::fcntl(fd, libc::F_SETFD, 0) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        Ok(Self { read, write })
    }

    pub(super) fn wait(self) -> Result<()> {
        let Self { mut read, write } = self;
        drop(write);
        let mut reply = Vec::new();
        read.read_to_end(&mut reply)?;
        ensure!(
            reply == b"R",
            "native sandbox setup failed: {}",
            String::from_utf8_lossy(reply.strip_prefix(b"R").unwrap_or(&reply))
        );
        Ok(())
    }
}
