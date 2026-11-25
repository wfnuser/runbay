use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

/// CPU percent is relative to one core. Memory and task counts apply to the whole process tree.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SandboxLimits {
    pub cpu_percent: NonZeroU32,
    pub memory_mib: NonZeroU32,
    pub max_pids: NonZeroU32,
}

impl Default for SandboxLimits {
    fn default() -> Self {
        Self {
            cpu_percent: NonZeroU32::new(100).unwrap(),
            memory_mib: NonZeroU32::new(256).unwrap(),
            max_pids: NonZeroU32::new(64).unwrap(),
        }
    }
}

impl SandboxLimits {
    pub fn fits_within(self, ceiling: Self) -> bool {
        self.cpu_percent <= ceiling.cpu_percent
            && self.memory_mib <= ceiling.memory_mib
            && self.max_pids <= ceiling.max_pids
    }
}

#[derive(Debug, Clone)]
pub struct SandboxCgroup {
    pub parent: PathBuf,
    pub run_id: String,
    pub limits: SandboxLimits,
}

/// An administrator supplies a cgroup v2 subtree delegated to the daemon.
/// Runbay only creates leaves inside it and never enables controllers on host ancestors.
#[cfg(target_os = "linux")]
fn check_controllers(parent: &Path) -> Result<()> {
    use std::os::fd::AsRawFd;
    let dir = fs::File::open(parent).context("open delegated cgroup directory")?;
    let mut stats = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: fstatfs initializes stats on success and dir is open throughout.
    if unsafe { libc::fstatfs(dir.as_raw_fd(), stats.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error().into());
    }
    if unsafe { stats.assume_init() }.f_type != libc::CGROUP2_SUPER_MAGIC {
        bail!("sandbox cgroup parent must be on a cgroup v2 filesystem");
    }
    let controllers = fs::read_to_string(parent.join("cgroup.subtree_control"))?;
    for name in ["cpu", "memory", "pids"] {
        if !controllers.split_whitespace().any(|c| c == name) {
            bail!(
                "controller {name} is not enabled under {}",
                parent.display()
            );
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
pub fn validate_cgroup_parent(parent: &Path) -> Result<()> {
    check_controllers(parent)?;
    // A fresh child has the same permissions and controller files a real run will get.
    let path = tempfile::Builder::new()
        .prefix(".runbay-probe-")
        .tempdir_in(parent)
        .context("create a leaf in the delegated cgroup")?
        .keep();
    let group = RunCgroup {
        path,
        removed: false,
    };
    for name in [
        "cpu.max",
        "pids.max",
        "memory.max",
        "memory.swap.max",
        "memory.oom.group",
        "cgroup.kill",
        "cgroup.procs",
    ] {
        fs::OpenOptions::new()
            .write(true)
            .open(group.path.join(name))
            .with_context(|| format!("delegated cgroup requires writable {name}"))?;
    }
    group.close()
}

#[cfg(not(target_os = "linux"))]
pub fn validate_cgroup_parent(_: &Path) -> Result<()> {
    bail!("sandbox cgroup limits require Linux");
}

fn leaf_path(parent: &Path, run_id: &str) -> Result<PathBuf> {
    if Path::new(run_id).file_name() != Some(std::ffi::OsStr::new(run_id)) {
        bail!("invalid cgroup run identifier");
    }
    Ok(parent.join(format!("runbay-{run_id}")))
}

pub fn clean_interrupted_cgroup(parent: &Path, run_id: &str) -> Result<()> {
    remove_leaf(&leaf_path(parent, run_id)?)
}

fn remove_leaf(path: &Path) -> Result<()> {
    match fs::remove_dir(path) {
        Ok(()) => return Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) if e.raw_os_error() == Some(libc::EBUSY) => {}
        Err(e) => return Err(e).context("remove sandbox cgroup"),
    }
    // A descendant may outlive the original process group. Kill only this run's leaf.
    fs::write(path.join("cgroup.kill"), "1").context("terminate remaining sandbox tasks")?;
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match fs::remove_dir(path) {
            Ok(()) => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) if e.raw_os_error() == Some(libc::EBUSY) && Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(e) => return Err(e).context("remove populated sandbox cgroup"),
        }
    }
}

pub(crate) struct RunCgroup {
    path: PathBuf,
    removed: bool,
}

impl RunCgroup {
    pub(crate) fn prepare(cmd: &mut Command, spec: &SandboxCgroup) -> Result<Self> {
        use std::os::fd::AsRawFd;
        use std::os::unix::process::CommandExt;

        #[cfg(target_os = "linux")]
        check_controllers(&spec.parent)?;
        #[cfg(not(target_os = "linux"))]
        validate_cgroup_parent(&spec.parent)?;
        let path = leaf_path(&spec.parent, &spec.run_id)?;
        fs::create_dir(&path).with_context(|| format!("create cgroup {}", path.display()))?;
        let group = Self {
            path,
            removed: false,
        };
        let limits = spec.limits;
        let quota_us = u64::from(limits.cpu_percent.get()) * 1000;
        let bytes = u64::from(limits.memory_mib.get()) * 1024 * 1024;
        for (name, value) in [
            ("cpu.max", format!("{quota_us} 100000")),
            ("memory.max", bytes.to_string()),
            ("memory.swap.max", "0".into()),
            ("memory.oom.group", "1".into()),
            ("pids.max", limits.max_pids.to_string()),
        ] {
            fs::write(group.path.join(name), value).with_context(|| format!("set {name}"))?;
        }
        fs::OpenOptions::new()
            .write(true)
            .open(group.path.join("cgroup.kill"))
            .context("cgroup.kill is required for process-tree cleanup")?;
        let procs = fs::OpenOptions::new()
            .write(true)
            .open(group.path.join("cgroup.procs"))?;
        // SAFETY: between fork and exec only use write(2) on a pre-opened CLOEXEC fd.
        // Writing "0" moves the calling child before the sandbox launcher can fork descendants.
        unsafe {
            cmd.pre_exec(move || loop {
                if libc::write(procs.as_raw_fd(), b"0".as_ptr().cast(), 1) == 1 {
                    return Ok(());
                }
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    return Err(error);
                }
            });
        }
        Ok(group)
    }

    pub(crate) fn close_with_oom(self) -> Result<bool> {
        let events = fs::read_to_string(self.path.join("memory.events"));
        self.close()?;
        let events = events.context("read sandbox memory.events")?;
        let count = events
            .lines()
            .find_map(|line| line.strip_prefix("oom_kill "))
            .context("missing oom_kill in memory.events")?
            .parse::<u64>()?;
        Ok(count > 0)
    }

    pub(crate) fn close(mut self) -> Result<()> {
        remove_leaf(&self.path)?;
        self.removed = true;
        Ok(())
    }
}

impl Drop for RunCgroup {
    fn drop(&mut self) {
        if !self.removed {
            if let Err(error) = remove_leaf(&self.path) {
                eprintln!(
                    "failed to clean sandbox cgroup {}: {error:#}",
                    self.path.display()
                );
            }
        }
    }
}
