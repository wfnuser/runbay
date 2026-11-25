#![cfg(target_os = "linux")]

use runbay_sandbox::{LocalOsSandbox, SandboxCgroup, SandboxLimits, SandboxRunOptions};
use std::fs;
use std::path::Path;
use std::time::Duration;
use tempfile::TempDir;

fn counter(group: &Path, file: &str, key: &str) -> u64 {
    fs::read_to_string(group.join(file))
        .unwrap()
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(' ')?;
            (name == key).then(|| value.parse().unwrap())
        })
        .unwrap()
}

#[test]
fn cgroup_limits_reject_an_ordinary_directory() {
    let tmp = TempDir::new().unwrap();
    let sb = LocalOsSandbox::new(tmp.path(), &[]);
    let opts = SandboxRunOptions {
        cgroup: Some(SandboxCgroup {
            parent: tmp.path().to_owned(),
            run_id: "invalid-parent".into(),
            limits: SandboxLimits::default(),
        }),
        ..Default::default()
    };
    let error = sb
        .run(&["/bin/true".into()], tmp.path(), &opts)
        .unwrap_err();
    assert!(
        error.to_string().contains("cgroup v2 filesystem"),
        "{error:#}"
    );
    assert!(!tmp.path().join("runbay-invalid-parent").exists());
}

#[tokio::test]
#[ignore = "requires a writable delegated cgroup v2 subtree; exercised by Linux CI"]
async fn cgroup_kernel_enforces_cpu_memory_and_task_budgets() {
    let parent =
        std::path::PathBuf::from(std::env::var_os("RUNBAY_CGROUP_ROOT").expect("delegated parent"));
    let run_id = format!("kernel-test-{}", std::process::id());
    let leaf = parent.join(format!("runbay-{run_id}"));
    let tmp = TempDir::new().unwrap();
    let ws = tmp.path().join("workspace");
    fs::create_dir(&ws).unwrap();
    let sandbox = LocalOsSandbox::new(tmp.path(), &[]);
    let opts = SandboxRunOptions {
        cgroup: Some(SandboxCgroup {
            parent,
            run_id,
            limits: SandboxLimits {
                cpu_percent: 20.try_into().unwrap(),
                memory_mib: 48.try_into().unwrap(),
                max_pids: 8.try_into().unwrap(),
            },
        }),
        ..Default::default()
    };

    // Finite probes: at most eight sleeping children, 0.7 seconds of CPU demand, then
    // one 64 MiB allocation in a fresh 48 MiB cgroup with swap disabled.
    let workload = r#"
import errno, subprocess, time
deadline = time.monotonic() + 0.7
while time.monotonic() < deadline:
    pass
children = []
try:
    for _ in range(8):
        children.append(subprocess.Popen(["/bin/sleep", "2"]))
except OSError as error:
    assert error.errno == errno.EAGAIN
else:
    raise AssertionError("task limit was not enforced")
finally:
    for child in children:
        child.terminate()
        child.wait()
"#;
    for (script, expect_oom) in [
        (workload, false),
        ("data = bytearray(64 * 1024 * 1024)", true),
    ] {
        let cmd = vec!["/usr/bin/python3".into(), "-c".into(), script.into()];
        let mut process = sandbox.spawn_async(&cmd, &ws, &opts).unwrap();
        assert_eq!(
            fs::read_to_string(leaf.join("cpu.max")).unwrap().trim(),
            "20000 100000"
        );
        assert_eq!(
            fs::read_to_string(leaf.join("memory.max")).unwrap().trim(),
            "50331648"
        );
        assert_eq!(
            fs::read_to_string(leaf.join("pids.max")).unwrap().trim(),
            "8"
        );
        let status = tokio::time::timeout(Duration::from_secs(12), process.child.wait())
            .await
            .unwrap()
            .unwrap();
        if expect_oom {
            assert!(!status.success());
            let oom_kills = counter(&leaf, "memory.events", "oom_kill");
            assert!(oom_kills > 0);
            println!("48 MiB sandbox: memory.events oom_kill={oom_kills}");
        } else {
            assert!(status.success(), "{status}");
            let throttled = counter(&leaf, "cpu.stat", "nr_throttled");
            let pid_denials = counter(&leaf, "pids.events", "max");
            assert!(throttled > 0 && pid_denials > 0);
            println!("20% CPU / 8 tasks: cpu.stat nr_throttled={throttled}, pids.events max={pid_denials}");
        }
        assert_eq!(
            process.finish_resources_with_oom().await.unwrap(),
            expect_oom
        );
        assert!(!leaf.exists());
    }
}
