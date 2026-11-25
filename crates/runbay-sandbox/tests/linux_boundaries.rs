#![cfg(target_os = "linux")]

use runbay_sandbox::{LocalOsSandbox, SandboxRunOptions};
use std::fs;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use tempfile::tempdir;

#[test]
fn readonly_names_remain_immutable_during_concurrent_workspace_updates() {
    let root = tempdir().unwrap();
    let pool = root.path().join("pool");
    let shared = pool.join("shared");
    let ws = pool.join("work");
    fs::create_dir_all(&shared).unwrap();
    fs::create_dir(&ws).unwrap();
    fs::write(shared.join("note"), "original").unwrap();
    fs::hard_link(shared.join("note"), shared.join("second-name")).unwrap();
    symlink(shared.join("second-name"), ws.join("reference")).unwrap();
    symlink(shared.join("note"), ws.join("alternate")).unwrap();
    let reference =
        std::ffi::CString::new(ws.join("reference").as_os_str().as_encoded_bytes()).unwrap();
    let alternate =
        std::ffi::CString::new(ws.join("alternate").as_os_str().as_encoded_bytes()).unwrap();
    let sb = LocalOsSandbox::new(&pool, &[]);
    if !sb.is_available() {
        assert!(
            std::env::var_os("RUNBAY_SANDBOX_BACKEND").is_none(),
            "selected backend unavailable"
        );
        return;
    }
    let opts = SandboxRunOptions {
        extra_readonly_paths: vec![shared.clone()],
        ..Default::default()
    };
    let barrier = Arc::new(Barrier::new(3));
    let finished = AtomicBool::new(false);
    std::thread::scope(|scope| {
        // Atomically rotate a public reference between two names of the same read-only
        // note while both workers use it. Every observation must retain its original policy.
        let changing = scope.spawn(|| {
            let mut updates = 0;
            while !finished.load(Ordering::Acquire) {
                // Keep both directory entries alive throughout the lookup, including on
                // the other bind mount. Nothing vanishes while the workers are reading.
                let result = unsafe {
                    libc::syscall(
                        libc::SYS_renameat2,
                        libc::AT_FDCWD,
                        reference.as_ptr(),
                        libc::AT_FDCWD,
                        alternate.as_ptr(),
                        libc::RENAME_EXCHANGE,
                    )
                };
                assert_eq!(result, 0, "{}", std::io::Error::last_os_error());
                updates += 1;
                std::thread::yield_now();
            }
            updates
        });
        let mut workers = Vec::new();
        for name in ["a", "b"] {
            let gate = Arc::clone(&barrier);
            let (sb, ws, opts) = (&sb, &ws, &opts);
            workers.push(scope.spawn(move || {
                gate.wait();
                let command = [
                    "/bin/sh".into(),
                    "-c".into(),
                    r#"set -eu
                       i=0
                       while [ "$i" -lt 12 ]; do
                           echo local > "$1.tmp"
                           mv -f "$1.tmp" "$1"
                           test "$(cat reference)" = original
                           if truncate -s 0 reference 2>/dev/null; then exit 1; fi
                           i=$((i+1))
                       done
                       test "$(cat reference)" = original
                       if truncate -s 0 reference 2>/dev/null; then exit 1; fi
                       if ln -- "$(readlink reference)" "$1.link" 2>/dev/null; then exit 1; fi
                    "#
                    .into(),
                    "worker".into(),
                    name.into(),
                ];
                let result = sb.run(&command, ws, opts).unwrap();
                assert_eq!(result.returncode, 0, "{}", result.stderr);
            }));
        }
        barrier.wait();
        let completed: Vec<_> = workers.into_iter().map(|worker| worker.join()).collect();
        finished.store(true, Ordering::Release);
        assert!(changing.join().unwrap() > 0);
        for result in completed {
            result.unwrap();
        }
    });
    for path in ["note", "second-name"] {
        assert_eq!(fs::read(shared.join(path)).unwrap(), b"original");
    }
    for name in ["a", "b"] {
        assert_eq!(fs::read(ws.join(name)).unwrap(), b"local\n");
    }
}

#[tokio::test]
async fn native_launch_error_precedes_started_child() {
    let tmp = tempdir().unwrap();
    let sb = LocalOsSandbox::new(tmp.path(), &[]);
    if sb.gateway_transport() != "linux_landlock_seccomp_unix_socket" {
        return;
    }
    let file = tmp.path().join("ordinary-text");
    fs::write(&file, "sample document").unwrap();
    fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).unwrap();
    let cmd = [file.to_str().unwrap().to_string()];
    let error = sb
        .spawn_async(&cmd, tmp.path(), &Default::default())
        .err()
        .expect("exec must fail");
    assert!(error.to_string().contains("Permission denied"), "{error:#}");
    assert!(!error.to_string().contains("failed: R/"), "{error:#}");
}

#[test]
fn writable_files_must_own_all_their_hard_links() {
    let tmp = tempdir().unwrap();
    let ws = tmp.path().join("work");
    fs::create_dir(&ws).unwrap();
    fs::write(ws.join("one"), "fixture").unwrap();
    fs::hard_link(ws.join("one"), ws.join("two")).unwrap();
    let sb = LocalOsSandbox::new(tmp.path(), &[]);
    if !sb.is_available() {
        return;
    }
    let command = ["/bin/true".into()];
    assert_eq!(
        sb.run(&command, &ws, &Default::default())
            .unwrap()
            .returncode,
        0
    );
    fs::hard_link(ws.join("one"), tmp.path().join("retained")).unwrap();
    let error = sb.run(&command, &ws, &Default::default()).unwrap_err();
    assert!(
        error.to_string().contains("hard link outside its scope"),
        "{error:#}"
    );
}

#[test]
fn native_worker_preserves_virtualenv_argv0_and_nested_readonly_paths() {
    let tmp = tempdir().unwrap();
    let workspace = tmp.path().join("workspace");
    let data = tmp.path().join("input");
    let nested = data.join("nested");
    fs::create_dir(&workspace).unwrap();
    fs::create_dir_all(&nested).unwrap();
    let sb = LocalOsSandbox::new(&workspace, &[]);
    if sb.gateway_transport() != "linux_landlock_seccomp_unix_socket" {
        return;
    }
    sb.check_available().unwrap();
    let venv = workspace.join("venv");
    let interpreter = venv.join("bin/python3");
    fs::create_dir_all(interpreter.parent().unwrap()).unwrap();
    fs::write(
        venv.join("pyvenv.cfg"),
        "home = /usr/bin\ninclude-system-site-packages = false\n",
    )
    .unwrap();
    std::os::unix::fs::symlink("/usr/bin/python3", &interpreter).unwrap();
    let command = [
        interpreter.to_str().unwrap().into(),
        "-c".into(),
        "import sys; print(sys.prefix)".into(),
    ];
    for paths in [vec![nested.clone(), data.clone()], vec![data, nested]] {
        let opts = SandboxRunOptions {
            extra_readonly_paths: paths,
            ..Default::default()
        };
        let result = sb.run(&command, &workspace, &opts).unwrap();
        assert_eq!(result.returncode, 0, "{}", result.stderr);
        assert_eq!(result.stdout.trim(), venv.to_str().unwrap());
    }
}
