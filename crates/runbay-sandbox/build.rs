use sha2::{Digest, Sha256};
use std::{env, fs, path::PathBuf};

fn main() {
    println!("cargo:rustc-check-cfg=cfg(runbay_native_linux)");
    let files = ["native/launcher.c", "native/policy.h"];
    for path in files {
        println!("cargo:rerun-if-changed={path}");
    }
    if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("linux")
        || !matches!(
            env::var("CARGO_CFG_TARGET_ARCH").as_deref(),
            Ok("x86_64" | "aarch64")
        )
    {
        return;
    }
    println!("cargo:rustc-cfg=runbay_native_linux");
    // The separately installed launcher can be built with a different toolchain.
    let mut digest = Sha256::new();
    for path in files {
        digest.update(fs::read(path).expect("read native launcher source"));
    }
    let id = format!("{:x}", digest.finalize());
    println!("cargo:rustc-env=RUNBAY_POLICY_ID={id}");

    let out = PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("runbay-landlock");
    let built = cc::Build::new()
        .try_get_compiler()
        .ok()
        .and_then(|cc| {
            cc.to_command()
                .args(["-std=gnu11", "-O2", "-Wall", "-Wextra", "-static"])
                .arg(format!(r#"-DRUNBAY_POLICY_ID="{id}""#))
                .args(["native/launcher.c", "-o"])
                .arg(&out)
                .status()
                .ok()
        })
        .is_some_and(|status| status.success());
    if built {
        println!("cargo:rustc-env=RUNBAY_BUILT_LAUNCHER={}", out.display());
    } else {
        println!("cargo:warning=Static Landlock launcher unavailable. Use bubblewrap or install a matching launcher with scripts/install-linux-launcher.sh.");
    }
}
