use anyhow::{ensure, Context, Result};
use runbay_sandbox::LocalOsSandbox;
use std::{env, process::Command};

fn monotonic_ns() -> Result<u64> {
    let mut t = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // The worker samples this same host monotonic clock at the beginning of main.
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut t) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(u64::try_from(t.tv_sec)? * 1_000_000_000 + u64::try_from(t.tv_nsec)?)
}

fn percentile(values: &mut [f64], p: usize) -> f64 {
    values.sort_by(f64::total_cmp);
    values[(values.len() * p).div_ceil(100) - 1]
}

fn main() -> Result<()> {
    let args: Vec<_> = env::args().skip(1).collect();
    ensure!(
        args.len() >= 2,
        "usage: isolation_bench bare|sandbox|external COMMAND [ARGS]"
    );
    let temp = tempfile::tempdir()?;
    let sandbox = LocalOsSandbox::new(temp.path(), &[]);
    let measure = |mode: &str| -> Result<(f64, f64)> {
        let start = monotonic_ns()?;
        let stdout = match mode {
            "sandbox" => {
                let result = sandbox.run(&args[1..], temp.path(), &Default::default())?;
                ensure!(result.returncode == 0, "{}", result.stderr);
                result.stdout
            }
            "bare" | "external" => {
                let result = Command::new(&args[1]).args(&args[2..]).output()?;
                ensure!(
                    result.status.success(),
                    "{}",
                    String::from_utf8_lossy(&result.stderr)
                );
                String::from_utf8(result.stdout)?
            }
            _ => anyhow::bail!("unknown mode {mode}"),
        };
        let mut fields = stdout.split_whitespace();
        let entry: u64 = fields
            .next()
            .context("missing worker entry time")?
            .parse()?;
        let launch_ns = entry
            .checked_sub(start)
            .context("different monotonic clock")?;
        let loop_ns: f64 = fields.next().context("missing loop time")?.parse()?;
        let calls: f64 = fields.next().context("missing read count")?.parse()?;
        ensure!(calls == 100000.0, "unexpected read count");
        Ok((launch_ns as f64 / 1e6, loop_ns / calls))
    };
    for _ in 0..3 {
        measure(&args[0])?;
    }
    let (mut launches, mut reads): (Vec<_>, Vec<_>) = (0..30)
        .map(|_| measure(&args[0]))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .unzip();
    println!(
        "{} n=30 launch_ms p50={:.3} p95={:.3} read_ns p50={:.1} p95={:.1}",
        args[0],
        percentile(&mut launches, 50),
        percentile(&mut launches, 95),
        percentile(&mut reads, 50),
        percentile(&mut reads, 95)
    );
    Ok(())
}
