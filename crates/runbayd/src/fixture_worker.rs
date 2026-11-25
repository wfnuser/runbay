use anyhow::{bail, Result};
use serde::Deserialize;
use std::env;
use std::fs;
use std::io::{self, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::process::Command;
use std::time::Duration;

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum FixtureDirective {
    Echo {
        #[serde(default)]
        lines: Vec<String>,
        #[serde(default)]
        delay_ms: u64,
    },
    WriteFile {
        rel_path: String,
        content: String,
    },
    CallGateway {
        #[serde(default = "default_model")]
        model: String,
        #[serde(default = "default_true")]
        check_direct_net: bool,
        /// Full user message sent through the secretless gateway. L2 drives
        /// shared hot prefixes + varied tails here to generate real
        /// `cached_tokens` telemetry. Default "ping" keeps e2e behavior.
        #[serde(default)]
        content: Option<String>,
        /// Completion token cap (cost control for L2 probe traffic).
        #[serde(default)]
        max_tokens: Option<u64>,
    },
    SleepWithGrandchild {
        #[serde(default = "default_sleep_secs")]
        sleep_secs: u64,
        #[serde(default = "default_pid_file")]
        pid_file: String,
    },
    Fail {
        #[serde(default = "default_fail_code")]
        exit_code: i32,
        #[serde(default)]
        message: String,
    },
    AttemptEscape {
        #[serde(default)]
        read_path: Option<String>,
        #[serde(default)]
        write_path: Option<String>,
    },
}

fn default_model() -> String {
    "test-model".to_string()
}

fn default_true() -> bool {
    true
}

fn default_sleep_secs() -> u64 {
    30
}

fn default_pid_file() -> String {
    "pids.json".to_string()
}

fn default_fail_code() -> i32 {
    7
}

fn emit_json_line(value: serde_json::Value) {
    let mut out = io::stdout().lock();
    let _ = writeln!(out, "{value}");
    let _ = out.flush();
}

/// Built-in deterministic protocol fixture executed as a real OS-sandboxed subprocess
/// (`runbayd __internal-fixture-worker`). Cannot execute arbitrary user commands.
pub async fn run_fixture_worker() -> Result<()> {
    let raw = env::var("RUNBAY_FIXTURE_DIRECTIVE").unwrap_or_default();
    let directive: FixtureDirective = match serde_json::from_str(&raw) {
        Ok(parsed) => parsed,
        Err(_) => FixtureDirective::Echo {
            lines: vec![format!("fixture_echo: {}", raw.trim())],
            delay_ms: 0,
        },
    };

    match directive {
        FixtureDirective::Echo { lines, delay_ms } => {
            for (idx, line) in lines.iter().enumerate() {
                emit_json_line(serde_json::json!({
                    "type": "fixture.line",
                    "index": idx,
                    "text": line
                }));
                if delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
            }
            emit_json_line(serde_json::json!({"type": "fixture.done", "count": lines.len()}));
            Ok(())
        }
        FixtureDirective::WriteFile { rel_path, content } => {
            let rel = Path::new(&rel_path);
            if rel.is_absolute()
                || rel
                    .components()
                    .any(|c| matches!(c, std::path::Component::ParentDir))
            {
                bail!("rel_path must stay inside workspace");
            }
            if let Some(parent) = rel.parent() {
                if !parent.as_os_str().is_empty() {
                    fs::create_dir_all(parent)?;
                }
            }
            fs::write(rel, content.as_bytes())?;
            emit_json_line(serde_json::json!({
                "type": "fixture.wrote_file",
                "rel_path": rel_path,
                "bytes": content.len()
            }));
            Ok(())
        }
        FixtureDirective::CallGateway {
            model,
            check_direct_net,
            content,
            max_tokens,
        } => {
            let direct_net_blocked = if check_direct_net {
                let addr: SocketAddr = "1.1.1.1:443".parse().unwrap();
                TcpStream::connect_timeout(&addr, Duration::from_millis(400)).is_err()
            } else {
                false
            };
            if check_direct_net && !direct_net_blocked {
                eprintln!("DIRECT_NETWORK_LEAKED");
                std::process::exit(3);
            }

            let token = env::var("OPENAI_API_KEY")
                .or_else(|_| env::var("RUNBAY_RUN_TOKEN"))
                .unwrap_or_default();
            let mut body_json = serde_json::json!({
                "model": model,
                "stream": false,
                "messages": [{"role": "user", "content": content.as_deref().unwrap_or("ping")}]
            });
            if let Some(mt) = max_tokens {
                body_json["max_tokens"] = serde_json::json!(mt);
            }
            let body = body_json.to_string();

            let response_body = if let Ok(sock) = env::var("RUNBAY_GATEWAY_SOCKET") {
                #[cfg(unix)]
                {
                    call_unix_http(&sock, &token, &body).await?
                }
                #[cfg(not(unix))]
                {
                    let _ = sock;
                    bail!("unix socket gateway unsupported on non-unix")
                }
            } else {
                let base_url = env::var("OPENAI_BASE_URL")
                    .or_else(|_| env::var("RUNBAY_GATEWAY_URL"))
                    .map_err(|_| anyhow::anyhow!("missing gateway URL or socket env"))?;
                let resp = reqwest::Client::builder()
                    .no_proxy()
                    .timeout(Duration::from_secs(5))
                    .build()?
                    .post(format!(
                        "{}/chat/completions",
                        base_url.trim_end_matches('/')
                    ))
                    .header("Authorization", format!("Bearer {token}"))
                    .header("Content-Type", "application/json")
                    .body(body)
                    .send()
                    .await?;
                let status = resp.status();
                let text = resp.text().await?;
                if !status.is_success() {
                    bail!("gateway returned HTTP {}: {}", status.as_u16(), text);
                }
                text
            };

            emit_json_line(serde_json::json!({
                "type": "fixture.gateway_ok",
                "direct_net_blocked": direct_net_blocked,
                "response_preview": response_body.chars().take(240).collect::<String>()
            }));
            Ok(())
        }
        FixtureDirective::SleepWithGrandchild {
            sleep_secs,
            pid_file,
        } => {
            let mut grandchild = Command::new("/bin/sleep")
                .arg(sleep_secs.to_string())
                .spawn()?;
            let gpid = grandchild.id();
            let my_pid = std::process::id();
            let pid_payload = serde_json::json!({
                "worker_pid": my_pid,
                "grandchild_pid": gpid
            });
            fs::write(&pid_file, pid_payload.to_string())?;
            emit_json_line(serde_json::json!({
                "type": "fixture.sleeping",
                "worker_pid": my_pid,
                "grandchild_pid": gpid
            }));
            let _ = grandchild.wait();
            Ok(())
        }
        FixtureDirective::Fail { exit_code, message } => {
            eprintln!("{message}");
            emit_json_line(serde_json::json!({
                "type": "fixture.failing",
                "exit_code": exit_code,
                "message": message
            }));
            std::process::exit(if exit_code == 0 { 1 } else { exit_code });
        }
        FixtureDirective::AttemptEscape {
            read_path,
            write_path,
        } => {
            let read_denied = match read_path {
                Some(p) => fs::read_to_string(&p).is_err(),
                None => true,
            };
            let write_denied = match write_path {
                Some(p) => fs::write(&p, b"escaped").is_err(),
                None => true,
            };
            emit_json_line(serde_json::json!({
                "type": "fixture.escape_check",
                "read_denied": read_denied,
                "write_denied": write_denied
            }));
            if !read_denied || !write_denied {
                eprintln!("SANDBOX_ESCAPE_SUCCEEDED");
                std::process::exit(9);
            }
            Ok(())
        }
    }
}

#[cfg(unix)]
async fn call_unix_http(sock_path: &str, token: &str, body: &str) -> Result<String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::UnixStream::connect(sock_path).await?;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\n\
         Host: localhost\r\n\
         Authorization: Bearer {token}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n\
         {body}",
        body.len()
    );
    stream.write_all(req.as_bytes()).await?;
    stream.flush().await?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await?;
    let text = String::from_utf8_lossy(&raw).to_string();
    let Some((headers, resp_body)) = text.split_once("\r\n\r\n") else {
        bail!("malformed HTTP response from unix gateway: {text}");
    };
    if !headers.starts_with("HTTP/1.1 200") {
        bail!("unix gateway non-200 response: {headers} / {resp_body}");
    }
    Ok(resp_body.to_string())
}
