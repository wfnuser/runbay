use futures_util::StreamExt;
use runbay_gateway::MockUpstreamServer;
use runbay_sandbox::LocalOsSandbox;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tempfile::TempDir;

struct SpawnedDaemon {
    child: Child,
    base_url: String,
    token: String,
    _state_dir_path: PathBuf,
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn test_native_outcomes_survive_large_events_and_successful_retries() {
    use std::os::unix::fs::PermissionsExt;
    let mock = MockUpstreamServer::start().await.unwrap();
    let tmp = TempDir::new().unwrap();
    let ws = tmp.path().join("workspace");
    fs::create_dir_all(&ws).unwrap();
    let failed = serde_json::json!({"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"x".repeat(12000)}],"stopReason":"length"}});
    let success = serde_json::json!({"type":"message_end","message":{"role":"assistant","content":[],"stopReason":"stop"}});
    let cases = [
        ("pi", vec![failed.clone()], "failed"),
        (
            "pi",
            vec![
                serde_json::json!({"type":"message_end","message":{"role":"assistant","stopReason":"aborted"}}),
            ],
            "failed",
        ),
        (
            "pi",
            vec![
                serde_json::json!({"type":"message_end","message":{"role":"assistant","stopReason":"error"}}),
            ],
            "failed",
        ),
        ("pi", vec![failed, success], "succeeded"),
        ("pi", vec![serde_json::json!({"type":"session"})], "failed"),
        (
            "claude",
            vec![
                serde_json::json!({"type":"result","subtype":"error_during_execution","is_error":true}),
            ],
            "failed",
        ),
    ];
    for (index, (runtime, events, expected)) in cases.into_iter().enumerate() {
        // Model only the external CLI protocol; exercise real supervisor, sandbox, and SSE.
        let cli = tmp.path().join(format!("fake-cli-{index}"));
        let mut script = "#!/bin/sh\n".to_string();
        for event in events {
            script.push_str(&format!("printf '%s\\n' '{}'\n", event));
        }
        fs::write(&cli, script).unwrap();
        fs::set_permissions(&cli, fs::Permissions::from_mode(0o755)).unwrap();
        let override_name = if runtime == "pi" {
            "RUNBAY_PI_BIN"
        } else {
            "RUNBAY_CLAUDE_BIN"
        };
        let daemon = SpawnedDaemon::spawn(
            &tmp.path().join(format!("state-{index}")),
            "rb_ctl_outcomes",
            &[
                "--allow-workspace-root",
                ws.to_str().unwrap(),
                "--upstream-url",
                &mock.base_url(),
            ],
            &[
                ("RUNBAY_UPSTREAM_API_KEY", "host-only-test-key"),
                (override_name, cli.to_str().unwrap()),
            ],
        );
        let client = reqwest::Client::new();
        let register = client.post(format!("{}/agents", daemon.base_url)).bearer_auth(&daemon.token)
            .json(&serde_json::json!({"agent_id":"outcome-test","runtime":runtime,"workspace_dir":ws}))
            .send().await.unwrap();
        assert_eq!(register.status(), 201);
        let run: serde_json::Value = client
            .post(format!("{}/agents/outcome-test/run", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&serde_json::json!({"prompt":"test","allowed_model":"test-model"}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let events = collect_run_sse(&client, &daemon, run["run_id"].as_str().unwrap(), None).await;
        let final_run = wait_for_run_state(
            &client,
            &daemon,
            run["run_id"].as_str().unwrap(),
            &["succeeded", "failed"],
        )
        .await;
        assert_eq!(final_run["state"], expected, "case {index}");
        assert_eq!(final_run["exit_code"], 0);
        if expected == "failed" {
            assert_eq!(final_run["error_code"], "runtime_reported_error");
        }
        assert!(events.iter().any(|e| e.event == format!("run.{expected}")));
    }
}

// Real installed CLIs, real daemon and OS sandbox; only the paid model is replaced.
#[cfg(target_os = "macos")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_real_claude_and_pi_via_anthropic_gateway_when_installed() {
    use bytes::Bytes;
    use http::{Request, Response};
    use http_body_util::{BodyExt, Full};
    use hyper::{body::Incoming, server::conn::http1, service::service_fn};
    use hyper_util::rt::TokioIo;
    use std::convert::Infallible;
    use std::sync::{Arc, Mutex};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = format!("http://{}/v1", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&requests);
    let upstream_task = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let seen = Arc::clone(&seen);
            tokio::spawn(async move {
                let svc = service_fn(move |req: Request<Incoming>| {
                    let seen = Arc::clone(&seen);
                    async move {
                        let path = req.uri().path().to_string();
                        let key = req
                            .headers()
                            .get("x-api-key")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("")
                            .to_string();
                        let body = req.into_body().collect().await.unwrap().to_bytes();
                        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
                        if body.to_string().contains("FORCE_UPSTREAM_AUTH_ERROR") {
                            return Ok::<_, Infallible>(Response::builder().status(401)
                                .header("content-type", "application/json")
                                .body(Full::new(Bytes::from_static(br#"{"type":"error","error":{"type":"authentication_error","message":"synthetic credential rejection"}}"#))).unwrap());
                        }
                        seen.lock().unwrap().push((path, key, body));
                        let events = [
                            serde_json::json!({"type":"message_start","message":{"id":"msg_runbay","type":"message","role":"assistant","content":[],"model":"test-model","stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":10,"output_tokens":0}}}),
                            serde_json::json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
                            serde_json::json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"RUNBAY_CLI_OK"}}),
                            serde_json::json!({"type":"content_block_stop","index":0}),
                            serde_json::json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":8}}),
                            serde_json::json!({"type":"message_stop"}),
                        ];
                        let data: String = events
                            .iter()
                            .map(|v| {
                                format!("event: {}\ndata: {}\n\n", v["type"].as_str().unwrap(), v)
                            })
                            .collect();
                        Ok::<_, Infallible>(
                            Response::builder()
                                .header("content-type", "text/event-stream")
                                .body(Full::new(Bytes::from(data)))
                                .unwrap(),
                        )
                    }
                });
                let _ = http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), svc)
                    .await;
            });
        }
    });
    let tmp = TempDir::new().unwrap();
    let ws = tmp.path().join("workspace");
    fs::create_dir_all(&ws).unwrap();
    let daemon = SpawnedDaemon::spawn(
        &tmp.path().join("state"),
        "rb_ctl_claude_pi_test",
        &[
            "--allow-workspace-root",
            ws.to_str().unwrap(),
            "--upstream-url",
            &upstream,
        ],
        &[("RUNBAY_UPSTREAM_API_KEY", "host-test-secret")],
    );
    let client = reqwest::Client::new();
    let status: serde_json::Value = client
        .get(format!("{}/v1/status", daemon.base_url))
        .bearer_auth(&daemon.token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    for runtime in ["claude", "pi"] {
        if status["adapters"][runtime]["available"] != true {
            eprintln!("SKIP real {runtime}: CLI not installed");
            continue;
        }
        let reg = client
            .post(format!("{}/agents", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&serde_json::json!({"agent_id":runtime,"runtime":runtime,"workspace_dir":ws}))
            .send()
            .await
            .unwrap();
        assert_eq!(reg.status(), 201);
        let run = client.post(format!("{}/agents/{runtime}/run", daemon.base_url)).bearer_auth(&daemon.token)
            .json(&serde_json::json!({"prompt":"Say RUNBAY_CLI_OK only","allowed_model":"test-model","timeout_secs":30}))
            .send().await.unwrap();
        assert_eq!(run.status(), 202);
        let run: serde_json::Value = run.json().await.unwrap();
        let run_id = run["run_id"].as_str().unwrap();
        let events = collect_run_sse(&client, &daemon, run_id, None).await;
        let result: serde_json::Value = client
            .get(format!("{}/runs/{run_id}", daemon.base_url))
            .bearer_auth(&daemon.token)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            result["state"], "succeeded",
            "{runtime}: {result} {events:?}"
        );
        for field in ["sandbox_enforced", "network_isolated", "secretless_gateway"] {
            assert_eq!(result[field], true, "{runtime}: {field}");
        }
        let output = format!("{events:?}");
        assert!(
            output.contains("RUNBAY_CLI_OK"),
            "{runtime}: no answer: {output}"
        );
        assert!(!output.contains("host-test-secret"));
        {
            let mut seen = requests.lock().unwrap();
            assert!(!seen.is_empty(), "real CLI must call the gateway");
            for (path, key, body) in seen.iter() {
                assert_eq!(path, "/v1/messages");
                assert_eq!(key, "host-test-secret");
                assert_eq!(body["model"], "test-model");
                assert!(body.get("stream_options").is_none());
            }
            seen.clear();
        }

        if runtime != "pi" {
            continue;
        }

        let rejected: serde_json::Value = client
            .post(format!("{}/agents/{runtime}/run", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&serde_json::json!({"prompt":"FORCE_UPSTREAM_AUTH_ERROR","allowed_model":"test-model","timeout_secs":30}))
            .send().await.unwrap().json().await.unwrap();
        let rejected_id = rejected["run_id"].as_str().unwrap();
        let events = collect_run_sse(&client, &daemon, rejected_id, None).await;
        let failed: serde_json::Value = client
            .get(format!("{}/runs/{rejected_id}", daemon.base_url))
            .bearer_auth(&daemon.token)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            failed["state"], "failed",
            "{runtime} must not report API failure as success: {events:?}"
        );
    }
    upstream_task.abort();
}

impl SpawnedDaemon {
    fn spawn(
        state_dir: &Path,
        token: &str,
        extra_args: &[&str],
        extra_env: &[(&str, &str)],
    ) -> Self {
        let bin = env!("CARGO_BIN_EXE_runbayd");
        let mut cmd = Command::new(bin);
        cmd.arg("serve")
            .arg("--port")
            .arg("0")
            .arg("--state-dir")
            .arg(state_dir)
            .arg("--token")
            .arg(token);
        for a in extra_args {
            cmd.arg(a);
        }
        for (k, v) in extra_env {
            cmd.env(k, v);
        }
        cmd.stdout(Stdio::piped()).stderr(Stdio::inherit());
        let mut child = cmd.spawn().expect("failed to spawn runbayd binary");
        let stdout = child.stdout.take().expect("runbayd stdout");
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let mut addr = None;
        for _ in 0..20 {
            line.clear();
            let n = reader.read_line(&mut line).expect("read readiness line");
            if n == 0 {
                break;
            }
            if let Some(rest) = line.trim().strip_prefix("RUNBAYD_READY ") {
                let v: serde_json::Value =
                    serde_json::from_str(rest).expect("valid readiness json");
                addr = v.get("addr").and_then(|a| a.as_str()).map(str::to_string);
                break;
            }
        }
        let addr = addr.expect("runbayd did not emit RUNBAYD_READY");
        // Drain remaining stdout on a background thread so child never blocks on pipe.
        std::thread::spawn(move || {
            let mut sink = String::new();
            while reader.read_line(&mut sink).unwrap_or(0) > 0 {
                sink.clear();
            }
        });
        Self {
            child,
            base_url: format!("http://{addr}"),
            token: token.to_string(),
            _state_dir_path: state_dir.to_path_buf(),
        }
    }

    fn stop_gracefully(&mut self) {
        #[cfg(unix)]
        unsafe {
            libc::kill(self.child.id() as i32, libc::SIGTERM);
        }
        let deadline = Instant::now() + Duration::from_secs(4);
        while Instant::now() < deadline {
            if self.child.try_wait().ok().flatten().is_some() {
                return;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for SpawnedDaemon {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            self.stop_gracefully();
        }
    }
}

#[derive(Debug, Clone)]
struct ParsedSse {
    id: u64,
    event: String,
    data: serde_json::Value,
}

fn parse_sse_blocks(raw: &str) -> Vec<ParsedSse> {
    let mut out = Vec::new();
    for block in raw.split("\n\n") {
        let trimmed = block.trim();
        if trimmed.is_empty() || trimmed.starts_with(':') {
            continue;
        }
        let mut id = 0u64;
        let mut event = String::new();
        let mut data_str = String::new();
        for line in trimmed.lines() {
            if let Some(v) = line.strip_prefix("id:") {
                id = v.trim().parse().unwrap_or(0);
            } else if let Some(v) = line.strip_prefix("event:") {
                event = v.trim().to_string();
            } else if let Some(v) = line.strip_prefix("data:") {
                if !data_str.is_empty() {
                    data_str.push('\n');
                }
                data_str.push_str(v.trim());
            }
        }
        if !event.is_empty() {
            let data = serde_json::from_str(&data_str).unwrap_or(serde_json::Value::Null);
            out.push(ParsedSse { id, event, data });
        }
    }
    out
}

async fn collect_run_sse(
    client: &reqwest::Client,
    daemon: &SpawnedDaemon,
    run_id: &str,
    last_event_id: Option<u64>,
) -> Vec<ParsedSse> {
    let mut req = client
        .get(format!("{}/runs/{}/events", daemon.base_url, run_id))
        .header("Authorization", format!("Bearer {}", daemon.token));
    if let Some(id) = last_event_id {
        req = req.header("Last-Event-ID", id.to_string());
    }
    let resp = tokio::time::timeout(Duration::from_secs(15), req.send())
        .await
        .expect("SSE connect timed out")
        .expect("SSE request failed");
    assert_eq!(resp.status(), 200);
    let body = tokio::time::timeout(Duration::from_secs(15), resp.text())
        .await
        .expect("SSE stream did not terminate in time")
        .expect("SSE read failed");
    parse_sse_blocks(&body)
}

async fn wait_for_run_state(
    client: &reqwest::Client,
    daemon: &SpawnedDaemon,
    run_id: &str,
    expected: &[&str],
) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let resp = client
            .get(format!("{}/runs/{}", daemon.base_url, run_id))
            .header("Authorization", format!("Bearer {}", daemon.token))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let v: serde_json::Value = resp.json().await.unwrap();
        let st = v["state"].as_str().unwrap_or_default();
        if expected.contains(&st) {
            return v;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    panic!("run {run_id} never reached any of {expected:?}");
}

#[tokio::test]
async fn test_daemon_auth_and_access_boundaries() {
    let tmp = TempDir::new().unwrap();
    let state_dir = tmp.path().join("state");
    let allowed_root = tmp.path().join("allowed");
    let ok_ws = allowed_root.join("repo");
    let outside_ws = tmp.path().join("outside");
    for d in [&ok_ws, &outside_ws] {
        fs::create_dir_all(d).unwrap();
    }

    // Launch WITHOUT --enable-fixture-adapter to verify fixture is off by default.
    let daemon = SpawnedDaemon::spawn(
        &state_dir,
        "rb_ctl_test_secret_token",
        &["--allow-workspace-root", allowed_root.to_str().unwrap()],
        &[],
    );
    let client = reqwest::Client::new();

    // /health is unauthenticated liveness only
    let health = client
        .get(format!("{}/health", daemon.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(health.status(), 200);

    // Unauthenticated & wrong-token requests MUST get 401
    let unauth = client
        .get(format!("{}/v1/status", daemon.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(unauth.status(), 401);

    let bad_tok = client
        .get(format!("{}/agents", daemon.base_url))
        .header("Authorization", "Bearer wrong-token")
        .send()
        .await
        .unwrap();
    assert_eq!(bad_tok.status(), 401);

    // Valid token gets status with honest capabilities
    let status: serde_json::Value = client
        .get(format!("{}/v1/status", daemon.base_url))
        .header("Authorization", format!("Bearer {}", daemon.token))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["service"], "runbayd");
    assert!(status["adapters"].get("fixture").is_none());
    assert_eq!(status["adapters"]["codex"]["tier"], "one_shot");
    assert_eq!(status["adapters"]["codex"]["supports_hot_process"], false);

    // Fixture adapter rejected when not opted-in
    let fixture_rejected = client
        .post(format!("{}/agents", daemon.base_url))
        .header("Authorization", format!("Bearer {}", daemon.token))
        .json(&serde_json::json!({
            "agent_id": "a1",
            "runtime": "fixture",
            "workspace_dir": ok_ws.to_str().unwrap()
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(fixture_rejected.status(), 400);

    // Traversal agent_id rejected -> 400
    let bad_id = client
        .post(format!("{}/agents", daemon.base_url))
        .header("Authorization", format!("Bearer {}", daemon.token))
        .json(&serde_json::json!({
            "agent_id": "../escape",
            "runtime": "codex",
            "workspace_dir": ok_ws.to_str().unwrap()
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(bad_id.status(), 400);

    // Workspace outside --allow-workspace-root rejected -> 403
    if status["adapters"]["codex"]["available"] == true {
        let bad_ws = client
            .post(format!("{}/agents", daemon.base_url))
            .header("Authorization", format!("Bearer {}", daemon.token))
            .json(&serde_json::json!({
                "agent_id": "codex-outside",
                "runtime": "codex",
                "workspace_dir": outside_ws.to_str().unwrap()
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(bad_ws.status(), 403);
    }

    // Oversized JSON body (>256 KiB) rejected -> 413
    let huge = "x".repeat(300 * 1024);
    let too_big = client
        .post(format!("{}/agents", daemon.base_url))
        .header("Authorization", format!("Bearer {}", daemon.token))
        .body(format!("{{\"agent_id\":\"a\",\"prompt\":\"{huge}\"}}"))
        .send()
        .await
        .unwrap();
    assert_eq!(too_big.status(), 413);
}

#[tokio::test]
async fn test_daemon_register_submit_sse_gateway_and_sandbox_isolation() {
    let probe_tmp = TempDir::new().unwrap();
    if !LocalOsSandbox::new(probe_tmp.path(), &[]).is_available() {
        panic!("FAIL_CLOSED: OS sandbox unavailable in test environment");
    }

    let mock = MockUpstreamServer::start().await.unwrap();
    let tmp = TempDir::new().unwrap();
    let state_dir = tmp.path().join("state");
    let ws = tmp.path().join("workspace");
    let host_secret_dir = tmp.path().join("host_secret");
    fs::create_dir_all(&ws).unwrap();
    fs::create_dir_all(&host_secret_dir).unwrap();
    fs::write(ws.join("README.md"), "# Synthetic Workspace\n").unwrap();
    let outside_write = host_secret_dir.join("pwned.txt");

    let daemon = SpawnedDaemon::spawn(
        &state_dir,
        "rb_ctl_e2e_token",
        &[
            "--enable-fixture-adapter",
            "--allow-workspace-root",
            ws.to_str().unwrap(),
            "--upstream-url",
            &mock.base_url(),
        ],
        &[("RUNBAY_UPSTREAM_API_KEY", "host-real-key-never-in-sandbox")],
    );
    let client = reqwest::Client::new();

    // 1. Register agent
    let reg = client
        .post(format!("{}/agents", daemon.base_url))
        .header("Authorization", format!("Bearer {}", daemon.token))
        .json(&serde_json::json!({
            "agent_id": "worker-1",
            "runtime": "fixture",
            "workspace_dir": ws.to_str().unwrap()
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(reg.status(), 201);
    let reg_json: serde_json::Value = reg.json().await.unwrap();
    assert_eq!(reg_json["agent_id"], "worker-1");
    assert_eq!(reg_json["capabilities"]["tier"], "one_shot");

    // 2. List & get agent
    let list_json: serde_json::Value = client
        .get(format!("{}/agents", daemon.base_url))
        .header("Authorization", format!("Bearer {}", daemon.token))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list_json["agents"].as_array().unwrap().len(), 1);

    // 3. Submit streaming echo run & observe full SSE lifecycle
    let echo_prompt = serde_json::json!({
        "action": "echo",
        "lines": ["hello-1", "hello-2", "hello-3"],
        "delay_ms": 15
    })
    .to_string();
    let sub1 = client
        .post(format!("{}/agents/worker-1/run", daemon.base_url))
        .header("Authorization", format!("Bearer {}", daemon.token))
        .json(&serde_json::json!({"prompt": echo_prompt}))
        .send()
        .await
        .unwrap();
    assert_eq!(sub1.status(), 202);
    let run1: serde_json::Value = sub1.json().await.unwrap();
    let run1_id = run1["run_id"].as_str().unwrap().to_string();

    let events1 = collect_run_sse(&client, &daemon, &run1_id, None).await;
    let ev_types: Vec<&str> = events1.iter().map(|e| e.event.as_str()).collect();
    assert!(ev_types.contains(&"run.queued"), "events: {ev_types:?}");
    assert!(ev_types.contains(&"run.started"), "events: {ev_types:?}");
    assert!(ev_types.contains(&"agent.event"), "events: {ev_types:?}");
    assert_eq!(ev_types.last().copied(), Some("run.succeeded"));
    let final1: serde_json::Value = client
        .get(format!("{}/runs/{}", daemon.base_url, run1_id))
        .header("Authorization", format!("Bearer {}", daemon.token))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(final1["state"], "succeeded");
    assert_eq!(final1["exit_code"], 0);
    assert_eq!(final1["sandbox_enforced"], true);
    assert_eq!(final1["network_isolated"], true);

    // 4. Submit secretless gateway run: MUST block direct internet (1.1.1.1) AND reach gateway
    let gw_prompt = serde_json::json!({
        "action": "call_gateway",
        "model": "test-model",
        "check_direct_net": true
    })
    .to_string();
    let sub2 = client
        .post(format!("{}/agents/worker-1/run", daemon.base_url))
        .header("Authorization", format!("Bearer {}", daemon.token))
        .json(&serde_json::json!({"prompt": gw_prompt, "allowed_model": "test-model"}))
        .send()
        .await
        .unwrap();
    assert_eq!(sub2.status(), 202);
    let run2_id = sub2.json::<serde_json::Value>().await.unwrap()["run_id"]
        .as_str()
        .unwrap()
        .to_string();
    let events2 = collect_run_sse(&client, &daemon, &run2_id, None).await;
    assert_eq!(
        events2.last().map(|e| e.event.as_str()),
        Some("run.succeeded"),
        "gateway run events: {events2:?}"
    );
    let gw_event = events2
        .iter()
        .find(|e| e.data["data"]["type"] == "fixture.gateway_ok")
        .expect("expected fixture.gateway_ok event");
    assert_eq!(gw_event.data["data"]["direct_net_blocked"], true);
    assert!(gw_event.data["data"]["response_preview"]
        .as_str()
        .unwrap_or_default()
        .contains("Multi-Version Concurrency Control"));
    // Real upstream key must NEVER leak into SSE output and MUST be injected on upstream hop
    let all_sse_str = format!("{events2:?}");
    assert!(!all_sse_str.contains("host-real-key-never-in-sandbox"));
    assert_eq!(
        mock.seen_auth_headers(),
        vec!["Bearer host-real-key-never-in-sandbox".to_string()]
    );

    // 5. Sandbox escape check: writing outside workspace & reading daemon control token MUST be denied
    let token_file = state_dir.join("control/control.token");
    let esc_prompt = serde_json::json!({
        "action": "attempt_escape",
        "read_path": token_file.to_str().unwrap(),
        "write_path": outside_write.to_str().unwrap()
    })
    .to_string();
    let sub3 = client
        .post(format!("{}/agents/worker-1/run", daemon.base_url))
        .header("Authorization", format!("Bearer {}", daemon.token))
        .json(&serde_json::json!({"prompt": esc_prompt}))
        .send()
        .await
        .unwrap();
    assert_eq!(sub3.status(), 202);
    let run3_id = sub3.json::<serde_json::Value>().await.unwrap()["run_id"]
        .as_str()
        .unwrap()
        .to_string();
    let events3 = collect_run_sse(&client, &daemon, &run3_id, None).await;
    assert_eq!(
        events3.last().map(|e| e.event.as_str()),
        Some("run.succeeded"),
        "escape check failed: {events3:?}"
    );
    assert!(!outside_write.exists());

    // 6. Isolated MVCC CoW workspace write does NOT mutate source workspace
    let wf_prompt = serde_json::json!({
        "action": "write_file",
        "rel_path": "generated.txt",
        "content": "isolated-cow-write"
    })
    .to_string();
    let sub4 = client
        .post(format!("{}/agents/worker-1/run", daemon.base_url))
        .header("Authorization", format!("Bearer {}", daemon.token))
        .json(&serde_json::json!({"prompt": wf_prompt}))
        .send()
        .await
        .unwrap();
    assert_eq!(sub4.status(), 202);
    let run4_id = sub4.json::<serde_json::Value>().await.unwrap()["run_id"]
        .as_str()
        .unwrap()
        .to_string();
    let events4 = collect_run_sse(&client, &daemon, &run4_id, None).await;
    assert_eq!(
        events4.last().map(|e| e.event.as_str()),
        Some("run.succeeded")
    );
    assert!(
        !ws.join("generated.txt").exists(),
        "source workspace must remain unmutated by isolated CoW run"
    );

    // 7. MVCC snapshot leases cleanly released
    let agent_after: serde_json::Value = client
        .get(format!("{}/agents/worker-1", daemon.base_url))
        .header("Authorization", format!("Bearer {}", daemon.token))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(agent_after["active_leases"], 0);
}

#[tokio::test]
async fn test_daemon_failure_and_sse_disconnect_reconnect() {
    let tmp = TempDir::new().unwrap();
    let state_dir = tmp.path().join("state");
    let ws = tmp.path().join("ws");
    fs::create_dir_all(&ws).unwrap();

    let daemon = SpawnedDaemon::spawn(
        &state_dir,
        "rb_ctl_reconnect_tok",
        &["--enable-fixture-adapter"],
        &[],
    );
    let client = reqwest::Client::new();

    let reg = client
        .post(format!("{}/agents", daemon.base_url))
        .header("Authorization", format!("Bearer {}", daemon.token))
        .json(&serde_json::json!({
            "agent_id": "worker-2",
            "runtime": "fixture",
            "workspace_dir": ws.to_str().unwrap()
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(reg.status(), 201);

    // Failure path: non-zero exit code surfaces cleanly
    let fail_prompt = serde_json::json!({
        "action": "fail",
        "exit_code": 7,
        "message": "simulated_worker_failure"
    })
    .to_string();
    let fail_id = client
        .post(format!("{}/agents/worker-2/run", daemon.base_url))
        .header("Authorization", format!("Bearer {}", daemon.token))
        .json(&serde_json::json!({"prompt": fail_prompt}))
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap()["run_id"]
        .as_str()
        .unwrap()
        .to_string();
    let fail_events = collect_run_sse(&client, &daemon, &fail_id, None).await;
    assert_eq!(
        fail_events.last().map(|e| e.event.as_str()),
        Some("run.failed")
    );
    let fail_status: serde_json::Value = client
        .get(format!("{}/runs/{}", daemon.base_url, fail_id))
        .header("Authorization", format!("Bearer {}", daemon.token))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(fail_status["state"], "failed");
    assert_eq!(fail_status["exit_code"], 7);
    assert_eq!(fail_status["error_code"], "nonzero_exit");

    // SSE client disconnect MUST NOT abort the run; reconnect with Last-Event-ID replays tail
    let slow_prompt = serde_json::json!({
        "action": "echo",
        "lines": ["part-0", "part-1", "part-2", "part-3"],
        "delay_ms": 150
    })
    .to_string();
    let slow_id = client
        .post(format!("{}/agents/worker-2/run", daemon.base_url))
        .header("Authorization", format!("Bearer {}", daemon.token))
        .json(&serde_json::json!({"prompt": slow_prompt}))
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap()["run_id"]
        .as_str()
        .unwrap()
        .to_string();

    // Open SSE stream, read first chunk, and immediately drop the connection mid-run!
    let resp = client
        .get(format!("{}/runs/{}/events", daemon.base_url, slow_id))
        .header("Authorization", format!("Bearer {}", daemon.token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let mut stream = resp.bytes_stream();
    let mut first_buf = String::new();
    let mut first_seq = 0u64;
    while let Some(chunk) = stream.next().await {
        let bytes = chunk.unwrap();
        first_buf.push_str(&String::from_utf8_lossy(&bytes));
        let parsed = parse_sse_blocks(&first_buf);
        if let Some(ev) = parsed.first() {
            first_seq = ev.id;
            break;
        }
    }
    drop(stream); // Simulate client disconnect!
    assert!(first_seq >= 1);

    // Wait for run to complete independently despite client disconnect
    let done = wait_for_run_state(&client, &daemon, &slow_id, &["succeeded", "failed"]).await;
    assert_eq!(
        done["state"], "succeeded",
        "client disconnect must not abort or fail the run"
    );

    // Reconnect using Last-Event-ID and verify strictly newer events + terminal event replay
    let replayed = collect_run_sse(&client, &daemon, &slow_id, Some(first_seq)).await;
    assert!(!replayed.is_empty());
    assert!(replayed.iter().all(|e| e.id > first_seq));
    assert_eq!(
        replayed.last().map(|e| e.event.as_str()),
        Some("run.succeeded")
    );
}

#[tokio::test]
async fn test_daemon_cancel_queue_limit_timeout_pgid_cleanup_and_delete() {
    let tmp = TempDir::new().unwrap();
    let state_dir = tmp.path().join("state");
    let ws = tmp.path().join("ws");
    fs::create_dir_all(&ws).unwrap();

    // 1 concurrent + 1 queued -> 3rd submission must return 429 queue_full
    let daemon = SpawnedDaemon::spawn(
        &state_dir,
        "rb_ctl_cancel_tok",
        &[
            "--enable-fixture-adapter",
            "--max-concurrent-runs",
            "1",
            "--max-queued-runs",
            "1",
        ],
        &[],
    );
    let client = reqwest::Client::new();

    assert_eq!(
        client
            .post(format!("{}/agents", daemon.base_url))
            .header("Authorization", format!("Bearer {}", daemon.token))
            .json(&serde_json::json!({
                "agent_id": "worker-cancel",
                "runtime": "fixture",
                "workspace_dir": ws.to_str().unwrap()
            }))
            .send()
            .await
            .unwrap()
            .status(),
        201
    );

    let sleep_prompt = serde_json::json!({
        "action": "sleep_with_grandchild",
        "sleep_secs": 30,
        "pid_file": "pids.json"
    })
    .to_string();

    let run1_id = client
        .post(format!("{}/agents/worker-cancel/run", daemon.base_url))
        .header("Authorization", format!("Bearer {}", daemon.token))
        .json(&serde_json::json!({"prompt": sleep_prompt}))
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap()["run_id"]
        .as_str()
        .unwrap()
        .to_string();

    // Wait until sleeper is actively running and grandchild PID is reported
    let run1_running = wait_for_run_state(&client, &daemon, &run1_id, &["running"]).await;
    let branch_ws = PathBuf::from(run1_running["branch_workspace"].as_str().unwrap());
    let mut pids: Option<(i32, i32)> = None;
    for _ in 0..80 {
        if let Ok(raw) = fs::read_to_string(branch_ws.join("pids.json")) {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&raw) {
                if let (Some(w), Some(g)) = (v["worker_pid"].as_i64(), v["grandchild_pid"].as_i64())
                {
                    pids = Some((w as i32, g as i32));
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let (worker_pid, grandchild_pid) = pids.expect("expected sleeper pids.json");

    // 2nd run enters bounded live queue (202)
    let sub2 = client
        .post(format!("{}/agents/worker-cancel/run", daemon.base_url))
        .header("Authorization", format!("Bearer {}", daemon.token))
        .json(&serde_json::json!({"prompt": "{\"action\":\"echo\",\"lines\":[\"q\"]}"}))
        .send()
        .await
        .unwrap();
    assert_eq!(sub2.status(), 202);
    let run2_id = sub2.json::<serde_json::Value>().await.unwrap()["run_id"]
        .as_str()
        .unwrap()
        .to_string();

    // 3rd run exceeds max_queued_runs=1 -> 429 Too Many Requests
    let sub3 = client
        .post(format!("{}/agents/worker-cancel/run", daemon.base_url))
        .header("Authorization", format!("Bearer {}", daemon.token))
        .json(&serde_json::json!({"prompt": "{\"action\":\"echo\",\"lines\":[\"overflow\"]}"}))
        .send()
        .await
        .unwrap();
    assert_eq!(sub3.status(), 429);

    // Cancel queued run #2 first
    let cancel2: serde_json::Value = client
        .post(format!("{}/runs/{}/cancel", daemon.base_url, run2_id))
        .header("Authorization", format!("Bearer {}", daemon.token))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(cancel2["state"], "cancelled");

    // Cancel running run #1 -> must kill & reap BOTH child and grandchild
    let cancel1: serde_json::Value = client
        .post(format!("{}/runs/{}/cancel", daemon.base_url, run1_id))
        .header("Authorization", format!("Bearer {}", daemon.token))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(cancel1["state"], "cancelled");

    #[cfg(target_os = "macos")]
    {
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_ne!(
            unsafe { libc::kill(worker_pid, 0) },
            0,
            "worker pid {worker_pid} must be terminated"
        );
        assert_ne!(
            unsafe { libc::kill(grandchild_pid, 0) },
            0,
            "grandchild pid {grandchild_pid} must be terminated"
        );
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (worker_pid, grandchild_pid);
    }

    // Timeout enforcement: timeout_secs=1 kills sleeper and transitions to failed (timeout)
    let timeout_id = client
        .post(format!("{}/agents/worker-cancel/run", daemon.base_url))
        .header("Authorization", format!("Bearer {}", daemon.token))
        .json(&serde_json::json!({
            "prompt": sleep_prompt,
            "timeout_secs": 1
        }))
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap()["run_id"]
        .as_str()
        .unwrap()
        .to_string();
    let timed_out =
        wait_for_run_state(&client, &daemon, &timeout_id, &["failed", "cancelled"]).await;
    assert_eq!(timed_out["state"], "failed");
    assert_eq!(timed_out["error_code"], "timeout");

    // Agent delete cleans up cleanly and subsequent GET returns 404
    let del = client
        .delete(format!("{}/agents/worker-cancel", daemon.base_url))
        .header("Authorization", format!("Bearer {}", daemon.token))
        .send()
        .await
        .unwrap();
    assert_eq!(del.status(), 200);
    let gone = client
        .get(format!("{}/agents/worker-cancel", daemon.base_url))
        .header("Authorization", format!("Bearer {}", daemon.token))
        .send()
        .await
        .unwrap();
    assert_eq!(gone.status(), 404);
}

#[tokio::test]
async fn test_daemon_restart_recovery_semantics() {
    let tmp = TempDir::new().unwrap();
    let state_dir = tmp.path().join("state");
    let ws = tmp.path().join("ws");
    fs::create_dir_all(&ws).unwrap();
    let client = reqwest::Client::new();

    {
        let mut daemon = SpawnedDaemon::spawn(
            &state_dir,
            "rb_ctl_restart_tok",
            &["--enable-fixture-adapter"],
            &[],
        );
        assert_eq!(
            client
                .post(format!("{}/agents", daemon.base_url))
                .header("Authorization", format!("Bearer {}", daemon.token))
                .json(&serde_json::json!({
                    "agent_id": "durable-agent",
                    "runtime": "fixture",
                    "workspace_dir": ws.to_str().unwrap()
                }))
                .send()
                .await
                .unwrap()
                .status(),
            201
        );
        daemon.stop_gracefully();
    }

    // Simulate an unclean daemon crash by inserting an interrupted `running` row into SQLite
    let conn = rusqlite::Connection::open(state_dir.join("runbayd.sqlite")).unwrap();
    conn.execute(
        "INSERT INTO runs (
            run_id, agent_id, runtime, prompt, state, priority, capability_tier,
            workspace_rev, sandbox_enforced, network_isolated, secretless_gateway,
            events_count, events_dropped, late_events_rejected, created_at
         ) VALUES (
            'run_crashed_before_restart', 'durable-agent', 'fixture', 'ping', 'running',
            'interactive', 'one_shot', 0, 1, 1, 0, 0, 0, 0, '2026-11-22T00:00:00Z'
         )",
        [],
    )
    .unwrap();
    drop(conn);

    // Restart runbayd on the same state_dir
    let daemon2 = SpawnedDaemon::spawn(
        &state_dir,
        "rb_ctl_restart_tok",
        &["--enable-fixture-adapter"],
        &[],
    );
    // Registered agent persists
    let ag = client
        .get(format!("{}/agents/durable-agent", daemon2.base_url))
        .header("Authorization", format!("Bearer {}", daemon2.token))
        .send()
        .await
        .unwrap();
    assert_eq!(ag.status(), 200);

    // Interrupted run is explicitly marked failed (daemon_restarted)
    let rec: serde_json::Value = client
        .get(format!(
            "{}/runs/run_crashed_before_restart",
            daemon2.base_url
        ))
        .header("Authorization", format!("Bearer {}", daemon2.token))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(rec["state"], "failed");
    assert_eq!(rec["error_code"], "daemon_restarted");
}

#[tokio::test]
async fn test_daemon_real_codex_binary_via_secretless_gateway_when_installed() {
    if !cfg!(target_os = "macos") {
        return;
    }
    let has_codex = ["/opt/homebrew/bin/codex", "/usr/local/bin/codex"]
        .iter()
        .any(|p| Path::new(p).is_file());
    if !has_codex {
        return;
    }

    let mock = MockUpstreamServer::start().await.unwrap();
    let tmp = TempDir::new().unwrap();
    let state_dir = tmp.path().join("state");
    let ws = tmp.path().join("ws");
    fs::create_dir_all(&ws).unwrap();
    fs::write(ws.join("note.txt"), "hello from runbay workspace\n").unwrap();

    let daemon = SpawnedDaemon::spawn(
        &state_dir,
        "rb_ctl_codex_gw_tok",
        &[
            "--allow-workspace-root",
            ws.to_str().unwrap(),
            "--upstream-url",
            &mock.base_url(),
        ],
        &[(
            "RUNBAY_UPSTREAM_API_KEY",
            "codex-host-secret-never-in-sandbox",
        )],
    );
    let client = reqwest::Client::new();

    let reg = client
        .post(format!("{}/agents", daemon.base_url))
        .header("Authorization", format!("Bearer {}", daemon.token))
        .json(&serde_json::json!({
            "agent_id": "codex-gw-agent",
            "runtime": "codex",
            "workspace_dir": ws.to_str().unwrap()
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(reg.status(), 201);

    let sub = client
        .post(format!("{}/agents/codex-gw-agent/run", daemon.base_url))
        .header("Authorization", format!("Bearer {}", daemon.token))
        .json(&serde_json::json!({
            "prompt": "Say RUNBAY_MOCK_OK",
            "allowed_model": "gpt-5.4-mini",
            "timeout_secs": 20
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(sub.status(), 202);
    let run_id = sub.json::<serde_json::Value>().await.unwrap()["run_id"]
        .as_str()
        .unwrap()
        .to_string();

    let events = collect_run_sse(&client, &daemon, &run_id, None).await;
    let final_run: serde_json::Value = client
        .get(format!("{}/runs/{}", daemon.base_url, run_id))
        .header("Authorization", format!("Bearer {}", daemon.token))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(
        final_run["state"], "succeeded",
        "real codex via gateway failed: run={final_run} events={events:?}"
    );
    assert_eq!(final_run["sandbox_enforced"], true);
    assert_eq!(final_run["network_isolated"], true);
    assert_eq!(final_run["secretless_gateway"], true);
    let sse_dump = format!("{events:?}");
    assert!(
        sse_dump.contains("RUNBAY_MOCK_OK"),
        "expected codex output in SSE: {sse_dump}"
    );
    assert!(!sse_dump.contains("codex-host-secret-never-in-sandbox"));
    assert!(
        mock.seen_auth_headers()
            .iter()
            .any(|h| h == "Bearer codex-host-secret-never-in-sandbox"),
        "upstream must see host-injected secret key"
    );
}
