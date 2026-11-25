use anyhow::{bail, Result};
use bytes::Bytes;
use futures_util::StreamExt;
use http::{Method, Request, Response, StatusCode};
use http_body_util::{combinators::BoxBody, BodyExt, Full, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, Notify};

pub type SpecboxCallback = Arc<dyn Fn(&str) -> Result<f64, String> + Send + Sync>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayCallRecord {
    pub call_id: u64,
    pub run_token_prefix: String,
    pub model: String,
    pub status_code: u16,
    pub ttft_ms: f64,
    pub total_ms: f64,
    pub prompt_tokens: u64,
    pub cached_tokens: u64,
    pub completion_tokens: u64,
    pub specbox_tool_detected: Option<String>,
    pub specbox_detect_ms: Option<f64>,
    pub specbox_prewarm_ms: Option<f64>,
    pub specbox_prewarm_error: Option<String>,
    pub aborted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayStats {
    pub gateway_port: u16,
    pub total_calls: usize,
    pub unauthorized_attempts_blocked: u64,
    pub calls: Vec<GatewayCallRecord>,
}

#[derive(Clone)]
struct TokenMeta {
    #[allow(dead_code)]
    run_id: String,
    allowed_model: Option<String>,
    revoked: Arc<AtomicBool>,
    cancel_notify: Arc<Notify>,
}

impl TokenMeta {
    fn is_revoked(&self) -> bool {
        self.revoked.load(Ordering::SeqCst)
    }

    fn revoke(&self) {
        self.revoked.store(true, Ordering::SeqCst);
        self.cancel_notify.notify_waiters();
    }
}

struct GatewayInner {
    upstream_base_url: reqwest::Url,
    real_api_key: String,
    client: reqwest::Client,
    on_specbox_tool: Option<SpecboxCallback>,
    tokens: Mutex<HashMap<String, TokenMeta>>,
    records: Mutex<Vec<GatewayCallRecord>>,
    unauthorized_attempts: AtomicU64,
    next_call_id: AtomicU64,
}

/// Strictly map incoming gateway routes to canonical upstream subpaths.
/// Rejects arbitrary routes and prevents authority-confusion attacks such as `/v1.attacker.example/...`.
pub fn resolve_allowed_upstream_url(
    base_url: &reqwest::Url,
    method: &Method,
    req_path: &str,
) -> Option<reqwest::Url> {
    if method != Method::POST {
        return None;
    }
    let allowed_subpath = match req_path {
        "/v1/chat/completions" => "/chat/completions",
        "/v1/responses" => "/responses",
        "/v1/embeddings" => "/embeddings",
        _ => return None,
    };

    let mut target = base_url.clone();
    let base_path = base_url.path().trim_end_matches('/');
    target.set_path(&format!("{base_path}{allowed_subpath}"));
    target.set_query(None);
    target.set_fragment(None);

    // Hard invariant: resolved destination MUST keep the exact trusted upstream origin!
    if target.origin() != base_url.origin() {
        return None;
    }
    Some(target)
}

/// Async 127.0.0.1 OpenAI-compatible reverse proxy that injects real API keys outside sandboxes.
/// On Unix, may also bind a Unix domain socket (`bind_unix_socket`) so Linux `bwrap --unshare-net`
/// sandboxes can reach ONLY the gateway with zero host network exposure.
pub struct SecretlessLoopbackGateway {
    inner: Arc<GatewayInner>,
    port: u16,
    shutdown_tx: Option<oneshot::Sender<()>>,
    unix_sockets: Mutex<Vec<(std::path::PathBuf, oneshot::Sender<()>)>>,
}

impl SecretlessLoopbackGateway {
    pub async fn start(
        upstream_base_url: impl Into<String>,
        real_api_key: impl Into<String>,
        on_specbox_tool: Option<SpecboxCallback>,
    ) -> Result<Self> {
        let raw_url = upstream_base_url.into();
        let parsed = reqwest::Url::parse(raw_url.trim())?;
        if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
            bail!("upstream_base_url must be a valid http(s) URL with host: {raw_url}");
        }

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr: SocketAddr = listener.local_addr()?;
        let port = addr.port();

        let mut client_builder =
            reqwest::Client::builder().timeout(std::time::Duration::from_secs(45));
        if matches!(parsed.host_str(), Some("127.0.0.1" | "localhost" | "::1")) {
            client_builder = client_builder.no_proxy();
        }
        let client = client_builder.build()?;

        let inner = Arc::new(GatewayInner {
            upstream_base_url: parsed,
            real_api_key: real_api_key.into(),
            client,
            on_specbox_tool,
            tokens: Mutex::new(HashMap::new()),
            records: Mutex::new(Vec::new()),
            unauthorized_attempts: AtomicU64::new(0),
            next_call_id: AtomicU64::new(1),
        });

        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
        let accept_inner = Arc::clone(&inner);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => {
                        break;
                    }
                    accepted = listener.accept() => {
                        let Ok((stream, _peer)) = accepted else {
                            break;
                        };
                        let io = TokioIo::new(stream);
                        let svc_inner = Arc::clone(&accept_inner);
                        tokio::spawn(async move {
                            let service = service_fn(move |req| {
                                let state = Arc::clone(&svc_inner);
                                async move { Ok::<_, Infallible>(handle_request(state, req).await) }
                            });
                            let _ = http1::Builder::new().serve_connection(io, service).await;
                        });
                    }
                }
            }
        });

        Ok(Self {
            inner,
            port,
            shutdown_tx: Some(shutdown_tx),
            unix_sockets: Mutex::new(Vec::new()),
        })
    }

    /// Bind an additional Unix domain socket listener serving the exact same secretless gateway.
    /// This enables Linux `bubblewrap --unshare-net` sandboxes to reach ONLY this gateway via
    /// `--bind <sock> <sock>` without disabling network namespace isolation.
    #[cfg(unix)]
    pub async fn bind_unix_socket(
        &self,
        socket_path: impl AsRef<std::path::Path>,
    ) -> Result<std::path::PathBuf> {
        use std::os::unix::fs::PermissionsExt;
        let path = socket_path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        if path.exists() {
            let _ = std::fs::remove_file(&path);
        }
        let listener = tokio::net::UnixListener::bind(&path)?;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
        let canonical = std::fs::canonicalize(&path).unwrap_or_else(|_| path.clone());

        let (tx, mut rx) = oneshot::channel::<()>();
        let accept_inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut rx => break,
                    accepted = listener.accept() => {
                        let Ok((stream, _peer)) = accepted else { break };
                        let io = TokioIo::new(stream);
                        let svc_inner = Arc::clone(&accept_inner);
                        tokio::spawn(async move {
                            let service = service_fn(move |req| {
                                let state = Arc::clone(&svc_inner);
                                async move { Ok::<_, Infallible>(handle_request(state, req).await) }
                            });
                            let _ = http1::Builder::new().serve_connection(io, service).await;
                        });
                    }
                }
            }
        });

        self.unix_sockets.lock().push((canonical.clone(), tx));
        Ok(canonical)
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}/v1", self.port)
    }

    /// Mint a 256-bit OS CSPRNG opaque bearer credential bound to `run_id` and optional `allowed_model`.
    /// Credentials are never reused or overwritten across mints.
    pub fn mint_run_token(&self, run_id: &str, allowed_model: Option<String>) -> String {
        let mut tokens = self.inner.tokens.lock();
        loop {
            let mut buf = [0u8; 32];
            getrandom::getrandom(&mut buf).expect("OS CSPRNG unavailable");
            let token = format!("rb_{}", hex::encode(buf));
            if !tokens.contains_key(&token) {
                tokens.insert(
                    token.clone(),
                    TokenMeta {
                        run_id: run_id.to_string(),
                        allowed_model,
                        revoked: Arc::new(AtomicBool::new(false)),
                        cancel_notify: Arc::new(Notify::new()),
                    },
                );
                return token;
            }
        }
    }

    /// Revoke a run credential and immediately wake any pending or in-flight requests using it.
    pub fn revoke_run_token(&self, token: &str) {
        if let Some(meta) = self.inner.tokens.lock().get(token) {
            meta.revoke();
        }
    }

    pub fn stats(&self) -> GatewayStats {
        let calls = self.inner.records.lock().clone();
        GatewayStats {
            gateway_port: self.port,
            total_calls: calls.len(),
            unauthorized_attempts_blocked: self.inner.unauthorized_attempts.load(Ordering::Relaxed),
            calls,
        }
    }

    pub fn stop(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        for (path, tx) in self.unix_sockets.lock().drain(..) {
            let _ = tx.send(());
            let _ = std::fs::remove_file(path);
        }
    }
}

impl Drop for SecretlessLoopbackGateway {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        for (path, tx) in self.unix_sockets.lock().drain(..) {
            let _ = tx.send(());
            let _ = std::fs::remove_file(path);
        }
    }
}

fn json_response(status: StatusCode, body: Bytes) -> Response<BoxBody<Bytes, Infallible>> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(body).boxed())
        .unwrap()
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

async fn handle_request(
    state: Arc<GatewayInner>,
    req: Request<Incoming>,
) -> Response<BoxBody<Bytes, Infallible>> {
    if req.method() == Method::GET && req.uri().path() == "/health" {
        return json_response(StatusCode::OK, Bytes::from_static(br#"{"status":"ok"}"#));
    }

    // 1. Validate route allowlist & trusted upstream origin BEFORE touching credentials
    let Some(upstream_url) =
        resolve_allowed_upstream_url(&state.upstream_base_url, req.method(), req.uri().path())
    else {
        state.unauthorized_attempts.fetch_add(1, Ordering::Relaxed);
        return json_response(
            StatusCode::NOT_FOUND,
            Bytes::from_static(br#"{"error":"unsupported_gateway_route"}"#),
        );
    };

    // 2. Authenticate opaque bearer token
    let auth = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let bearer = auth
        .strip_prefix("Bearer ")
        .map(str::trim)
        .unwrap_or("")
        .to_string();

    let token_meta = {
        let g = state.tokens.lock();
        g.get(&bearer).cloned()
    };
    let Some(meta) = token_meta else {
        state.unauthorized_attempts.fetch_add(1, Ordering::Relaxed);
        return json_response(
            StatusCode::UNAUTHORIZED,
            Bytes::from_static(br#"{"error":"invalid_or_revoked_runbay_token"}"#),
        );
    };
    if meta.is_revoked() {
        state.unauthorized_attempts.fetch_add(1, Ordering::Relaxed);
        return json_response(
            StatusCode::UNAUTHORIZED,
            Bytes::from_static(br#"{"error":"invalid_or_revoked_runbay_token"}"#),
        );
    }

    // 3. Collect request body while listening for mid-body revocation
    let raw_bytes = tokio::select! {
        biased;
        _ = meta.cancel_notify.notified(), if !meta.is_revoked() => {
            state.unauthorized_attempts.fetch_add(1, Ordering::Relaxed);
            return json_response(
                StatusCode::UNAUTHORIZED,
                Bytes::from_static(br#"{"error":"token_revoked_during_request"}"#),
            );
        }
        collected = req.into_body().collect() => {
            match collected {
                Ok(b) => b.to_bytes(),
                Err(e) => {
                    return json_response(
                        StatusCode::BAD_REQUEST,
                        Bytes::from(serde_json::json!({"error": e.to_string()}).to_string()),
                    );
                }
            }
        }
    };

    // Re-check revocation after body read and before upstream dispatch
    if meta.is_revoked() {
        state.unauthorized_attempts.fetch_add(1, Ordering::Relaxed);
        return json_response(
            StatusCode::UNAUTHORIZED,
            Bytes::from_static(br#"{"error":"token_revoked_before_dispatch"}"#),
        );
    }

    let mut req_json: serde_json::Value = match serde_json::from_slice(&raw_bytes) {
        Ok(v) => v,
        Err(_) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                Bytes::from_static(br#"{"error":"invalid_json_body"}"#),
            );
        }
    };

    // 4. Enforce model scope before forwarding with host credentials
    let req_model_opt = req_json
        .get("model")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty());

    if let Some(ref allowed) = meta.allowed_model {
        if req_model_opt != Some(allowed.as_str()) {
            state.unauthorized_attempts.fetch_add(1, Ordering::Relaxed);
            return json_response(
                StatusCode::FORBIDDEN,
                Bytes::from_static(br#"{"error":"model_not_allowed_for_token"}"#),
            );
        }
    }

    let model = req_model_opt.unwrap_or("default").to_string();
    let is_stream = req_json
        .get("stream")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if is_stream && req_json.get("stream_options").is_none() {
        if let Some(obj) = req_json.as_object_mut() {
            obj.insert(
                "stream_options".to_string(),
                serde_json::json!({"include_usage": true}),
            );
        }
    }
    let forward_body = serde_json::to_vec(&req_json).unwrap_or_else(|_| raw_bytes.to_vec());

    let t0 = Instant::now();
    let call_id = state.next_call_id.fetch_add(1, Ordering::Relaxed);
    let token_prefix: String = bearer.chars().take(16).collect();

    // 5. Dispatch upstream request while watching for immediate revocation
    let send_fut = state
        .client
        .post(upstream_url)
        .header("Authorization", format!("Bearer {}", state.real_api_key))
        .header("Content-Type", "application/json")
        .header(
            "Accept",
            if is_stream {
                "text/event-stream"
            } else {
                "application/json"
            },
        )
        .body(forward_body)
        .send();

    let up_resp = tokio::select! {
        biased;
        _ = async {
            if meta.is_revoked() {
                return;
            }
            meta.cancel_notify.notified().await;
        } => {
            let total_ms = round2(t0.elapsed().as_secs_f64() * 1000.0);
            state.records.lock().push(GatewayCallRecord {
                call_id,
                run_token_prefix: token_prefix,
                model,
                status_code: 499,
                ttft_ms: total_ms,
                total_ms,
                prompt_tokens: 0,
                cached_tokens: 0,
                completion_tokens: 0,
                specbox_tool_detected: None,
                specbox_detect_ms: None,
                specbox_prewarm_ms: None,
                specbox_prewarm_error: None,
                aborted: true,
            });
            return json_response(
                StatusCode::UNAUTHORIZED,
                Bytes::from_static(br#"{"error":"token_revoked_in_flight"}"#),
            );
        }
        res = send_fut => match res {
            Ok(r) => r,
            Err(e) => {
                let total_ms = round2(t0.elapsed().as_secs_f64() * 1000.0);
                state.records.lock().push(GatewayCallRecord {
                    call_id,
                    run_token_prefix: token_prefix,
                    model,
                    status_code: 502,
                    ttft_ms: total_ms,
                    total_ms,
                    prompt_tokens: 0,
                    cached_tokens: 0,
                    completion_tokens: 0,
                    specbox_tool_detected: None,
                    specbox_detect_ms: None,
                    specbox_prewarm_ms: None,
                    specbox_prewarm_error: None,
                    aborted: false,
                });
                return json_response(
                    StatusCode::BAD_GATEWAY,
                    Bytes::from(serde_json::json!({"error": e.to_string()}).to_string()),
                );
            }
        }
    };

    let status_u16 = up_resp.status().as_u16();
    let status = StatusCode::from_u16(status_u16).unwrap_or(StatusCode::BAD_GATEWAY);

    if !is_stream {
        let resp_bytes = tokio::select! {
            biased;
            _ = async {
                if meta.is_revoked() {
                    return;
                }
                meta.cancel_notify.notified().await;
            } => {
                let total_ms = round2(t0.elapsed().as_secs_f64() * 1000.0);
                state.records.lock().push(GatewayCallRecord {
                    call_id,
                    run_token_prefix: token_prefix,
                    model,
                    status_code: 499,
                    ttft_ms: total_ms,
                    total_ms,
                    prompt_tokens: 0,
                    cached_tokens: 0,
                    completion_tokens: 0,
                    specbox_tool_detected: None,
                    specbox_detect_ms: None,
                    specbox_prewarm_ms: None,
                    specbox_prewarm_error: None,
                    aborted: true,
                });
                return json_response(
                    StatusCode::UNAUTHORIZED,
                    Bytes::from_static(br#"{"error":"token_revoked_in_flight"}"#),
                );
            }
            b = up_resp.bytes() => b.unwrap_or_default()
        };
        let total_ms = round2(t0.elapsed().as_secs_f64() * 1000.0);
        let (pt, ct, cmp) = extract_usage_from_json(&resp_bytes);
        state.records.lock().push(GatewayCallRecord {
            call_id,
            run_token_prefix: token_prefix,
            model,
            status_code: status_u16,
            ttft_ms: total_ms,
            total_ms,
            prompt_tokens: pt,
            cached_tokens: ct,
            completion_tokens: cmp,
            specbox_tool_detected: None,
            specbox_detect_ms: None,
            specbox_prewarm_ms: None,
            specbox_prewarm_error: None,
            aborted: false,
        });
        return json_response(status, resp_bytes);
    }

    // 6. Streaming SSE forwarding with immediate revocation wakeup on BOTH upstream read & downstream send
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Frame<Bytes>, Infallible>>(32);
    let stream_state = Arc::clone(&state);

    tokio::spawn(async move {
        let mut up_stream = up_resp.bytes_stream();
        let mut line_buf = String::new();
        let mut ttft_ms: Option<f64> = None;
        let mut prompt_tokens = 0u64;
        let mut cached_tokens = 0u64;
        let mut completion_tokens = 0u64;
        let mut spec_tool: Option<String> = None;
        let mut spec_detect_ms: Option<f64> = None;
        let mut prewarm_task: Option<tokio::task::JoinHandle<Result<f64, String>>> = None;
        let mut aborted = false;

        loop {
            if meta.is_revoked() {
                aborted = true;
                break;
            }
            let next_chunk = tokio::select! {
                biased;
                _ = meta.cancel_notify.notified() => {
                    aborted = true;
                    break;
                }
                item = up_stream.next() => item
            };
            let Some(chunk_res) = next_chunk else {
                break;
            };
            let Ok(chunk) = chunk_res else {
                aborted = true;
                break;
            };
            if ttft_ms.is_none() && !chunk.is_empty() {
                ttft_ms = Some(round2(t0.elapsed().as_secs_f64() * 1000.0));
            }

            // Passive telemetry + mid-stream SpecBox tool detection
            line_buf.push_str(&String::from_utf8_lossy(&chunk));
            while let Some(nl_pos) = line_buf.find('\n') {
                let line = line_buf[..nl_pos].trim().to_string();
                line_buf.drain(..=nl_pos);
                if let Some(dstr) = line.strip_prefix("data:").map(str::trim) {
                    if !dstr.is_empty() && dstr != "[DONE]" {
                        if let Ok(ch) = serde_json::from_str::<serde_json::Value>(dstr) {
                            if let Some(u) = ch.get("usage") {
                                if let Some(v) = u.get("prompt_tokens").and_then(|x| x.as_u64()) {
                                    prompt_tokens = v;
                                }
                                if let Some(v) = u.get("completion_tokens").and_then(|x| x.as_u64())
                                {
                                    completion_tokens = v;
                                }
                                if let Some(v) = u
                                    .get("prompt_tokens_details")
                                    .and_then(|d| d.get("cached_tokens"))
                                    .and_then(|x| x.as_u64())
                                {
                                    cached_tokens = v;
                                }
                            }
                            if spec_tool.is_none() {
                                if let Some(fn_name) = extract_first_tool_name(&ch) {
                                    spec_tool = Some(fn_name.clone());
                                    spec_detect_ms =
                                        Some(round2(t0.elapsed().as_secs_f64() * 1000.0));
                                    if let Some(cb) = stream_state.on_specbox_tool.as_ref() {
                                        let cb_clone = Arc::clone(cb);
                                        let name_owned = fn_name.clone();
                                        prewarm_task =
                                            Some(tokio::task::spawn_blocking(move || {
                                                cb_clone(&name_owned)
                                            }));
                                    }
                                }
                            }
                        }
                    }
                }
            }

            let sent = tokio::select! {
                biased;
                _ = meta.cancel_notify.notified() => {
                    aborted = true;
                    break;
                }
                res = tx.send(Ok(Frame::data(chunk))) => res
            };
            if sent.is_err() {
                aborted = true;
                break;
            }
        }

        let (specbox_prewarm_ms, specbox_prewarm_error) = match prewarm_task {
            Some(handle) => match handle.await {
                Ok(Ok(ms)) => (Some(round2(ms)), None),
                Ok(Err(err)) => (None, Some(err)),
                Err(join_err) => (None, Some(join_err.to_string())),
            },
            None => (None, None),
        };

        let total_ms = round2(t0.elapsed().as_secs_f64() * 1000.0);
        stream_state.records.lock().push(GatewayCallRecord {
            call_id,
            run_token_prefix: token_prefix,
            model,
            status_code: status_u16,
            ttft_ms: ttft_ms.unwrap_or(total_ms),
            total_ms,
            prompt_tokens,
            cached_tokens,
            completion_tokens,
            specbox_tool_detected: spec_tool,
            specbox_detect_ms: spec_detect_ms,
            specbox_prewarm_ms,
            specbox_prewarm_error,
            aborted,
        });
    });

    let stream = futures_util::stream::unfold(rx, |mut rx| async {
        rx.recv().await.map(|item| (item, rx))
    });
    let body = BodyExt::boxed(StreamBody::new(stream));
    Response::builder()
        .status(status)
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .body(body)
        .unwrap()
}

fn extract_usage_from_json(bytes: &[u8]) -> (u64, u64, u64) {
    let Ok(j) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        return (0, 0, 0);
    };
    let Some(u) = j.get("usage") else {
        return (0, 0, 0);
    };
    let pt = u.get("prompt_tokens").and_then(|v| v.as_u64()).unwrap_or(0);
    let cmp = u
        .get("completion_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let ct = u
        .get("prompt_tokens_details")
        .and_then(|d| d.get("cached_tokens"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    (pt, ct, cmp)
}

fn extract_first_tool_name(ch: &serde_json::Value) -> Option<String> {
    let choices = ch.get("choices")?.as_array()?;
    for c in choices {
        let Some(tcs) = c
            .get("delta")
            .and_then(|d| d.get("tool_calls"))
            .and_then(|t| t.as_array())
        else {
            continue;
        };
        for tc in tcs {
            if let Some(name) = tc
                .get("function")
                .and_then(|f| f.get("name"))
                .and_then(|n| n.as_str())
            {
                if !name.is_empty() {
                    return Some(name.to_string());
                }
            }
        }
    }
    None
}

/// Deterministic local mock OpenAI-compatible HTTP/SSE upstream for offline testing & benchmarking.
pub struct MockUpstreamServer {
    port: u16,
    seen_auth_headers: Arc<Mutex<Vec<String>>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl MockUpstreamServer {
    pub async fn start() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let seen_auth = Arc::new(Mutex::new(Vec::new()));
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

        let auth_clone = Arc::clone(&seen_auth);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else { break };
                        let io = TokioIo::new(stream);
                        let seen = Arc::clone(&auth_clone);
                        tokio::spawn(async move {
                            let svc = service_fn(move |req: Request<Incoming>| {
                                let seen_ref = Arc::clone(&seen);
                                async move {
                                    if let Some(a) = req.headers().get("authorization").and_then(|v| v.to_str().ok()) {
                                        seen_ref.lock().push(a.to_string());
                                    }
                                    let req_path = req.uri().path().to_string();
                                    let body_bytes = req.into_body().collect().await.map(|b| b.to_bytes()).unwrap_or_default();
                                    let j: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap_or_default();
                                    let is_stream = j.get("stream").and_then(|v| v.as_bool()).unwrap_or(false);
                                    let stall_stream = j.get("stall_stream").and_then(|v| v.as_bool()).unwrap_or(false);
                                    if req_path.ends_with("/responses") {
                                        if is_stream {
                                            let sse = concat!(
                                                "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_mock_1\",\"status\":\"in_progress\"}}\n\n",
                                                "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"msg_mock_1\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"in_progress\",\"content\":[]}}\n\n",
                                                "event: response.content_part.added\ndata: {\"type\":\"response.content_part.added\",\"item_id\":\"msg_mock_1\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"\"}}\n\n",
                                                "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_mock_1\",\"output_index\":0,\"content_index\":0,\"delta\":\"RUNBAY_MOCK_OK\"}\n\n",
                                                "event: response.output_text.done\ndata: {\"type\":\"response.output_text.done\",\"item_id\":\"msg_mock_1\",\"output_index\":0,\"content_index\":0,\"text\":\"RUNBAY_MOCK_OK\"}\n\n",
                                                "event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"msg_mock_1\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"RUNBAY_MOCK_OK\"}]}}\n\n",
                                                "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_mock_1\",\"status\":\"completed\",\"output\":[{\"id\":\"msg_mock_1\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"RUNBAY_MOCK_OK\"}]}],\"usage\":{\"input_tokens\":24,\"output_tokens\":4,\"total_tokens\":28,\"input_tokens_details\":{\"cached_tokens\":0},\"output_tokens_details\":{\"reasoning_tokens\":0}}}}\n\n"
                                            );
                                            let resp = Response::builder()
                                                .status(StatusCode::OK)
                                                .header("content-type", "text/event-stream")
                                                .body(Full::new(Bytes::from_static(sse.as_bytes())).boxed())
                                                .unwrap();
                                            return Ok::<_, Infallible>(resp);
                                        }
                                        let payload = serde_json::json!({
                                            "id": "resp_mock_1",
                                            "status": "completed",
                                            "output": [{
                                                "id": "msg_mock_1",
                                                "type": "message",
                                                "role": "assistant",
                                                "status": "completed",
                                                "content": [{"type": "output_text", "text": "RUNBAY_MOCK_OK"}]
                                            }],
                                            "usage": {"input_tokens": 24, "output_tokens": 4}
                                        });
                                        return Ok::<_, Infallible>(json_response(StatusCode::OK, Bytes::from(payload.to_string())));
                                    }
                                    if stall_stream {
                                        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Frame<Bytes>, Infallible>>(4);
                                        tokio::spawn(async move {
                                            let _ = tx.send(Ok(Frame::data(Bytes::from_static(
                                                b"data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n"
                                            )))).await;
                                            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                                        });
                                        let stream = futures_util::stream::unfold(rx, |mut rx| async {
                                            rx.recv().await.map(|item| (item, rx))
                                        });
                                        let resp = Response::builder()
                                            .status(StatusCode::OK)
                                            .header("content-type", "text/event-stream")
                                            .body(BodyExt::boxed(StreamBody::new(stream)))
                                            .unwrap();
                                        return Ok::<_, Infallible>(resp);
                                    }
                                    if !is_stream {
                                        let payload = serde_json::json!({
                                            "id": "mock-nonstream-1",
                                            "choices": [{"message": {"role": "assistant", "content": "Multi-Version Concurrency Control"}}],
                                            "usage": {"prompt_tokens": 18, "completion_tokens": 5, "prompt_tokens_details": {"cached_tokens": 0}}
                                        });
                                        return Ok::<_, Infallible>(json_response(StatusCode::OK, Bytes::from(payload.to_string())));
                                    }
                                    let sse = concat!(
                                        "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"function\":{\"name\":\"search_workspace\",\"arguments\":\"{\\\"q\\\":\\\"\"}}]}}]}\n\n",
                                        "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"function\":{\"arguments\":\"mvcc\\\"}\"}}]}}]}\n\n",
                                        "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":1200,\"completion_tokens\":24,\"prompt_tokens_details\":{\"cached_tokens\":1024}}}\n\n",
                                        "data: [DONE]\n\n"
                                    );
                                    let resp = Response::builder()
                                        .status(StatusCode::OK)
                                        .header("content-type", "text/event-stream")
                                        .body(Full::new(Bytes::from_static(sse.as_bytes())).boxed())
                                        .unwrap();
                                    Ok::<_, Infallible>(resp)
                                }
                            });
                            let _ = http1::Builder::new().serve_connection(io, svc).await;
                        });
                    }
                }
            }
        });

        Ok(Self {
            port,
            seen_auth_headers: seen_auth,
            shutdown_tx: Some(shutdown_tx),
        })
    }

    pub fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}/v1", self.port)
    }

    pub fn seen_auth_headers(&self) -> Vec<String> {
        self.seen_auth_headers.lock().clone()
    }
}

impl Drop for MockUpstreamServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runbay_sandbox::{LocalOsSandbox, SandboxRunOptions};
    use std::collections::BTreeMap;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_route_allowlist_blocks_authority_confusion() {
        let origin_only = reqwest::Url::parse("https://provider.example").unwrap();
        assert!(resolve_allowed_upstream_url(
            &origin_only,
            &Method::POST,
            "/v1.attacker.example/chat/completions"
        )
        .is_none());
        assert!(resolve_allowed_upstream_url(&origin_only, &Method::POST, "/v1/files").is_none());
        let ok = resolve_allowed_upstream_url(&origin_only, &Method::POST, "/v1/chat/completions")
            .unwrap();
        assert_eq!(ok.as_str(), "https://provider.example/chat/completions");
        assert_eq!(ok.origin(), origin_only.origin());
    }

    #[tokio::test]
    async fn test_tokens_are_unique_csprng_and_model_scope_enforced() {
        let mock = MockUpstreamServer::start().await.unwrap();
        let gw = SecretlessLoopbackGateway::start(mock.base_url(), "host-key-secret", None)
            .await
            .unwrap();

        let t1 = gw.mint_run_token("same-run", Some("allowed-model".into()));
        let t2 = gw.mint_run_token("same-run", Some("allowed-model".into()));
        assert_ne!(t1, t2, "two mints for same run_id must never collide");
        assert!(t1.starts_with("rb_") && t1.len() == 67);
        assert!(!t1.contains("same-run"));

        let client = reqwest::Client::new();
        // Wrong model -> 403 Forbidden, 0 upstream calls
        let bad_model = client
            .post(format!("{}/chat/completions", gw.base_url()))
            .header("Authorization", format!("Bearer {t1}"))
            .json(&serde_json::json!({"model": "expensive-other-model", "stream": false}))
            .send()
            .await
            .unwrap();
        assert_eq!(bad_model.status(), 403);

        // Missing model -> 403 Forbidden
        let no_model = client
            .post(format!("{}/chat/completions", gw.base_url()))
            .header("Authorization", format!("Bearer {t1}"))
            .json(&serde_json::json!({"stream": false}))
            .send()
            .await
            .unwrap();
        assert_eq!(no_model.status(), 403);
        assert!(mock.seen_auth_headers().is_empty());

        // Allowed model -> 200 OK
        let ok_resp = client
            .post(format!("{}/chat/completions", gw.base_url()))
            .header("Authorization", format!("Bearer {t1}"))
            .json(&serde_json::json!({"model": "allowed-model", "stream": false}))
            .send()
            .await
            .unwrap();
        assert_eq!(ok_resp.status(), 200);
        assert_eq!(mock.seen_auth_headers().len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_revocation_wakes_and_aborts_stalled_in_flight_stream() {
        let mock = MockUpstreamServer::start().await.unwrap();
        let gw = Arc::new(
            SecretlessLoopbackGateway::start(mock.base_url(), "host-key-secret", None)
                .await
                .unwrap(),
        );
        let tok = gw.mint_run_token("stall-run", None);

        let url = format!("{}/chat/completions", gw.base_url());
        let tok_clone = tok.clone();
        let t0 = Instant::now();
        let req_handle = tokio::spawn(async move {
            let resp = reqwest::Client::new()
                .post(url)
                .header("Authorization", format!("Bearer {tok_clone}"))
                .json(&serde_json::json!({"model": "test", "stream": true, "stall_stream": true}))
                .send()
                .await
                .unwrap();
            let _ = resp.text().await;
        });

        // Wait briefly for 1st chunk then revoke token mid-stall
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        gw.revoke_run_token(&tok);

        tokio::time::timeout(std::time::Duration::from_secs(2), req_handle)
            .await
            .expect("revocation must wake stalled stream promptly")
            .unwrap();
        assert!(t0.elapsed().as_millis() < 1500);

        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        let stats = gw.stats();
        assert_eq!(stats.total_calls, 1);
        assert!(stats.calls[0].aborted);
    }

    #[tokio::test]
    async fn test_gateway_rejects_unauthorized_and_streams_specbox_tool() {
        let mock = MockUpstreamServer::start().await.unwrap();
        let detected = Arc::new(Mutex::new(Vec::<String>::new()));
        let detected_cb = Arc::clone(&detected);

        let gw = SecretlessLoopbackGateway::start(
            mock.base_url(),
            "host-only-secret-key-xyz",
            Some(Arc::new(move |tool_name: &str| {
                detected_cb.lock().push(tool_name.to_string());
                Ok(1.25)
            })),
        )
        .await
        .unwrap();

        let client = reqwest::Client::new();
        let unauth = client
            .post(format!("{}/chat/completions", gw.base_url()))
            .header("Authorization", "Bearer bogus-token")
            .json(&serde_json::json!({"model": "test-model", "stream": false}))
            .send()
            .await
            .unwrap();
        assert_eq!(unauth.status(), 401);

        let tok = gw.mint_run_token("run-sse", None);
        let resp = client
            .post(format!("{}/chat/completions", gw.base_url()))
            .header("Authorization", format!("Bearer {tok}"))
            .json(&serde_json::json!({"model": "test-model", "stream": true}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let _body = resp.text().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;

        let stats = gw.stats();
        assert_eq!(stats.unauthorized_attempts_blocked, 1);
        assert_eq!(stats.total_calls, 1);
        let rec = &stats.calls[0];
        assert_eq!(
            rec.specbox_tool_detected.as_deref(),
            Some("search_workspace")
        );
        assert_eq!(rec.specbox_prewarm_ms, Some(1.25));
        assert!(rec.specbox_prewarm_error.is_none());
        assert_eq!(rec.cached_tokens, 1024);
        assert_eq!(detected.lock().as_slice(), &["search_workspace"]);
        assert_eq!(
            mock.seen_auth_headers(),
            vec!["Bearer host-only-secret-key-xyz".to_string()]
        );

        gw.revoke_run_token(&tok);
        let revoked = client
            .post(format!("{}/chat/completions", gw.base_url()))
            .header("Authorization", format!("Bearer {tok}"))
            .json(&serde_json::json!({"model": "test-model", "stream": false}))
            .send()
            .await
            .unwrap();
        assert_eq!(revoked.status(), 401);
    }

    #[tokio::test]
    async fn test_gateway_surfaces_specbox_prewarm_error() {
        let mock = MockUpstreamServer::start().await.unwrap();
        let gw = SecretlessLoopbackGateway::start(
            mock.base_url(),
            "host-only-secret-key-xyz",
            Some(Arc::new(|_tool: &str| {
                Err("simulated_cow_enospc_failure".to_string())
            })),
        )
        .await
        .unwrap();

        let tok = gw.mint_run_token("run-err", None);
        let resp = reqwest::Client::new()
            .post(format!("{}/chat/completions", gw.base_url()))
            .header("Authorization", format!("Bearer {tok}"))
            .json(&serde_json::json!({"model": "test-model", "stream": true}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let _ = resp.text().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;

        let stats = gw.stats();
        assert_eq!(
            stats.calls[0].specbox_prewarm_error.as_deref(),
            Some("simulated_cow_enospc_failure")
        );
        assert!(stats.calls[0].specbox_prewarm_ms.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_os_sandbox_to_rust_secretless_gateway_e2e() {
        let mock = MockUpstreamServer::start().await.unwrap();
        let gw = SecretlessLoopbackGateway::start(
            mock.base_url(),
            "host-real-key-never-in-sandbox",
            None,
        )
        .await
        .unwrap();

        let tmp = TempDir::new().unwrap();
        let pool = tmp.path().join("pool");
        let ws = pool.join("agent_run");
        let secrets = tmp.path().join("secrets");
        fs::create_dir_all(&ws).unwrap();
        fs::create_dir_all(&secrets).unwrap();
        fs::write(
            secrets.join(".env"),
            "API_KEY=host-real-key-never-in-sandbox\n",
        )
        .unwrap();

        let sb = LocalOsSandbox::new(&pool, std::slice::from_ref(&secrets));
        if !sb.is_available() {
            return;
        }

        let tok = gw.mint_run_token("sandbox-e2e", Some("test-model".into()));
        let mut env_overrides = BTreeMap::new();
        env_overrides.insert("RUNBAY_RUN_TOKEN".into(), tok);

        // Both macOS (Seatbelt) and Linux (bwrap --unshare-net + bound Unix socket) MUST
        // deny direct outbound internet while allowing ONLY the secretless gateway channel.
        let (opts, curl_call) = if cfg!(target_os = "macos") {
            env_overrides.insert("RUNBAY_GATEWAY_URL".into(), gw.base_url());
            (
                SandboxRunOptions {
                    allow_network: false,
                    allowed_loopback_port: Some(gw.port()),
                    env_overrides,
                    ..Default::default()
                },
                "curl -sS --max-time 5 -X POST \"$RUNBAY_GATEWAY_URL/chat/completions\" \
                   -H \"Authorization: Bearer $RUNBAY_RUN_TOKEN\" \
                   -H \"Content-Type: application/json\" \
                   -d '{\"model\":\"test-model\",\"stream\":false}'"
                    .to_string(),
            )
        } else {
            // Positive control for the exact host port tested from the isolated namespace below.
            tokio::net::TcpStream::connect(("127.0.0.1", gw.port()))
                .await
                .unwrap();
            env_overrides.insert("HOST_GATEWAY_PORT".into(), gw.port().to_string());
            let sock_path = gw
                .bind_unix_socket(tmp.path().join("gw_sock/gateway.sock"))
                .await
                .unwrap();
            env_overrides.insert(
                "RUNBAY_GATEWAY_SOCKET".into(),
                sock_path.to_string_lossy().to_string(),
            );
            (
                SandboxRunOptions {
                    allow_network: false,
                    allowed_loopback_port: None,
                    gateway_unix_socket: Some(sock_path),
                    env_overrides,
                    ..Default::default()
                },
                r#"python3 -c '
import errno, os, socket
for address in [("127.0.0.1", int(os.environ["HOST_GATEWAY_PORT"])), ("1.1.1.1", 443)]:
    try:
        connection = socket.socket()
    except OSError as e:
        if e.errno in (errno.EAFNOSUPPORT, errno.EPERM):
            continue
        raise
    with connection:
        connection.settimeout(1)
        if connection.connect_ex(address) == 0:
            raise SystemExit(f"unexpected TCP connection to {address}")
' && curl -fsS --max-time 5 --unix-socket "$RUNBAY_GATEWAY_SOCKET" -X POST \
    http://localhost/v1/chat/completions -H "Authorization: Bearer $RUNBAY_RUN_TOKEN" \
    -H 'Content-Type: application/json' -d '{"model":"test-model","stream":false}'"#
                    .to_string(),
            )
        };

        let script = format!(
            "if cat \"{}/.env\" >/dev/null 2>&1; then echo ENV_LEAKED; exit 2; fi; \
             if curl -sS --max-time 1 https://1.1.1.1 >/dev/null 2>&1; then echo NET_LEAKED; exit 3; fi; \
             {curl_call}",
            secrets.display()
        );

        let ws_clone = ws.clone();
        let res = tokio::task::spawn_blocking(move || {
            sb.run(&["/bin/sh".into(), "-c".into(), script], &ws_clone, &opts)
        })
        .await
        .unwrap()
        .unwrap();

        assert_eq!(res.returncode, 0, "stderr: {}", res.stderr);
        assert!(res.stdout.contains("Multi-Version Concurrency Control"));
        assert_eq!(gw.stats().calls[0].status_code, 200);
        assert!(!res.stdout.contains("host-real-key-never-in-sandbox"));
        assert_eq!(
            mock.seen_auth_headers(),
            vec!["Bearer host-real-key-never-in-sandbox".to_string()]
        );
    }
}
