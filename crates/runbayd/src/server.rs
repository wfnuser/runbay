use crate::auth::{constant_time_eq, extract_bearer_token, MAX_BODY_BYTES};
use crate::models::{RegisterAgentRequest, RunEvent, SubmitRunRequest};
use crate::supervisor::{write_control_token_file, DaemonConfig, DaemonSupervisor};
use anyhow::Result;
use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use http_body_util::{combinators::BoxBody, BodyExt, Full, Limited, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;

type BoxedResponse = Response<BoxBody<Bytes, Infallible>>;

fn json_resp(status: StatusCode, payload: serde_json::Value) -> BoxedResponse {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .header("cache-control", "no-store")
        .body(Full::new(Bytes::from(payload.to_string())).boxed())
        .unwrap()
}

fn error_resp(status: StatusCode, code: &str, message: impl Into<String>) -> BoxedResponse {
    json_resp(
        status,
        serde_json::json!({
            "error": code,
            "message": message.into()
        }),
    )
}

fn format_sse_frame(ev: &RunEvent) -> Bytes {
    let data = serde_json::to_string(ev).unwrap_or_else(|_| "{}".to_string());
    Bytes::from(format!(
        "id: {}\nevent: {}\ndata: {}\n\n",
        ev.seq, ev.event_type, data
    ))
}

fn is_terminal_event(ev: &RunEvent) -> bool {
    matches!(
        ev.event_type.as_str(),
        "run.succeeded" | "run.failed" | "run.cancelled"
    )
}

fn parse_after_seq(req: &Request<Incoming>) -> u64 {
    if let Some(q) = req.uri().query() {
        for pair in q.split('&') {
            if let Some(val) = pair.strip_prefix("after_seq=") {
                if let Ok(parsed) = val.parse::<u64>() {
                    return parsed;
                }
            }
        }
    }
    if let Some(raw) = req
        .headers()
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
    {
        if let Ok(parsed) = raw.trim().parse::<u64>() {
            return parsed;
        }
    }
    0
}

fn parse_query_param(query: Option<&str>, key: &str) -> Option<String> {
    let q = query?;
    for pair in q.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == key && !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    None
}

async fn read_json_body<T: serde::de::DeserializeOwned>(
    req: Request<Incoming>,
) -> Result<T, Box<BoxedResponse>> {
    let limited = Limited::new(req.into_body(), MAX_BODY_BYTES);
    let collected = limited.collect().await.map_err(|err| {
        if err
            .downcast_ref::<http_body_util::LengthLimitError>()
            .is_some()
        {
            Box::new(error_resp(
                StatusCode::PAYLOAD_TOO_LARGE,
                "payload_too_large",
                format!("Request body exceeds {} byte limit", MAX_BODY_BYTES),
            ))
        } else {
            Box::new(error_resp(
                StatusCode::BAD_REQUEST,
                "invalid_body",
                format!("Failed to read request body: {err}"),
            ))
        }
    })?;
    let bytes = collected.to_bytes();
    serde_json::from_slice::<T>(&bytes).map_err(|err| {
        Box::new(error_resp(
            StatusCode::BAD_REQUEST,
            "invalid_json",
            format!("Invalid JSON payload: {err}"),
        ))
    })
}

fn map_domain_error(err: anyhow::Error) -> BoxedResponse {
    let msg = err.to_string();
    if msg.starts_with("NOT_FOUND:") {
        error_resp(StatusCode::NOT_FOUND, "not_found", msg)
    } else if msg.starts_with("AGENT_EXISTS:") {
        error_resp(StatusCode::CONFLICT, "agent_exists", msg)
    } else if msg.starts_with("WORKSPACE_FORBIDDEN:") {
        error_resp(StatusCode::FORBIDDEN, "workspace_forbidden", msg)
    } else if msg.starts_with("QUEUE_FULL:") {
        error_resp(StatusCode::TOO_MANY_REQUESTS, "queue_full", msg)
    } else if msg.starts_with("PAYLOAD_TOO_LARGE:") {
        error_resp(StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large", msg)
    } else if msg.starts_with("UNKNOWN_RUNTIME:")
        || msg.starts_with("ADAPTER_UNAVAILABLE:")
        || msg.starts_with("INVALID_REQUEST:")
        || msg.starts_with("INVALID_SANDBOX_MODE:")
        || msg.starts_with("FAIL_CLOSED:")
        || msg.starts_with("AUTH_UNAVAILABLE:")
    {
        error_resp(StatusCode::BAD_REQUEST, "invalid_request", msg)
    } else {
        error_resp(StatusCode::BAD_REQUEST, "bad_request", msg)
    }
}

async fn handle_http_request(sup: Arc<DaemonSupervisor>, req: Request<Incoming>) -> BoxedResponse {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let query = req.uri().query().map(str::to_string);

    // Minimal unauthenticated liveness endpoint
    if method == Method::GET && path == "/health" {
        return json_resp(StatusCode::OK, serde_json::json!({"status": "ok"}));
    }

    // All control endpoints require constant-time Bearer token auth
    let auth_header = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok());
    let authorized = extract_bearer_token(auth_header)
        .is_some_and(|tok| constant_time_eq(tok, &sup.config.control_token));
    if !authorized {
        return error_resp(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Missing or invalid Bearer control token",
        );
    }

    // GET /v1/status
    if method == Method::GET && path == "/v1/status" {
        let status = tokio::task::spawn_blocking(move || sup.status())
            .await
            .unwrap_or_else(|e| Err(e.into()));
        return match status {
            Ok(st) => json_resp(StatusCode::OK, serde_json::to_value(st).unwrap()),
            Err(e) => error_resp(
                StatusCode::INTERNAL_SERVER_ERROR,
                "status_error",
                e.to_string(),
            ),
        };
    }

    // POST /agents
    if method == Method::POST && path == "/agents" {
        let payload: RegisterAgentRequest = match read_json_body(req).await {
            Ok(v) => v,
            Err(resp) => return *resp,
        };
        return match sup.register_agent(payload) {
            Ok(view) => json_resp(StatusCode::CREATED, serde_json::to_value(view).unwrap()),
            Err(e) => map_domain_error(e),
        };
    }

    // GET /agents
    if method == Method::GET && path == "/agents" {
        return match sup.list_agent_views() {
            Ok(list) => json_resp(StatusCode::OK, serde_json::json!({"agents": list})),
            Err(e) => error_resp(
                StatusCode::INTERNAL_SERVER_ERROR,
                "list_error",
                e.to_string(),
            ),
        };
    }

    // /agents/:id, /agents/:id/run, /agents/:id/events
    if let Some(rest) = path.strip_prefix("/agents/") {
        let segments: Vec<&str> = rest.split('/').collect();
        match (method.clone(), segments.as_slice()) {
            (Method::GET, [agent_id]) => {
                return match sup.get_agent_view(agent_id) {
                    Ok(Some(view)) => {
                        json_resp(StatusCode::OK, serde_json::to_value(view).unwrap())
                    }
                    Ok(None) => error_resp(
                        StatusCode::NOT_FOUND,
                        "not_found",
                        format!("Agent '{agent_id}' not found"),
                    ),
                    Err(e) => map_domain_error(e),
                };
            }
            (Method::DELETE, [agent_id]) => {
                return match sup.delete_agent(agent_id).await {
                    Ok(true) => json_resp(
                        StatusCode::OK,
                        serde_json::json!({"deleted": true, "agent_id": agent_id}),
                    ),
                    Ok(false) => error_resp(
                        StatusCode::NOT_FOUND,
                        "not_found",
                        format!("Agent '{agent_id}' not found"),
                    ),
                    Err(e) => map_domain_error(e),
                };
            }
            (Method::POST, [agent_id, "run"]) => {
                let payload: SubmitRunRequest = match read_json_body(req).await {
                    Ok(v) => v,
                    Err(resp) => return *resp,
                };
                return match sup.submit_run(agent_id, payload) {
                    Ok(run) => json_resp(StatusCode::ACCEPTED, serde_json::to_value(run).unwrap()),
                    Err(e) => map_domain_error(e),
                };
            }
            (Method::GET, [agent_id, "events"]) => {
                return stream_agent_sse(sup, agent_id.to_string()).await;
            }
            _ => {}
        }
    }

    // GET /runs
    if method == Method::GET && path == "/runs" {
        let agent_filter = parse_query_param(query.as_deref(), "agent_id");
        return match sup.list_runs(agent_filter.as_deref()) {
            Ok(runs) => json_resp(StatusCode::OK, serde_json::json!({"runs": runs})),
            Err(e) => map_domain_error(e),
        };
    }

    // /runs/:run_id, /runs/:run_id/cancel, /runs/:run_id/events
    if let Some(rest) = path.strip_prefix("/runs/") {
        let segments: Vec<&str> = rest.split('/').collect();
        match (method, segments.as_slice()) {
            (Method::GET, [run_id]) => {
                return match sup.get_run(run_id) {
                    Ok(Some(run)) => json_resp(StatusCode::OK, serde_json::to_value(run).unwrap()),
                    Ok(None) => error_resp(
                        StatusCode::NOT_FOUND,
                        "not_found",
                        format!("Run '{run_id}' not found"),
                    ),
                    Err(e) => map_domain_error(e),
                };
            }
            (Method::POST, [run_id, "cancel"]) => {
                return match sup.cancel_run(run_id, "caller_cancelled").await {
                    Ok(Some(run)) => json_resp(StatusCode::OK, serde_json::to_value(run).unwrap()),
                    Ok(None) => error_resp(
                        StatusCode::NOT_FOUND,
                        "not_found",
                        format!("Run '{run_id}' not found"),
                    ),
                    Err(e) => map_domain_error(e),
                };
            }
            (Method::GET, [run_id, "events"]) => {
                let after_seq = parse_after_seq(&req);
                return stream_run_sse(sup, run_id.to_string(), after_seq).await;
            }
            _ => {}
        }
    }

    error_resp(StatusCode::NOT_FOUND, "not_found", "Unknown route")
}

async fn stream_run_sse(
    sup: Arc<DaemonSupervisor>,
    run_id: String,
    after_seq: u64,
) -> BoxedResponse {
    match sup.get_run(&run_id) {
        Ok(Some(_)) => {}
        Ok(None) => {
            return error_resp(
                StatusCode::NOT_FOUND,
                "not_found",
                format!("Run '{run_id}' not found"),
            )
        }
        Err(e) => return map_domain_error(e),
    }

    // Subscribe BEFORE reading persisted history to guarantee zero missed events on handoff.
    let live_sub = sup.subscribe_run(&run_id);
    let history = sup
        .store
        .list_events_after(&run_id, after_seq)
        .unwrap_or_default();

    let (tx, rx) = mpsc::channel::<Result<Frame<Bytes>, Infallible>>(64);
    tokio::spawn(async move {
        let mut last_seq = after_seq;
        let mut saw_terminal = false;

        for ev in history {
            if ev.seq <= last_seq {
                continue;
            }
            last_seq = ev.seq;
            let term = is_terminal_event(&ev);
            if tx
                .send(Ok(Frame::data(format_sse_frame(&ev))))
                .await
                .is_err()
            {
                // Client disconnected: exit SSE task cleanly without touching child process.
                return;
            }
            if term {
                saw_terminal = true;
                break;
            }
        }

        if saw_terminal {
            return;
        }

        let Some((mut bcast_rx, done_notify)) = live_sub else {
            // Run finished right before subscribe; drain any remaining persisted events and exit.
            if let Ok(tail) = sup.store.list_events_after(&run_id, last_seq) {
                for ev in tail {
                    if ev.seq > last_seq {
                        last_seq = ev.seq;
                        if tx
                            .send(Ok(Frame::data(format_sse_frame(&ev))))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                }
            }
            return;
        };

        let mut keepalive = tokio::time::interval(Duration::from_secs(15));
        keepalive.tick().await;

        loop {
            tokio::select! {
                _ = keepalive.tick() => {
                    if tx.send(Ok(Frame::data(Bytes::from_static(b": keepalive\n\n")))).await.is_err() {
                        return;
                    }
                }
                recv = bcast_rx.recv() => {
                    match recv {
                        Ok(ev) => {
                            if ev.seq <= last_seq {
                                continue;
                            }
                            last_seq = ev.seq;
                            let term = is_terminal_event(&ev);
                            if tx.send(Ok(Frame::data(format_sse_frame(&ev)))).await.is_err() {
                                return;
                            }
                            if term {
                                return;
                            }
                        }
                        Err(RecvError::Lagged(_)) => {
                            // Catch up from SQLite bounded store without losing lifecycle transitions.
                            if let Ok(missed) = sup.store.list_events_after(&run_id, last_seq) {
                                for ev in missed {
                                    if ev.seq <= last_seq {
                                        continue;
                                    }
                                    last_seq = ev.seq;
                                    let term = is_terminal_event(&ev);
                                    if tx.send(Ok(Frame::data(format_sse_frame(&ev)))).await.is_err() {
                                        return;
                                    }
                                    if term {
                                        return;
                                    }
                                }
                            }
                        }
                        Err(RecvError::Closed) => {
                            if let Ok(tail) = sup.store.list_events_after(&run_id, last_seq) {
                                for ev in tail {
                                    if ev.seq > last_seq {
                                        last_seq = ev.seq;
                                        let _ = tx.send(Ok(Frame::data(format_sse_frame(&ev)))).await;
                                    }
                                }
                            }
                            return;
                        }
                    }
                }
                _ = done_notify.notified() => {
                    if let Ok(tail) = sup.store.list_events_after(&run_id, last_seq) {
                        for ev in tail {
                            if ev.seq > last_seq {
                                last_seq = ev.seq;
                                if tx.send(Ok(Frame::data(format_sse_frame(&ev)))).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                    return;
                }
            }
        }
    });

    let stream = futures_util::stream::unfold(rx, |mut rx| async {
        rx.recv().await.map(|item| (item, rx))
    });
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .header("x-accel-buffering", "no")
        .body(BodyExt::boxed(StreamBody::new(stream)))
        .unwrap()
}

async fn stream_agent_sse(sup: Arc<DaemonSupervisor>, agent_id: String) -> BoxedResponse {
    match sup.get_agent_view(&agent_id) {
        Ok(Some(_)) => {}
        Ok(None) => {
            return error_resp(
                StatusCode::NOT_FOUND,
                "not_found",
                format!("Agent '{agent_id}' not found"),
            )
        }
        Err(e) => return map_domain_error(e),
    }

    let mut bcast_rx = sup.subscribe_agent(&agent_id);
    let history = sup
        .store
        .list_agent_events(&agent_id, 128)
        .unwrap_or_default();
    let (tx, rx) = mpsc::channel::<Result<Frame<Bytes>, Infallible>>(64);

    tokio::spawn(async move {
        for ev in history {
            if tx
                .send(Ok(Frame::data(format_sse_frame(&ev))))
                .await
                .is_err()
            {
                return;
            }
        }
        let mut keepalive = tokio::time::interval(Duration::from_secs(15));
        keepalive.tick().await;
        loop {
            tokio::select! {
                _ = keepalive.tick() => {
                    if tx.send(Ok(Frame::data(Bytes::from_static(b": keepalive\n\n")))).await.is_err() {
                        return;
                    }
                }
                recv = bcast_rx.recv() => {
                    match recv {
                        Ok(ev) => {
                            if tx.send(Ok(Frame::data(format_sse_frame(&ev)))).await.is_err() {
                                return;
                            }
                        }
                        Err(RecvError::Lagged(_)) => continue,
                        Err(RecvError::Closed) => return,
                    }
                }
            }
        }
    });

    let stream = futures_util::stream::unfold(rx, |mut rx| async {
        rx.recv().await.map(|item| (item, rx))
    });
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .header("x-accel-buffering", "no")
        .body(BodyExt::boxed(StreamBody::new(stream)))
        .unwrap()
}

pub async fn run_loopback_server(config: DaemonConfig) -> Result<()> {
    let port = config.port;
    let sup = DaemonSupervisor::new(config).await?;
    let token_path = write_control_token_file(&sup.config.state_dir, &sup.config.control_token)?;
    let listener = TcpListener::bind(("127.0.0.1", port)).await?;
    let local: SocketAddr = listener.local_addr()?;

    let ready = serde_json::json!({
        "addr": local.to_string(),
        "state_dir": sup.config.state_dir.to_string_lossy(),
        "token_file": token_path.to_string_lossy(),
        "gateway_enabled": sup.gateway.is_some(),
        "fixture_adapter_enabled": sup.config.enable_fixture_adapter
    });
    println!("RUNBAYD_READY {ready}");

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                sup.shutdown_all().await;
                break;
            }
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else { break };
                let io = TokioIo::new(stream);
                let sup_conn = Arc::clone(&sup);
                tokio::spawn(async move {
                    let svc = service_fn(move |req: Request<Incoming>| {
                        let s = Arc::clone(&sup_conn);
                        async move { Ok::<_, Infallible>(handle_http_request(s, req).await) }
                    });
                    let _ = http1::Builder::new().serve_connection(io, svc).await;
                });
            }
        }
    }
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
