use axum::body::Body;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};
use underclass::claude::ClaudeBackend;
use underclass::codex::CodexBackend;
use underclass::copilot::CopilotBackend;
use underclass::flows::FlowRegistry;
use underclass::models::{
    Account, AccountStatus, BackendId, ModelInfo, RequestLogEntry, Surface, now_ms,
};
use underclass::pool::PoolCore;
use underclass::provider::BackendMap;
use underclass::proxy::AppState;
use underclass::store::Store;
use underclass::tokens::TokenManager;

#[derive(Default)]
struct MockState {
    failing: BTreeSet<String>,
    served_by: VecDeque<String>,
    fourtwonined: VecDeque<String>,
    chat_usage_options: VecDeque<bool>,
    reject_stream_usage: bool,
    /// Remaining 403 responses to serve per bearer token, modelling a redemption-triggered
    /// credential rejection that a token refresh clears.
    forbidden: std::collections::HashMap<String, u32>,
}

const MEDIA_SUCCESS: &str = concat!(
    "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\"}}\n\n",
    "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"A fixture joke.\"}]}]}}\n\n"
);
const MEDIA_OVERLOAD: &str = "data: {\"type\":\"error\",\"code\":\"server_is_overloaded\"}\n\n";
const MEDIA_JSON: &str = "{\"status\":\"completed\",\"output\":[]}";
const MEDIA_HTTP_ERROR: &str = "{\"error\":{\"code\":\"upstream_error\"}}";

async fn mock_responses(
    State(state): State<Arc<Mutex<MockState>>>,
    headers: axum::http::HeaderMap,
    uri: axum::http::Uri,
    body: String,
) -> Response {
    assert!(
        !headers.contains_key("x-underclass-retry-safe"),
        "client retry authority must not reach upstream"
    );
    let bearer = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("unknown")
        .to_string();
    let wants_stream = body.contains("\"stream\": true") || body.contains("\"stream\":true");

    if bearer.starts_with("bad-") {
        return (
            axum::http::StatusCode::UNAUTHORIZED,
            [(
                axum::http::header::CONTENT_TYPE,
                "application/json".to_string(),
            )],
            r#"{"error":{"message":"bad token"}}"#.to_string(),
        )
            .into_response();
    }

    let remaining = state
        .lock()
        .unwrap()
        .forbidden
        .get(&bearer)
        .copied()
        .unwrap_or(0);
    if remaining > 0 {
        state
            .lock()
            .unwrap()
            .forbidden
            .insert(bearer.clone(), remaining - 1);
        return (
            axum::http::StatusCode::FORBIDDEN,
            [(
                axum::http::header::CONTENT_TYPE,
                "application/json".to_string(),
            )],
            r#"{"error":{"message":"credential rejected"}}"#.to_string(),
        )
            .into_response();
    }

    // A model-scoped refusal applies to one model only, the way Anthropic's weekly
    // other-models window does.
    let requested_model = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|value| {
            value
                .get("model")
                .and_then(|m| m.as_str())
                .map(str::to_string)
        })
        .unwrap_or_default();
    let model_scoped_refusal = bearer.starts_with("claude-model-quota-")
        && uri.path().ends_with("/messages")
        && requested_model == "claude-opus-5-5";
    let failing = state.lock().unwrap().failing.contains(&bearer)
        || (bearer.starts_with("claude-quota-") && uri.path().ends_with("/messages"))
        || model_scoped_refusal;
    if failing {
        state.lock().unwrap().fourtwonined.push_back(bearer.clone());
        if uri.path().ends_with("/messages") {
            // Anthropic reports the exhausted subscription window on the response headers: a
            // rejected shared window blocks the account, a rejected model window blocks the
            // model only.
            let reset = (now_ms() / 1000 + 3600).to_string();
            let model_scoped = bearer.starts_with("claude-model-quota-");
            fn name(value: &'static str) -> axum::http::header::HeaderName {
                axum::http::header::HeaderName::from_static(value)
            }
            let mut headers = vec![
                (
                    name("anthropic-ratelimit-unified-status"),
                    "rejected".to_string(),
                ),
                (name("anthropic-ratelimit-unified-reset"), reset),
            ];
            if model_scoped {
                headers.push((
                    name("anthropic-ratelimit-unified-5h-status"),
                    "allowed".to_string(),
                ));
                headers.push((
                    name("anthropic-ratelimit-unified-7d-status"),
                    "allowed".to_string(),
                ));
                headers.push((
                    name("anthropic-ratelimit-unified-7d_oi-status"),
                    "rejected".to_string(),
                ));
            }
            let mut builder = axum::response::Response::builder()
                .status(axum::http::StatusCode::TOO_MANY_REQUESTS)
                .header(axum::http::header::CONTENT_TYPE, "application/json");
            for (name, value) in headers {
                builder = builder.header(name, value);
            }
            return builder
                .body(axum::body::Body::from(
                    r#"{"type":"error","error":{"type":"rate_limit_error","message":"rate limit exceeded"}}"#,
                ))
                .unwrap();
        }
        return (
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            [(
                axum::http::header::CONTENT_TYPE,
                "application/json".to_string(),
            )],
            r#"{"error":{"code":"usage_limit_reached","message":"usage limit reached"}}"#
                .to_string(),
        )
            .into_response();
    }

    state.lock().unwrap().served_by.push_back(bearer.clone());
    if let Some(case) = bearer.strip_prefix("tok-media-") {
        let (status, payload) = match case {
            "success-json" | "success-missing" => (200, MEDIA_SUCCESS),
            "overload-json" | "overload-missing" => (200, MEDIA_OVERLOAD),
            "json" => (200, MEDIA_JSON),
            "http-error" => (503, MEDIA_HTTP_ERROR),
            _ => panic!("unknown media fixture"),
        };
        let mut response = Response::builder().status(status);
        if !case.ends_with("-missing") {
            response = response.header("content-type", "application/json");
        }
        return response.body(Body::from(payload)).unwrap();
    }
    if uri.path().ends_with("/messages") {
        // Echo what the Anthropic upstream received, so the test can assert the credential
        // swap, the forwarded client headers, and the verbatim body.
        let header = |name: &str| {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string()
        };
        return (
            axum::http::StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/json".to_string())],
            serde_json::json!({
                "account": bearer,
                "app": header("x-app"),
                "beta": header("anthropic-beta"),
                "session": header("x-claude-code-session-id"),
                "body": serde_json::from_str::<serde_json::Value>(&body).unwrap_or(serde_json::Value::Null),
                "raw": body,
                "usage": {"input_tokens": 11, "output_tokens": 3}
            })
            .to_string(),
        )
            .into_response();
    }
    let chat = uri.path().ends_with("/chat/completions");
    if chat {
        let enabled = serde_json::from_str::<serde_json::Value>(&body)
            .ok()
            .and_then(|value| {
                value
                    .pointer("/stream_options/include_usage")
                    .and_then(|v| v.as_bool())
            })
            == Some(true);
        let mut mock = state.lock().unwrap();
        mock.chat_usage_options.push_back(enabled);
        if enabled && mock.reject_stream_usage {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                [(
                    axum::http::header::CONTENT_TYPE,
                    "application/json".to_string(),
                )],
                r#"{"error":{"message":"stream_options.include_usage unsupported"}}"#.to_string(),
            )
                .into_response();
        }
    }
    if serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|value| value.get("overload").and_then(|flag| flag.as_bool()))
        == Some(true)
    {
        // A 200 stream that fails before any output, like upstream capacity errors in practice.
        return (
            axum::http::StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
            "event: response.created\ndata: {\"type\":\"response.created\"}\n\nevent: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_is_overloaded\",\"message\":\"Selected model is at capacity. Please try a different model.\"}}}\n\n",
        ).into_response();
    }
    if let Some(case) = bearer.strip_prefix("tok-proof-") {
        let overload = "data: {\"type\":\"error\",\"code\":\"server_is_overloaded\"}\n\n";
        let (status, payload) = match case {
            "malformed" => (200, format!("data: {{invalid\n\n{overload}")),
            "unknown" => (
                200,
                format!("data: {{\"type\":\"future.event\"}}\n\n{overload}"),
            ),
            "tool" => (
                200,
                format!(
                    "data: {{\"type\":\"response.output_item.added\",\"item\":{{\"type\":\"function_call\"}}}}\n\n{overload}"
                ),
            ),
            "http" => (
                503,
                "{\"error\":{\"code\":\"server_is_overloaded\"}}".to_string(),
            ),
            "policy" => (
                200,
                "data: {\"type\":\"error\",\"code\":\"cyber_policy\"}\n\n".to_string(),
            ),
            "quota" => (
                200,
                "data: {\"type\":\"error\",\"code\":\"usage_limit_reached\"}\n\n".to_string(),
            ),
            "broad" => (
                200,
                "data: {\"type\":\"error\",\"code\":\"overloaded_error\"}\n\n".to_string(),
            ),
            "sanitized" => (
                200,
                "data: {\"type\":\"error\",\"code\":\"server_is_overloaded!\"}\n\n".to_string(),
            ),
            "truncated" => (200, "data: {\"type\":\"response.created\"}\n\n".to_string()),
            "success" => (
                200,
                "data: {\"type\":\"response.completed\"}\n\n".to_string(),
            ),
            _ => (200, overload.to_string()),
        };
        return Response::builder()
            .status(status)
            .header("content-type", "text/event-stream")
            .header("x-underclass-retry-safe", "pre-output-overload")
            .body(Body::from(payload))
            .unwrap();
    }
    if bearer.starts_with("tok-ovl-") && wants_stream {
        // The shape Codex upstream sends today: a 200 stream, a pause, then a stream `error` event
        // whose code is nested under `error`.
        let stream = futures::stream::unfold(0, |step| async move {
            match step {
                0 => Some((
                    Ok::<_, std::io::Error>(axum::body::Bytes::from_static(
                        b"event: response.created\ndata: {\"type\":\"response.created\"}\n\n",
                    )),
                    1,
                )),
                1 => {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    Some((Ok(axum::body::Bytes::from_static(b"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"service_unavailable_error\",\"code\":\"server_is_overloaded\",\"message\":\"Selected model is at capacity. Please try a different model.\"}}\n\n")), 2))
                }
                _ => None,
            }
        });
        return (
            axum::http::StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
            Body::from_stream(stream),
        )
            .into_response();
    }
    if bearer.starts_with("tok-late-ovl-") && wants_stream {
        return (
            axum::http::StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\nevent: error\ndata: {\"type\":\"error\",\"error\":{\"code\":\"server_is_overloaded\"}}\n\n",
        ).into_response();
    }
    if serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|value| value.get("slow_abort").and_then(|flag| flag.as_bool()))
        == Some(true)
    {
        let stream = futures::stream::unfold(0, |step| async move {
            match step {
                0 => Some((Ok::<_, std::io::Error>(axum::body::Bytes::from_static(b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n")), 1)),
                1 => {
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    Some((Ok(axum::body::Bytes::from_static(b"data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":11,\"output_tokens\":3}}}\n\n")), 2))
                }
                _ => None,
            }
        });
        return (
            axum::http::StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
            Body::from_stream(stream),
        )
            .into_response();
    }
    let usage_event = if chat {
        "data: {\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":3}}\n\n"
    } else {
        "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":11,\"output_tokens\":3}}}\n\n"
    };
    let payload = format!(
        "data: {{\"type\":\"response.started\",\"account\":\"{bearer}\"}}\n\ndata: {{\"type\":\"output_text.delta\",\"delta\":\"hello from {bearer}\"}}\n\n{usage_event}data: [DONE]\n\n"
    );
    if wants_stream {
        (
            axum::http::StatusCode::OK,
            [(
                axum::http::header::CONTENT_TYPE,
                "text/event-stream".to_string(),
            )],
            payload,
        )
            .into_response()
    } else {
        (
            axum::http::StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/json".to_string())],
            if chat { format!(r#"{{"account":"{bearer}","usage":{{"prompt_tokens":11,"completion_tokens":3}}}}"#) }
            else { format!(r#"{{"account":"{bearer}","output_text":"hello from {bearer}","usage":{{"input_tokens":11,"output_tokens":3}}}}"#) },
        ).into_response()
    }
}

/// Starts the shared mock upstream on a dedicated OS thread and returns its state.
///
/// The server deliberately does not live on a test's own runtime: every `#[tokio::test]` builds
/// and then drops its runtime, which would cancel the server task and leave the cached URL
/// pointing at a dead port for every later test in the binary. A dedicated thread with its own
/// runtime keeps the server reachable for the lifetime of the test process.
fn mock_upstream() -> &'static Arc<Mutex<MockState>> {
    static STATE: OnceLock<Arc<Mutex<MockState>>> = OnceLock::new();
    static URL: OnceLock<String> = OnceLock::new();
    let state = STATE.get_or_init(Default::default);
    URL.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel();
        let served = state.clone();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let app = axum::Router::new()
                    .fallback(mock_responses)
                    .with_state(served);
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                tx.send(addr).unwrap();
                axum::serve(listener, app).await.unwrap();
            });
        });
        let addr = rx.recv().expect("mock upstream failed to bind");
        unsafe {
            std::env::set_var("UNDERCLASS_CODEX_UPSTREAM", format!("http://{addr}"));
            std::env::set_var("UNDERCLASS_COPILOT_UPSTREAM", format!("http://{addr}"));
            std::env::set_var("UNDERCLASS_CLAUDE_UPSTREAM", format!("http://{addr}"));
        }
        format!("http://{addr}")
    });
    state
}

fn account(id: &str, backend: BackendId, token: &str) -> Account {
    let ts = now_ms();
    Account {
        id: id.into(),
        backend,
        label: id.into(),
        refresh_token: Some(format!("refresh-{id}")),
        access_token: Some(token.into()),
        expires_at: ts + 3_600_000,
        token_refreshed_at: ts,
        account_id: Some(format!("chatgpt-{id}")),
        residency: None,
        enterprise_url: None,
        plan: None,
        disabled_reason: None,
        status: AccountStatus::Healthy,
        reset_at: 0,
        created_at: ts,
        updated_at: ts,
    }
}

fn codex_account(id: &str, token: &str) -> Account {
    account(id, BackendId::Codex, token)
}

fn copilot_account(id: &str, token: &str) -> Account {
    let mut a = account(id, BackendId::Copilot, "unused-access");
    a.refresh_token = Some(token.to_string());
    a
}

/// The surfaces the end-to-end app serves.
fn openai_surfaces() -> std::collections::HashMap<BackendId, Vec<Surface>> {
    std::collections::HashMap::from([
        (BackendId::Codex, vec![Surface::OpenAI]),
        (BackendId::Copilot, vec![Surface::OpenAI]),
        (BackendId::Claude, vec![Surface::Anthropic]),
    ])
}

fn backend_surfaces(backends: &BackendMap) -> std::collections::HashMap<BackendId, Vec<Surface>> {
    backends
        .iter()
        .map(|(id, backend)| (*id, backend.surfaces().to_vec()))
        .collect()
}

fn model(id: &str) -> ModelInfo {
    ModelInfo {
        id: id.into(),
        name: id.into(),
        context: 400_000,
        input: 272_000,
        output: 128_000,
    }
}

async fn spawn_app(
    store: Arc<Store>,
    cooldown_ms: i64,
) -> (String, Arc<Mutex<VecDeque<RequestLogEntry>>>) {
    spawn_app_with_codex(store, cooldown_ms, Arc::new(CodexBackend { cooldown_ms })).await
}

async fn spawn_app_with_codex(
    store: Arc<Store>,
    cooldown_ms: i64,
    codex: Arc<dyn underclass::provider::Backend>,
) -> (String, Arc<Mutex<VecDeque<RequestLogEntry>>>) {
    let client = reqwest::Client::new();
    let tokens = Arc::new(TokenManager::new(store.clone(), client.clone()));
    let mut backends: BackendMap = std::collections::HashMap::new();
    backends.insert(BackendId::Codex, codex);
    backends.insert(BackendId::Copilot, Arc::new(CopilotBackend { cooldown_ms }));
    backends.insert(BackendId::Claude, Arc::new(ClaudeBackend { cooldown_ms }));
    let core = PoolCore::new(&store, backend_surfaces(&backends));
    let pool = Arc::new(Mutex::new(core));
    let logs: Arc<Mutex<VecDeque<RequestLogEntry>>> = Arc::new(Mutex::new(VecDeque::new()));
    let state = Arc::new(AppState {
        store,
        pool,
        tokens,
        backends: Arc::new(backends),
        client,
        logs: logs.clone(),
        flows: FlowRegistry::default(),
        proxy_key: Some("test-key".to_string()),
        ui_token: "unused".to_string(),
        resets: Arc::new(underclass::resets::ResetManager::new(
            reqwest::Client::new(),
            "http://127.0.0.1:1".to_string(),
            false,
        )),
        stream_usage_unsupported: Mutex::new(Default::default()),
        observations: Mutex::new(Default::default()),
        stats_retention_days: 730,
    });

    let v1 = axum::Router::new()
        .route("/models", axum::routing::get(underclass::proxy::models))
        .route("/responses", axum::routing::post(underclass::proxy::infer))
        .route(
            "/chat/completions",
            axum::routing::post(underclass::proxy::infer),
        )
        .route(
            "/messages",
            axum::routing::post(underclass::proxy::messages),
        )
        .route(
            "/messages/count_tokens",
            axum::routing::post(underclass::proxy::messages),
        )
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            underclass::proxy::require_proxy_key,
        ))
        .with_state(state.clone());

    let anthropic_alias = axum::Router::new()
        .route(
            "/messages",
            axum::routing::post(underclass::proxy::messages),
        )
        .route(
            "/messages/count_tokens",
            axum::routing::post(underclass::proxy::messages),
        )
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            underclass::proxy::require_proxy_key,
        ))
        .with_state(state.clone());

    let admin = axum::Router::new()
        .route(
            "/admin/api/monitor",
            axum::routing::get(underclass::monitor::snapshot),
        )
        .route(
            "/admin/api/usage",
            axum::routing::get(underclass::ui::usage_summary),
        )
        .route(
            "/admin/api/usage/requests",
            axum::routing::get(underclass::ui::usage_requests),
        )
        .route(
            "/admin/api/stats",
            axum::routing::get(underclass::stats_api::stats_handler),
        )
        .route(
            "/admin/api/accounts/{id}/enable",
            axum::routing::post(underclass::ui::enable_account),
        )
        .route(
            "/admin/api/accounts/{id}/refresh",
            axum::routing::post(underclass::ui::refresh_account),
        )
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            underclass::ui::require_ui_token,
        ));
    let app = axum::Router::new()
        .nest("/v1", v1)
        .merge(anthropic_alias)
        .merge(admin)
        .layer(axum::middleware::from_fn(
            underclass::correlation::middleware,
        ))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), logs)
}

#[tokio::test(flavor = "multi_thread")]
async fn e2e_mid_stream_overload_is_recorded() {
    mock_upstream();
    let store = Arc::new(Store::in_memory().unwrap());
    store.set_catalog(BackendId::Codex, &[model("gpt-5.5")]);
    store.upsert_account(&codex_account("acc-ovl", "tok-overload"));
    let (base, logs) = spawn_app(store.clone(), 400).await;
    let http = reqwest::Client::new();
    let response = http.post(format!("{base}/v1/responses"))
        .header("Authorization", "Bearer test-key")
        .json(&serde_json::json!({"model": "gpt-5.5", "input": "hi", "stream": true, "overload": true}))
        .send().await.unwrap();
    assert_eq!(
        response.status(),
        200,
        "mid-stream failures keep the upstream status"
    );
    assert!(
        response.text().await.unwrap().contains("response.failed"),
        "failure passes through to the client"
    );

    let mut row = None;
    for _ in 0..50 {
        row = store
            .usage_records(&Default::default())
            .unwrap()
            .into_iter()
            .next();
        if row.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let row = row.expect("usage row written when the stream ends");
    assert_eq!(row.stream_outcome.as_deref(), Some("failed"));
    assert_eq!(row.error_code.as_deref(), Some("server_is_overloaded"));
    assert_eq!(row.saw_output, Some(false));
    assert!(row.duration_ms.is_some());
    let entry = logs
        .lock()
        .unwrap()
        .iter()
        .find(|e| e.request_id == row.request_id)
        .cloned()
        .unwrap();
    assert_eq!(entry.stream_outcome.as_deref(), Some("failed"));
    assert_eq!(entry.error_code.as_deref(), Some("server_is_overloaded"));

    // Windows are half-open at `now`; step past the millisecond the event was stamped with.
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let stats: serde_json::Value = http
        .get(format!("{base}/admin/api/stats?since=1h&bucket=minute"))
        .header("Authorization", "Bearer unused")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(stats["totals"]["events"]["overloaded"], 1, "{stats}");
    assert_eq!(stats["totals"]["outcomes"]["failed"], 1, "{stats}");
    assert_eq!(stats["totals"]["requests"], 1);
    assert!(
        !stats.to_string().contains("capacity. Please"),
        "upstream message text must not be stored"
    );
    let denied = http
        .get(format!("{base}/admin/api/stats"))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), 401);
}

#[tokio::test(flavor = "multi_thread")]
async fn e2e_full_pool_story() {
    let mock = mock_upstream();

    let store = Arc::new(Store::in_memory().unwrap());
    let m = model("gpt-5.5");
    store.set_catalog(BackendId::Codex, &[m.clone()]);
    store.set_catalog(BackendId::Copilot, &[m]);
    store.upsert_account(&codex_account("acc-1", "tok-acc-1"));
    store.upsert_account(&copilot_account("acc-2", "cop-acc-2"));
    let (base, logs) = spawn_app(store.clone(), 400).await;
    let http = reqwest::Client::new();

    let post = |key: Option<&str>, stream: bool| {
        let http = http.clone();
        let base = base.clone();
        let key = key.map(str::to_string);
        async move {
            let mut body = serde_json::json!({"model": "gpt-5.5", "input": "hi", "stream": stream});
            if let Some(k) = &key {
                body["prompt_cache_key"] = serde_json::Value::String(k.clone());
            }
            let request_id = uuid::Uuid::now_v7().to_string();
            let response = http
                .post(format!("{base}/v1/responses"))
                .header("Authorization", "Bearer test-key")
                .header("x-request-id", &request_id)
                .json(&body)
                .send()
                .await
                .unwrap();
            assert_eq!(response.headers()["x-request-id"], request_id);
            response
        }
    };

    // phase 1: success + streaming passthrough + stickiness
    let r1 = post(Some("sess-a"), true).await;
    assert_eq!(r1.status(), 200);
    assert_eq!(
        r1.headers().get(axum::http::header::CONTENT_TYPE).unwrap(),
        "text/event-stream"
    );
    let body1 = r1.text().await.unwrap();
    assert!(
        body1.contains("[DONE]"),
        "stream passthrough broken: {body1}"
    );
    assert!(
        body1.contains("hello from tok-acc-1"),
        "unexpected payload: {body1}"
    );

    let r2 = post(Some("sess-a"), true).await;
    let body2 = r2.text().await.unwrap();
    assert!(
        body2.contains("hello from tok-acc-1"),
        "sticky session moved accounts: {body2}"
    );
    let rows = store
        .usage_records(&underclass::store::UsageQuery {
            cache_key: Some("sess-a".into()),
            ..Default::default()
        })
        .unwrap();
    let denied = http
        .get(format!("{base}/admin/api/monitor"))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), 401);
    let monitor = http
        .get(format!("{base}/admin/api/monitor"))
        .bearer_auth("unused")
        .send()
        .await
        .unwrap();
    assert_eq!(monitor.status(), 200);
    let monitor_text = monitor.text().await.unwrap();
    assert!(!monitor_text.contains("sess-a"));
    assert!(!monitor_text.contains("tok-acc-1"));
    assert!(!monitor_text.contains("refresh-acc-1"));
    let monitor: serde_json::Value = serde_json::from_str(&monitor_text).unwrap();
    assert_eq!(monitor["accounts"].as_array().unwrap().len(), 2);
    assert_eq!(monitor["month"]["attempts"], 2);
    assert_eq!(monitor["minute_bins"].as_array().unwrap().len(), 60);
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|r| r.input_tokens == Some(11)
        && r.output_tokens == Some(3)
        && r.account_id == "acc-1"));

    // phase 2: an unsticky request is served by the pool
    let r3 = post(None, false).await;
    assert_eq!(r3.status(), 200);

    // phase 3: quota on the bound account -> transparent failover to the other backend
    mock.lock().unwrap().failing.insert("tok-acc-1".into());
    let r4 = post(Some("sess-a"), true).await;
    assert_eq!(r4.status(), 200, "quota on bound account must fail over");
    let failover_id = r4.headers()["x-request-id"].to_str().unwrap().to_owned();
    let body4 = r4.text().await.unwrap();
    assert!(
        body4.contains("hello from cop-acc-2"),
        "failover should land on the healthy copilot account (bearer = its stored token): {body4}"
    );
    assert!(
        mock.lock()
            .unwrap()
            .fourtwonined
            .contains(&"tok-acc-1".to_string()),
        "mock never saw the 429-causing request for acc-1"
    );
    assert!(
        logs.lock()
            .unwrap()
            .iter()
            .filter(|entry| entry.request_id == failover_id)
            .count()
            >= 2,
        "failed and successful attempts must share the correlation ID in request history"
    );
    let rows = store
        .usage_records(&underclass::store::UsageQuery {
            cache_key: Some("sess-a".into()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        rows.iter().filter(|r| r.request_id == failover_id).count(),
        2
    );
    assert_eq!(
        rows.iter()
            .find(|r| r.request_id == failover_id && r.account_id == "acc-1")
            .unwrap()
            .input_tokens,
        None
    );
    assert_eq!(
        rows.iter()
            .find(|r| r.request_id == failover_id && r.account_id == "acc-2")
            .unwrap()
            .input_tokens,
        Some(11)
    );

    let summary = http
        .get(format!(
            "{base}/admin/api/usage?from_ms=0&group_by=account_id"
        ))
        .header("Authorization", "Bearer unused")
        .send()
        .await
        .unwrap();
    assert_eq!(summary.status(), 200);
    let groups: serde_json::Value = summary.json().await.unwrap();
    assert!(
        groups["groups"]
            .as_array()
            .unwrap()
            .iter()
            .any(|g| g["account_id"] == "acc-2" && g["input_tokens"].as_i64().unwrap() >= 11)
    );
    let filtered = http.get(format!("{base}/admin/api/usage/requests?from_ms=0&to_ms={}&model=gpt-5.5&account_id=acc-2&cache_key=sess-a", now_ms() + 100_000))
        .header("Authorization", "Bearer unused").send().await.unwrap();
    assert_eq!(filtered.status(), 200);
    let filtered_body: serde_json::Value = filtered.json().await.unwrap();
    let filtered_rows = filtered_body["requests"].as_array().unwrap();
    assert_eq!(filtered_rows.len(), 1);
    assert_eq!(filtered_rows[0]["input_tokens"], 11);
    let forbidden = http
        .get(format!("{base}/admin/api/usage"))
        .send()
        .await
        .unwrap();
    assert_eq!(forbidden.status(), 401);
    let forbidden_details = http
        .get(format!("{base}/admin/api/usage/requests"))
        .send()
        .await
        .unwrap();
    assert_eq!(forbidden_details.status(), 401);

    // phase 4: both accounts exhausted -> fail fast with earliest reset
    mock.lock().unwrap().failing.insert("cop-acc-2".into());
    let r5 = post(Some("sess-c"), true).await;
    assert_eq!(r5.status(), 429, "saturation must fail fast");
    let retry_after = r5
        .headers()
        .get(axum::http::header::RETRY_AFTER)
        .expect("saturation response missing Retry-After")
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        retry_after.parse::<u64>().is_ok(),
        "Retry-After not numeric"
    );

    let accounts = store.list_accounts();
    let cooling: Vec<_> = accounts
        .iter()
        .filter(|a| a.status == AccountStatus::Cooling)
        .collect();
    assert_eq!(
        cooling.len(),
        2,
        "both accounts should be cooling after 429s"
    );

    // phase 5: cooldown expiry restores the pool automatically
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    mock.lock().unwrap().failing.clear();
    let r6 = post(Some("sess-d"), true).await;
    assert_eq!(
        r6.status(),
        200,
        "accounts must return to rotation after cooldown"
    );
    let body6 = r6.text().await.unwrap();
    assert!(
        body6.contains("hello from tok-acc-"),
        "served by a pool account"
    );

    // persisted cooling rows are normalized (what the sync loop / a restart does)
    let mut fresh_core = PoolCore::new(&store, openai_surfaces());
    fresh_core.sync_from_store(&store);
    let accounts = store.list_accounts();
    assert!(
        accounts.iter().all(|a| a.status == AccountStatus::Healthy),
        "stale cooling rows must be normalized in the store: {accounts:?}"
    );

    // phase 6: request log records the serving account
    {
        let log = logs.lock().unwrap();
        let entry = log
            .iter()
            .find(|e| e.sticky == "sess-a")
            .expect("log entry for sess-a");
        assert_eq!(entry.account.as_deref(), Some("acc-1"));
        assert_eq!(entry.backend.as_deref(), Some("codex"));
        assert_eq!(entry.status, 200);
    }

    // phase 7: 401 on one backend fails over and marks auth_error
    let store2 = Arc::new(Store::in_memory().unwrap());
    let m2 = model("m");
    store2.set_catalog(BackendId::Codex, &[m2.clone()]);
    store2.set_catalog(BackendId::Copilot, &[m2]);
    store2.upsert_account(&codex_account("acc-1", "bad-token"));
    store2.upsert_account(&copilot_account("acc-2", "tok-good"));
    let (base2, _logs2) = spawn_app(store2.clone(), 400).await;
    let resp = http
        .post(format!("{base2}/v1/chat/completions"))
        .header("Authorization", "Bearer test-key")
        .json(&serde_json::json!({"model": "m", "messages": [{"role":"user","content":"hi"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "401 on codex must fail over to copilot account"
    );
    let acc1 = store2.get_account("acc-1").unwrap();
    assert_eq!(
        acc1.status,
        AccountStatus::AuthError,
        "401 must mark auth_error"
    );
    let acc2 = store2.get_account("acc-2").unwrap();
    assert_eq!(acc2.status, AccountStatus::Healthy);
    let streamed_chat = http.post(format!("{base2}/v1/chat/completions"))
        .header("Authorization", "Bearer test-key")
        .json(&serde_json::json!({"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true,"prompt_cache_key":"chat-session"}))
        .send().await.unwrap();
    assert_eq!(streamed_chat.status(), 200);
    streamed_chat.text().await.unwrap();
    assert_eq!(mock.lock().unwrap().chat_usage_options.back(), Some(&true));
    let chat_rows = store2
        .usage_records(&underclass::store::UsageQuery {
            cache_key: Some("chat-session".into()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(chat_rows.len(), 1);
    assert_eq!(
        (chat_rows[0].input_tokens, chat_rows[0].output_tokens),
        (Some(11), Some(3))
    );
    mock.lock().unwrap().reject_stream_usage = true;
    let fallback_chat = http.post(format!("{base2}/v1/chat/completions"))
        .header("Authorization", "Bearer test-key")
        .json(&serde_json::json!({"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true,"prompt_cache_key":"chat-fallback"}))
        .send().await.unwrap();
    assert_eq!(fallback_chat.status(), 200);
    fallback_chat.text().await.unwrap();
    let after_fallback = http.post(format!("{base2}/v1/chat/completions"))
        .header("Authorization", "Bearer test-key")
        .json(&serde_json::json!({"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true,"prompt_cache_key":"chat-after-fallback"}))
        .send().await.unwrap();
    assert_eq!(after_fallback.status(), 200);
    after_fallback.text().await.unwrap();
    let options = mock.lock().unwrap().chat_usage_options.clone();
    assert!(options.len() >= 4);
    assert_eq!(
        options.iter().rev().take(3).copied().collect::<Vec<_>>(),
        vec![false, false, true]
    );
    let fallback_rows = store2
        .usage_records(&underclass::store::UsageQuery {
            cache_key: Some("chat-fallback".into()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(fallback_rows.len(), 2);
    assert!(
        fallback_rows
            .iter()
            .any(|r| r.status == 400 && r.input_tokens.is_none())
    );
    assert!(
        fallback_rows
            .iter()
            .any(|r| r.status == 200 && r.input_tokens == Some(11))
    );
    mock.lock().unwrap().reject_stream_usage = false;

    let aborted = http.post(format!("{base2}/v1/responses"))
        .header("Authorization", "Bearer test-key")
        .json(&serde_json::json!({"model":"m","input":"hi","stream":true,"prompt_cache_key":"aborted-session","slow_abort":true}))
        .send().await.unwrap();
    assert_eq!(aborted.status(), 200);
    drop(aborted);
    let abort_rows = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let rows = store2
                .usage_records(&underclass::store::UsageQuery {
                    cache_key: Some("aborted-session".into()),
                    ..Default::default()
                })
                .unwrap();
            if !rows.is_empty() {
                break rows;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("aborted stream was never recorded");
    assert_eq!(abort_rows.len(), 1);
    assert_eq!(
        (abort_rows[0].input_tokens, abort_rows[0].output_tokens),
        (None, None)
    );

    // phase 8: models endpoint + auth enforcement
    let models = http
        .get(format!("{base}/v1/models"))
        .header("Authorization", "Bearer test-key")
        .send()
        .await
        .unwrap();
    assert_eq!(models.status(), 200);
    let models_body: serde_json::Value = models.json().await.unwrap();
    let ids: Vec<&str> = models_body["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&"gpt-5.5"));

    let unauth = http
        .post(format!("{base}/v1/responses"))
        .json(&serde_json::json!({"model": "gpt-5.5"}))
        .send()
        .await
        .unwrap();
    assert_eq!(unauth.status(), 401, "missing proxy key must be rejected");

    store.delete_account("acc-1");
    let history = store
        .usage_records(&underclass::store::UsageQuery {
            account_id: Some("acc-1".into()),
            ..Default::default()
        })
        .unwrap();
    assert!(
        !history.is_empty(),
        "account removal must retain token accounting history"
    );
}

/// A 403 that clears after a token refresh must not escalate the account to `auth_error`:
/// selection never returns an `AuthError` account, so a single redemption-triggered 403 would
/// otherwise strand a perfectly good subscription outside the pool with no way back.
#[tokio::test(flavor = "multi_thread")]
async fn e2e_forbidden_403_refreshes_instead_of_bricking_account() {
    let mock = mock_upstream();
    // Copilot accounts are used deliberately: their `force_refresh` returns the stored token
    // without a network round trip, so this exercises the 403 path hermetically.
    let store = Arc::new(Store::in_memory().unwrap());
    let m = model("m");
    store.set_catalog(BackendId::Codex, &[m.clone()]);
    store.set_catalog(BackendId::Copilot, &[m]);
    store.upsert_account(&copilot_account("cop-403", "cop-403-token"));
    store.upsert_account(&copilot_account("cop-spare", "cop-spare-token"));
    mock.lock()
        .unwrap()
        .forbidden
        .insert("cop-403-token".into(), 1);
    let (base, _logs) = spawn_app(store.clone(), 400).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/v1/responses"))
        .header("Authorization", "Bearer test-key")
        .json(&serde_json::json!({"model": "m", "input": "hi", "stream": true}))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert_eq!(status, 200, "403 account body: {body}");
    assert!(
        body.contains("cop-403-token"),
        "the 403 account must be retried after refresh and serve the request, got: {body}"
    );
    assert_eq!(
        store.get_account("cop-403").unwrap().status,
        AccountStatus::Healthy,
        "a 403 that survives one token refresh must not mark the account auth_error"
    );
    assert_eq!(
        mock.lock().unwrap().forbidden.get("cop-403-token"),
        Some(&0)
    );
}

/// `enable` is the operator's recovery lever. It must clear a stranded `auth_error` account, and it
/// must not silently discard an upstream quota deadline that `PoolCore::sweep` would honour.
#[tokio::test(flavor = "multi_thread")]
async fn e2e_enable_clears_auth_error_but_preserves_quota_cooldown() {
    let _mock = mock_upstream();
    let store = Arc::new(Store::in_memory().unwrap());
    let m = model("m");
    store.set_catalog(BackendId::Codex, &[m.clone()]);
    store.set_catalog(BackendId::Copilot, &[m]);
    store.upsert_account(&copilot_account("acct-auth", "tok-auth"));
    store.upsert_account(&copilot_account("acct-cooling", "tok-cooling"));
    store.upsert_account(&copilot_account("acct-disabled", "tok-disabled"));
    let (base, _logs) = spawn_app(store.clone(), 400).await;
    let http = reqwest::Client::new();

    let enable = |id: &str| {
        let http = http.clone();
        let base = base.clone();
        let id = id.to_string();
        async move {
            http.post(format!("{base}/admin/api/accounts/{id}/enable"))
                .header("Authorization", "Bearer unused")
                .send()
                .await
                .unwrap()
        }
    };

    assert_eq!(enable("no-such-account").await.status(), 404);

    // A stranded auth_error account is exactly the dead end this endpoint exists to undo.
    store.update_account_status("acct-auth", AccountStatus::AuthError, 0);
    assert_eq!(enable("acct-auth").await.status(), 204);
    let authed = store.get_account("acct-auth").unwrap();
    assert_eq!(authed.status, AccountStatus::Healthy);
    assert_eq!(authed.reset_at, 0);

    // An operator-set disable still clears.
    store.update_account_status("acct-disabled", AccountStatus::Disabled, 0);
    assert_eq!(enable("acct-disabled").await.status(), 204);
    assert_eq!(
        store.get_account("acct-disabled").unwrap().status,
        AccountStatus::Healthy
    );

    // A quota cooldown is upstream state, not an operator block: enabling must not wipe it,
    // otherwise the account is re-selected while still out of quota.
    let deadline = now_ms() + 3_600_000;
    store.update_account_status("acct-cooling", AccountStatus::Cooling, deadline);
    assert_eq!(enable("acct-cooling").await.status(), 204);
    let cooling = store.get_account("acct-cooling").unwrap();
    assert_eq!(cooling.status, AccountStatus::Cooling);
    assert_eq!(
        cooling.reset_at, deadline,
        "enable must preserve an upstream quota deadline"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn e2e_failed_manual_refresh_leaves_account_state_alone() {
    let _mock = mock_upstream();
    let store = Arc::new(Store::in_memory().unwrap());
    store.upsert_account(&copilot_account("acct-copilot", "tok-copilot"));
    // No stored refresh token makes the refresh fail before it reaches any issuer.
    let mut codex = codex_account("acct-codex", "tok-codex");
    codex.refresh_token = None;
    store.upsert_account(&codex);
    let deadline = now_ms() + 3_600_000;
    store.update_account_status("acct-codex", AccountStatus::Cooling, deadline);
    let (base, _logs) = spawn_app(store.clone(), 400).await;
    let http = reqwest::Client::new();

    let refresh = |id: &str| {
        let http = http.clone();
        let base = base.clone();
        let id = id.to_string();
        async move {
            let response = http
                .post(format!("{base}/admin/api/accounts/{id}/refresh"))
                .header("Authorization", "Bearer unused")
                .send()
                .await
                .unwrap();
            let status = response.status().as_u16();
            let body: serde_json::Value = response.json().await.unwrap_or_default();
            (status, body)
        }
    };

    let (status, body) = refresh("no-such-account").await;
    assert_eq!(status, 404);
    assert_eq!(body["error"], "unknown_account");

    // A Copilot token is never exchanged, so a "refresh" would prove nothing about it.
    store.update_account_status("acct-copilot", AccountStatus::AuthError, 0);
    let (status, body) = refresh("acct-copilot").await;
    assert_eq!(status, 409);
    assert_eq!(body["error"], "not_refreshable");
    assert_eq!(
        store.get_account("acct-copilot").unwrap().status,
        AccountStatus::AuthError
    );

    let (status, body) = refresh("acct-codex").await;
    assert_eq!(status, 502);
    assert_eq!(body["error"], "refresh_failed");
    let after = store.get_account("acct-codex").unwrap();
    assert_eq!(after.status, AccountStatus::Cooling);
    assert_eq!(
        after.reset_at, deadline,
        "a failed refresh must keep the quota deadline"
    );
}

async fn wait_for_rows(store: &Store, count: usize) -> Vec<underclass::store::UsageRecord> {
    for _ in 0..100 {
        let rows = store.usage_records(&Default::default()).unwrap();
        if rows.len() >= count {
            return rows;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    store.usage_records(&Default::default()).unwrap()
}

async fn post_stream(base: &str, key: &str) -> (u16, String) {
    let response = reqwest::Client::new()
        .post(format!("{base}/v1/responses"))
        .header("Authorization", "Bearer test-key")
        .json(&serde_json::json!({"model": "gpt-5.5", "input": "hi", "stream": true, "prompt_cache_key": key}))
        .send()
        .await
        .unwrap();
    let status = response.status().as_u16();
    (status, response.text().await.unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn e2e_overload_before_output_fails_over_to_another_account() {
    mock_upstream();
    let store = Arc::new(Store::in_memory().unwrap());
    store.set_catalog(BackendId::Codex, &[model("gpt-5.5")]);
    // `fo-a` sorts first, so a new sticky key binds to the overloaded account.
    store.upsert_account(&codex_account("fo-a", "tok-ovl-fo-a"));
    store.upsert_account(&codex_account("fo-b", "tok-fo-b"));
    let (base, _logs) = spawn_app(store.clone(), 400).await;

    let (status, body) = post_stream(&base, "failover-session").await;
    assert_eq!(status, 200);
    assert!(
        body.contains("hello from tok-fo-b"),
        "served by the healthy account: {body}"
    );
    assert!(
        !body.contains("server_is_overloaded"),
        "the discarded attempt never reaches the client"
    );

    let rows = wait_for_rows(&store, 2).await;
    let failed = rows
        .iter()
        .find(|r| r.account_id == "fo-a")
        .expect("overloaded attempt recorded");
    assert_eq!(failed.stream_outcome.as_deref(), Some("failed"));
    assert_eq!(failed.error_code.as_deref(), Some("server_is_overloaded"));
    assert_eq!(failed.saw_output, Some(false));
    let served = rows
        .iter()
        .find(|r| r.account_id == "fo-b")
        .expect("retry recorded");
    assert_eq!(
        served.request_id, failed.request_id,
        "one client request, two attempts"
    );

    // The session was rebound, so the next turn goes straight to the healthy account.
    let (_, body) = post_stream(&base, "failover-session").await;
    assert!(body.contains("hello from tok-fo-b"));
    assert_eq!(
        wait_for_rows(&store, 3).await.len(),
        3,
        "no second overloaded attempt"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn e2e_overload_on_every_account_replays_upstream_error() {
    mock_upstream();
    let store = Arc::new(Store::in_memory().unwrap());
    store.set_catalog(BackendId::Codex, &[model("gpt-5.5")]);
    store.upsert_account(&codex_account("all-a", "tok-ovl-all-a"));
    store.upsert_account(&codex_account("all-b", "tok-ovl-all-b"));
    let (base, _logs) = spawn_app(store.clone(), 400).await;

    let (status, body) = post_stream(&base, "doomed-session").await;
    assert_eq!(status, 200, "the upstream status is preserved");
    assert!(
        body.contains("server_is_overloaded"),
        "the client sees the upstream error: {body}"
    );
    let rows = wait_for_rows(&store, 2).await;
    let accounts: BTreeSet<_> = rows.iter().map(|r| r.account_id.as_str()).collect();
    assert_eq!(
        accounts,
        BTreeSet::from(["all-a", "all-b"]),
        "each account tried once"
    );
    for account in ["all-a", "all-b"] {
        let healthy = store
            .list_accounts()
            .into_iter()
            .find(|a| a.id == account)
            .unwrap()
            .status;
        assert_eq!(
            healthy,
            AccountStatus::Healthy,
            "an overload must not cool the account"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn e2e_overload_after_output_passes_through() {
    mock_upstream();
    let store = Arc::new(Store::in_memory().unwrap());
    store.set_catalog(BackendId::Codex, &[model("gpt-5.5")]);
    store.upsert_account(&codex_account("late-a", "tok-late-ovl-late-a"));
    store.upsert_account(&codex_account("late-b", "tok-late-b"));
    let (base, _logs) = spawn_app(store.clone(), 400).await;

    let (status, body) = post_stream(&base, "late-session").await;
    assert_eq!(status, 200);
    assert!(
        body.contains("partial") && body.contains("server_is_overloaded"),
        "{body}"
    );
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let rows = wait_for_rows(&store, 1).await;
    assert_eq!(
        rows.len(),
        1,
        "output already reached the client, so no retry"
    );
    assert_eq!(rows[0].saw_output, Some(true));
}

/// The Anthropic surface serves only official Claude Code clients, forwards their headers and
/// body verbatim, and spends the pooled account's credential instead of the client's own.
#[tokio::test]
async fn e2e_anthropic_surface_serves_claude_code_clients_only() {
    let mock = mock_upstream();
    let store = Arc::new(Store::in_memory().unwrap());
    let mut claude = account("claude-1", BackendId::Claude, "claude-token-1");
    claude.refresh_token = Some("claude-refresh-1".into());
    store.upsert_account(&claude);
    let (base, _logs) = spawn_app(store, 60_000).await;

    // A client that is not Claude Code is rejected before a pooled credential is spent.
    let foreign = reqwest::Client::new()
        .post(format!("{base}/v1/messages"))
        .header("Authorization", "Bearer test-key")
        .json(&serde_json::json!({
            "model": "claude-haiku-5-5",
            "max_tokens": 16,
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        foreign.status(),
        403,
        "only Claude Code clients are admitted"
    );
    let spent = mock
        .lock()
        .unwrap()
        .served_by
        .iter()
        .any(|token| token == "claude-token-1");
    assert!(
        !spent,
        "a rejected client must not spend a pooled credential"
    );

    // Claude Code traffic travels upstream with the pooled credential and the client's own
    // headers and body.
    let response = reqwest::Client::new()
        .post(format!("{base}/v1/messages?beta=true"))
        .header("Authorization", "Bearer test-key")
        .header("x-app", "cli")
        .header("user-agent", "claude-cli/2.1.304 (external, sdk-cli)")
        .header(
            "anthropic-beta",
            "oauth-2025-04-20,claude-code-20250219",
        )
        .header("x-claude-code-session-id", "sess-claude")
        .json(&serde_json::json!({
            "model": "claude-haiku-5-5",
            "max_tokens": 16,
            "system": [
                {"type": "text", "text": "x-anthropic-billing-header: cc_version=2.1.304.c61; cc_entrypoint=sdk-cli;"},
                {"type": "text", "text": "You are a Claude agent, built on Anthropic's Claude Agent SDK."}
            ],
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(
        body["account"], "claude-token-1",
        "the pooled credential replaces the client's own"
    );
    assert_eq!(body["app"], "cli", "client headers travel upstream");
    assert_eq!(body["session"], "sess-claude");
    assert!(
        body["beta"].as_str().unwrap().contains("oauth-2025-04-20"),
        "the oauth beta is present: {}",
        body["beta"]
    );
    assert_eq!(
        body["body"]["system"][0]["text"],
        "x-anthropic-billing-header: cc_version=2.1.304.c61; cc_entrypoint=sdk-cli;",
        "the request body travels upstream unchanged"
    );
}

/// A Claude Code subscription window that is exhausted cools the account until the window
/// resets, and the next Claude account serves the request.
#[tokio::test]
async fn e2e_anthropic_window_quota_cools_the_account() {
    let mock = mock_upstream();
    let store = Arc::new(Store::in_memory().unwrap());
    for id in ["claude-1", "claude-2"] {
        let token = if id == "claude-1" {
            "claude-quota-1".to_string()
        } else {
            format!("{id}-token")
        };
        let mut claude = account(id, BackendId::Claude, &token);
        claude.refresh_token = Some(format!("{id}-refresh"));
        store.upsert_account(&claude);
    }
    let (base, _logs) = spawn_app(store.clone(), 60_000).await;

    let request = || async {
        reqwest::Client::new()
            .post(format!("{base}/v1/messages"))
            .header("Authorization", "Bearer test-key")
            .header("x-app", "cli")
            .header("user-agent", "claude-cli/2.1.304 (external, sdk-cli)")
            .header(
                "anthropic-beta",
                "oauth-2025-04-20,claude-code-20250219",
            )
            .json(&serde_json::json!({
                "model": "claude-haiku-5-5",
                "max_tokens": 16,
                "system": [{"type": "text", "text": "x-anthropic-billing-header: cc_version=2.1.304.c61; cc_entrypoint=sdk-cli;"}],
                "messages": [{"role": "user", "content": "hi"}]
            }))
            .send()
            .await
            .unwrap()
    };

    let first = request().await;
    assert_eq!(
        first.status(),
        200,
        "the second Claude account serves the request"
    );
    let served_healthy = mock
        .lock()
        .unwrap()
        .served_by
        .iter()
        .any(|token| token == "claude-2-token");
    assert!(served_healthy, "the healthy account served the request");

    // The exhausted account is cooling until the window reset the upstream reported.
    let accounts = store.list_accounts();
    let cooling = accounts
        .iter()
        .find(|a| a.id == "claude-1")
        .expect("the exhausted account");
    assert_eq!(cooling.status, AccountStatus::Cooling);
    assert!(
        cooling.reset_at > now_ms(),
        "the cooldown follows the window reset"
    );
}

/// A model-scoped rejection cools that model on the account; the account keeps serving its
/// other models.
#[tokio::test]
async fn e2e_anthropic_model_scoped_limit_leaves_other_models_alone() {
    let mock = mock_upstream();
    let store = Arc::new(Store::in_memory().unwrap());
    let mut claude = account("claude-1", BackendId::Claude, "claude-model-quota-1");
    claude.refresh_token = Some("claude-refresh-1".into());
    store.upsert_account(&claude);
    let (base, _logs) = spawn_app(store.clone(), 60_000).await;

    let request = |model: &'static str| {
        let base = base.clone();
        async move {
            reqwest::Client::new()
            .post(format!("{base}/v1/messages"))
            .header("Authorization", "Bearer test-key")
            .header("x-app", "cli")
            .header("user-agent", "claude-cli/2.1.304 (external, sdk-cli)")
            .header(
                "anthropic-beta",
                "oauth-2025-04-20,claude-code-20250219",
            )
            .json(&serde_json::json!({
                "model": model,
                "max_tokens": 16,
                "system": [{"type": "text", "text": "x-anthropic-billing-header: cc_version=2.1.304.c61; cc_entrypoint=sdk-cli;"}],
                "messages": [{"role": "user", "content": "hi"}]
            }))
            .send()
            .await
            .unwrap()
        }
    };

    let refused = request("claude-opus-5-5").await;
    assert_eq!(
        refused.status(),
        429,
        "the refused model is reported to the client"
    );

    // The account is not cooling as a whole: another model still reaches it.
    let accounts = store.list_accounts();
    let account = accounts.first().expect("the account");
    assert_ne!(
        account.status,
        AccountStatus::Cooling,
        "a model-scoped limit must not cool the account"
    );

    let other = request("claude-sonnet-5-5").await;
    assert_eq!(
        other.status(),
        200,
        "the account still serves its other models"
    );
    let served = mock
        .lock()
        .unwrap()
        .served_by
        .iter()
        .any(|t| t == "claude-model-quota-1");
    assert!(served, "the account served the other model");
}

/// The root-level `/messages` alias serves the same Anthropic surface as `/v1/messages`.
#[tokio::test]
async fn e2e_anthropic_messages_alias_serves_the_same_surface() {
    let mock = mock_upstream();
    let store = Arc::new(Store::in_memory().unwrap());
    let mut claude = account("claude-1", BackendId::Claude, "claude-alias-token");
    claude.refresh_token = Some("claude-refresh-1".into());
    store.upsert_account(&claude);
    let (base, _logs) = spawn_app(store, 60_000).await;

    let response = reqwest::Client::new()
        .post(format!("{base}/messages"))
        .header("Authorization", "Bearer test-key")
        .header("x-app", "cli")
        .header("user-agent", "claude-cli/2.1.304 (external, sdk-cli)")
        .header("anthropic-beta", "oauth-2025-04-20,claude-code-20250219")
        .json(&serde_json::json!({
            "model": "claude-haiku-5-5",
            "max_tokens": 16,
            "system": [{"type": "text", "text": "x-anthropic-billing-header: cc_version=2.1.304.c61; cc_entrypoint=sdk-cli;"}],
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["account"], "claude-alias-token");
    let served = mock
        .lock()
        .unwrap()
        .served_by
        .iter()
        .any(|token| token == "claude-alias-token");
    assert!(served, "the alias reached the upstream");
}

async fn retry_response(base: &str) -> reqwest::Response {
    let request_id = uuid::Uuid::now_v7().to_string();
    let response = reqwest::Client::new()
        .post(format!("{base}/v1/responses"))
        .header("Authorization", "Bearer test-key")
        .header("x-request-id", &request_id)
        .header("x-underclass-retry-safe", "pre-output-overload")
        .json(&serde_json::json!({"model":"gpt-5.5","input":"hi","stream":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.headers()["x-request-id"], request_id);
    response
}

/// @cc [owner:ghuntley,label:verification;proxy] streamed-media-type-http-regression
/// The real proxy HTTP boundary MUST label requested SSE correctly even when
/// upstream supplies application/json or no media type. Success and certified
/// overload replay MUST retain exact bytes and correlation; non-stream JSON
/// and HTTP failures MUST retain their media types without retry authority.
#[tokio::test]
async fn e2e_stream_media_type_matches_live_and_replayed_bytes() {
    mock_upstream();
    for (case, streaming, status, media_type, payload, certified) in [
        (
            "success-json",
            true,
            200,
            "text/event-stream",
            MEDIA_SUCCESS,
            false,
        ),
        (
            "success-missing",
            true,
            200,
            "text/event-stream",
            MEDIA_SUCCESS,
            false,
        ),
        (
            "overload-json",
            true,
            200,
            "text/event-stream",
            MEDIA_OVERLOAD,
            true,
        ),
        (
            "overload-missing",
            true,
            200,
            "text/event-stream",
            MEDIA_OVERLOAD,
            true,
        ),
        ("json", false, 200, "application/json", MEDIA_JSON, false),
        (
            "http-error",
            true,
            503,
            "application/json",
            MEDIA_HTTP_ERROR,
            false,
        ),
    ] {
        let store = Arc::new(Store::in_memory().unwrap());
        store.set_catalog(BackendId::Codex, &[model("gpt-5.5")]);
        store.upsert_account(&codex_account("media-type", &format!("tok-media-{case}")));
        let (base, _) = spawn_app(store, 400).await;
        let request_id = uuid::Uuid::now_v7().to_string();
        let response = reqwest::Client::new()
            .post(format!("{base}/v1/responses"))
            .header("Authorization", "Bearer test-key")
            .header("x-request-id", &request_id)
            .json(&serde_json::json!({"model":"gpt-5.5","input":"hi","stream":streaming}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), status, "case: {case}");
        assert_eq!(
            response.headers()["content-type"],
            media_type,
            "case: {case}"
        );
        assert_eq!(
            response.headers()["x-request-id"],
            request_id,
            "case: {case}"
        );
        assert_eq!(
            response
                .headers()
                .get("x-underclass-retry-safe")
                .map(|value| value.to_str().unwrap()),
            certified.then_some("pre-output-overload"),
            "case: {case}"
        );
        assert_eq!(response.text().await.unwrap(), payload, "case: {case}");
    }
}

#[tokio::test]
async fn e2e_retry_certificate_covers_all_and_only_proven_overload_chains() {
    mock_upstream();
    for (tokens, expected) in [
        (vec!["tok-proof-exact"], true),
        (vec!["tok-ovl-first", "tok-ovl-second"], true),
        (vec!["tok-proof-malformed"], false),
        (vec!["tok-proof-unknown"], false),
        (vec!["tok-proof-tool"], false),
        (vec!["tok-proof-http"], false),
        (vec!["tok-proof-policy"], false),
        (vec!["tok-proof-quota"], false),
        (vec!["tok-proof-broad"], false),
        (vec!["tok-proof-sanitized"], false),
        (vec!["tok-proof-truncated"], false),
        (vec!["tok-proof-success"], false),
        (vec!["tok-ovl-first", "tok-proof-success"], false),
        (vec!["tok-proof-http", "tok-ovl-second"], false),
        (vec!["tok-ovl-first", "tok-proof-http"], false),
        (vec!["missing-credential", "tok-ovl-second"], false),
        (vec!["tok-ovl-first", "missing-credential"], false),
        (vec!["tok-late-ovl-first", "tok-proof-exact"], false),
    ] {
        let store = Arc::new(Store::in_memory().unwrap());
        store.set_catalog(BackendId::Codex, &[model("gpt-5.5")]);
        for (index, token) in tokens.iter().enumerate() {
            let mut account = codex_account(&format!("proof-{index}"), token);
            if *token == "missing-credential" {
                account.access_token = None;
                account.refresh_token = None;
            }
            store.upsert_account(&account);
        }
        let (base, _) = spawn_app(store.clone(), 400).await;
        let response = retry_response(&base).await;
        assert_eq!(
            response
                .headers()
                .get("x-underclass-retry-safe")
                .map(|v| v.to_str().unwrap()),
            expected.then_some("pre-output-overload"),
            "chain: {tokens:?}"
        );
        let body = response.text().await.unwrap();
        if expected {
            assert!(body.contains("server_is_overloaded"));
            assert_eq!(
                wait_for_rows(&store, tokens.len()).await.len(),
                tokens.len()
            );
        }
    }
}

struct DisconnectedCodex {
    inner: CodexBackend,
    address: std::net::SocketAddr,
}

impl underclass::provider::Backend for DisconnectedCodex {
    fn id(&self) -> BackendId {
        BackendId::Codex
    }
    fn surfaces(&self) -> &'static [Surface] {
        &[Surface::OpenAI]
    }
    fn default_cooldown_ms(&self) -> i64 {
        self.inner.default_cooldown_ms()
    }
    fn rewrite_url(&self, path: &str, account: &Account) -> String {
        if account.access_token.as_deref() == Some("disconnect") {
            format!("http://{}{path}", self.address)
        } else {
            self.inner.rewrite_url(path, account)
        }
    }
    fn inject_headers(
        &self,
        account: &Account,
        token: &str,
        sticky: Option<&str>,
        body: &serde_json::Value,
        client: &http::HeaderMap,
        headers: &mut http::HeaderMap,
    ) {
        self.inner
            .inject_headers(account, token, sticky, body, client, headers);
    }
}

#[tokio::test]
async fn e2e_saved_overload_cannot_certify_a_later_transport_failure() {
    mock_upstream();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let store = Arc::new(Store::in_memory().unwrap());
    store.set_catalog(BackendId::Codex, &[model("gpt-5.5")]);
    store.upsert_account(&codex_account("transport-a", "tok-ovl-transport"));
    store.upsert_account(&codex_account("transport-b", "disconnect"));
    let backend = Arc::new(DisconnectedCodex {
        inner: CodexBackend { cooldown_ms: 400 },
        address,
    });
    let (base, _) = spawn_app_with_codex(store.clone(), 400, backend).await;
    let response = retry_response(&base).await;
    assert!(!response.headers().contains_key("x-underclass-retry-safe"));
    assert!(
        response
            .text()
            .await
            .unwrap()
            .contains("server_is_overloaded")
    );
    let rows = wait_for_rows(&store, 2).await;
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter()
            .any(|row| row.account_id == "transport-b" && row.status == 0)
    );
}
