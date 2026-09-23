use axum::extract::State;
use axum::response::IntoResponse;
use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};
use underclass::codex::CodexBackend;
use underclass::copilot::CopilotBackend;
use underclass::flows::FlowRegistry;
use underclass::models::{now_ms, Account, AccountStatus, BackendId, ModelInfo, RequestLogEntry};
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
}

async fn mock_responses(
    State(state): State<Arc<Mutex<MockState>>>,
    headers: axum::http::HeaderMap,
    body: String,
) -> impl IntoResponse {
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
            [(axum::http::header::CONTENT_TYPE, "application/json".to_string())],
            r#"{"error":{"message":"bad token"}}"#.to_string(),
        );
    }

    let failing = state.lock().unwrap().failing.contains(&bearer);
    if failing {
        state.lock().unwrap().fourtwonined.push_back(bearer);
        return (
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            [(axum::http::header::CONTENT_TYPE, "application/json".to_string())],
            r#"{"error":{"code":"usage_limit_reached","message":"usage limit reached"}}"#.to_string(),
        );
    }

    state.lock().unwrap().served_by.push_back(bearer.clone());
    let payload = format!(
        "data: {{\"type\":\"response.started\",\"account\":\"{bearer}\"}}\n\ndata: {{\"type\":\"output_text.delta\",\"delta\":\"hello from {bearer}\"}}\n\ndata: [DONE]\n\n"
    );
    if wants_stream {
        (
            axum::http::StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "text/event-stream".to_string())],
            payload,
        )
    } else {
        (
            axum::http::StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/json".to_string())],
            format!(r#"{{"account":"{bearer}","output_text":"hello from {bearer}"}}"#),
        )
    }
}

async fn mock_upstream() -> &'static Arc<Mutex<MockState>> {
    static STATE: OnceLock<Arc<Mutex<MockState>>> = OnceLock::new();
    static URL: OnceLock<String> = OnceLock::new();
    STATE.get_or_init(Default::default);
    if URL.get().is_none() {
        let app = axum::Router::new()
            .fallback(mock_responses)
            .with_state(STATE.get().unwrap().clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        unsafe {
            std::env::set_var("UNDERCLASS_CODEX_UPSTREAM", format!("http://{addr}"));
            std::env::set_var("UNDERCLASS_COPILOT_UPSTREAM", format!("http://{addr}"));
        }
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        URL.set(format!("http://{addr}")).ok();
    }
    STATE.get().unwrap()
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
        account_id: Some(format!("chatgpt-{id}")),
        residency: None,
        enterprise_url: None,
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

fn model(id: &str) -> ModelInfo {
    ModelInfo {
        id: id.into(),
        name: id.into(),
        context: 400_000,
        input: 272_000,
        output: 128_000,
    }
}

async fn spawn_app(store: Arc<Store>, cooldown_ms: i64) -> (String, Arc<Mutex<VecDeque<RequestLogEntry>>>) {
    let client = reqwest::Client::new();
    let core = PoolCore::new(&store);
    let pool = Arc::new(Mutex::new(core));
    let tokens = Arc::new(TokenManager::new(store.clone(), client.clone()));
    let mut backends: BackendMap = std::collections::HashMap::new();
    backends.insert(BackendId::Codex, Arc::new(CodexBackend { cooldown_ms }));
    backends.insert(BackendId::Copilot, Arc::new(CopilotBackend { cooldown_ms }));
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
            reqwest::Client::new(), "http://127.0.0.1:1".to_string(), false,
        )),
    });

    let v1 = axum::Router::new()
        .route("/models", axum::routing::get(underclass::proxy::models))
        .route("/responses", axum::routing::post(underclass::proxy::infer))
        .route(
            "/chat/completions",
            axum::routing::post(underclass::proxy::infer),
        )
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            underclass::proxy::require_proxy_key,
        ))
        .with_state(state.clone());

    let app = axum::Router::new().nest("/v1", v1)
        .layer(axum::middleware::from_fn(underclass::correlation::middleware))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), logs)
}

#[tokio::test(flavor = "multi_thread")]
async fn e2e_full_pool_story() {
    let mock = mock_upstream().await;

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
            let request_id = uuid::Uuid::new_v4().to_string();
            let response = http.post(format!("{base}/v1/responses"))
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
    assert!(body1.contains("[DONE]"), "stream passthrough broken: {body1}");
    assert!(body1.contains("hello from tok-acc-1"), "unexpected payload: {body1}");

    let r2 = post(Some("sess-a"), true).await;
    let body2 = r2.text().await.unwrap();
    assert!(body2.contains("hello from tok-acc-1"), "sticky session moved accounts: {body2}");

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
        mock.lock().unwrap().fourtwonined.contains(&"tok-acc-1".to_string()),
        "mock never saw the 429-causing request for acc-1"
    );
    assert!(logs.lock().unwrap().iter().filter(|entry| entry.request_id == failover_id).count() >= 2,
        "failed and successful attempts must share the correlation ID in request history");

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
    assert!(retry_after.parse::<u64>().is_ok(), "Retry-After not numeric");

    let accounts = store.list_accounts();
    let cooling: Vec<_> = accounts
        .iter()
        .filter(|a| a.status == AccountStatus::Cooling)
        .collect();
    assert_eq!(cooling.len(), 2, "both accounts should be cooling after 429s");

    // phase 5: cooldown expiry restores the pool automatically
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    mock.lock().unwrap().failing.clear();
    let r6 = post(Some("sess-d"), true).await;
    assert_eq!(r6.status(), 200, "accounts must return to rotation after cooldown");
    let body6 = r6.text().await.unwrap();
    assert!(body6.contains("hello from tok-acc-"), "served by a pool account");

    // persisted cooling rows are normalized (what the sync loop / a restart does)
    let mut fresh_core = PoolCore::new(&store);
    fresh_core.sync_from_store(&store);
    let accounts = store.list_accounts();
    assert!(
        accounts.iter().all(|a| a.status == AccountStatus::Healthy),
        "stale cooling rows must be normalized in the store: {accounts:?}"
    );

    // phase 6: request log records the serving account
    {
        let log = logs.lock().unwrap();
        let entry = log.iter().find(|e| e.sticky == "sess-a").expect("log entry for sess-a");
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
    assert_eq!(resp.status(), 200, "401 on codex must fail over to copilot account");
    let acc1 = store2.get_account("acc-1").unwrap();
    assert_eq!(acc1.status, AccountStatus::AuthError, "401 must mark auth_error");
    let acc2 = store2.get_account("acc-2").unwrap();
    assert_eq!(acc2.status, AccountStatus::Healthy);

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
}
