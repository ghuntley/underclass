use crate::models::{now_ms, AccountStatus, BackendId, RequestLogEntry};
use crate::proxy::AppState;
use axum::extract::{Path, Query, State};
use crate::store::UsageQuery;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use serde_json::json;
use chrono::Datelike;
use std::sync::Arc;

pub const UI_HTML: &str = include_str!("ui.html");

pub async fn index() -> impl IntoResponse {
    (
        [(http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
        UI_HTML,
    )
}

/// @cc [owner:ghuntley,label:security] admin-token-gate
/// Every `/admin/api/*` route MUST be rejected with 401 unless the `Authorization` header carries
/// exactly `Bearer <ui_token>`; the HTML page at `/` MUST stay reachable without a token so the
/// token can be entered in the UI.
pub async fn require_ui_token(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let provided = headers
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or_default();
    if provided != state.ui_token {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    next.run(req).await
}

fn account_view(state: &AppState, account: &crate::models::Account) -> serde_json::Value {
    let pool = state.pool.lock().unwrap();
    let inflight = pool
        .accounts
        .get(&account.id)
        .map(|s| s.inflight)
        .unwrap_or(0);
    let binding_count = pool
        .bindings()
        .iter()
        .filter(|b| b.account_id == account.id)
        .count();
    json!({
        "id": account.id,
        "backend": account.backend.as_str(),
        "label": account.label,
        "status": account.status,
        "reset_at": account.reset_at,
        "cooling_remaining_ms": (account.reset_at - now_ms()).max(0),
        "inflight": inflight,
        "sticky_sessions": binding_count,
        "account_id": account.account_id,
        "enterprise_url": account.enterprise_url,
        "created_at": account.created_at,
        "usage": state.resets.snapshot(&account.id),
    })
}

pub async fn state(State(state): State<Arc<AppState>>) -> Response {
    let accounts: Vec<serde_json::Value> = state
        .store
        .list_accounts()
        .iter()
        .map(|a| account_view(&state, a))
        .collect();
    let requests: Vec<RequestLogEntry> = state.logs.lock().unwrap().iter().rev().cloned().collect();
    let catalog: serde_json::Map<String, serde_json::Value> = BackendId::all()
        .iter()
        .map(|b| {
            (
                b.as_str().to_string(),
                json!(state.store.catalog(*b)),
            )
        })
        .collect();
    Json(json!({
        "accounts": accounts,
        "requests": requests,
        "catalog": catalog,
        "now_ms": now_ms(),
    }))
    .into_response()
}

fn usage_query(mut query: UsageQuery) -> Result<UsageQuery, Response> {
    if query.from_ms.is_none() && query.to_ms.is_none() {
        let now = chrono::Utc::now();
        let start = now.date_naive().with_day(1).unwrap().and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_millis();
        query.from_ms = Some(start);
    }
    if query.from_ms.zip(query.to_ms).is_some_and(|(from, to)| from >= to) {
        return Err((StatusCode::BAD_REQUEST, "from_ms must precede to_ms").into_response());
    }
    if query.group_by.as_deref().unwrap_or("").split(',').any(|part| !part.is_empty() && !matches!(part, "model" | "account_id" | "cache_key")) {
        return Err((StatusCode::BAD_REQUEST, "invalid group_by").into_response());
    }
    if query.cache_key.is_some() && query.missing_key == Some(true) {
        return Err((StatusCode::BAD_REQUEST, "cache_key conflicts with missing_key").into_response());
    }
    Ok(query)
}

/// @cc [owner:ghuntley,label:security] usage-admin-query
/// Usage summaries MUST be available only under the admin token gate and MUST report unknown
/// requests separately from measured input and output totals.
pub async fn usage_summary(State(state): State<Arc<AppState>>, Query(query): Query<UsageQuery>) -> Response {
    let query = match usage_query(query) { Ok(query) => query, Err(response) => return response };
    match state.store.usage_summary(&query) {
        Ok(groups) => {
            let total_query = UsageQuery { group_by: None, ..query.clone() };
            match state.store.usage_summary(&total_query) {
                Ok(totals) => Json(json!({"groups": groups, "totals": totals.first(), "from_ms": query.from_ms, "to_ms": query.to_ms})).into_response(),
                Err(error) => { tracing::error!(error = %error, "usage.query_failed"); (StatusCode::INTERNAL_SERVER_ERROR, "usage query failed").into_response() }
            }
        },
        Err(error) => { tracing::error!(error = %error, "usage.query_failed"); (StatusCode::INTERNAL_SERVER_ERROR, "usage query failed").into_response() }
    }
}

pub async fn usage_requests(State(state): State<Arc<AppState>>, Query(query): Query<UsageQuery>) -> Response {
    let query = match usage_query(query) { Ok(query) => query, Err(response) => return response };
    match state.store.usage_records(&query) {
        Ok(requests) => Json(json!({"requests": requests, "from_ms": query.from_ms, "to_ms": query.to_ms})).into_response(),
        Err(error) => { tracing::error!(error = %error, "usage.query_failed"); (StatusCode::INTERNAL_SERVER_ERROR, "usage query failed").into_response() }
    }
}

#[derive(Deserialize)]
pub struct StartFlowBody {
    pub backend: String,
    #[serde(default)]
    pub enterprise_url: Option<String>,
}

pub async fn start_flow(
    State(state): State<Arc<AppState>>,
    body: Option<Json<StartFlowBody>>,
) -> Response {
    let Some(Json(body)) = body else {
        return (StatusCode::BAD_REQUEST, "missing body").into_response();
    };
    let Some(backend) = BackendId::parse(&body.backend) else {
        return (StatusCode::BAD_REQUEST, "unknown backend").into_response();
    };
    let result = match backend {
        BackendId::Codex => crate::codex::start_flow(
            state.client.clone(),
            state.store.clone(),
            state.flows.clone(),
            None,
        )
        .await
        .map_err(|e| e.to_string()),
        BackendId::Copilot => crate::copilot::start_flow(
            state.client.clone(),
            state.store.clone(),
            state.flows.clone(),
            None,
            body.enterprise_url.clone(),
        )
        .await
        .map_err(|e| e.to_string()),
    };
    match result {
        Ok(flow_id) => {
            let flow = state.flows.get(&flow_id);
            Json(json!({ "flow_id": flow_id, "flow": flow })).into_response()
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            format!("failed to start device flow: {e}"),
        )
            .into_response(),
    }
}

pub async fn flow_status(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    match state.flows.get(&id) {
        Some(flow) => Json(json!({ "flow_id": id, "flow": flow })).into_response(),
        None => (StatusCode::NOT_FOUND, "unknown flow").into_response(),
    }
}

pub async fn delete_account(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    state.store.delete_account(&id);
    state.pool.lock().unwrap().remove_account(&id);
    StatusCode::NO_CONTENT.into_response()
}

pub async fn disable_account(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    set_status(&state, &id, AccountStatus::Disabled, 0)
}

/// @cc [owner:ghuntley,label:pool] enable-clears-recoverable-status
/// `enable_account` MUST return 404 for an unknown account id. For a `Disabled` or `AuthError`
/// account it MUST set the status to `Healthy` and clear `reset_at` to 0, so an operator can
/// recover an account that `PoolCore::select` would otherwise never return. For a `Cooling`
/// account it MUST preserve both the `Cooling` status and its existing `reset_at` deadline, since
/// that deadline originates from upstream quota rather than an operator action. For an already
/// `Healthy` account it MUST leave the status `Healthy` and clear `reset_at` to 0.
pub async fn enable_account(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    let Some(account) = state.store.get_account(&id) else {
        return (StatusCode::NOT_FOUND, "unknown account").into_response();
    };
    let (next, reset_at) = match account.status {
        AccountStatus::Cooling => (AccountStatus::Cooling, account.reset_at),
        AccountStatus::Disabled | AccountStatus::AuthError => (AccountStatus::Healthy, 0),
        AccountStatus::Healthy => (AccountStatus::Healthy, 0),
    };
    set_status(&state, &id, next, reset_at)
}

pub async fn relogin_account(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    let Some(account) = state.store.get_account(&id) else {
        return (StatusCode::NOT_FOUND, "unknown account").into_response();
    };
    let result = match account.backend {
        BackendId::Codex => crate::codex::start_flow(
            state.client.clone(),
            state.store.clone(),
            state.flows.clone(),
            Some(id.clone()),
        )
        .await
        .map_err(|e| e.to_string()),
        BackendId::Copilot => crate::copilot::start_flow(
            state.client.clone(),
            state.store.clone(),
            state.flows.clone(),
            Some(id.clone()),
            account.enterprise_url.clone(),
        )
        .await
        .map_err(|e| e.to_string()),
    };
    match result {
        Ok(flow_id) => {
            let flow = state.flows.get(&flow_id);
            Json(json!({ "flow_id": flow_id, "flow": flow })).into_response()
        }
        Err(e) => (StatusCode::BAD_GATEWAY, format!("failed to start flow: {e}")).into_response(),
    }
}

fn set_status(state: &Arc<AppState>, id: &str, status: AccountStatus, reset_at: i64) -> Response {
    if state.store.get_account(id).is_none() {
        return (StatusCode::NOT_FOUND, "unknown account").into_response();
    }
    state.store.update_account_status(id, status, reset_at);
    state.pool.lock().unwrap().set_status(id, status, reset_at);
    StatusCode::NO_CONTENT.into_response()
}

#[derive(Deserialize)]
pub struct CatalogBody {
    pub models: Vec<crate::models::ModelInfo>,
}

pub async fn get_catalog(
    State(state): State<Arc<AppState>>,
    Path(backend): Path<String>,
) -> Response {
    let Some(backend) = BackendId::parse(&backend) else {
        return (StatusCode::NOT_FOUND, "unknown backend").into_response();
    };
    Json(json!({ "backend": backend.as_str(), "models": state.store.catalog(backend) })).into_response()
}

pub async fn put_catalog(
    State(state): State<Arc<AppState>>,
    Path(backend): Path<String>,
    Json(body): Json<CatalogBody>,
) -> Response {
    let Some(backend) = BackendId::parse(&backend) else {
        return (StatusCode::NOT_FOUND, "unknown backend").into_response();
    };
    state.store.set_catalog(backend, &body.models);
    let ids: Vec<String> = body.models.iter().map(|m| m.id.clone()).collect();
    state.pool.lock().unwrap().set_catalog(backend, ids);
    Json(json!({ "backend": backend.as_str(), "models": state.store.catalog(backend) })).into_response()
}

pub async fn client_key(State(state): State<Arc<AppState>>) -> Response {
    Json(json!({ "proxy_key": state.proxy_key })).into_response()
}

pub async fn refresh_copilot_catalog(State(state): State<Arc<AppState>>) -> Response {
    let accounts: Vec<_> = state
        .store
        .list_accounts()
        .into_iter()
        .filter(|a| a.backend == BackendId::Copilot)
        .collect();
    let mut refreshed = 0usize;
    let mut errors = Vec::new();
    for account in accounts {
        let Some(token) = account.refresh_token.clone() else {
            continue;
        };
        match crate::copilot::fetch_catalog(&state.client, &account, &token).await {
            Ok(models) if !models.is_empty() => {
                state.store.set_catalog(BackendId::Copilot, &models);
                let ids: Vec<String> = models.iter().map(|m| m.id.clone()).collect();
                state.pool.lock().unwrap().set_catalog(BackendId::Copilot, ids);
                refreshed += 1;
            }
            Ok(_) => {}
            Err(e) => errors.push(format!("{}: {e}", account.label)),
        }
    }
    Json(json!({ "refreshed_accounts": refreshed, "errors": errors })).into_response()
}
