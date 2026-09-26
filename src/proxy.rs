use crate::logging;
use crate::models::{now_ms, BackendId, Outcome, RequestLogEntry};
use crate::pool::{Decision, PoolCore, SelectError, Selection};
use crate::provider::BackendMap;
use crate::store::Store;
use crate::store::UsageRecord;
use crate::usage::{UsageTap, usage_from_json};
use crate::tokens::TokenManager;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tower_http::request_id::RequestId;

pub const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;
const LOG_BUFFER: usize = 200;

pub struct AppState {
    pub store: Arc<Store>,
    pub pool: Arc<Mutex<PoolCore>>,
    pub tokens: Arc<TokenManager>,
    pub backends: Arc<BackendMap>,
    pub client: reqwest::Client,
    pub logs: Arc<Mutex<VecDeque<RequestLogEntry>>>,
    pub flows: crate::flows::FlowRegistry,
    pub proxy_key: Option<String>,
    pub ui_token: String,
    pub resets: Arc<crate::resets::ResetManager>,
    pub stream_usage_unsupported: Mutex<std::collections::HashSet<BackendId>>,
}

struct UsageFinish {
    store: Arc<Store>,
    record: UsageRecord,
    tap: UsageTap,
}

impl Drop for UsageFinish {
    fn drop(&mut self) {
        if let Some(counts) = self.tap.counts() {
            self.record.input_tokens = Some(counts.input_tokens);
            self.record.output_tokens = Some(counts.output_tokens);
        }
        persist_usage(&self.store, &self.record);
    }
}

fn persist_usage(store: &Store, record: &UsageRecord) {
    if let Err(error) = store.insert_usage(record) {
        tracing::error!(request_id = %record.request_id, error = %error, "usage.persist_failed");
    } else {
        tracing::info!(request_id = %record.request_id, measured = record.input_tokens.is_some(), input_tokens = record.input_tokens, output_tokens = record.output_tokens, "usage.recorded");
    }
}

fn usage_record(request_id: &str, path: &str, model: &str, sticky: Option<&str>, selection: &Selection, account: &crate::models::Account, status: u16) -> UsageRecord {
    UsageRecord {
        id: 0, request_id: request_id.to_string(), ts: now_ms(), endpoint: path.to_string(), backend: selection.backend.as_str().to_string(), model: model.to_string(), account_id: selection.account_id.clone(), account_label: account.label.clone(), cache_key: sticky.map(str::to_string), status, input_tokens: None, output_tokens: None,
    }
}

pub fn extract_sticky_key(body: &Value, session_header: Option<&str>) -> Option<String> {
    body.get("prompt_cache_key")
        .or_else(|| body.get("promptCacheKey"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .or_else(|| session_header.map(str::to_string))
        .filter(|s| !s.is_empty())
}

fn usage_is_stream(body: &Value, content_type: &[u8]) -> bool {
    body.get("stream").and_then(Value::as_bool) == Some(true)
        || content_type.starts_with(b"text/event-stream")
}

/// @cc [owner:ghuntley,label:security] proxy-key-gate
/// When a proxy key is configured, requests to `/v1/*` MUST be rejected with 401 unless the
/// `Authorization` header carries exactly `Bearer <proxy_key>`. When no key is configured
/// (localhost-only mode) requests pass.
pub async fn require_proxy_key(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Response {
    if let Some(expected) = &state.proxy_key {
        let provided = req
            .headers()
            .get(http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .map(str::to_string);
        match provided {
            Some(key) if key == *expected => {}
            _ => {
                return (
                    StatusCode::UNAUTHORIZED,
                    axum::Json(serde_json::json!({
                        "error": {"message": "invalid or missing proxy api key", "type": "authentication_error"}
                    })),
                )
                    .into_response();
            }
        }
    }
    next.run(req).await
}

/// @cc [owner:ghuntley,label:proxy] inflight-released-on-drop
/// Dropping `InflightGuard` MUST decrement the guarded account's in-flight counter exactly once —
/// including on early returns, error paths, and client-aborted streams.
struct InflightGuard {
    pool: Arc<Mutex<PoolCore>>,
    account_id: String,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.pool.lock().unwrap().release(&self.account_id);
    }
}

fn error_response(status: StatusCode, message: &str, error_type: &str) -> Response {
    (
        status,
        axum::Json(serde_json::json!({
            "error": {"message": message, "type": error_type}
        })),
    )
        .into_response()
}

fn upstream_error_response(status: StatusCode, content_type: &str, body: String) -> Response {
    (
        status,
        [
            (http::header::CONTENT_TYPE, content_type.to_string()),
        ],
        body,
    )
        .into_response()
}

fn saturated_response(until_ms: i64) -> Response {
    let now = now_ms();
    let retry_after_secs = ((until_ms - now).max(0) + 999) / 1000;
    let mut resp = error_response(
        StatusCode::TOO_MANY_REQUESTS,
        "all subscriptions for this model are out of quota; retry when the pool resets",
        "quota_exhausted",
    )
    .into_response();
    resp.headers_mut().insert(
        http::header::RETRY_AFTER,
        http::HeaderValue::from_str(&retry_after_secs.to_string()).expect("retry-after"),
    );
    resp
}

fn attach_request_id(mut resp: Response, request_id: &str) -> Response {
    if let Ok(v) = http::HeaderValue::from_str(request_id) {
        resp.headers_mut().insert("x-request-id", v);
    }
    resp
}

enum Attempt {
    Respond(Response),
    Failover(Option<Response>),
}

/// @cc [owner:ghuntley,label:proxy] saturation-fail-fast-429
/// When selection returns `Saturated`, `infer` MUST first make one automatic Codex reset attempt
/// for a live Codex-pool outage, then select again. If no account recovers, it MUST respond with
/// 429 and `Retry-After` equal to the seconds until `until_ms`.
pub async fn infer(State(state): State<Arc<AppState>>, req: Request) -> Response {
    let started = Instant::now();
    let request_id = req
        .extensions()
        .get::<RequestId>()
        .and_then(|r| r.header_value().to_str().ok())
        .unwrap_or("unknown")
        .to_string();
    let path = req.uri().path().to_string();
    let session_header = req
        .headers()
        .get("session-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    let body_bytes = match axum::body::to_bytes(req.into_body(), MAX_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(e) => {
            return attach_request_id(
                error_response(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    &format!("request body error: {e}"),
                    "invalid_request_error",
                ),
                &request_id,
            );
        }
    };

    let body: Value = match serde_json::from_slice(&body_bytes) {
        Ok(v) => v,
        Err(e) => {
            return attach_request_id(
                error_response(
                    StatusCode::BAD_REQUEST,
                    &format!("request body must be valid json: {e}"),
                    "invalid_request_error",
                ),
                &request_id,
            );
        }
    };

    let model = body
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let sticky = extract_sticky_key(&body, session_header.as_deref());

    state.resets.maybe_reset(&state.pool, &state.store, &state.tokens, &request_id).await;

    let total_accounts = state.pool.lock().unwrap().accounts.len().max(1);
    let mut last_upstream: Option<Response> = None;
    let mut refreshed: std::collections::HashSet<String> = Default::default();
    let mut reset_after_quota = false;
    let mut attempts = 0;
    let mut max_attempts = total_accounts;

    while attempts < max_attempts {
        attempts += 1;
        let mut selection = state
            .pool
            .lock()
            .unwrap()
            .select(now_ms(), sticky.as_deref(), &model);

        if matches!(selection, Err(SelectError::Saturated { .. })) && !reset_after_quota {
            reset_after_quota = true;
            if state.resets.maybe_reset(&state.pool, &state.store, &state.tokens, &request_id).await {
                selection = state.pool.lock().unwrap().select(now_ms(), sticky.as_deref(), &model);
            }
        }

        let selection = match selection {
            Ok(sel) => sel,
            Err(SelectError::Saturated { until_ms }) => {
                logging::log_saturated(&request_id, until_ms);
                return attach_request_id(saturated_response(until_ms), &request_id);
            }
            Err(SelectError::NoAccounts) => {
                return attach_request_id(
                    last_upstream.take().unwrap_or_else(|| {
                        error_response(
                            StatusCode::SERVICE_UNAVAILABLE,
                            "no subscriptions are configured for this model",
                            "no_accounts",
                        )
                    }),
                    &request_id,
                );
            }
        };

        logging::log_request_selected(
            &request_id,
            &model,
            selection.decision,
            selection.backend.as_str(),
            &selection.account_id,
            account_label(&state, &selection.account_id).as_deref(),
        );

        if selection.decision == Decision::NewBinding || selection.decision == Decision::StickyRebind {
            if let Some(key) = &sticky {
                if let Some(binding) = state.pool.lock().unwrap().bindings().into_iter().find(|b| b.cache_key == *key) {
                    state.store.upsert_binding(&binding);
                    state.store.prune_bindings(
                        now_ms(),
                        crate::pool::BINDING_TTL_MS,
                        crate::pool::DEFAULT_BINDING_CAP,
                    );
                }
            }
        }

        match attempt_account(
            &state,
            &request_id,
            &path,
            &model,
            sticky.as_deref(),
            &selection,
            body_bytes.clone(),
            &mut refreshed,
        )
        .await
        {
            Attempt::Respond(resp) => {
                logging::log_request_completed(&request_id, resp.status().as_u16(), started.elapsed().as_millis() as u64);
                return resp;
            }
            Attempt::Failover(passthrough) => {
                last_upstream = passthrough.or(last_upstream);
                if attempts == max_attempts && !reset_after_quota {
                    reset_after_quota = true;
                    if state.resets.maybe_reset(&state.pool, &state.store, &state.tokens, &request_id).await {
                        max_attempts += 1;
                    }
                }
            }
        }
    }

    if let Err(SelectError::Saturated { until_ms }) = state.pool.lock().unwrap().select(now_ms(), sticky.as_deref(), &model) {
        logging::log_saturated(&request_id, until_ms);
        return attach_request_id(saturated_response(until_ms), &request_id);
    }

    attach_request_id(
        last_upstream.take().unwrap_or_else(|| {
            error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "pool exhausted: every subscription attempt failed",
                "pool_exhausted",
            )
        }),
        &request_id,
    )
}

/// @cc [owner:ghuntley,label:proxy] failover-bounded-by-accounts
/// A request MUST make at most as many attempts as configured accounts, plus one additional attempt
/// after a successful banked reset, and the first successful upstream response MUST be returned
/// to the client; upstream error bodies MUST be preserved and
/// returned when every attempt fails (or a 503 `pool_exhausted` error when no upstream response
/// was ever received). An upstream 401 or 403 MUST trigger exactly one forced token refresh and one
/// same-account retry per request; a second 401 or 403 (or a failed refresh) MUST classify the
/// account `AuthFailed` and fail over.
async fn attempt_account(
    state: &Arc<AppState>,
    request_id: &str,
    path: &str,
    model: &str,
    sticky: Option<&str>,
    selection: &Selection,
    body_bytes: axum::body::Bytes,
    refreshed: &mut std::collections::HashSet<String>,
) -> Attempt {
    let started = Instant::now();
    let Some(backend) = state.backends.get(&selection.backend).cloned() else {
        return Attempt::Failover(None);
    };
    let account = state.pool.lock().unwrap().account(&selection.account_id).cloned();
    let Some(account) = account else {
        return Attempt::Failover(None);
    };
    let label = Some(account.label.as_str());

    {
        let mut pool = state.pool.lock().unwrap();
        pool.acquire(&selection.account_id);
    }
    let guard = InflightGuard {
        pool: state.pool.clone(),
        account_id: selection.account_id.clone(),
    };

    let token = match state.tokens.access_token(&account).await {
        Ok(token) => token,
        Err(e) => {
            tracing::warn!(request_id = %request_id, error = %e, "token.unavailable");
            report_outcome(state, &selection.account_id, Outcome::AuthFailed);
            logging::log_account_state(&selection.account_id, label, "auth_error", "missing or unusable credential");
            drop(guard);
            return Attempt::Failover(None);
        }
    };

    let url = backend.rewrite_url(path, &account);
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/json"),
    );
    headers.insert(
        http::header::ACCEPT,
        http::HeaderValue::from_static("text/event-stream, application/json"),
    );
    let mut outbound_body: Value = serde_json::from_slice(&body_bytes).unwrap_or(Value::Null);
    backend.inject_headers(&account, &token, sticky, &outbound_body, &mut headers);
    backend.prepare_body(&mut outbound_body);
    let fallback_body = outbound_body.clone();
    let auto_usage = !state.stream_usage_unsupported.lock().unwrap().contains(&selection.backend)
        && backend.auto_stream_usage(path, &mut outbound_body);
    let outbound_bytes = serde_json::to_vec(&outbound_body).unwrap_or_else(|_| body_bytes.to_vec());

    let mut response = match state
        .client
        .post(&url)
        .headers(headers.clone())
        .body(outbound_bytes)
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(e) => {
            tracing::warn!(request_id = %request_id, error = %e, backend = %selection.backend.as_str(), "upstream.network_error");
            let record = usage_record(request_id, path, model, sticky, selection, &account, 0);
            persist_usage(&state.store, &record);
            report_outcome(state, &selection.account_id, Outcome::Transient);
            drop(guard);
            return Attempt::Failover(None);
        }
    };

    if auto_usage && matches!(response.status().as_u16(), 400 | 422) {
        let status = response.status();
        let rejection = response.text().await.unwrap_or_default();
        if rejection.contains("stream_options") || rejection.contains("include_usage") {
            let rejected = usage_record(request_id, path, model, sticky, selection, &account, status.as_u16());
            persist_usage(&state.store, &rejected);
            state.stream_usage_unsupported.lock().unwrap().insert(selection.backend);
            tracing::warn!(request_id = %request_id, backend = %selection.backend.as_str(), "usage.stream_option_unsupported");
            let retry_bytes = serde_json::to_vec(&fallback_body).unwrap_or_else(|_| body_bytes.to_vec());
            response = match state.client.post(&url).headers(headers).body(retry_bytes).send().await {
                Ok(resp) => resp,
                Err(error) => {
                    tracing::warn!(request_id = %request_id, error = %error, "upstream.network_error");
                    let record = usage_record(request_id, path, model, sticky, selection, &account, 0);
                    persist_usage(&state.store, &record);
                    drop(guard);
                    return Attempt::Failover(None);
                }
            };
        } else {
            let record = usage_record(request_id, path, model, sticky, selection, &account, status.as_u16());
            persist_usage(&state.store, &record);
            push_log(state, request_id, model, Some(selection.backend.as_str().to_string()), Some(selection.account_id.clone()), Some(account.label.clone()), sticky.unwrap_or("none"), status.as_u16(), started.elapsed().as_millis() as u64);
            drop(guard);
            return Attempt::Failover(Some(upstream_error_response(status, "application/json", rejection)));
        }
    }

    let status = response.status();

    if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN)
        && refreshed.insert(selection.account_id.clone())
    {
        let body_text = response.text().await.unwrap_or_default();
        let mut record = usage_record(request_id, path, model, sticky, selection, &account, status.as_u16());
        if let Ok(value) = serde_json::from_str::<Value>(&body_text)
            && let Some(counts) = usage_from_json(&value, backend.usage_is_chat(path)) {
            record.input_tokens = Some(counts.input_tokens);
            record.output_tokens = Some(counts.output_tokens);
        }
        persist_usage(&state.store, &record);
        match state.tokens.force_refresh(&selection.account_id).await {
            Ok(_) => {
                report_outcome(state, &selection.account_id, Outcome::Transient);
                drop(guard);
                return Attempt::Failover(None);
            }
            Err(e) => {
                tracing::warn!(request_id = %request_id, error = %e, "token.refresh_failed");
                report_outcome(state, &selection.account_id, Outcome::AuthFailed);
                logging::log_account_state(&selection.account_id, label, "auth_error", "refresh rejected");
                drop(guard);
                return Attempt::Failover(None);
            }
        }
    }

    if !status.is_success() {
        let resp_headers = response.headers().clone();
        let body_text = response.text().await.unwrap_or_default();
        let mut record = usage_record(request_id, path, model, sticky, selection, &account, status.as_u16());
        if let Ok(value) = serde_json::from_str::<Value>(&body_text)
            && let Some(counts) = usage_from_json(&value, backend.usage_is_chat(path)) {
            record.input_tokens = Some(counts.input_tokens);
            record.output_tokens = Some(counts.output_tokens);
        }
        persist_usage(&state.store, &record);
        let outcome = backend.classify(status.as_u16(), &body_text, &resp_headers, now_ms());
        if let Some(new_status) = {
            let mut pool = state.pool.lock().unwrap();
            pool.report(&selection.account_id, outcome.clone())
        } {
            let reset_at = {
                let pool = state.pool.lock().unwrap();
                pool.account(&selection.account_id).map(|a| a.reset_at).unwrap_or(0)
            };
            state
                .store
                .update_account_status(&selection.account_id, new_status, reset_at);
            match &outcome {
                Outcome::QuotaExhausted { until_ms } => {
                    logging::log_account_state(&selection.account_id, label, "cooling", &format!("until {until_ms}"));
                }
                Outcome::AuthFailed => {
                    logging::log_account_state(&selection.account_id, label, "auth_error", "upstream rejected credentials");
                }
                _ => {}
            }
        }
        report_outcome(state, &selection.account_id, Outcome::Ok);
        drop(guard);
        push_log(
            state,
            request_id,
            model,
            Some(selection.backend.as_str().to_string()),
            Some(selection.account_id.clone()),
            Some(account.label.clone()),
            sticky.unwrap_or("none"),
            status.as_u16(),
            started.elapsed().as_millis() as u64,
        );
        Attempt::Failover(Some(upstream_error_response(
            status,
            &resp_headers
                .get(http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("application/json")
                .to_string(),
            body_text,
        )))
    } else {
        let content_type = response
            .headers()
            .get(http::header::CONTENT_TYPE)
            .cloned()
            .unwrap_or(http::HeaderValue::from_static("application/json"));
        let status_code = status;
        let stream = response.bytes_stream();
        let finish = UsageFinish {
            store: state.store.clone(),
            record: usage_record(request_id, path, model, sticky, selection, &account, status.as_u16()),
            tap: UsageTap::new(
                usage_is_stream(&outbound_body, content_type.as_bytes()),
                backend.usage_is_chat(path),
            ),
        };
        let body_stream = Body::from_stream(futures::stream::unfold(
            (stream, Some(guard), finish),
            |(mut stream, guard, mut finish)| async move {
                match stream.next().await {
                    Some(Ok(chunk)) => {
                        finish.tap.feed(&chunk);
                        Some((Ok::<_, std::io::Error>(chunk), (stream, guard, finish)))
                    },
                    Some(Err(_)) => Some((
                        Err(std::io::Error::new(
                            std::io::ErrorKind::Interrupted,
                            "upstream stream error",
                        )),
                        (stream, guard, finish),
                    )),
                    None => None,
                }
            },
        ));
        push_log(
            state,
            request_id,
            model,
            Some(selection.backend.as_str().to_string()),
            Some(selection.account_id.clone()),
            Some(account.label.clone()),
            sticky.unwrap_or("none"),
            status_code.as_u16(),
            started.elapsed().as_millis() as u64,
        );
        let mut resp = Response::builder()
            .status(status_code)
            .body(body_stream)
            .expect("valid stream response");
        resp.headers_mut().insert(http::header::CONTENT_TYPE, content_type);
        Attempt::Respond(resp)
    }
}

fn account_label(state: &AppState, account_id: &str) -> Option<String> {
    state
        .pool
        .lock()
        .unwrap()
        .account(account_id)
        .map(|a| a.label.clone())
}

fn report_outcome(state: &Arc<AppState>, account_id: &str, outcome: Outcome) {
    let status_change = {
        let mut pool = state.pool.lock().unwrap();
        pool.report(account_id, outcome)
    };
    if let Some(new_status) = status_change {
        let reset_at = {
            let pool = state.pool.lock().unwrap();
            pool.account(account_id).map(|a| a.reset_at).unwrap_or(0)
        };
        state.store.update_account_status(account_id, new_status, reset_at);
    }
}

#[allow(clippy::too_many_arguments)]
fn push_log(
    state: &Arc<AppState>,
    request_id: &str,
    model: &str,
    backend: Option<String>,
    account: Option<String>,
    label: Option<String>,
    sticky: &str,
    status: u16,
    duration_ms: u64,
) {
    let mut logs = state.logs.lock().unwrap();
    logs.push_back(RequestLogEntry {
        ts: now_ms(),
        request_id: request_id.to_string(),
        model: model.to_string(),
        backend,
        account,
        label,
        sticky: sticky.to_string(),
        status,
        duration_ms,
    });
    while logs.len() > LOG_BUFFER {
        logs.pop_front();
    }
}

pub async fn models(State(state): State<Arc<AppState>>) -> Response {
    let mut merged: std::collections::BTreeMap<String, Value> = Default::default();
    for backend in BackendId::all() {
        for m in state.store.catalog(backend) {
            let entry = serde_json::json!({
                "id": m.id,
                "object": "model",
                "name": m.name,
                "context_window": m.context,
                "max_input_tokens": m.input,
                "max_output_tokens": m.output,
                "owned_by": format!("underclass-{}", backend.as_str()),
            });
            merged
                .entry(m.id)
                .and_modify(|existing| {
                    if m.context > existing["context_window"].as_i64().unwrap_or(0) {
                        *existing = entry.clone();
                    }
                })
                .or_insert(entry);
        }
    }
    let data: Vec<Value> = merged.into_values().collect();
    axum::Json(serde_json::json!({ "object": "list", "data": data })).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sticky_key_from_body_cache_key() {
        let body = json!({"model": "gpt-5.5", "prompt_cache_key": "sess-1"});
        assert_eq!(extract_sticky_key(&body, None).as_deref(), Some("sess-1"));
    }

    #[test]
    fn sticky_key_camel_case_fallback() {
        let body = json!({"model": "gpt-5.5", "promptCacheKey": "sess-2"});
        assert_eq!(extract_sticky_key(&body, None).as_deref(), Some("sess-2"));
    }

    #[test]
    fn sticky_key_falls_back_to_session_header() {
        let body = json!({"model": "gpt-5.5"});
        assert_eq!(extract_sticky_key(&body, Some("sess-3")).as_deref(), Some("sess-3"));
    }

    #[test]
    fn sticky_key_none_when_absent() {
        let body = json!({"model": "gpt-5.5"});
        assert!(extract_sticky_key(&body, None).is_none());
        let empty = json!({"prompt_cache_key": ""});
        assert!(extract_sticky_key(&empty, None).is_none());
    }

    #[test]
    fn stream_request_overrides_incorrect_json_content_type() {
        let body = json!({"stream": true});
        assert!(usage_is_stream(&body, b"application/json"));
        assert!(usage_is_stream(&json!({}), b"text/event-stream; charset=utf-8"));
        assert!(!usage_is_stream(&json!({}), b"application/json"));
    }
}
