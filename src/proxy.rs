use crate::correlation::RequestId;
use crate::logging;
use crate::models::{AccountStatus, BackendId, Outcome, RequestLogEntry, Surface, now_ms};
use crate::pool::{Decision, PoolCore, SelectError, Selection};
use crate::provider::BackendMap;
use crate::store::Store;
use crate::store::{UpstreamEvent, UsageRecord};
use crate::tokens::TokenManager;
use crate::usage::{StreamEnd, StreamOutcome, UsageTap, usage_from_dialect};
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

pub const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;
const LOG_BUFFER: usize = 200;
/// How long a streamed response is held back waiting for its first output or terminal event.
/// Upstream capacity failures before output observed in practice arrive within 60s (265 of 266).
const PRE_OUTPUT_HOLD: std::time::Duration = std::time::Duration::from_secs(60);
/// Upper bound on bytes buffered while holding a stream before its first output.
const PRE_OUTPUT_HOLD_BYTES: usize = 1024 * 1024;

/// Client headers that never travel upstream: credentials and transport-level headers. The
/// proxy injects the selected account's credential itself (CONTRACTS
/// `outbound-credential-stripping`), and the transport headers belong to the client's hop.
const STRIPPED_CLIENT_HEADERS: &[&str] = &[
    "authorization",
    "x-api-key",
    "cookie",
    "host",
    "content-length",
    "connection",
    "keep-alive",
    "proxy-authorization",
    "proxy-authenticate",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "accept-encoding",
    "x-forwarded-for",
    "x-forwarded-host",
    "x-forwarded-proto",
    "x-request-id",
    "x-underclass-retry-safe",
];

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
    /// Per-account observations a backend reported about its last upstream response, keyed by
    /// account id. Backends decide what to report; the proxy only stores it.
    pub observations: Mutex<std::collections::HashMap<String, Value>>,
    /// How long usage rows, upstream events, and daily rollups are kept.
    pub stats_retention_days: i64,
}

/// @cc [owner:ghuntley,label:accounting] stream-finish-recorded
/// When a 2xx response body ends for any reason (EOF, upstream error, or client drop),
/// `UsageFinish` MUST persist exactly one usage row carrying the stream outcome, sanitized error
/// code, `saw_output`, and duration; MUST log `stream.finished`; MUST annotate the attempt's
/// ring-buffer entry; and MUST record an upstream event for every `failed` outcome and for an
/// `incomplete` outcome whose reason is a capacity, policy, or quota code.
struct UsageFinish {
    store: Arc<Store>,
    logs: Arc<Mutex<VecDeque<RequestLogEntry>>>,
    record: UsageRecord,
    tap: UsageTap,
    ended: Option<StreamEnd>,
    /// Where an upstream event surfaced: `stream`, or `stream_failover` when the attempt was
    /// discarded and retried on another account.
    detail: &'static str,
}

impl Drop for UsageFinish {
    fn drop(&mut self) {
        if let Some(counts) = self.tap.counts() {
            self.record.input_tokens = Some(counts.input_tokens);
            self.record.output_tokens = Some(counts.output_tokens);
        }
        let report = self.tap.report(self.ended.unwrap_or(StreamEnd::Dropped));
        let duration_ms = (now_ms() - self.record.ts).max(0);
        self.record.duration_ms = Some(duration_ms);
        self.record.stream_outcome = Some(report.outcome.as_str().to_string());
        self.record.error_code = report.error_code.clone();
        self.record.saw_output = Some(report.saw_output);
        logging::log_stream_finished(
            &self.record.request_id,
            &self.record.model,
            &self.record.account_label,
            report.outcome.as_str(),
            report.error_code.as_deref(),
            report.saw_output,
            duration_ms,
        );
        if let Some(entry) = self.logs.lock().unwrap().iter_mut().rev().find(|entry| {
            entry.request_id == self.record.request_id
                && entry.account.as_deref() == Some(self.record.account_id.as_str())
        }) {
            entry.stream_outcome = self.record.stream_outcome.clone();
            entry.error_code = self.record.error_code.clone();
        }
        let kind = crate::usage::event_kind(report.error_code.as_deref());
        let notable = match report.outcome {
            StreamOutcome::Failed => true,
            StreamOutcome::Incomplete => kind != "upstream_failed",
            _ => false,
        };
        if notable {
            self.store.insert_upstream_event(&event_for(
                &self.record,
                kind,
                self.detail,
                report.error_code.clone(),
            ));
        }
        // Written last: once the usage row is visible, the event and annotation already exist.
        persist_usage(&self.store, &self.record);
    }
}

fn event_for(
    record: &UsageRecord,
    kind: &str,
    detail: &str,
    error_code: Option<String>,
) -> UpstreamEvent {
    UpstreamEvent {
        ts: now_ms(),
        source: "proxy".into(),
        kind: kind.into(),
        detail: Some(detail.into()),
        error_code,
        backend: Some(record.backend.clone()),
        model: Some(record.model.clone()),
        account_id: Some(record.account_id.clone()),
        account_label: Some(record.account_label.clone()),
        request_id: Some(record.request_id.clone()),
        until_ms: None,
        dedupe_key: None,
    }
}

fn record_saturated(state: &AppState, request_id: &str, model: &str, until_ms: i64) {
    let now = now_ms();
    logging::log_saturated(request_id, until_ms, now);
    state.store.insert_upstream_event(&UpstreamEvent {
        ts: now,
        source: "proxy".into(),
        kind: "pool_saturated".into(),
        detail: Some("pool".into()),
        model: Some(model.to_string()),
        request_id: Some(request_id.to_string()),
        until_ms: Some(until_ms),
        ..Default::default()
    });
}

fn persist_usage(store: &Store, record: &UsageRecord) {
    let mut record = record.clone();
    if record.duration_ms.is_none() {
        record.duration_ms = Some((now_ms() - record.ts).max(0));
    }
    let record = &record;
    if let Err(error) = store.insert_usage(record) {
        tracing::error!(request_id = %record.request_id, error = %error, "usage.persist_failed");
    } else {
        tracing::info!(request_id = %record.request_id, measured = record.input_tokens.is_some(), input_tokens = record.input_tokens, output_tokens = record.output_tokens, "usage.recorded");
    }
}

/// `started_ms` is when the attempt began, so `ts .. ts + duration_ms` is the attempt's lifetime.
#[allow(clippy::too_many_arguments)]
fn usage_record(
    request_id: &str,
    path: &str,
    model: &str,
    sticky: Option<&str>,
    selection: &Selection,
    account: &crate::models::Account,
    status: u16,
    started_ms: i64,
) -> UsageRecord {
    UsageRecord {
        id: 0,
        request_id: request_id.to_string(),
        ts: started_ms,
        endpoint: path.to_string(),
        backend: selection.backend.as_str().to_string(),
        model: model.to_string(),
        account_id: selection.account_id.clone(),
        account_label: account.label.clone(),
        cache_key: sticky.map(str::to_string),
        status,
        input_tokens: None,
        output_tokens: None,
        ..Default::default()
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

/// @cc [owner:ghuntley,label:proxy] successful-stream-media-type
/// A successful response classified as streaming by the outbound request or
/// upstream media type MUST be published as text/event-stream, for both live
/// forwarding and held replay. Missing or incorrect upstream JSON headers MUST
/// NOT make clients parse SSE as JSON. Non-stream responses and HTTP errors
/// retain their upstream media type and every response retains its body bytes.
fn usage_is_stream(body: &Value, content_type: &[u8]) -> bool {
    body.get("stream").and_then(Value::as_bool) == Some(true)
        || content_type.starts_with(b"text/event-stream")
}

/// @cc [owner:ghuntley,label:security] proxy-key-gate
/// When a proxy key is configured, requests to `/v1/*` MUST be rejected with 401 unless the
/// `Authorization` header carries exactly `Bearer <proxy_key>`, or the `x-api-key` header
/// carries exactly `<proxy_key>`. When no key is configured (localhost-only mode) requests pass.
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
            .map(str::to_string)
            .or_else(|| {
                req.headers()
                    .get("x-api-key")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string)
            });
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
        [(http::header::CONTENT_TYPE, content_type.to_string())],
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

const RETRY_SAFE_HEADER: &str = "x-underclass-retry-safe";
const RETRY_SAFE_VALUE: &str = "pre-output-overload";

#[derive(Default)]
struct RetryProof {
    saw_overload: bool,
    disqualified: bool,
}

impl RetryProof {
    fn overload(&mut self, proven: bool) {
        self.saw_overload = true;
        self.disqualified |= !proven;
    }

    fn disqualify(&mut self) {
        self.disqualified = true;
    }

    fn certified(&self) -> bool {
        self.saw_overload && !self.disqualified
    }
}

/// @cc [owner:ghuntley,label:proxy;security] retry-certificate-finalization
/// Only this finalizer MAY attach the retry certificate, and only when every
/// attempted inference proved the exact server_is_overloaded failure before
/// output. Any generic failover permanently disqualifies the request. External
/// headers and saved responses MUST NOT carry independent retry authority.
fn finish_response(mut resp: Response, request_id: &str, retry_safe: bool) -> Response {
    resp.headers_mut().remove(RETRY_SAFE_HEADER);
    if retry_safe {
        resp.headers_mut().insert(
            RETRY_SAFE_HEADER,
            http::HeaderValue::from_static(RETRY_SAFE_VALUE),
        );
    }
    if let Ok(v) = http::HeaderValue::from_str(request_id) {
        resp.headers_mut().insert("x-request-id", v);
    }
    resp
}

fn attach_request_id(resp: Response, request_id: &str) -> Response {
    finish_response(resp, request_id, false)
}

enum Attempt {
    Respond(Response),
    Failover(Option<Response>),
    /// The stream failed with a capacity code before any output; `replay` holds what upstream
    /// sent, for the client if no other account succeeds.
    Overloaded {
        account_id: String,
        replay: Response,
        retry_safe: bool,
    },
}

/// @cc [owner:ghuntley,label:proxy] saturation-fail-fast-429
/// When selection returns `Saturated`, `infer` MUST first make one automatic Codex reset attempt
/// for a live Codex-pool outage, then select again. If no account recovers, it MUST respond with
/// 429 and `Retry-After` equal to the seconds until `until_ms`. Exception: once an account has
/// been excluded for an overload before output, a failed selection MUST return that held upstream
/// response instead, without a reset attempt.
pub async fn infer(State(state): State<Arc<AppState>>, req: Request) -> Response {
    run(state, req, Surface::OpenAI).await
}

/// The Anthropic Messages API surface. Only official Claude Code clients are admitted.
pub async fn messages(State(state): State<Arc<AppState>>, req: Request) -> Response {
    run(state, req, Surface::Anthropic).await
}

/// @cc [owner:ghuntley,label:proxy] surface-client-admission
/// A request MUST be rejected with 403 before any pooled credential is used when no backend
/// serving the requested surface admits it as a client.
fn surface_accepts_client(
    state: &AppState,
    surface: Surface,
    headers: &HeaderMap,
    body: &Value,
) -> bool {
    let serving: Vec<_> = state
        .backends
        .values()
        .filter(|backend| backend.surfaces().contains(&surface))
        .collect();
    if serving.is_empty() {
        return true;
    }
    serving
        .iter()
        .any(|backend| backend.accepts_client(headers, body))
}

async fn run(state: Arc<AppState>, req: Request, surface: Surface) -> Response {
    let started = Instant::now();
    let request_id = req
        .extensions()
        .get::<RequestId>()
        .and_then(|r| r.header_value().to_str().ok())
        .unwrap_or("unknown")
        .to_string();
    let path = req.uri().path().to_string();
    // The routes are nested under `/v1`, so the router hands the handler a path without that
    // prefix. The upstream URL is built from it; the usage record keeps the client's route.
    let endpoint = req
        .extensions()
        .get::<axum::extract::OriginalUri>()
        .map(|uri| uri.path().to_string())
        .unwrap_or_else(|| path.clone());
    // The Anthropic API is called with the client's own query string (`?beta=true`).
    let target = match surface {
        Surface::Anthropic => req
            .uri()
            .path_and_query()
            .map(|value| value.as_str().to_string())
            .unwrap_or_else(|| path.clone()),
        Surface::OpenAI => path.clone(),
    };
    let mut client_headers = req.headers().clone();
    client_headers.remove(RETRY_SAFE_HEADER);
    let session_header = client_headers
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

    if !surface_accepts_client(&state, surface, &client_headers, &body) {
        return attach_request_id(
            error_response(
                StatusCode::FORBIDDEN,
                "this endpoint only accepts official Claude Code clients",
                "client_not_allowed",
            ),
            &request_id,
        );
    }

    let model = crate::models::normalize_model_name(
        body.get("model")
            .and_then(|v| v.as_str())
            .unwrap_or_default(),
    );
    let sticky = match surface {
        Surface::Anthropic => crate::claude::sticky_key(&client_headers, &body),
        Surface::OpenAI => extract_sticky_key(&body, session_header.as_deref()),
    };

    state
        .resets
        .maybe_reset(&state.pool, &state.store, &state.tokens, &request_id)
        .await;

    let total_accounts = state.pool.lock().unwrap().accounts.len().max(1);
    let mut last_upstream: Option<Response> = None;
    let mut retry_proof = RetryProof::default();
    let mut refreshed: std::collections::HashSet<String> = Default::default();
    let mut reset_after_quota = false;
    let mut attempts = 0;
    let mut max_attempts = total_accounts;
    // Accounts whose stream failed with a capacity code before output, skipped for this request.
    let mut overloaded: std::collections::HashSet<String> = Default::default();

    while attempts < max_attempts {
        attempts += 1;
        let mut selection = state.pool.lock().unwrap().select_excluding(
            now_ms(),
            sticky.as_deref(),
            &model,
            surface,
            &overloaded,
        );

        if selection.is_err()
            && !overloaded.is_empty()
            && let Some(replay) = last_upstream.take()
        {
            logging::log_request_completed(
                &request_id,
                replay.status().as_u16(),
                started.elapsed().as_millis() as u64,
            );
            return finish_response(replay, &request_id, retry_proof.certified());
        }

        if matches!(selection, Err(SelectError::Saturated { .. })) && !reset_after_quota {
            reset_after_quota = true;
            // Always select again: the reset attempt can wait on the shared reset lock and on
            // usage fetches, during which cooling deadlines may pass even if nothing was redeemed.
            state
                .resets
                .maybe_reset(&state.pool, &state.store, &state.tokens, &request_id)
                .await;
            selection = state.pool.lock().unwrap().select_excluding(
                now_ms(),
                sticky.as_deref(),
                &model,
                surface,
                &overloaded,
            );
        }

        let selection = match selection {
            Ok(sel) => sel,
            Err(SelectError::Saturated { until_ms }) => {
                record_saturated(&state, &request_id, &model, until_ms);
                return attach_request_id(saturated_response(until_ms), &request_id);
            }
            Err(SelectError::NoAccounts) => {
                return finish_response(
                    last_upstream.take().unwrap_or_else(|| {
                        error_response(
                            StatusCode::SERVICE_UNAVAILABLE,
                            "no subscriptions are configured for this model",
                            "no_accounts",
                        )
                    }),
                    &request_id,
                    retry_proof.certified(),
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

        if selection.decision == Decision::NewBinding
            || selection.decision == Decision::StickyRebind
        {
            if let Some(key) = &sticky {
                if let Some(binding) = state
                    .pool
                    .lock()
                    .unwrap()
                    .bindings()
                    .into_iter()
                    .find(|b| b.cache_key == *key)
                {
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
            &endpoint,
            &target,
            &model,
            sticky.as_deref(),
            &selection,
            body_bytes.clone(),
            &client_headers,
            &mut refreshed,
        )
        .await
        {
            Attempt::Respond(resp) => {
                logging::log_request_completed(
                    &request_id,
                    resp.status().as_u16(),
                    started.elapsed().as_millis() as u64,
                );
                return finish_response(resp, &request_id, false);
            }
            Attempt::Overloaded {
                account_id,
                replay,
                retry_safe,
            } => {
                retry_proof.overload(retry_safe);
                overloaded.insert(account_id);
                last_upstream = Some(replay);
            }
            Attempt::Failover(passthrough) => {
                retry_proof.disqualify();
                last_upstream = passthrough.or(last_upstream);
                if attempts == max_attempts && !reset_after_quota {
                    reset_after_quota = true;
                    if state
                        .resets
                        .maybe_reset(&state.pool, &state.store, &state.tokens, &request_id)
                        .await
                    {
                        max_attempts += 1;
                    }
                }
            }
        }
    }

    let saturated = state
        .pool
        .lock()
        .unwrap()
        .select(now_ms(), sticky.as_deref(), &model, surface);
    if let Err(SelectError::Saturated { until_ms }) = saturated {
        record_saturated(&state, &request_id, &model, until_ms);
        return attach_request_id(saturated_response(until_ms), &request_id);
    }

    finish_response(
        last_upstream.take().unwrap_or_else(|| {
            error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "pool exhausted: every subscription attempt failed",
                "pool_exhausted",
            )
        }),
        &request_id,
        retry_proof.certified(),
    )
}

/// @cc [owner:ghuntley,label:proxy] failover-bounded-by-accounts
/// A request MUST make at most as many attempts as configured accounts, plus one additional attempt
/// after a successful banked reset, and the first successful upstream response MUST be returned
/// to the client; a 2xx stream that fails with a capacity code before any output is not successful:
/// the account MUST be excluded for the rest of the request (without changing its health) and
/// the next account tried. Upstream error bodies MUST be preserved and
/// returned when every attempt fails (or a 503 `pool_exhausted` error when no upstream response
/// was ever received). An upstream 401 or 403 MUST trigger exactly one forced token refresh and one
/// same-account retry per request; a second 401 or 403 (or a failed refresh) MUST classify the
/// account `AuthFailed` and fail over.
async fn attempt_account(
    state: &Arc<AppState>,
    request_id: &str,
    endpoint: &str,
    target: &str,
    model: &str,
    sticky: Option<&str>,
    selection: &Selection,
    body_bytes: axum::body::Bytes,
    client_headers: &HeaderMap,
    refreshed: &mut std::collections::HashSet<String>,
) -> Attempt {
    let started = Instant::now();
    let started_ms = now_ms();
    let mut retry_safe = true;
    let Some(backend) = state.backends.get(&selection.backend).cloned() else {
        return Attempt::Failover(None);
    };
    let account = state
        .pool
        .lock()
        .unwrap()
        .account(&selection.account_id)
        .cloned();
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
            logging::log_account_state(
                &selection.account_id,
                label,
                "auth_error",
                "missing or unusable credential",
            );
            drop(guard);
            return Attempt::Failover(None);
        }
    };

    let url = backend.rewrite_url(target, &account);
    let mut headers = HeaderMap::new();
    if backend.forward_client_headers() {
        for (name, value) in client_headers.iter() {
            if STRIPPED_CLIENT_HEADERS.contains(&name.as_str()) {
                continue;
            }
            headers.append(name.clone(), value.clone());
        }
    }
    headers.insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/json"),
    );
    if !headers.contains_key(http::header::ACCEPT) {
        headers.insert(
            http::header::ACCEPT,
            http::HeaderValue::from_static("text/event-stream, application/json"),
        );
    }
    let mut outbound_body: Value = serde_json::from_slice(&body_bytes).unwrap_or(Value::Null);
    backend.inject_headers(
        &account,
        &token,
        sticky,
        &outbound_body,
        client_headers,
        &mut headers,
    );
    let fallback_body = outbound_body.clone();
    let auto_usage = !state
        .stream_usage_unsupported
        .lock()
        .unwrap()
        .contains(&selection.backend)
        && backend.auto_stream_usage(endpoint, &mut outbound_body);
    let outbound_bytes = if backend.forward_body_verbatim() {
        // The upstream validates the client's own body, so it travels byte for byte.
        body_bytes.to_vec()
    } else {
        backend.prepare_body(&mut outbound_body);
        serde_json::to_vec(&outbound_body).unwrap_or_else(|_| body_bytes.to_vec())
    };

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
            let record = usage_record(
                request_id, endpoint, model, sticky, selection, &account, 0, started_ms,
            );
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
            retry_safe = false;
            let rejected = usage_record(
                request_id,
                endpoint,
                model,
                sticky,
                selection,
                &account,
                status.as_u16(),
                started_ms,
            );
            persist_usage(&state.store, &rejected);
            state
                .stream_usage_unsupported
                .lock()
                .unwrap()
                .insert(selection.backend);
            tracing::warn!(request_id = %request_id, backend = %selection.backend.as_str(), "usage.stream_option_unsupported");
            let retry_bytes =
                serde_json::to_vec(&fallback_body).unwrap_or_else(|_| body_bytes.to_vec());
            response = match state
                .client
                .post(&url)
                .headers(headers)
                .body(retry_bytes)
                .send()
                .await
            {
                Ok(resp) => resp,
                Err(error) => {
                    tracing::warn!(request_id = %request_id, error = %error, "upstream.network_error");
                    let record = usage_record(
                        request_id, endpoint, model, sticky, selection, &account, 0, started_ms,
                    );
                    persist_usage(&state.store, &record);
                    drop(guard);
                    return Attempt::Failover(None);
                }
            };
        } else {
            let record = usage_record(
                request_id,
                endpoint,
                model,
                sticky,
                selection,
                &account,
                status.as_u16(),
                started_ms,
            );
            persist_usage(&state.store, &record);
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
            drop(guard);
            return Attempt::Failover(Some(upstream_error_response(
                status,
                "application/json",
                rejection,
            )));
        }
    }

    let response_headers = response.headers().clone();
    let status = response.status();
    observe_response(
        state,
        &*backend,
        &account,
        &selection.account_id,
        &response_headers,
    );

    if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN)
        && refreshed.insert(selection.account_id.clone())
    {
        let body_text = response.text().await.unwrap_or_default();
        let mut record = usage_record(
            request_id,
            endpoint,
            model,
            sticky,
            selection,
            &account,
            status.as_u16(),
            started_ms,
        );
        if let Ok(value) = serde_json::from_str::<Value>(&body_text)
            && let Some(counts) = usage_from_dialect(&value, backend.usage_dialect(endpoint))
        {
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
                logging::log_account_state(
                    &selection.account_id,
                    label,
                    "auth_error",
                    "refresh rejected",
                );
                drop(guard);
                return Attempt::Failover(None);
            }
        }
    }

    if !status.is_success() {
        let resp_headers = response.headers().clone();
        let body_text = response.text().await.unwrap_or_default();
        let mut record = usage_record(
            request_id,
            endpoint,
            model,
            sticky,
            selection,
            &account,
            status.as_u16(),
            started_ms,
        );
        if let Ok(value) = serde_json::from_str::<Value>(&body_text) {
            if let Some(counts) = usage_from_dialect(&value, backend.usage_dialect(endpoint)) {
                record.input_tokens = Some(counts.input_tokens);
                record.output_tokens = Some(counts.output_tokens);
            }
            record.error_code = value
                .get("error")
                .and_then(crate::usage::error_code)
                .or_else(|| crate::usage::error_code(&value));
        }
        persist_usage(&state.store, &record);
        let kind = crate::usage::event_kind(record.error_code.as_deref());
        if record.error_code.is_some() && matches!(kind, "overloaded" | "policy_blocked") {
            state.store.insert_upstream_event(&event_for(
                &record,
                kind,
                "http",
                record.error_code.clone(),
            ));
        }
        let outcome = backend.classify(status.as_u16(), &body_text, &resp_headers, now_ms());
        if let Outcome::ModelQuotaExhausted { until_ms } = outcome {
            state
                .pool
                .lock()
                .unwrap()
                .report_model_quota(&selection.account_id, &model, until_ms);
            let mut event = event_for(&record, "quota_cooling", "http", record.error_code.clone());
            event.until_ms = Some(until_ms);
            state.store.insert_upstream_event(&event);
            logging::log_account_state(
                &selection.account_id,
                label,
                "model_cooling",
                &format!("{model} until {until_ms}"),
            );
        }
        if let Some(new_status) = {
            let mut pool = state.pool.lock().unwrap();
            pool.report(&selection.account_id, outcome.clone())
        } {
            let reset_at = {
                let pool = state.pool.lock().unwrap();
                pool.account(&selection.account_id)
                    .map(|a| a.reset_at)
                    .unwrap_or(0)
            };
            state
                .store
                .update_account_status(&selection.account_id, new_status, reset_at);
            match &outcome {
                Outcome::QuotaExhausted { until_ms } => {
                    logging::log_account_state(
                        &selection.account_id,
                        label,
                        "cooling",
                        &format!("until {until_ms}"),
                    );
                    let mut event =
                        event_for(&record, "quota_cooling", "http", record.error_code.clone());
                    event.until_ms = Some(*until_ms);
                    state.store.insert_upstream_event(&event);
                }
                Outcome::AuthFailed => {
                    logging::log_account_state(
                        &selection.account_id,
                        label,
                        "auth_error",
                        "upstream rejected credentials",
                    );
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
        let is_stream = usage_is_stream(&outbound_body, content_type.as_bytes());
        let content_type = if is_stream {
            http::HeaderValue::from_static("text/event-stream")
        } else {
            content_type
        };
        let mut stream = response.bytes_stream();
        let mut finish = UsageFinish {
            store: state.store.clone(),
            logs: state.logs.clone(),
            record: usage_record(
                request_id,
                endpoint,
                model,
                sticky,
                selection,
                &account,
                status.as_u16(),
                started_ms,
            ),
            tap: UsageTap::new(is_stream, backend.usage_dialect(endpoint)),
            ended: None,
            detail: "stream",
        };
        let mut held: VecDeque<axum::body::Bytes> = VecDeque::new();
        if is_stream {
            hold_until_output(&mut stream, &mut finish, &mut held).await;
        }
        if let Some(code) = finish.tap.overload_before_output().map(str::to_string) {
            logging::log_overload_failover(
                request_id,
                model,
                &selection.account_id,
                &account.label,
                &code,
                started.elapsed().as_millis() as u64,
            );
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
            retry_safe &= finish.tap.retry_safe_overload();
            finish.ended.get_or_insert(StreamEnd::Eof);
            finish.detail = "stream_failover";
            // Dropping `finish` persists the failed attempt and its `overloaded` event.
            drop(finish);
            drop(guard);
            let mut replay = Response::builder()
                .status(status_code)
                .body(Body::from(held.into_iter().flatten().collect::<Vec<u8>>()))
                .expect("valid replay response");
            replay
                .headers_mut()
                .insert(http::header::CONTENT_TYPE, content_type);
            return Attempt::Overloaded {
                account_id: selection.account_id.clone(),
                replay,
                retry_safe,
            };
        }
        let body_stream = Body::from_stream(futures::stream::unfold(
            (stream, Some(guard), finish, held),
            |(mut stream, guard, mut finish, mut held)| async move {
                if let Some(chunk) = held.pop_front() {
                    return Some((
                        Ok::<_, std::io::Error>(chunk),
                        (stream, guard, finish, held),
                    ));
                }
                if finish.ended.is_some() {
                    // The hold already reached EOF or an upstream error; do not poll again.
                    return match finish.ended {
                        Some(StreamEnd::UpstreamError) if guard.is_some() => Some((
                            Err(std::io::Error::new(
                                std::io::ErrorKind::Interrupted,
                                "upstream stream error",
                            )),
                            (stream, None, finish, held),
                        )),
                        _ => None,
                    };
                }
                match stream.next().await {
                    Some(Ok(chunk)) => {
                        finish.tap.feed(&chunk);
                        Some((
                            Ok::<_, std::io::Error>(chunk),
                            (stream, guard, finish, held),
                        ))
                    }
                    Some(Err(_)) => {
                        finish.ended.get_or_insert(StreamEnd::UpstreamError);
                        Some((
                            Err(std::io::Error::new(
                                std::io::ErrorKind::Interrupted,
                                "upstream stream error",
                            )),
                            (stream, None, finish, held),
                        ))
                    }
                    None => {
                        finish.ended.get_or_insert(StreamEnd::Eof);
                        None
                    }
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
        resp.headers_mut()
            .insert(http::header::CONTENT_TYPE, content_type);
        Attempt::Respond(resp)
    }
}

/// @cc [owner:ghuntley,label:proxy] pre-output-hold-bounded
/// `hold_until_output` MUST buffer upstream chunks (fed to the tap, in order) only until the tap
/// sees model output or a terminal event, the stream ends or errors, `PRE_OUTPUT_HOLD` elapses,
/// or `PRE_OUTPUT_HOLD_BYTES` are buffered, whichever comes first. It MUST NOT drop or reorder a
/// chunk, and a stream end observed while holding MUST be recorded in `finish.ended`.
async fn hold_until_output<S>(
    stream: &mut S,
    finish: &mut UsageFinish,
    held: &mut VecDeque<axum::body::Bytes>,
) where
    S: futures::Stream<Item = reqwest::Result<axum::body::Bytes>> + Unpin,
{
    let deadline = tokio::time::Instant::now() + PRE_OUTPUT_HOLD;
    let mut bytes = 0usize;
    while !finish.tap.decided() && bytes < PRE_OUTPUT_HOLD_BYTES {
        match tokio::time::timeout_at(deadline, stream.next()).await {
            Ok(Some(Ok(chunk))) => {
                finish.tap.feed(&chunk);
                bytes += chunk.len();
                held.push_back(chunk);
            }
            Ok(Some(Err(_))) => {
                finish.ended = Some(StreamEnd::UpstreamError);
                return;
            }
            Ok(None) => {
                finish.ended = Some(StreamEnd::Eof);
                return;
            }
            Err(_) => return,
        }
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

/// Records what the backend observed about an upstream response, and cools the account when the
/// upstream reports a window it cannot serve.
fn observe_response(
    state: &Arc<AppState>,
    backend: &dyn crate::provider::Backend,
    account: &crate::models::Account,
    account_id: &str,
    headers: &HeaderMap,
) {
    let now = now_ms();
    if let Some(observation) = backend.observe_response(account, headers, now) {
        state
            .observations
            .lock()
            .unwrap()
            .insert(account_id.to_string(), observation);
    }
    let Some(until) = backend.exhausted_until_ms(headers) else {
        return;
    };
    if until <= now {
        return;
    }
    let current = {
        state
            .pool
            .lock()
            .unwrap()
            .account(account_id)
            .map(|a| a.reset_at)
            .unwrap_or(0)
    };
    if until <= current {
        return;
    }
    state
        .pool
        .lock()
        .unwrap()
        .set_status(account_id, AccountStatus::Cooling, until);
    state
        .store
        .update_account_status(account_id, AccountStatus::Cooling, until);
    logging::log_account_state(
        account_id,
        Some(&account.label),
        "cooling",
        &format!("upstream window exhausted until {until}"),
    );
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
        state
            .store
            .update_account_status(account_id, new_status, reset_at);
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
        stream_outcome: None,
        error_code: None,
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

    #[hegel::test]
    fn retry_proof_cannot_recover_after_an_uncertain_attempt(tc: hegel::TestCase) {
        use hegel::generators as gs;
        let attempts: Vec<u8> =
            tc.draw(gs::vecs(gs::integers::<u8>().min_value(0).max_value(2)).max_size(50));
        let mut proof = RetryProof::default();
        for attempt in &attempts {
            match attempt {
                0 => proof.overload(true),
                1 => proof.overload(false),
                _ => proof.disqualify(),
            }
        }
        assert_eq!(
            proof.certified(),
            !attempts.is_empty() && attempts.iter().all(|attempt| *attempt == 0)
        );
    }

    #[test]
    fn finalizer_removes_forged_and_stale_certificates() {
        let mut response = Response::new(Body::empty());
        response.headers_mut().append(
            RETRY_SAFE_HEADER,
            http::HeaderValue::from_static(RETRY_SAFE_VALUE),
        );
        response
            .headers_mut()
            .append(RETRY_SAFE_HEADER, http::HeaderValue::from_static("forged"));
        let response = finish_response(response, "019a45b7-0000-7000-8000-000000000001", true);
        assert_eq!(
            response.headers().get_all(RETRY_SAFE_HEADER).iter().count(),
            1
        );
        let response = finish_response(response, "019a45b7-0000-7000-8000-000000000001", false);
        assert!(!response.headers().contains_key(RETRY_SAFE_HEADER));
    }

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
        assert_eq!(
            extract_sticky_key(&body, Some("sess-3")).as_deref(),
            Some("sess-3")
        );
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
        assert!(usage_is_stream(
            &json!({}),
            b"text/event-stream; charset=utf-8"
        ));
        assert!(!usage_is_stream(&json!({}), b"application/json"));
    }
}
