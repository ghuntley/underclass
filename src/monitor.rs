//! Compact, read-only data for the terminal dashboard.
use crate::models::{AccountStatus, BackendId, now_ms};
use crate::proxy::AppState;
use crate::resets::UsageSnapshot;
use crate::store::{UsageQuery, UsageSummary};
use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use chrono::Datelike;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MonitorAccount {
    pub id: String,
    pub label: String,
    pub backend: BackendId,
    pub status: AccountStatus,
    pub reset_at: i64,
    pub inflight: u32,
    pub sticky_sessions: usize,
    pub month: Counters,
    pub quota: Option<UsageSnapshot>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Counters {
    pub attempts: i64,
    pub unknown: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
}

impl From<&UsageSummary> for Counters {
    fn from(value: &UsageSummary) -> Self {
        Self {
            attempts: value.requests,
            unknown: value.unknown_requests,
            input_tokens: value.input_tokens,
            output_tokens: value.output_tokens,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MonitorRequest {
    pub ts: i64,
    pub model: String,
    pub backend: Option<String>,
    pub label: Option<String>,
    pub status: u16,
    pub duration_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MonitorSnapshot {
    pub now_ms: i64,
    pub month_start_ms: i64,
    pub accounts: Vec<MonitorAccount>,
    pub month: Counters,
    pub last_minute: Counters,
    pub minute_bins: Vec<i64>,
    pub recent: Vec<MonitorRequest>,
}

/// @cc [owner:ghuntley,label:security] monitor-read-only-redacted
/// The admin-gated monitor snapshot MUST perform no writes or upstream calls and MUST expose no
/// credentials, request bodies, prompt cache keys, or raw account IDs in recent request entries.
pub async fn snapshot(State(state): State<Arc<AppState>>) -> Response {
    match collect(&state) {
        Ok(snapshot) => Json(snapshot).into_response(),
        Err(error) => {
            tracing::error!(error = %error, "monitor.query_failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "monitor query failed").into_response()
        }
    }
}

fn collect(state: &AppState) -> rusqlite::Result<MonitorSnapshot> {
    let now = now_ms();
    let month_start = chrono::Utc::now()
        .date_naive()
        .with_day(1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis();
    let by_account_rows = state
        .store
        .monitor_account_totals(month_start, now.saturating_add(1))?;
    let month = by_account_rows
        .iter()
        .fold(Counters::default(), |mut total, row| {
            total.attempts += row.requests;
            total.unknown += row.unknown_requests;
            total.input_tokens += row.input_tokens;
            total.output_tokens += row.output_tokens;
            total
        });
    let by_account: HashMap<String, Counters> = by_account_rows
        .into_iter()
        .filter_map(|row| row.account_id.clone().map(|id| (id, Counters::from(&row))))
        .collect();
    let last_minute = state.store.usage_summary(&UsageQuery {
        from_ms: Some(now.saturating_sub(60_000)),
        to_ms: Some(now.saturating_add(1)),
        ..Default::default()
    })?;
    let minute_bins = state.store.monitor_minute_bins(now)?;
    let mut bindings: HashMap<String, usize> = HashMap::new();
    let mut accounts = {
        let pool = state.pool.lock().unwrap();
        for binding in pool.bindings() {
            *bindings.entry(binding.account_id).or_default() += 1;
        }
        pool.accounts
            .values()
            .map(|item| {
                let account = &item.account;
                MonitorAccount {
                    id: account.id.clone(),
                    label: account.label.clone(),
                    backend: account.backend,
                    status: account.status,
                    reset_at: account.reset_at,
                    inflight: item.inflight,
                    sticky_sessions: bindings.get(&account.id).copied().unwrap_or(0),
                    month: by_account.get(&account.id).cloned().unwrap_or_default(),
                    quota: if account.backend == BackendId::Codex {
                        state.resets.snapshot(&account.id)
                    } else {
                        None
                    },
                }
            })
            .collect::<Vec<_>>()
    };
    accounts.sort_by(|a, b| a.label.cmp(&b.label).then_with(|| a.id.cmp(&b.id)));
    let recent = state
        .logs
        .lock()
        .unwrap()
        .iter()
        .rev()
        .take(20)
        .map(|entry| MonitorRequest {
            ts: entry.ts,
            model: entry.model.clone(),
            backend: entry.backend.clone(),
            label: entry.label.clone(),
            status: entry.status,
            duration_ms: entry.duration_ms,
        })
        .collect();
    Ok(MonitorSnapshot {
        now_ms: now,
        month_start_ms: month_start,
        accounts,
        month,
        last_minute: last_minute.first().map(Counters::from).unwrap_or_default(),
        minute_bins,
        recent,
    })
}
