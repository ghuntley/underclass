//! Codex usage snapshots and banked reset redemption.
use crate::logging;
use crate::models::{Account, AccountStatus, BackendId, now_ms};
use crate::pool::PoolCore;
use crate::store::Store;
use crate::tokens::TokenManager;
use chrono::DateTime;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as AsyncMutex;

const CHECK_INTERVAL_MS: i64 = 30_000;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Window {
    pub used_percent: f64,
    pub reset_at: Option<i64>,
    pub limit_window_seconds: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RateLimit {
    pub allowed: Option<bool>,
    pub limit_reached: Option<bool>,
    pub primary_window: Option<Window>,
    pub secondary_window: Option<Window>,
}

#[derive(Clone, Debug, Deserialize)]
struct UsageResponse {
    account_id: Option<String>,
    rate_limit: Option<RateLimit>,
    rate_limit_reset_credits: Option<CreditCount>,
}

#[derive(Clone, Debug, Deserialize)]
struct CreditCount {
    available_count: i64,
}

#[derive(Clone, Debug, Deserialize)]
struct CreditList {
    credits: Vec<Credit>,
}

#[derive(Clone, Debug, Deserialize)]
struct Credit {
    id: String,
    reset_type: String,
    status: String,
    expires_at: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct ConsumeResponse {
    code: ConsumeCode,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ConsumeCode {
    Reset,
    NothingToReset,
    NoCredit,
    AlreadyRedeemed,
}

#[derive(Clone, Debug, Serialize)]
pub struct UsageSnapshot {
    pub rate_limit: Option<RateLimit>,
    pub available_resets: i64,
    pub credit_expirations: Vec<Option<i64>>,
    pub fetched_at_ms: i64,
}

/// @cc [owner:ghuntley,label:pool] natural-recovery-ranking
/// A blocked Codex account's natural recovery MUST be the latest future reset timestamp among
/// exhausted quota windows. A missing or non-exhausted window MUST NOT extend that wait.
pub fn natural_recovery_ms(rate_limit: &RateLimit, now: i64) -> Option<i64> {
    if rate_limit.allowed != Some(false) || rate_limit.limit_reached == Some(false) {
        return None;
    }
    [
        rate_limit.primary_window.as_ref(),
        rate_limit.secondary_window.as_ref(),
    ]
    .into_iter()
    .flatten()
    .filter(|window| window.used_percent >= 99.0)
    .filter_map(|window| {
        window
            .reset_at
            .and_then(|seconds| seconds.checked_mul(1000))
    })
    .filter(|deadline| *deadline > now)
    .max()
}

#[derive(Clone, Debug)]
pub struct Candidate {
    pub account_id: String,
    pub recovery_ms: i64,
    pub credit_id: String,
    pub credit_expiry_ms: Option<i64>,
}

/// @cc [owner:ghuntley,label:pool] longest-blocked-first
/// Candidate selection MUST prefer the latest natural recovery across all eligible Codex
/// accounts. Equal recoveries MUST prefer the earliest-expiring credit, then account ID.
pub fn choose_candidate(candidates: impl IntoIterator<Item = Candidate>) -> Option<Candidate> {
    candidates.into_iter().max_by(|a, b| {
        a.recovery_ms
            .cmp(&b.recovery_ms)
            .then_with(|| {
                b.credit_expiry_ms
                    .unwrap_or(i64::MAX)
                    .cmp(&a.credit_expiry_ms.unwrap_or(i64::MAX))
            })
            .then_with(|| b.account_id.cmp(&a.account_id))
    })
}

fn set_status_if(
    pool: &Arc<Mutex<PoolCore>>,
    store: &Store,
    account_id: &str,
    expected: AccountStatus,
    next: AccountStatus,
    reset_at: i64,
) -> bool {
    let mut core = pool.lock().unwrap();
    if core
        .account(account_id)
        .is_none_or(|a| a.status != expected)
    {
        return false;
    }
    core.set_status(account_id, next, reset_at);
    store.update_account_status(account_id, next, reset_at);
    true
}

pub struct ResetManager {
    client: reqwest::Client,
    base_url: String,
    enabled: bool,
    snapshots: Mutex<HashMap<String, UsageSnapshot>>,
    last_check_ms: Mutex<i64>,
    lock: AsyncMutex<()>,
}

impl ResetManager {
    pub fn new(client: reqwest::Client, base_url: String, enabled: bool) -> Self {
        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            enabled,
            snapshots: Mutex::new(HashMap::new()),
            last_check_ms: Mutex::new(0),
            lock: AsyncMutex::new(()),
        }
    }

    pub fn snapshot(&self, account_id: &str) -> Option<UsageSnapshot> {
        self.snapshots.lock().unwrap().get(account_id).cloned()
    }

    pub async fn poll_account(
        &self,
        account: &Account,
        tokens: &TokenManager,
        pool: &Arc<Mutex<PoolCore>>,
        store: &Store,
    ) {
        if account.backend != BackendId::Codex || account.status == AccountStatus::Disabled {
            return;
        }
        match self.fetch_usage(account, tokens).await {
            Ok(usage) => {
                if let Some(limit) = &usage.rate_limit {
                    if limit.allowed == Some(true) {
                        store.clear_reset_attempt(&account.id);
                        set_status_if(
                            pool,
                            store,
                            &account.id,
                            AccountStatus::Cooling,
                            AccountStatus::Healthy,
                            0,
                        );
                    } else {
                        if let Some(recovery) = natural_recovery_ms(limit, now_ms()) {
                            set_status_if(
                                pool,
                                store,
                                &account.id,
                                AccountStatus::Healthy,
                                AccountStatus::Cooling,
                                recovery,
                            );
                        }
                    }
                }
                if usage.available_resets > 0
                    && let Err(reason) = self.fetch_credits(account, tokens).await
                {
                    logging::log_reset_fetch(&account.id, &account.label, reason);
                }
            }
            Err(reason) => logging::log_reset_fetch(&account.id, &account.label, reason),
        }
    }

    async fn fetch_usage(
        &self,
        account: &Account,
        tokens: &TokenManager,
    ) -> Result<UsageSnapshot, &'static str> {
        let token = tokens
            .access_token(account)
            .await
            .map_err(|_| "token_unavailable")?;
        let mut request = self
            .client
            .get(format!("{}/wham/usage", self.base_url))
            .timeout(std::time::Duration::from_secs(8))
            .header(reqwest::header::USER_AGENT, crate::codex::USER_AGENT)
            .bearer_auth(&token);
        if let Some(id) = &account.account_id {
            request = request.header("ChatGPT-Account-Id", id);
        }
        let response = request.send().await.map_err(|_| "network_error")?;
        if !response.status().is_success() {
            return Err("upstream_error");
        }
        let usage: UsageResponse = response.json().await.map_err(|_| "invalid_usage")?;
        if usage
            .account_id
            .as_ref()
            .zip(account.account_id.as_ref())
            .is_some_and(|(actual, expected)| actual != expected)
        {
            return Err("account_mismatch");
        }
        let available_resets = usage
            .rate_limit_reset_credits
            .as_ref()
            .map_or(0, |c| c.available_count.max(0));
        let snapshot = UsageSnapshot {
            rate_limit: usage.rate_limit,
            available_resets,
            credit_expirations: if available_resets > 0 {
                self.snapshot(&account.id)
                    .map_or_else(Vec::new, |s| s.credit_expirations)
            } else {
                Vec::new()
            },
            fetched_at_ms: now_ms(),
        };
        self.snapshots
            .lock()
            .unwrap()
            .insert(account.id.clone(), snapshot.clone());
        Ok(snapshot)
    }

    async fn fetch_credits(
        &self,
        account: &Account,
        tokens: &TokenManager,
    ) -> Result<Vec<Credit>, &'static str> {
        let token = tokens
            .access_token(account)
            .await
            .map_err(|_| "token_unavailable")?;
        let mut request = self
            .client
            .get(format!("{}/wham/rate-limit-reset-credits", self.base_url))
            .timeout(std::time::Duration::from_secs(8))
            .header(reqwest::header::USER_AGENT, crate::codex::USER_AGENT)
            .bearer_auth(&token);
        if let Some(id) = &account.account_id {
            request = request.header("ChatGPT-Account-Id", id);
        }
        let response = request.send().await.map_err(|_| "network_error")?;
        if !response.status().is_success() {
            return Err("upstream_error");
        }
        let details: CreditList = response.json().await.map_err(|_| "invalid_credits")?;
        let credits: Vec<Credit> = details
            .credits
            .into_iter()
            .filter(|credit| {
                credit.reset_type == "codex_rate_limits" && credit.status == "available"
            })
            .collect();
        let expirations = credits
            .iter()
            .map(|credit| expiry_ms(credit.expires_at.as_deref()))
            .collect();
        if let Some(snapshot) = self.snapshots.lock().unwrap().get_mut(&account.id) {
            snapshot.credit_expirations = expirations;
        }
        Ok(credits)
    }

    async fn consume(
        &self,
        account: &Account,
        tokens: &TokenManager,
        request_id: &str,
        credit_id: &str,
    ) -> Result<ConsumeCode, &'static str> {
        let token = tokens
            .access_token(account)
            .await
            .map_err(|_| "token_unavailable")?;
        let mut request = self
            .client
            .post(format!(
                "{}/wham/rate-limit-reset-credits/consume",
                self.base_url
            ))
            .timeout(std::time::Duration::from_secs(8))
            .header(reqwest::header::USER_AGENT, crate::codex::USER_AGENT)
            .bearer_auth(&token)
            .json(&serde_json::json!({"redeem_request_id": request_id, "credit_id": credit_id}));
        if let Some(id) = &account.account_id {
            request = request.header("ChatGPT-Account-Id", id);
        }
        let response = request.send().await.map_err(|_| "network_error")?;
        if !response.status().is_success() {
            return Err("upstream_error");
        }
        Ok(response
            .json::<ConsumeResponse>()
            .await
            .map_err(|_| "invalid_outcome")?
            .code)
    }

    /// @cc [owner:ghuntley,label:pool] reset-only-on-live-codex-outage
    /// Redemption MUST occur only for a live request when no enabled Codex account is healthy,
    /// and MUST consume at most one credit per attempt across the entire Codex pool.
    pub async fn maybe_reset(
        &self,
        pool: &Arc<Mutex<PoolCore>>,
        store: &Store,
        tokens: &TokenManager,
        request_id: &str,
    ) -> bool {
        if !self.enabled {
            return false;
        }
        let _guard = self.lock.lock().await;
        let now = now_ms();
        let mut accounts: Vec<Account> = {
            let mut core = pool.lock().unwrap();
            core.sweep(now);
            let codex: Vec<Account> = core
                .accounts
                .values()
                .map(|s| s.account.clone())
                .filter(|a| a.backend == BackendId::Codex && a.status != AccountStatus::Disabled)
                .collect();
            if codex.is_empty() || codex.iter().any(|a| a.healthy()) {
                return false;
            }
            codex
                .into_iter()
                .filter(|a| a.status == AccountStatus::Cooling)
                .collect()
        };
        if accounts.is_empty() {
            return false;
        }
        let pending: std::collections::HashSet<String> = accounts
            .iter()
            .filter(|a| {
                store
                    .reset_attempt(&a.id)
                    .is_some_and(|(_, _, state)| state == "pending")
            })
            .map(|a| a.id.clone())
            .collect();
        if !pending.is_empty() {
            accounts.retain(|a| pending.contains(&a.id));
        }
        {
            let mut last = self.last_check_ms.lock().unwrap();
            if now.saturating_sub(*last) < CHECK_INTERVAL_MS {
                return false;
            }
            *last = now;
        }
        logging::log_reset_decision(request_id, "codex_pool_exhausted", accounts.len(), None);

        let mut candidates = Vec::new();
        for account in &accounts {
            let usage = match self.fetch_usage(account, tokens).await {
                Ok(usage) => usage,
                Err(reason) => {
                    logging::log_reset_fetch(&account.id, &account.label, reason);
                    continue;
                }
            };
            if usage.rate_limit.as_ref().and_then(|r| r.allowed) == Some(true) {
                set_status_if(
                    pool,
                    store,
                    &account.id,
                    AccountStatus::Cooling,
                    AccountStatus::Healthy,
                    0,
                );
                store.clear_reset_attempt(&account.id);
                logging::log_reset_account(
                    request_id,
                    &account.id,
                    &account.label,
                    "recovered_without_credit",
                    0,
                );
                return true;
            }
            let Some(recovery_ms) = usage
                .rate_limit
                .as_ref()
                .and_then(|r| natural_recovery_ms(r, now))
            else {
                logging::log_reset_account(
                    request_id,
                    &account.id,
                    &account.label,
                    "no_verified_quota_window",
                    0,
                );
                continue;
            };
            if usage.available_resets == 0 && store.reset_attempt(&account.id).is_none() {
                continue;
            }
            if let Some((credit_id, _, state)) = store.reset_attempt(&account.id) {
                if state == "pending" {
                    candidates.push(Candidate {
                        account_id: account.id.clone(),
                        recovery_ms,
                        credit_id,
                        credit_expiry_ms: None,
                    });
                }
                continue;
            }
            let credits = match self.fetch_credits(account, tokens).await {
                Ok(credits) => credits,
                Err(reason) => {
                    logging::log_reset_fetch(&account.id, &account.label, reason);
                    continue;
                }
            };
            if let Some(credit) = credits
                .into_iter()
                .min_by_key(|c| expiry_ms(c.expires_at.as_deref()).unwrap_or(i64::MAX))
            {
                candidates.push(Candidate {
                    account_id: account.id.clone(),
                    recovery_ms,
                    credit_id: credit.id.clone(),
                    credit_expiry_ms: expiry_ms(credit.expires_at.as_deref()),
                });
            }
        }
        let Some(candidate) = choose_candidate(candidates) else {
            logging::log_reset_decision(request_id, "no_eligible_credit", accounts.len(), None);
            return false;
        };
        let account = accounts
            .iter()
            .find(|a| a.id == candidate.account_id)
            .unwrap();
        {
            let core = pool.lock().unwrap();
            let codex: Vec<&Account> = core
                .accounts
                .values()
                .map(|s| &s.account)
                .filter(|a| a.backend == BackendId::Codex && a.status != AccountStatus::Disabled)
                .collect();
            if codex.iter().any(|a| a.healthy())
                || core
                    .account(&account.id)
                    .is_none_or(|a| a.status != AccountStatus::Cooling)
            {
                logging::log_reset_decision(request_id, "pool_changed", accounts.len(), None);
                return false;
            }
        }
        if store.reset_attempt(&account.id).is_none() {
            let id = uuid::Uuid::new_v4().to_string();
            store.save_reset_attempt(&account.id, &candidate.credit_id, &id);
        }
        let Some((credit_id, redemption_id, state)) = store.reset_attempt(&account.id) else {
            return false;
        };
        if state != "pending" {
            return false;
        }
        logging::log_reset_account(
            request_id,
            &account.id,
            &account.label,
            "redeeming",
            candidate.recovery_ms.saturating_sub(now),
        );
        let outcome = match self
            .consume(account, tokens, &redemption_id, &credit_id)
            .await
        {
            Ok(code) => code,
            Err(reason) => {
                logging::log_reset_fetch(&account.id, &account.label, reason);
                return false;
            }
        };
        match outcome {
            ConsumeCode::Reset | ConsumeCode::AlreadyRedeemed => {
                store.finish_reset_attempt(&account.id);
                logging::log_reset_account(
                    request_id,
                    &account.id,
                    &account.label,
                    "redeemed",
                    candidate.recovery_ms.saturating_sub(now),
                );
                for _ in 0..3 {
                    if let Ok(usage) = self.fetch_usage(account, tokens).await
                        && usage.rate_limit.as_ref().and_then(|r| r.allowed) == Some(true)
                    {
                        set_status_if(
                            pool,
                            store,
                            &account.id,
                            AccountStatus::Cooling,
                            AccountStatus::Healthy,
                            0,
                        );
                        store.clear_reset_attempt(&account.id);
                        return true;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                false
            }
            ConsumeCode::NothingToReset | ConsumeCode::NoCredit => {
                store.clear_reset_attempt(&account.id);
                logging::log_reset_account(
                    request_id,
                    &account.id,
                    &account.label,
                    "not_redeemed",
                    0,
                );
                false
            }
        }
    }
}

fn expiry_ms(value: Option<&str>) -> Option<i64> {
    value
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .and_then(|date| date.timestamp().checked_mul(1000))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json, Router,
        extract::State,
        routing::{get, post},
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[test]
    fn six_day_account_beats_tomorrow_account() {
        let selected = choose_candidate([
            Candidate {
                account_id: "A".into(),
                recovery_ms: 86_400_000,
                credit_id: "a".into(),
                credit_expiry_ms: None,
            },
            Candidate {
                account_id: "B".into(),
                recovery_ms: 6 * 86_400_000,
                credit_id: "b".into(),
                credit_expiry_ms: None,
            },
        ])
        .unwrap();
        assert_eq!(selected.account_id, "B");
    }

    #[derive(Default)]
    struct MockState {
        allowed: AtomicBool,
        has_credit: AtomicBool,
        fail_once: AtomicBool,
        consumes: AtomicUsize,
        redemption_ids: Mutex<Vec<String>>,
    }

    async fn usage(State(state): State<Arc<MockState>>) -> Json<serde_json::Value> {
        let allowed = state.allowed.load(Ordering::SeqCst);
        Json(serde_json::json!({
            "account_id": "chatgpt-account",
            "rate_limit": {
                "allowed": allowed,
                "limit_reached": !allowed,
                "primary_window": {"used_percent": if allowed { 0 } else { 100 }, "reset_at": now_ms() / 1000 + 3600, "limit_window_seconds": 18000},
                "secondary_window": {"used_percent": if allowed { 0 } else { 100 }, "reset_at": now_ms() / 1000 + 6 * 86400, "limit_window_seconds": 604800}
            },
            "rate_limit_reset_credits": {"available_count": if allowed || !state.has_credit.load(Ordering::SeqCst) { 0 } else { 1 }}
        }))
    }

    async fn credits(State(state): State<Arc<MockState>>) -> Json<serde_json::Value> {
        let credits = if state.has_credit.load(Ordering::SeqCst) {
            serde_json::json!([{
                "id": "credit-1", "reset_type": "codex_rate_limits", "status": "available",
                "expires_at": "2026-12-01T00:00:00Z"
            }])
        } else {
            serde_json::json!([])
        };
        Json(
            serde_json::json!({"credits": credits, "available_count": if state.has_credit.load(Ordering::SeqCst) { 1 } else { 0 }}),
        )
    }

    async fn consume(
        State(state): State<Arc<MockState>>,
        Json(body): Json<serde_json::Value>,
    ) -> (axum::http::StatusCode, Json<serde_json::Value>) {
        assert_eq!(body["credit_id"], "credit-1");
        assert!(
            body["redeem_request_id"]
                .as_str()
                .is_some_and(|id| !id.is_empty())
        );
        state
            .redemption_ids
            .lock()
            .unwrap()
            .push(body["redeem_request_id"].as_str().unwrap().to_string());
        state.consumes.fetch_add(1, Ordering::SeqCst);
        if state.fail_once.swap(false, Ordering::SeqCst) {
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "temporary"})),
            );
        }
        state.allowed.store(true, Ordering::SeqCst);
        (
            axum::http::StatusCode::OK,
            Json(serde_json::json!({"code": "reset", "windows_reset": 2})),
        )
    }

    #[tokio::test]
    async fn concurrent_outage_requests_redeem_once_and_readmit() {
        let mock = Arc::new(MockState::default());
        mock.has_credit.store(true, Ordering::SeqCst);
        let app = Router::new()
            .route("/wham/usage", get(usage))
            .route("/wham/rate-limit-reset-credits", get(credits))
            .route("/wham/rate-limit-reset-credits/consume", post(consume))
            .with_state(mock.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(axum::serve(listener, app).into_future());

        let store = Arc::new(Store::in_memory().unwrap());
        let account = Account {
            id: "account-1".into(),
            backend: BackendId::Codex,
            label: "owner@example.test".into(),
            refresh_token: Some("unused".into()),
            access_token: Some("test-token".into()),
            expires_at: i64::MAX,
            account_id: Some("chatgpt-account".into()),
            residency: None,
            enterprise_url: None,
            status: AccountStatus::Cooling,
            reset_at: now_ms() + 3_600_000,
            created_at: 0,
            updated_at: 0,
        };
        store.upsert_account(&account);
        let pool = Arc::new(Mutex::new(PoolCore::new(&store)));
        let client = reqwest::Client::new();
        let tokens = TokenManager::new(store.clone(), client.clone());
        let manager = ResetManager::new(client, base.clone(), true);
        let (first, second) = tokio::join!(
            manager.maybe_reset(&pool, &store, &tokens, "request-1"),
            manager.maybe_reset(&pool, &store, &tokens, "request-2")
        );
        assert!(first || second);
        assert_eq!(mock.consumes.load(Ordering::SeqCst), 1);
        assert_eq!(
            pool.lock().unwrap().account("account-1").unwrap().status,
            AccountStatus::Healthy
        );
        assert!(store.reset_attempt("account-1").is_none());
        assert!(
            !manager
                .maybe_reset(&pool, &store, &tokens, "healthy-request")
                .await
        );

        mock.allowed.store(false, Ordering::SeqCst);
        mock.fail_once.store(true, Ordering::SeqCst);
        set_status_if(
            &pool,
            &store,
            "account-1",
            AccountStatus::Healthy,
            AccountStatus::Cooling,
            now_ms() + 3_600_000,
        );
        let failed = ResetManager::new(reqwest::Client::new(), base.clone(), true);
        assert!(
            !failed
                .maybe_reset(&pool, &store, &tokens, "failed-request")
                .await
        );
        assert!(store.reset_attempt("account-1").is_some());
        let restarted = ResetManager::new(reqwest::Client::new(), base.clone(), true);
        assert!(
            restarted
                .maybe_reset(&pool, &store, &tokens, "retry-request")
                .await
        );
        {
            let ids = mock.redemption_ids.lock().unwrap();
            assert_eq!(
                ids[1], ids[2],
                "retry must reuse the persisted idempotency key"
            );
        }

        mock.allowed.store(false, Ordering::SeqCst);
        mock.has_credit.store(false, Ordering::SeqCst);
        set_status_if(
            &pool,
            &store,
            "account-1",
            AccountStatus::Healthy,
            AccountStatus::Cooling,
            now_ms() + 3_600_000,
        );
        let no_credit = ResetManager::new(reqwest::Client::new(), base, true);
        assert!(
            !no_credit
                .maybe_reset(&pool, &store, &tokens, "no-credit-request")
                .await
        );
        assert_eq!(mock.consumes.load(Ordering::SeqCst), 3);
        assert_eq!(
            pool.lock().unwrap().account("account-1").unwrap().status,
            AccountStatus::Cooling
        );
        server.abort();
    }
}
