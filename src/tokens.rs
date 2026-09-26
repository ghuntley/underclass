use crate::jwt;
use crate::models::{now_ms, Account};
use crate::store::Store;
use reqwest::Client;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as AsyncMutex;

const PROACTIVE_REFRESH_MARGIN_MS: i64 = 5 * 60 * 1000;

/// How often a Codex refresh token is rotated proactively, independent of access-token expiry.
///
/// Codex access tokens are valid for roughly 6.6-10 days, so without this the stored refresh token
/// would sit untouched for over a week. Copilot is excluded: its GitHub OAuth token does not expire
/// and `force_refresh` returns it verbatim, so there is nothing to rotate.
pub const ROTATION_INTERVAL_MS: i64 = 24 * 60 * 60 * 1000;

/// Reports whether `account`'s refresh token is due for proactive rotation.
///
/// Only Codex accounts that actually hold a refresh token are ever due, so an account mid-onboarding
/// is skipped instead of raising a missing-credential error. An account that has never been rotated
/// carries `token_refreshed_at == 0`, which makes its age the entire elapsed epoch and therefore due
/// on the first tick; that establishes the baseline later comparisons measure from.
pub fn rotation_due(account: &Account, now_ms: i64) -> bool {
    account.backend == crate::models::BackendId::Codex
        && account.refresh_token.is_some()
        && now_ms.saturating_sub(account.token_refreshed_at) >= ROTATION_INTERVAL_MS
}

#[derive(Debug, thiserror::Error)]
pub enum TokenError {
    #[error("account has no stored credential")]
    MissingToken,
    #[error("token refresh failed: {0}")]
    RefreshFailed(String),
}

pub struct TokenManager {
    store: Arc<Store>,
    client: Client,
    locks: Mutex<HashMap<String, Arc<AsyncMutex<()>>>>,
}

impl TokenManager {
    pub fn new(store: Arc<Store>, client: Client) -> Self {
        Self {
            store,
            client,
            locks: Mutex::new(HashMap::new()),
        }
    }

    fn lock_for(&self, account_id: &str) -> Arc<AsyncMutex<()>> {
        self.locks
            .lock()
            .unwrap()
            .entry(account_id.to_string())
            .or_default()
            .clone()
    }

    /// @cc [owner:ghuntley,label:auth] proactive-refresh-margin
    /// `access_token` MUST return the cached Codex access token while it remains valid beyond
    /// `PROACTIVE_REFRESH_MARGIN_MS` from expiry, and MUST force a refresh otherwise. For Copilot
    /// it MUST return the stored GitHub token directly (it never expires or refreshes).
    pub async fn access_token(&self, account: &Account) -> Result<String, TokenError> {
        match account.backend {
            crate::models::BackendId::Copilot => account
                .refresh_token
                .clone()
                .ok_or(TokenError::MissingToken),
            crate::models::BackendId::Codex => {
                if let Some(token) = &account.access_token {
                    if account.expires_at.saturating_sub(PROACTIVE_REFRESH_MARGIN_MS) > now_ms() {
                        return Ok(token.clone());
                    }
                }
                self.force_refresh(&account.id).await
            }
        }
    }

    /// @cc [owner:ghuntley,label:auth] force-refresh-single-flight-and-rotation
    /// `force_refresh` MUST serialize refreshes per account (single-flight lock), MUST NOT skip
    /// the refresh based on a still-valid cached token (callers rely on it to recover from 401),
    /// and MUST persist the rotated refresh token, new access token, expiry, `token_refreshed_at`,
    /// and any newly learned ChatGPT account id/residency to the store before returning the access
    /// token.
    pub async fn force_refresh(&self, account_id: &str) -> Result<String, TokenError> {
        let lock = self.lock_for(account_id).clone();
        let _guard = lock.lock().await;

        let account = self
            .store
            .get_account(account_id)
            .ok_or(TokenError::MissingToken)?;

        let refresh_token = account.refresh_token.clone().ok_or(TokenError::MissingToken)?;

        match account.backend {
            crate::models::BackendId::Copilot => Ok(refresh_token),
            crate::models::BackendId::Codex => {
                let tokens = crate::codex::refresh(&self.client, &refresh_token)
                    .await
                    .map_err(|e| TokenError::RefreshFailed(e.to_string()))?;
                let claims = jwt::parse_jwt_claims(&tokens.access_token);
                let chatgpt_account_id = claims
                    .as_ref()
                    .and_then(|c| jwt::extract_account_id(c))
                    .or(account.account_id.clone());
                let residency = claims.as_ref().and_then(|c| jwt::extract_residency(c));
                let expires_at = now_ms() + (tokens.expires_in.unwrap_or(3600) as i64) * 1000;
                self.store.update_tokens(
                    account_id,
                    Some(&tokens.refresh_token),
                    Some(&tokens.access_token),
                    expires_at,
                    chatgpt_account_id.as_deref(),
                    residency.as_deref(),
                );
                Ok(tokens.access_token)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{AccountStatus, BackendId};

    fn account(backend: BackendId, refresh_token: Option<&str>, refreshed_at: i64) -> Account {
        Account {
            id: "acct".into(),
            backend,
            label: "owner@example.test".into(),
            refresh_token: refresh_token.map(str::to_string),
            access_token: Some("access".into()),
            expires_at: i64::MAX,
            token_refreshed_at: refreshed_at,
            account_id: None,
            residency: None,
            enterprise_url: None,
            status: AccountStatus::Healthy,
            reset_at: 0,
            created_at: 0,
            updated_at: refreshed_at,
        }
    }

    const DAY: i64 = 24 * 60 * 60 * 1000;

    #[test]
    fn codex_is_due_once_the_interval_elapses() {
        let due = account(BackendId::Codex, Some("refresh"), 1_000);
        assert!(!rotation_due(&due, 1_000 + DAY - 1));
        assert!(rotation_due(&due, 1_000 + DAY));
        assert!(rotation_due(&due, 1_000 + 10 * DAY));
    }

    #[test]
    fn never_rotated_codex_is_due_on_first_run() {
        // A store written before this column carries 0, so the age is the whole epoch and the
        // account rotates on the first tick, establishing the baseline for later comparisons.
        let never = account(BackendId::Codex, Some("refresh"), 0);
        assert!(rotation_due(&never, 1_800_000_000_000));
        // At a degenerate clock of 0 there is no elapsed time yet, so nothing is due.
        assert!(!rotation_due(&never, 0));
    }

    #[test]
    fn copilot_and_credential_less_accounts_are_never_due() {
        assert!(!rotation_due(&account(BackendId::Copilot, Some("gh-token"), 0), i64::MAX));
        assert!(!rotation_due(&account(BackendId::Codex, None, 0), i64::MAX));
    }

    #[test]
    fn a_clock_behind_the_last_rotation_is_not_due() {
        let due = account(BackendId::Codex, Some("refresh"), 10 * DAY);
        assert!(!rotation_due(&due, 0), "saturating_sub must not wrap into a false due");
    }
}
