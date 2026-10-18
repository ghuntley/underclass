use crate::models::{BackendId, ModelInfo};
use crate::proxy::AppState;
use std::collections::BTreeMap;
use std::sync::Arc;

pub async fn refresh_codex(state: &Arc<AppState>) -> (usize, Vec<String>) {
    let accounts = state
        .store
        .list_accounts()
        .into_iter()
        .filter(|account| account.backend == BackendId::Codex)
        .collect::<Vec<_>>();
    let mut merged = BTreeMap::<String, ModelInfo>::new();
    let mut refreshed = 0usize;
    let mut errors = Vec::new();
    for account in accounts {
        let token = match state.tokens.access_token(&account).await {
            Ok(token) => token,
            Err(error) => {
                errors.push(format!("{}: token unavailable: {error}" , account.label));
                continue;
            }
        };
        match crate::codex::fetch_catalog(&state.client, &account, &token).await {
            Ok(models) => {
                refreshed += 1;
                for model in models {
                    merged.entry(model.id.clone()).or_insert(model);
                }
            }
            Err(error) => errors.push(format!("{}: {error}", account.label)),
        }
    }
    if !merged.is_empty() {
        let models = merged.into_values().collect::<Vec<_>>();
        state.store.set_catalog(BackendId::Codex, &models);
        state.pool.lock().unwrap().set_catalog(
            BackendId::Codex,
            models.iter().map(|model| model.id.clone()).collect(),
        );
    }
    (refreshed, errors)
}

pub async fn refresh_all(state: &Arc<AppState>) -> serde_json::Value {
    let (codex_refreshed, mut errors) = refresh_codex(state).await;
    let copilot_accounts = state
        .store
        .list_accounts()
        .into_iter()
        .filter(|account| account.backend == BackendId::Copilot)
        .collect::<Vec<_>>();
    let mut copilot_refreshed = 0usize;
    for account in copilot_accounts {
        let Some(token) = account.refresh_token.clone() else { continue };
        match crate::copilot::fetch_catalog(&state.client, &account, &token).await {
            Ok(models) if !models.is_empty() => {
                state.store.set_catalog(BackendId::Copilot, &models);
                state.pool.lock().unwrap().set_catalog(
                    BackendId::Copilot,
                    models.iter().map(|model| model.id.clone()).collect(),
                );
                copilot_refreshed += 1;
            }
            Ok(_) => errors.push(format!("{}: empty catalog", account.label)),
            Err(error) => errors.push(format!("{}: {error}", account.label)),
        }
    }
    serde_json::json!({
        "codex_refreshed_accounts": codex_refreshed,
        "copilot_refreshed_accounts": copilot_refreshed,
        "errors": errors,
    })
}
