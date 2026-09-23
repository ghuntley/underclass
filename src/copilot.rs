use crate::flows::{FlowRegistry, FlowState};
use crate::models::{now_ms, Account, BackendId, ModelInfo};
use crate::provider::Backend;
use crate::store::Store;
use http::HeaderMap;
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

pub const CLIENT_ID: &str = "Ov23li8tweQw6odWQebz";
pub const API_VERSION: &str = "2026-06-01";
pub const DEFAULT_API_BASE: &str = "https://api.githubcopilot.com";
pub const USER_AGENT: &str = concat!("opencode/", env!("CARGO_PKG_VERSION"), " (underclass-pool-proxy)");

#[derive(Debug, Deserialize)]
pub struct DeviceCodeInit {
    pub verification_uri: String,
    pub user_code: String,
    pub device_code: String,
    pub interval: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct DeviceCodePoll {
    pub access_token: Option<String>,
    pub error: Option<String>,
    pub interval: Option<u64>,
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("unexpected status {0}")]
    Status(u16),
    #[error("authorization failed: {0}")]
    Failed(String),
    #[error("pending")]
    Pending { interval: Option<u64> },
    #[error("slow down")]
    SlowDown { interval: Option<u64> },
}

/// @cc [owner:ghuntley,label:proxy] copilot-enterprise-url-rewrite
/// Copilot requests for an account with an `enterprise_url` MUST target
/// `https://copilot-api.<domain>`; all others MUST target `https://api.githubcopilot.com`.
fn api_base(account: &Account) -> String {
    match &account.enterprise_url {
        Some(url) => {
            let domain = url
                .trim_start_matches("https://")
                .trim_start_matches("http://")
                .trim_end_matches('/');
            format!("https://copilot-api.{domain}")
        }
        None => std::env::var("UNDERCLASS_COPILOT_UPSTREAM")
            .unwrap_or_else(|_| DEFAULT_API_BASE.to_string()),
    }
}

pub fn normalize_domain(url: &str) -> String {
    url.trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_end_matches('/')
        .to_string()
}

async fn init_device_code(client: &Client, domain: &str) -> Result<DeviceCodeInit, AuthError> {
    let resp = client
        .post(format!("https://{domain}/login/device/code"))
        .header("Accept", "application/json")
        .header("User-Agent", USER_AGENT)
        .json(&serde_json::json!({ "client_id": CLIENT_ID, "scope": "read:user" }))
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(AuthError::Status(resp.status().as_u16()));
    }
    Ok(resp.json().await?)
}

async fn poll_access_token(
    client: &Client,
    domain: &str,
    init: &DeviceCodeInit,
) -> Result<DeviceCodePoll, AuthError> {
    let resp = client
        .post(format!("https://{domain}/login/oauth/access_token"))
        .header("Accept", "application/json")
        .header("User-Agent", USER_AGENT)
        .json(&serde_json::json!({
            "client_id": CLIENT_ID,
            "device_code": init.device_code,
            "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
        }))
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(AuthError::Status(resp.status().as_u16()));
    }
    let poll: DeviceCodePoll = resp.json().await?;
    match poll.error.as_deref() {
        Some("authorization_pending") => Err(AuthError::Pending { interval: poll.interval }),
        Some("slow_down") => Err(AuthError::SlowDown { interval: poll.interval }),
        Some(other) => Err(AuthError::Failed(other.to_string())),
        None => {
            if poll.access_token.is_some() {
                Ok(poll)
            } else {
                Err(AuthError::Failed("no access token in response".into()))
            }
        }
    }
}

pub async fn fetch_catalog(client: &Client, account: &Account, token: &str) -> Result<Vec<ModelInfo>, AuthError> {
    let url = format!("{}/models", api_base(account));
    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {token}"))
        .header("X-GitHub-Api-Version", API_VERSION)
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", USER_AGENT)
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(AuthError::Status(resp.status().as_u16()));
    }
    let body: Value = resp.json().await?;
    Ok(parse_models_response(&body))
}

/// @cc [owner:ghuntley,label:pool] copilot-catalog-usability-filters
/// `parse_models_response` MUST include a model only when its `policy.state` is not `disabled`,
/// its `capabilities.limits` declare both `max_output_tokens` and `max_prompt_tokens`, and its
/// capabilities report `tool_calls`; context MUST fall back to `max_prompt_tokens` when
/// `max_context_window_tokens` is absent. Malformed entries MUST be skipped, never panic.
pub fn parse_models_response(body: &Value) -> Vec<ModelInfo> {
    let Some(items) = body.get("data").and_then(|d| d.as_array()) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for item in items {
        let id = item.get("id").and_then(|v| v.as_str()).unwrap_or_default();
        if id.is_empty() {
            continue;
        }
        if item
            .get("policy")
            .and_then(|p| p.get("state"))
            .and_then(|v| v.as_str())
            .is_some_and(|s| s == "disabled")
        {
            continue;
        }
        let limits = item
            .get("capabilities")
            .and_then(|c| c.get("limits"));
        let Some(limits) = limits else { continue };
        let max_output = limits.get("max_output_tokens").and_then(|v| v.as_i64());
        let max_prompt = limits.get("max_prompt_tokens").and_then(|v| v.as_i64());
        let (Some(max_output), Some(max_prompt)) = (max_output, max_prompt) else {
            continue;
        };
        if item
            .get("capabilities")
            .and_then(|c| c.get("supports"))
            .and_then(|s| s.get("tool_calls"))
            .and_then(|v| v.as_bool())
            .is_none()
        {
            continue;
        }
        let context = limits
            .get("max_context_window_tokens")
            .and_then(|v| v.as_i64())
            .unwrap_or(max_prompt);
        out.push(ModelInfo {
            id: id.to_string(),
            name: item
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or(id)
                .to_string(),
            context,
            input: max_prompt,
            output: max_output,
        });
    }
    out.sort_by(|a, b| a.id.cmp(&b.id));
    out
}

pub fn parse_github_user(body: &Value) -> Option<String> {
    body.get("login")
        .and_then(|v| v.as_str())
        .map(String::from)
        .or_else(|| {
            body.get("name")
                .and_then(|v| v.as_str())
                .map(String::from)
        })
        .filter(|s| !s.is_empty())
}

pub async fn fetch_github_identity(client: &Client, token: &str) -> Option<String> {
    let resp = client
        .get("https://api.github.com/user")
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", USER_AGENT)
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let body: Value = resp.json().await.ok()?;
    parse_github_user(&body)
}

/// @cc [owner:ghuntley,label:identity] copilot-onboarding-identity-label
/// A completed Copilot device flow MUST create (or replace the tokens of) an account persisted to
/// the store, labeled with the GitHub `login` (or display name) from `api.github.com/user`,
/// falling back to the enterprise URL or `github` when identity is unavailable.
pub async fn start_flow(
    client: Client,
    store: Arc<Store>,
    flows: FlowRegistry,
    replace_account: Option<String>,
    enterprise_url: Option<String>,
) -> Result<String, AuthError> {
    let domain = normalize_domain(enterprise_url.as_deref().unwrap_or("github.com"));
    let init = init_device_code(&client, &domain).await?;
    let interval = init.interval.unwrap_or(5).max(1);
    let flow_id = flows.create(
        BackendId::Copilot,
        FlowState::Pending {
            user_code: init.user_code.clone(),
            verify_url: init.verification_uri.clone(),
        },
        replace_account,
    );

    let flows = flows.clone();
    let store = store.clone();
    let flow_id_out = flow_id.clone();
    tokio::spawn(async move {
        let mut current_interval = interval;
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(current_interval * 1000 + 3000)).await;
            match poll_access_token(&client, &domain, &init).await {
                Err(AuthError::Pending { interval }) => {
                    current_interval = interval.unwrap_or(current_interval);
                    continue;
                }
                Err(AuthError::SlowDown { interval }) => {
                    current_interval = interval.unwrap_or(current_interval + 5);
                    continue;
                }
                Err(e) => {
                    flows.set_state(&flow_id, FlowState::Failed { message: e.to_string() });
                    return;
                }
                Ok(poll) => {
                    let token = poll.access_token.unwrap_or_default();
                    let ts = now_ms();
                    let account = match flows.get(&flow_id).and_then(|f| f.replace_account) {
                        Some(existing_id) => {
                            if let Some(mut account) = store.get_account(&existing_id) {
                                account.refresh_token = Some(token.clone());
                                account.status = crate::models::AccountStatus::Healthy;
                                account.reset_at = 0;
                                account.updated_at = ts;
                                store.upsert_account(&account);
                                account
                            } else {
                                flows.set_state(&flow_id, FlowState::Failed {
                                    message: "account being replaced no longer exists".into(),
                                });
                                return;
                            }
                        }
                        None => {
                            let identity = fetch_github_identity(&client, &token).await;
                            let mut account = crate::flows::new_account(
                                BackendId::Copilot,
                                identity.unwrap_or_else(|| {
                                    enterprise_url.clone().unwrap_or_else(|| "github".into())
                                }),
                            );
                            account.enterprise_url = enterprise_url.clone();
                            account.refresh_token = Some(token.clone());
                            store.upsert_account(&account);
                            account
                        }
                    };
                    if let Ok(catalog) = fetch_catalog(&client, &account, &token).await {
                        if !catalog.is_empty() {
                            store.set_catalog(BackendId::Copilot, &catalog);
                        }
                    }
                    flows.set_state(&flow_id, FlowState::Authorized {
                        account_id: account.id,
                    });
                    return;
                }
            }
        }
    });
    Ok(flow_id_out)
}

fn body_has_image(v: &Value) -> bool {
    match v {
        Value::Object(map) => {
            if map
                .get("type")
                .and_then(|t| t.as_str())
                .is_some_and(|t| matches!(t, "image_url" | "input_image" | "image"))
            {
                return true;
            }
            map.values().any(body_has_image)
        }
        Value::Array(items) => items.iter().any(body_has_image),
        _ => false,
    }
}

pub struct CopilotBackend {
    pub cooldown_ms: i64,
}

impl CopilotBackend {
    fn base_for(&self, account: &Account) -> String {
        api_base(account)
    }
}

impl Backend for CopilotBackend {
    /// @cc [owner:ghuntley,label:accounting] copilot-stream-usage
    /// For streamed chat completions, request final usage only when the client has not supplied
    /// `stream_options.include_usage`; explicit client settings MUST be preserved.
    fn auto_stream_usage(&self, path: &str, body: &mut Value) -> bool {
        if !path.ends_with("/chat/completions") || body.get("stream").and_then(Value::as_bool) != Some(true) { return false; }
        let Some(obj) = body.as_object_mut() else { return false; };
        if let Some(options) = obj.get_mut("stream_options") {
            let Some(options) = options.as_object_mut() else { return false; };
            if options.contains_key("include_usage") { return false; }
            options.insert("include_usage".into(), Value::Bool(true));
        } else {
            obj.insert("stream_options".into(), serde_json::json!({"include_usage": true}));
        }
        true
    }

    fn id(&self) -> BackendId {
        BackendId::Copilot
    }

    fn default_cooldown_ms(&self) -> i64 {
        self.cooldown_ms
    }

    fn rewrite_url(&self, path: &str, account: &Account) -> String {
        format!("{}{}", self.base_for(account), path)
    }

    /// @cc [owner:ghuntley,label:proxy] copilot-vision-header
    /// Outbound Copilot requests MUST carry `Copilot-Vision-Request: true` exactly when the
    /// request body contains image parts (`image_url`, `input_image`, or `image` typed objects),
    /// and MUST NOT carry it for text-only bodies.
    fn inject_headers(
        &self,
        account: &Account,
        token: &str,
        sticky: Option<&str>,
        body: &Value,
        headers: &mut HeaderMap,
    ) {
        headers.insert(
            http::header::AUTHORIZATION,
            http::HeaderValue::from_str(&format!("Bearer {token}")).expect("bearer"),
        );
        headers.insert("X-GitHub-Api-Version", http::HeaderValue::from_static(API_VERSION));
        if let Ok(v) = http::HeaderValue::from_str(USER_AGENT) {
            headers.insert(http::header::USER_AGENT, v);
        }
        headers.insert("x-initiator", http::HeaderValue::from_static("user"));
        headers.insert(
            "Openai-Intent",
            http::HeaderValue::from_static("conversation-edits"),
        );
        if let Some(sticky) = sticky {
            if let Ok(v) = http::HeaderValue::from_str(sticky) {
                headers.insert("X-Interaction-Id", v);
            }
        }
        if body_has_image(body) {
            headers.insert(
                "Copilot-Vision-Request",
                http::HeaderValue::from_static("true"),
            );
        }
        let _ = account;
    }
}

pub fn fallback_catalog() -> Vec<ModelInfo> {
    let entries: &[(&str, &str, i64)] = &[
        ("gpt-4.1", "GPT-4.1", 1_000_000),
        ("gpt-5.4", "GPT-5.4", 400_000),
        ("gpt-5.4-mini", "GPT-5.4 Mini", 400_000),
        ("gpt-5.4-nano", "GPT-5.4 Nano", 400_000),
        ("gpt-5.5", "GPT-5.5", 400_000),
    ];
    entries
        .iter()
        .map(|(id, name, context)| ModelInfo {
            id: id.to_string(),
            name: name.to_string(),
            context: *context,
            input: *context,
            output: 128_000,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn stream_usage_preserves_explicit_client_choice() {
        let backend = CopilotBackend { cooldown_ms: 1000 };
        let mut body = json!({"stream": true, "stream_options": {"include_usage": false, "other": 1}});
        assert!(!backend.auto_stream_usage("/v1/chat/completions", &mut body));
        assert_eq!(body["stream_options"], json!({"include_usage": false, "other": 1}));
        let mut responses = json!({"stream": true});
        assert!(!backend.auto_stream_usage("/v1/responses", &mut responses));
        assert!(responses.get("stream_options").is_none());
    }

    #[test]
    fn enterprise_urls_rewrite_to_copilot_api_host() {
        let backend = CopilotBackend { cooldown_ms: 1000 };
        let mut account = crate::flows::new_account(BackendId::Copilot, "x".into());
        assert_eq!(
            backend.rewrite_url("/v1/responses", &account),
            "https://api.githubcopilot.com/v1/responses"
        );
        account.enterprise_url = Some("https://company.ghe.com".into());
        assert_eq!(
            backend.rewrite_url("/v1/chat/completions", &account),
            "https://copilot-api.company.ghe.com/v1/chat/completions"
        );
        account.enterprise_url = Some("company.ghe.com".into());
        assert_eq!(
            backend.rewrite_url("/v1/responses", &account),
            "https://copilot-api.company.ghe.com/v1/responses"
        );
    }

    #[test]
    fn parses_models_response_and_applies_usability_filters() {
        let body = json!({
            "data": [
                {
                    "id": "gpt-5.5",
                    "name": "GPT-5.5",
                    "model_picker_enabled": true,
                    "capabilities": {
                        "supports": { "tool_calls": true },
                        "limits": {
                            "max_context_window_tokens": 400000,
                            "max_prompt_tokens": 272000,
                            "max_output_tokens": 128000
                        }
                    }
                },
                {
                    "id": "disabled-model",
                    "name": "Nope",
                    "policy": { "state": "disabled" },
                    "capabilities": { "supports": { "tool_calls": true }, "limits": {} }
                },
                {
                    "id": "no-limits",
                    "name": "Nope",
                    "capabilities": { "supports": { "tool_calls": true } }
                },
                {
                    "id": "gpt-4.1",
                    "name": "GPT-4.1",
                    "capabilities": {
                        "supports": { "tool_calls": true },
                        "limits": { "max_prompt_tokens": 1000000, "max_output_tokens": 32000 }
                    }
                }
            ]
        });
        let models = parse_models_response(&body);
        let ids: Vec<&str> = models.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, vec!["gpt-4.1", "gpt-5.5"]);
        let gpt55 = models.iter().find(|m| m.id == "gpt-5.5").unwrap();
        assert_eq!(gpt55.context, 400_000);
        assert_eq!(gpt55.input, 272_000);
        assert_eq!(gpt55.output, 128_000);
        let gpt41 = models.iter().find(|m| m.id == "gpt-4.1").unwrap();
        assert_eq!(gpt41.context, 1_000_000);
    }

    #[test]
    fn vision_detection_finds_image_parts() {
        let chat = json!({"messages": [
            {"role": "user", "content": [
                {"type": "text", "text": "look"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,xx"}}
            ]}
        ]});
        assert!(body_has_image(&chat));
        let responses = json!({"input": [
            {"role": "user", "content": [{"type": "input_image", "url": "x"}]}
        ]});
        assert!(body_has_image(&responses));
        let plain = json!({"messages": [{"role": "user", "content": "just text"}]});
        assert!(!body_has_image(&plain));
        let text_only = json!({"messages": [{"role": "user", "content": "the string image_url here"}]});
        assert!(!body_has_image(&text_only));
    }
}
