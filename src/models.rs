use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackendId {
    Codex,
    Copilot,
}

impl BackendId {
    pub fn as_str(self) -> &'static str {
        match self {
            BackendId::Codex => "codex",
            BackendId::Copilot => "copilot",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "codex" => Some(BackendId::Codex),
            "copilot" => Some(BackendId::Copilot),
            _ => None,
        }
    }

    pub fn all() -> [BackendId; 2] {
        [BackendId::Codex, BackendId::Copilot]
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountStatus {
    Healthy,
    Cooling,
    AuthError,
    Disabled,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Account {
    pub id: String,
    pub backend: BackendId,
    pub label: String,
    #[serde(default)]
    pub refresh_token: Option<String>,
    #[serde(default)]
    pub access_token: Option<String>,
    #[serde(default)]
    pub expires_at: i64,
    /// Wall-clock time of the last successful token refresh, used to schedule proactive
    /// refresh-token rotation. `0` means the token was never rotated by underclass.
    #[serde(default)]
    pub token_refreshed_at: i64,
    #[serde(default)]
    pub account_id: Option<String>,
    #[serde(default)]
    pub residency: Option<String>,
    #[serde(default)]
    pub enterprise_url: Option<String>,
    pub status: AccountStatus,
    #[serde(default)]
    pub reset_at: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

impl Account {
    pub fn healthy(&self) -> bool {
        self.status == AccountStatus::Healthy
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModelInfo {
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub context: i64,
    #[serde(default)]
    pub input: i64,
    #[serde(default)]
    pub output: i64,
}

#[derive(Clone, Debug)]
pub struct Binding {
    pub cache_key: String,
    pub account_id: String,
    pub backend: BackendId,
    pub bound_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Ok,
    QuotaExhausted { until_ms: i64 },
    AuthFailed,
    Transient,
}

#[derive(Clone, Debug, Serialize)]
pub struct RequestLogEntry {
    pub ts: i64,
    pub request_id: String,
    pub model: String,
    pub backend: Option<String>,
    pub account: Option<String>,
    pub label: Option<String>,
    pub sticky: String,
    pub status: u16,
    pub duration_ms: u64,
}

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

pub fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}
