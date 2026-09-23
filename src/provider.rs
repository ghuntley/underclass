use crate::models::{Account, BackendId, Outcome};
use http::HeaderMap;
use serde_json::Value;

pub trait Backend: Send + Sync {
    fn id(&self) -> BackendId;

    fn default_cooldown_ms(&self) -> i64;

    fn rewrite_url(&self, path: &str, account: &Account) -> String;

    fn inject_headers(
        &self,
        account: &Account,
        token: &str,
        sticky: Option<&str>,
        body: &Value,
        headers: &mut HeaderMap,
    );

    fn prepare_body(&self, _body: &mut Value) {}

    fn auto_stream_usage(&self, _path: &str, _body: &mut Value) -> bool {
        false
    }

    fn usage_is_chat(&self, path: &str) -> bool {
        path.ends_with("/chat/completions")
    }

    fn classify(&self, status: u16, _body: &str, headers: &HeaderMap, now_ms: i64) -> Outcome {
        crate::health::classify(status, headers, self.default_cooldown_ms(), now_ms)
    }
}

pub type BackendMap = std::collections::HashMap<BackendId, std::sync::Arc<dyn Backend>>;
