use crate::models::{new_id, now_ms, Account, AccountStatus, BackendId};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum FlowState {
    Pending {
        user_code: String,
        verify_url: String,
    },
    Authorized {
        account_id: String,
    },
    Failed {
        message: String,
    },
}

#[derive(Clone, Debug, Serialize)]
pub struct Flow {
    pub backend: BackendId,
    pub state: FlowState,
    pub replace_account: Option<String>,
}

#[derive(Clone, Default)]
pub struct FlowRegistry {
    inner: Arc<Mutex<HashMap<String, Flow>>>,
}

impl FlowRegistry {
    pub fn create(&self, backend: BackendId, state: FlowState, replace_account: Option<String>) -> String {
        let id = new_id();
        self.inner.lock().unwrap().insert(
            id.clone(),
            Flow {
                backend,
                state,
                replace_account,
            },
        );
        id
    }

    pub fn get(&self, id: &str) -> Option<Flow> {
        self.inner.lock().unwrap().get(id).cloned()
    }

    pub fn set_state(&self, id: &str, state: FlowState) {
        if let Some(flow) = self.inner.lock().unwrap().get_mut(id) {
            flow.state = state;
        }
    }
}

pub fn new_account(backend: BackendId, label: String) -> Account {
    let ts = now_ms();
    Account {
        id: new_id(),
        backend,
        label,
        refresh_token: None,
        access_token: None,
        expires_at: 0,
        token_refreshed_at: 0,
        account_id: None,
        residency: None,
        enterprise_url: None,
        status: AccountStatus::Healthy,
        reset_at: 0,
        created_at: ts,
        updated_at: ts,
    }
}
