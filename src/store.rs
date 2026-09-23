use crate::models::{Account, AccountStatus, BackendId, Binding, ModelInfo};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;
use std::sync::Mutex;

pub struct Store {
    conn: Mutex<Connection>,
}

fn schema() -> &'static str {
    r#"
    CREATE TABLE IF NOT EXISTS accounts (
        id TEXT PRIMARY KEY,
        backend TEXT NOT NULL,
        label TEXT NOT NULL,
        refresh_token TEXT,
        access_token TEXT,
        expires_at INTEGER NOT NULL DEFAULT 0,
        account_id TEXT,
        residency TEXT,
        enterprise_url TEXT,
        status TEXT NOT NULL DEFAULT 'healthy',
        reset_at INTEGER NOT NULL DEFAULT 0,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS bindings (
        cache_key TEXT PRIMARY KEY,
        account_id TEXT NOT NULL,
        backend TEXT NOT NULL,
        bound_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS catalog (
        backend TEXT NOT NULL,
        model_id TEXT NOT NULL,
        name TEXT NOT NULL DEFAULT '',
        context INTEGER NOT NULL DEFAULT 0,
        input INTEGER NOT NULL DEFAULT 0,
        output INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (backend, model_id)
    );
    CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS reset_attempts (
        account_id TEXT PRIMARY KEY,
        credit_id TEXT NOT NULL,
        request_id TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT 'pending',
        started_at INTEGER NOT NULL
    );
    "#
}

impl Store {
    pub fn open(path: &Path) -> rusqlite::Result<Self> {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let conn = Connection::open(path)?;
        conn.execute_batch(schema())?;
        Ok(Self { conn: Mutex::new(conn) })
    }

    #[allow(dead_code)]
    pub fn in_memory() -> rusqlite::Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(schema())?;
        Ok(Self { conn: Mutex::new(conn) })
    }

    pub fn list_accounts(&self) -> Vec<Account> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT id, backend, label, refresh_token, access_token, expires_at, account_id,
                        residency, enterprise_url, status, reset_at, created_at, updated_at
                 FROM accounts ORDER BY created_at",
            )
            .expect("prepare accounts");
        let rows = stmt
            .query_map([], |row| {
                let backend: String = row.get(1)?;
                let status: String = row.get(9)?;
                Ok(Account {
                    id: row.get(0)?,
                    backend: BackendId::parse(&backend).unwrap_or(BackendId::Codex),
                    label: row.get(2)?,
                    refresh_token: row.get(3)?,
                    access_token: row.get(4)?,
                    expires_at: row.get(5)?,
                    account_id: row.get(6)?,
                    residency: row.get(7)?,
                    enterprise_url: row.get(8)?,
                    status: parse_status(&status),
                    reset_at: row.get(10)?,
                    created_at: row.get(11)?,
                    updated_at: row.get(12)?,
                })
            })
            .expect("query accounts");
        rows.filter_map(Result::ok).collect()
    }

    pub fn get_account(&self, id: &str) -> Option<Account> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT id, backend, label, refresh_token, access_token, expires_at, account_id,
                        residency, enterprise_url, status, reset_at, created_at, updated_at
                 FROM accounts WHERE id = ?1",
            )
            .expect("prepare account");
        stmt.query_row(params![id], |row| {
            let backend: String = row.get(1)?;
            let status: String = row.get(9)?;
            Ok(Account {
                id: row.get(0)?,
                backend: BackendId::parse(&backend).unwrap_or(BackendId::Codex),
                label: row.get(2)?,
                refresh_token: row.get(3)?,
                access_token: row.get(4)?,
                expires_at: row.get(5)?,
                account_id: row.get(6)?,
                residency: row.get(7)?,
                enterprise_url: row.get(8)?,
                status: parse_status(&status),
                reset_at: row.get(10)?,
                created_at: row.get(11)?,
                updated_at: row.get(12)?,
            })
        })
        .optional()
        .expect("query account")
    }

    pub fn upsert_account(&self, a: &Account) {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO accounts (id, backend, label, refresh_token, access_token, expires_at, account_id,
                                   residency, enterprise_url, status, reset_at, created_at, updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)
             ON CONFLICT(id) DO UPDATE SET
                label=?3, refresh_token=?4, access_token=?5, expires_at=?6, account_id=?7,
                residency=?8, enterprise_url=?9, status=?10, reset_at=?11, updated_at=?13",
            params![
                a.id,
                a.backend.as_str(),
                a.label,
                a.refresh_token,
                a.access_token,
                a.expires_at,
                a.account_id,
                a.residency,
                a.enterprise_url,
                status_str(a.status),
                a.reset_at,
                a.created_at,
                a.updated_at,
            ],
        )
        .expect("upsert account");
    }

    pub fn delete_account(&self, id: &str) {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM accounts WHERE id = ?1", params![id])
            .expect("delete account");
        conn.execute("DELETE FROM bindings WHERE account_id = ?1", params![id])
            .expect("delete bindings of account");
        conn.execute("DELETE FROM reset_attempts WHERE account_id = ?1", params![id])
            .expect("delete reset attempt of account");
    }

    /// @cc [owner:ghuntley,label:persistence] reset-attempt-idempotency
    /// A pending reset MUST retain the same request and credit IDs until the upstream outcome is
    /// known, so a retry after a transport failure or restart cannot consume a second credit.
    pub fn reset_attempt(&self, account_id: &str) -> Option<(String, String, String)> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT credit_id, request_id, state FROM reset_attempts WHERE account_id = ?1",
            params![account_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .expect("query reset attempt")
    }

    pub fn save_reset_attempt(&self, account_id: &str, credit_id: &str, request_id: &str) {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO reset_attempts (account_id, credit_id, request_id, started_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![account_id, credit_id, request_id, crate::models::now_ms()],
        )
        .expect("save reset attempt");
    }

    pub fn clear_reset_attempt(&self, account_id: &str) {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM reset_attempts WHERE account_id = ?1", params![account_id])
            .expect("clear reset attempt");
    }

    pub fn finish_reset_attempt(&self, account_id: &str) {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE reset_attempts SET state = 'completed' WHERE account_id = ?1",
            params![account_id],
        )
        .expect("finish reset attempt");
    }

    pub fn update_account_status(&self, id: &str, status: AccountStatus, reset_at: i64) {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE accounts SET status = ?2, reset_at = ?3, updated_at = ?4 WHERE id = ?1",
            params![id, status_str(status), reset_at, crate::models::now_ms()],
        )
        .expect("update status");
    }

    /// @cc [owner:ghuntley,label:persistence] token-rotation-write-through
    /// `update_tokens` MUST persist the new access token, expiry, and (when present) rotated
    /// refresh token, ChatGPT account id, and residency immediately; a `None` refresh token MUST
    /// leave the previously stored one intact.
    pub fn update_tokens(
        &self,
        id: &str,
        refresh_token: Option<&str>,
        access_token: Option<&str>,
        expires_at: i64,
        chatgpt_account_id: Option<&str>,
        residency: Option<&str>,
    ) {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE accounts SET refresh_token = COALESCE(?2, refresh_token),
                    access_token = ?3, expires_at = ?4,
                    account_id = COALESCE(?5, account_id),
                    residency = COALESCE(?6, residency),
                    updated_at = ?7
             WHERE id = ?1",
            params![id, refresh_token, access_token, expires_at, chatgpt_account_id, residency, crate::models::now_ms()],
        )
        .expect("update tokens");
    }

    pub fn list_bindings(&self) -> Vec<Binding> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT cache_key, account_id, backend, bound_at FROM bindings")
            .expect("prepare bindings");
        let rows = stmt
            .query_map([], |row| {
                let backend: String = row.get(2)?;
                Ok(Binding {
                    cache_key: row.get(0)?,
                    account_id: row.get(1)?,
                    backend: BackendId::parse(&backend).unwrap_or(BackendId::Codex),
                    bound_at: row.get(3)?,
                })
            })
            .expect("query bindings");
        rows.filter_map(Result::ok).collect()
    }

    pub fn upsert_binding(&self, b: &Binding) {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO bindings (cache_key, account_id, backend, bound_at) VALUES (?1,?2,?3,?4)
             ON CONFLICT(cache_key) DO UPDATE SET account_id=?2, backend=?3, bound_at=?4",
            params![b.cache_key, b.account_id, b.backend.as_str(), b.bound_at],
        )
        .expect("upsert binding");
    }

    /// @cc [owner:ghuntley,label:storage] binding-storage-bounded
    /// `prune_bindings` MUST delete bindings older than `ttl_ms` and then retain at most `cap`
    /// newest rows. It MUST perform both deletions in one transaction.
    pub fn prune_bindings(&self, now_ms: i64, ttl_ms: i64, cap: usize) {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().expect("binding prune tx");
        tx.execute(
            "DELETE FROM bindings WHERE bound_at < ?1",
            params![now_ms.saturating_sub(ttl_ms)],
        )
        .expect("delete expired bindings");
        tx.execute(
            "DELETE FROM bindings WHERE cache_key IN (
               SELECT cache_key FROM bindings ORDER BY bound_at DESC, cache_key DESC
               LIMIT -1 OFFSET ?1
             )",
            params![i64::try_from(cap).unwrap_or(i64::MAX)],
        )
        .expect("cap bindings");
        tx.commit().expect("commit binding prune");
    }

    pub fn catalog(&self, backend: BackendId) -> Vec<ModelInfo> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT model_id, name, context, input, output FROM catalog
                 WHERE backend = ?1 ORDER BY model_id",
            )
            .expect("prepare catalog");
        let rows = stmt
            .query_map(params![backend.as_str()], |row| {
                Ok(ModelInfo {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    context: row.get(2)?,
                    input: row.get(3)?,
                    output: row.get(4)?,
                })
            })
            .expect("query catalog");
        rows.filter_map(Result::ok).collect()
    }

    pub fn set_catalog(&self, backend: BackendId, models: &[ModelInfo]) {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().expect("catalog tx");
        tx.execute("DELETE FROM catalog WHERE backend = ?1", params![backend.as_str()])
            .expect("clear catalog");
        for m in models {
            tx.execute(
                "INSERT INTO catalog (backend, model_id, name, context, input, output) VALUES (?1,?2,?3,?4,?5,?6)",
                params![backend.as_str(), m.id, m.name, m.context, m.input, m.output],
            )
            .expect("insert catalog row");
        }
        tx.commit().expect("catalog commit");
    }

    pub fn config_get(&self, key: &str) -> Option<String> {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT value FROM config WHERE key = ?1", params![key], |r| r.get(0))
            .optional()
            .expect("config get")
    }

    pub fn config_set(&self, key: &str, value: &str) {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO config (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = ?2",
            params![key, value],
        )
        .expect("config set");
    }
}

fn parse_status(s: &str) -> AccountStatus {
    match s {
        "cooling" => AccountStatus::Cooling,
        "auth_error" => AccountStatus::AuthError,
        "disabled" => AccountStatus::Disabled,
        _ => AccountStatus::Healthy,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(key: &str, bound_at: i64) -> Binding {
        Binding {
            cache_key: key.into(),
            account_id: "account".into(),
            backend: BackendId::Codex,
            bound_at,
        }
    }

    #[test]
    fn pruning_removes_expired_rows_and_caps_newest_rows() {
        let store = Store::in_memory().unwrap();
        for row in [binding("expired", 1), binding("old", 100), binding("new", 200)] {
            store.upsert_binding(&row);
        }

        store.prune_bindings(250, 200, 1);

        let rows = store.list_bindings();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].cache_key, "new");
    }
}

fn status_str(s: AccountStatus) -> &'static str {
    match s {
        AccountStatus::Healthy => "healthy",
        AccountStatus::Cooling => "cooling",
        AccountStatus::AuthError => "auth_error",
        AccountStatus::Disabled => "disabled",
    }
}
