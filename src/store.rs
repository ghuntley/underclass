use crate::models::{Account, AccountStatus, BackendId, Binding, ModelInfo};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;
use std::sync::Mutex;
use serde::Serialize;
use rusqlite::types::Value;

#[derive(Clone, Debug, Serialize)]
pub struct UsageRecord {
    pub id: i64,
    pub request_id: String,
    pub ts: i64,
    pub endpoint: String,
    pub backend: String,
    pub model: String,
    pub account_id: String,
    pub account_label: String,
    pub cache_key: Option<String>,
    pub status: u16,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
pub struct UsageQuery {
    pub from_ms: Option<i64>,
    pub to_ms: Option<i64>,
    pub model: Option<String>,
    pub account_id: Option<String>,
    pub cache_key: Option<String>,
    pub missing_key: Option<bool>,
    pub group_by: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

impl UsageQuery {
    fn where_sql(&self) -> (String, Vec<Value>) {
        let mut predicates = vec!["1=1".to_string()];
        let mut values = Vec::new();
        for (column, value) in [
            ("ts >=", self.from_ms.map(Value::Integer)),
            ("ts <", self.to_ms.map(Value::Integer)),
            ("model =", self.model.clone().map(Value::Text)),
            ("account_id =", self.account_id.clone().map(Value::Text)),
            ("cache_key =", self.cache_key.clone().map(Value::Text)),
        ] {
            if let Some(value) = value {
                predicates.push(format!("{column} ?"));
                values.push(value);
            }
        }
        if self.missing_key == Some(true) { predicates.push("cache_key IS NULL".into()); }
        (predicates.join(" AND "), values)
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct UsageSummary {
    pub model: Option<String>,
    pub account_id: Option<String>,
    pub account_label: Option<String>,
    pub cache_key: Option<String>,
    pub requests: i64,
    pub measured_requests: i64,
    pub unknown_requests: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
}

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
        token_refreshed_at INTEGER NOT NULL DEFAULT 0,
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
    CREATE TABLE IF NOT EXISTS usage_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        request_id TEXT NOT NULL,
        ts INTEGER NOT NULL,
        endpoint TEXT NOT NULL,
        backend TEXT NOT NULL,
        model TEXT NOT NULL,
        account_id TEXT NOT NULL,
        account_label TEXT NOT NULL,
        cache_key TEXT,
        status INTEGER NOT NULL,
        input_tokens INTEGER,
        output_tokens INTEGER
    );
    CREATE INDEX IF NOT EXISTS usage_records_ts ON usage_records(ts, id);
    CREATE INDEX IF NOT EXISTS usage_records_dims ON usage_records(model, account_id, cache_key, ts);
    "#
}

/// Adds columns introduced after the initial schema.
///
/// `CREATE TABLE IF NOT EXISTS` silently leaves an already-created table alone, so a new column
/// must be applied explicitly. `PRAGMA table_info` is consulted rather than matching on the
/// "duplicate column name" error text, so the check does not depend on SQLite's message wording.
fn migrate(conn: &Connection) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare("PRAGMA table_info(accounts)")?;
    let columns: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<_>>()?;
    if !columns.iter().any(|c| c == "token_refreshed_at") {
        conn.execute(
            "ALTER TABLE accounts ADD COLUMN token_refreshed_at INTEGER NOT NULL DEFAULT 0",
            [],
        )?;
    }
    Ok(())
}

impl Store {
    /// @cc [owner:ghuntley,label:accounting] monitor-bins-bounded
    /// `monitor_minute_bins` MUST count only upstream attempts in the trailing 60 complete-or-
    /// partial minute buckets and MUST return exactly 60 counts without exposing usage rows.
    pub fn monitor_minute_bins(&self, now_ms: i64) -> rusqlite::Result<Vec<i64>> {
        let current_minute = now_ms.div_euclid(60_000);
        let first_minute = current_minute - 59;
        let mut bins = vec![0; 60];
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT CASE WHEN ts >= 0 THEN ts / 60000 ELSE (ts - 59999) / 60000 END AS minute, COUNT(*) FROM usage_records \
             WHERE ts >= ?1 AND ts <= ?2 GROUP BY minute",
        )?;
        let rows = stmt.query_map(params![first_minute * 60_000, now_ms], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
        })?;
        for row in rows {
            let (minute, count) = row?;
            if let Ok(index) = usize::try_from(minute - first_minute)
                && index < bins.len()
            {
                bins[index] = count;
            }
        }
        Ok(bins)
    }

    /// @cc [owner:ghuntley,label:accounting] monitor-account-totals-complete
    /// `monitor_account_totals` MUST include every upstream attempt in `[from_ms, to_ms)` for
    /// every account without pagination, and MUST count missing token pairs as unknown.
    pub fn monitor_account_totals(&self, from_ms: i64, to_ms: i64) -> rusqlite::Result<Vec<UsageSummary>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT account_id, MAX(account_label), COUNT(*), \
             SUM(CASE WHEN input_tokens IS NULL OR output_tokens IS NULL THEN 1 ELSE 0 END), \
             COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0) \
             FROM usage_records WHERE ts >= ?1 AND ts < ?2 GROUP BY account_id"
        )?;
        let rows = stmt.query_map(params![from_ms, to_ms], |r| {
            let requests: i64 = r.get(2)?;
            let unknown: i64 = r.get(3)?;
            Ok(UsageSummary {
                model: None, account_id: r.get(0)?, account_label: r.get(1)?, cache_key: None,
                requests, measured_requests: requests - unknown, unknown_requests: unknown,
                input_tokens: r.get(4)?, output_tokens: r.get(5)?,
            })
        })?;
        rows.collect()
    }

    /// @cc [owner:ghuntley,label:persistence] usage-write-through
    /// Each upstream attempt MUST create one durable usage row, retaining its account label and
    /// unknown token counts as NULL even when the account is later removed.
    pub fn insert_usage(&self, record: &UsageRecord) -> rusqlite::Result<()> {
        self.conn.lock().unwrap().execute(
            "INSERT INTO usage_records (request_id,ts,endpoint,backend,model,account_id,account_label,cache_key,status,input_tokens,output_tokens) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
            params![record.request_id, record.ts, record.endpoint, record.backend, record.model, record.account_id, record.account_label, record.cache_key, record.status, record.input_tokens, record.output_tokens],
        )?;
        Ok(())
    }

    pub fn usage_summary(&self, query: &UsageQuery) -> rusqlite::Result<Vec<UsageSummary>> {
        let (filter, mut values) = query.where_sql();
        let requested: Vec<&str> = query.group_by.as_deref().unwrap_or("").split(',').filter(|s| !s.is_empty()).collect();
        let groups: Vec<&str> = ["model", "account_id", "cache_key"].into_iter().filter(|name| requested.contains(name)).collect();
        let select_dim = |name: &str| if groups.contains(&name) { name.to_string() } else { format!("NULL AS {name}") };
        let label = if groups.contains(&"account_id") { "MAX(account_label) AS account_label" } else { "NULL AS account_label" };
        let group_sql = if groups.is_empty() { String::new() } else { format!(" GROUP BY {}", groups.join(",")) };
        let pagination = if groups.is_empty() { String::new() } else {
            values.push(Value::Integer(query.limit.unwrap_or(100).clamp(1, 1000) as i64));
            values.push(Value::Integer(query.offset.unwrap_or(0).min(i64::MAX as usize) as i64));
            " LIMIT ? OFFSET ?".to_string()
        };
        let sql = format!("SELECT {},{},{},{},COUNT(*),COALESCE(SUM(CASE WHEN input_tokens IS NOT NULL AND output_tokens IS NOT NULL THEN 1 ELSE 0 END),0),COALESCE(SUM(CASE WHEN input_tokens IS NULL OR output_tokens IS NULL THEN 1 ELSE 0 END),0),COALESCE(SUM(input_tokens),0) AS input_tokens,COALESCE(SUM(output_tokens),0) AS output_tokens FROM usage_records WHERE {filter}{group_sql} ORDER BY input_tokens DESC,model,account_id,cache_key{pagination}", select_dim("model"), select_dim("account_id"), label, select_dim("cache_key"));
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(values), |r| Ok(UsageSummary {
            model: r.get(0)?, account_id: r.get(1)?, account_label: r.get(2)?, cache_key: r.get(3)?, requests: r.get(4)?, measured_requests: r.get(5)?, unknown_requests: r.get(6)?, input_tokens: r.get(7)?, output_tokens: r.get(8)?,
        }))?;
        rows.collect()
    }

    pub fn usage_records(&self, query: &UsageQuery) -> rusqlite::Result<Vec<UsageRecord>> {
        let (filter, mut values) = query.where_sql();
        let limit = query.limit.unwrap_or(100).clamp(1, 1000) as i64;
        let offset = query.offset.unwrap_or(0).min(i64::MAX as usize) as i64;
        values.push(Value::Integer(limit));
        values.push(Value::Integer(offset));
        let sql = format!("SELECT id,request_id,ts,endpoint,backend,model,account_id,account_label,cache_key,status,input_tokens,output_tokens FROM usage_records WHERE {filter} ORDER BY ts DESC,id DESC LIMIT ? OFFSET ?");
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(values), |r| Ok(UsageRecord {
            id: r.get(0)?, request_id: r.get(1)?, ts: r.get(2)?, endpoint: r.get(3)?, backend: r.get(4)?, model: r.get(5)?, account_id: r.get(6)?, account_label: r.get(7)?, cache_key: r.get(8)?, status: r.get(9)?, input_tokens: r.get(10)?, output_tokens: r.get(11)?,
        }))?;
        rows.collect()
    }
    pub fn open(path: &Path) -> rusqlite::Result<Self> {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let conn = Connection::open(path)?;
        conn.execute_batch(schema())?;
        migrate(&conn)?;
        Ok(Self { conn: Mutex::new(conn) })
    }

    #[allow(dead_code)]
    pub fn in_memory() -> rusqlite::Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(schema())?;
        migrate(&conn)?;
        Ok(Self { conn: Mutex::new(conn) })
    }

    pub fn list_accounts(&self) -> Vec<Account> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT id, backend, label, refresh_token, access_token, expires_at, account_id,
                        residency, enterprise_url, status, reset_at, created_at, updated_at,
                        token_refreshed_at
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
                    token_refreshed_at: row.get(13)?,
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
                        residency, enterprise_url, status, reset_at, created_at, updated_at,
                        token_refreshed_at
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
                token_refreshed_at: row.get(13)?,
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
                                   residency, enterprise_url, status, reset_at, created_at, updated_at,
                                   token_refreshed_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)
             ON CONFLICT(id) DO UPDATE SET
                label=?3, refresh_token=?4, access_token=?5, expires_at=?6, account_id=?7,
                residency=?8, enterprise_url=?9, status=?10, reset_at=?11, updated_at=?13,
                token_refreshed_at=?14",
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
                a.token_refreshed_at,
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
    /// `update_tokens` MUST persist the new access token, expiry, `token_refreshed_at`, and (when
    /// present) rotated refresh token, ChatGPT account id, and residency immediately; a `None`
    /// refresh token MUST leave the previously stored one intact. `token_refreshed_at` MUST be set
    /// to the same instant as `updated_at` so scheduled rotation measures from the write.
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
        let now = crate::models::now_ms();
        conn.execute(
            "UPDATE accounts SET refresh_token = COALESCE(?2, refresh_token),
                    access_token = ?3, expires_at = ?4,
                    account_id = COALESCE(?5, account_id),
                    residency = COALESCE(?6, residency),
                    token_refreshed_at = ?7,
                    updated_at = ?7
             WHERE id = ?1",
            params![id, refresh_token, access_token, expires_at, chatgpt_account_id, residency, now],
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
    use super::{Store, UsageQuery, UsageRecord};

    /// A store created before `token_refreshed_at` existed must gain the column on open, keep its
    /// rows, and default the new column to 0 so the account reads back rather than erroring.
    #[test]
    fn opening_a_legacy_store_adds_the_rotation_column_without_losing_rows() {
        let path = std::env::temp_dir().join(format!("underclass-legacy-{}.db", uuid::Uuid::new_v4()));
        {
            // Recreate the pre-migration accounts table exactly as it shipped.
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE accounts (
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
                INSERT INTO accounts (id, backend, label, refresh_token, access_token, expires_at,
                                      status, reset_at, created_at, updated_at)
                 VALUES ('legacy', 'codex', 'owner@example.test', 'refresh', 'access', 999,
                         'cooling', 4242, 7, 8);",
            )
            .unwrap();
        }

        let store = Store::open(&path).unwrap();
        let account = store.get_account("legacy").expect("legacy row must survive migration");
        assert_eq!(account.label, "owner@example.test");
        assert_eq!(account.expires_at, 999);
        assert_eq!(account.reset_at, 4242);
        assert_eq!(account.token_refreshed_at, 0, "new column must default to never-rotated");

        // Re-opening must be a no-op rather than a duplicate-column error.
        drop(store);
        let reopened = Store::open(&path).unwrap();
        assert_eq!(reopened.list_accounts().len(), 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn monitor_buckets_and_account_totals_respect_bounds_and_unknowns() {
        let store = Store::in_memory().unwrap();
        for (ts, account, input, output) in [
            (59_999, "a", Some(5), Some(2)),
            (60_000, "a", Some(10), Some(4)),
            (119_999, "a", None, None),
            (120_000, "b", Some(3), Some(1)),
        ] {
            store.insert_usage(&UsageRecord {
                id: 0, request_id: format!("request-{ts}"), ts, endpoint: "/v1/responses".into(),
                backend: "codex".into(), model: "model".into(), account_id: account.into(),
                account_label: account.into(), cache_key: Some("secret-session".into()),
                status: 200, input_tokens: input, output_tokens: output,
            }).unwrap();
        }
        let bins = store.monitor_minute_bins(120_000).unwrap();
        assert_eq!(bins.len(), 60);
        assert_eq!(&bins[57..], &[1, 2, 1]);
        let totals = store.monitor_account_totals(60_000, 120_000).unwrap();
        assert_eq!(totals.len(), 1);
        assert_eq!((totals[0].requests, totals[0].unknown_requests, totals[0].input_tokens, totals[0].output_tokens), (2, 1, 10, 4));
    }

    #[test]
    fn usage_survives_reopen_and_splits_unknown_from_measured() {
        let path = std::env::temp_dir().join(format!("underclass-usage-{}.db", uuid::Uuid::new_v4()));
        {
            let store = Store::open(&path).unwrap();
            for (key, input) in [(Some("session-a"), Some(12)), (Some("session-a"), None), (Some("session-b"), Some(5))] {
                store.insert_usage(&UsageRecord {
                    id: 0, request_id: uuid::Uuid::new_v4().to_string(), ts: 100, endpoint: "/v1/responses".into(), backend: "codex".into(), model: "model-a".into(), account_id: "account-a".into(), account_label: "alice".into(), cache_key: key.map(str::to_string), status: 200, input_tokens: input, output_tokens: input.map(|n| n / 2),
                }).unwrap();
            }
        }
        let store = Store::open(&path).unwrap();
        let query = UsageQuery { from_ms: Some(0), group_by: Some("model,account_id,cache_key".into()), cache_key: Some("session-a".into()), ..Default::default() };
        let group = &store.usage_summary(&query).unwrap()[0];
        assert_eq!((group.requests, group.measured_requests, group.unknown_requests, group.input_tokens, group.output_tokens), (2, 1, 1, 12, 6));
        assert_eq!(group.account_label.as_deref(), Some("alice"));
        assert_eq!(store.usage_records(&query).unwrap().len(), 2);
        let page = store.usage_summary(&UsageQuery { from_ms: Some(0), group_by: Some("cache_key".into()), limit: Some(1), offset: Some(1), ..Default::default() }).unwrap();
        assert_eq!(page.len(), 1);
        drop(store);
        std::fs::remove_file(path).unwrap();
    }
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
