use crate::models::{Account, AccountStatus, BackendId, Binding, ModelInfo};
use crate::stats::{self, CountRow, Dim, Interval, Series, TallyRow, Totals};
use rusqlite::types::Value;
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use std::path::Path;
use std::sync::Mutex;

fn usage_dim_sql(dim: Dim) -> (&'static str, &'static str) {
    match dim {
        Dim::Total => ("'*'", "'*'"),
        Dim::Model => ("model", "model"),
        Dim::Account => ("account_id", "account_label"),
    }
}

#[derive(Clone, Debug, Default, Serialize)]
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
    /// Attempt start to finish (for streams: until the body ended), when known.
    pub duration_ms: Option<i64>,
    /// How a 2xx body ended (`usage::StreamOutcome::as_str`); `None` for non-2xx attempts.
    pub stream_outcome: Option<String>,
    /// Sanitized upstream error code, from the error body or the failing stream event.
    pub error_code: Option<String>,
    /// Whether model output reached the client before the stream ended.
    pub saw_output: Option<bool>,
}

/// A notable upstream or pool event for long-range capacity and refusal statistics.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct UpstreamEvent {
    pub ts: i64,
    /// `proxy` for events observed by this server, `codex_import` for backfilled client logs.
    pub source: String,
    /// `overloaded`, `policy_blocked`, `quota_exhausted`, `retries_exhausted`, `upstream_failed`,
    /// `quota_cooling`, or `pool_saturated`.
    pub kind: String,
    /// Where the event surfaced: `stream`, `http`, `pool`, or `codex_log`.
    pub detail: Option<String>,
    pub error_code: Option<String>,
    pub backend: Option<String>,
    pub model: Option<String>,
    pub account_id: Option<String>,
    pub account_label: Option<String>,
    pub request_id: Option<String>,
    pub until_ms: Option<i64>,
    /// Unique per imported event so a re-import inserts nothing.
    pub dedupe_key: Option<String>,
}

pub const EVENT_KINDS: [&str; 7] = [
    "overloaded",
    "policy_blocked",
    "quota_exhausted",
    "retries_exhausted",
    "upstream_failed",
    "quota_cooling",
    "pool_saturated",
];

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
        if self.missing_key == Some(true) {
            predicates.push("cache_key IS NULL".into());
        }
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
        plan TEXT,
        status TEXT NOT NULL DEFAULT 'healthy',
        disabled_reason TEXT,
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
        output_tokens INTEGER,
        duration_ms INTEGER,
        stream_outcome TEXT,
        error_code TEXT,
        saw_output INTEGER
    );
    CREATE INDEX IF NOT EXISTS usage_records_ts ON usage_records(ts, id);
    CREATE INDEX IF NOT EXISTS usage_records_dims ON usage_records(model, account_id, cache_key, ts);
    CREATE TABLE IF NOT EXISTS upstream_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,
        source TEXT NOT NULL,
        kind TEXT NOT NULL,
        detail TEXT,
        error_code TEXT,
        backend TEXT,
        model TEXT,
        account_id TEXT,
        account_label TEXT,
        request_id TEXT,
        until_ms INTEGER,
        dedupe_key TEXT UNIQUE
    );
    CREATE INDEX IF NOT EXISTS upstream_events_ts ON upstream_events(ts);
    CREATE TABLE IF NOT EXISTS stats_daily (
        day INTEGER NOT NULL,
        dim TEXT NOT NULL,
        key TEXT NOT NULL,
        label TEXT NOT NULL,
        totals TEXT NOT NULL,
        computed_at INTEGER NOT NULL,
        PRIMARY KEY (day, dim, key)
    );
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
    if !columns.iter().any(|c| c == "plan") {
        conn.execute("ALTER TABLE accounts ADD COLUMN plan TEXT", [])?;
    }
    if !columns.iter().any(|c| c == "disabled_reason") {
        conn.execute("ALTER TABLE accounts ADD COLUMN disabled_reason TEXT", [])?;
    }
    let mut stmt = conn.prepare("PRAGMA table_info(usage_records)")?;
    let columns: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<_>>()?;
    for (name, ty) in [
        ("duration_ms", "INTEGER"),
        ("stream_outcome", "TEXT"),
        ("error_code", "TEXT"),
        ("saw_output", "INTEGER"),
    ] {
        if !columns.iter().any(|c| c == name) {
            conn.execute(
                &format!("ALTER TABLE usage_records ADD COLUMN {name} {ty}"),
                [],
            )?;
        }
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
    pub fn monitor_account_totals(
        &self,
        from_ms: i64,
        to_ms: i64,
    ) -> rusqlite::Result<Vec<UsageSummary>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT account_id, MAX(account_label), COUNT(*), \
             SUM(CASE WHEN input_tokens IS NULL OR output_tokens IS NULL THEN 1 ELSE 0 END), \
             COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0) \
             FROM usage_records WHERE ts >= ?1 AND ts < ?2 GROUP BY account_id",
        )?;
        let rows = stmt.query_map(params![from_ms, to_ms], |r| {
            let requests: i64 = r.get(2)?;
            let unknown: i64 = r.get(3)?;
            Ok(UsageSummary {
                model: None,
                account_id: r.get(0)?,
                account_label: r.get(1)?,
                cache_key: None,
                requests,
                measured_requests: requests - unknown,
                unknown_requests: unknown,
                input_tokens: r.get(4)?,
                output_tokens: r.get(5)?,
            })
        })?;
        rows.collect()
    }

    /// @cc [owner:ghuntley,label:persistence] usage-write-through
    /// Each upstream attempt MUST create one durable usage row, retaining its account label and
    /// unknown token counts as NULL even when the account is later removed.
    pub fn insert_usage(&self, record: &UsageRecord) -> rusqlite::Result<()> {
        self.conn.lock().unwrap().execute(
            "INSERT INTO usage_records (request_id,ts,endpoint,backend,model,account_id,account_label,cache_key,status,input_tokens,output_tokens,duration_ms,stream_outcome,error_code,saw_output) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
            params![record.request_id, record.ts, record.endpoint, record.backend, record.model, record.account_id, record.account_label, record.cache_key, record.status, record.input_tokens, record.output_tokens, record.duration_ms, record.stream_outcome, record.error_code, record.saw_output],
        )?;
        Ok(())
    }

    pub fn usage_summary(&self, query: &UsageQuery) -> rusqlite::Result<Vec<UsageSummary>> {
        let (filter, mut values) = query.where_sql();
        let requested: Vec<&str> = query
            .group_by
            .as_deref()
            .unwrap_or("")
            .split(',')
            .filter(|s| !s.is_empty())
            .collect();
        let groups: Vec<&str> = ["model", "account_id", "cache_key"]
            .into_iter()
            .filter(|name| requested.contains(name))
            .collect();
        let select_dim = |name: &str| {
            if groups.contains(&name) {
                name.to_string()
            } else {
                format!("NULL AS {name}")
            }
        };
        let label = if groups.contains(&"account_id") {
            "MAX(account_label) AS account_label"
        } else {
            "NULL AS account_label"
        };
        let group_sql = if groups.is_empty() {
            String::new()
        } else {
            format!(" GROUP BY {}", groups.join(","))
        };
        let pagination = if groups.is_empty() {
            String::new()
        } else {
            values.push(Value::Integer(
                query.limit.unwrap_or(100).clamp(1, 1000) as i64
            ));
            values.push(Value::Integer(
                query.offset.unwrap_or(0).min(i64::MAX as usize) as i64,
            ));
            " LIMIT ? OFFSET ?".to_string()
        };
        let sql = format!(
            "SELECT {},{},{},{},COUNT(*),COALESCE(SUM(CASE WHEN input_tokens IS NOT NULL AND output_tokens IS NOT NULL THEN 1 ELSE 0 END),0),COALESCE(SUM(CASE WHEN input_tokens IS NULL OR output_tokens IS NULL THEN 1 ELSE 0 END),0),COALESCE(SUM(input_tokens),0) AS input_tokens,COALESCE(SUM(output_tokens),0) AS output_tokens FROM usage_records WHERE {filter}{group_sql} ORDER BY input_tokens DESC,model,account_id,cache_key{pagination}",
            select_dim("model"),
            select_dim("account_id"),
            label,
            select_dim("cache_key")
        );
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(values), |r| {
            Ok(UsageSummary {
                model: r.get(0)?,
                account_id: r.get(1)?,
                account_label: r.get(2)?,
                cache_key: r.get(3)?,
                requests: r.get(4)?,
                measured_requests: r.get(5)?,
                unknown_requests: r.get(6)?,
                input_tokens: r.get(7)?,
                output_tokens: r.get(8)?,
            })
        })?;
        rows.collect()
    }

    pub fn usage_records(&self, query: &UsageQuery) -> rusqlite::Result<Vec<UsageRecord>> {
        let (filter, mut values) = query.where_sql();
        let limit = query.limit.unwrap_or(100).clamp(1, 1000) as i64;
        let offset = query.offset.unwrap_or(0).min(i64::MAX as usize) as i64;
        values.push(Value::Integer(limit));
        values.push(Value::Integer(offset));
        let sql = format!(
            "SELECT id,request_id,ts,endpoint,backend,model,account_id,account_label,cache_key,status,input_tokens,output_tokens,duration_ms,stream_outcome,error_code,saw_output FROM usage_records WHERE {filter} ORDER BY ts DESC,id DESC LIMIT ? OFFSET ?"
        );
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(values), |r| {
            Ok(UsageRecord {
                id: r.get(0)?,
                request_id: r.get(1)?,
                ts: r.get(2)?,
                endpoint: r.get(3)?,
                backend: r.get(4)?,
                model: r.get(5)?,
                account_id: r.get(6)?,
                account_label: r.get(7)?,
                cache_key: r.get(8)?,
                status: r.get(9)?,
                input_tokens: r.get(10)?,
                output_tokens: r.get(11)?,
                duration_ms: r.get(12)?,
                stream_outcome: r.get(13)?,
                error_code: r.get(14)?,
                saw_output: r.get(15)?,
            })
        })?;
        rows.collect()
    }
    /// @cc [owner:ghuntley,label:persistence] upstream-event-write-through
    /// `insert_upstream_events` MUST persist each event immediately, MUST insert an event whose
    /// `dedupe_key` already exists zero times, and MUST discard the finished-day rollups for every day
    /// that gained an event so maintenance recomputes them.
    pub fn insert_upstream_events(&self, events: &[UpstreamEvent]) -> rusqlite::Result<usize> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let mut inserted = 0;
        let mut days = std::collections::BTreeSet::new();
        for event in events {
            let changed = tx.execute(
                "INSERT OR IGNORE INTO upstream_events (ts,source,kind,detail,error_code,backend,model,account_id,account_label,request_id,until_ms,dedupe_key) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
                params![event.ts, event.source, event.kind, event.detail, event.error_code, event.backend, event.model, event.account_id, event.account_label, event.request_id, event.until_ms, event.dedupe_key],
            )?;
            if changed > 0 {
                inserted += changed;
                days.insert(event.ts.div_euclid(stats::DAY_MS));
            }
        }
        for day in days {
            tx.execute("DELETE FROM stats_daily WHERE day = ?1", params![day])?;
        }
        tx.commit()?;
        Ok(inserted)
    }

    pub fn insert_upstream_event(&self, event: &UpstreamEvent) {
        if let Err(error) = self.insert_upstream_events(std::slice::from_ref(event)) {
            tracing::error!(error = %error, kind = %event.kind, "upstream_event.persist_failed");
        }
    }

    /// Per-bucket counts from `usage_records` with `ts` in `[from, to)`.
    pub fn stats_counts(
        &self,
        from: i64,
        to: i64,
        bucket_ms: i64,
        dim: Dim,
    ) -> rusqlite::Result<Vec<CountRow>> {
        let (key, label) = usage_dim_sql(dim);
        let sql = format!(
            "SELECT (ts - ?1) / ?3 AS b, {key} AS k, MAX({label}), COUNT(DISTINCT request_id), COUNT(*), COUNT(DISTINCT cache_key), \
             COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0), \
             SUM(CASE WHEN status BETWEEN 200 AND 299 AND (input_tokens IS NULL OR output_tokens IS NULL) THEN 1 ELSE 0 END) \
             FROM usage_records WHERE ts >= ?1 AND ts < ?2 GROUP BY b, k"
        );
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![from, to, bucket_ms], |r| {
            Ok(CountRow {
                bucket: r.get(0)?,
                key: r.get(1)?,
                label: r.get(2)?,
                requests: r.get(3)?,
                attempts: r.get(4)?,
                sessions: r.get(5)?,
                input_tokens: r.get(6)?,
                output_tokens: r.get(7)?,
                unmeasured: r.get(8)?,
            })
        })?;
        rows.collect()
    }

    /// Per-bucket attempt outcomes: the recorded stream outcome, `http_<status>` for non-2xx,
    /// `network_error` for no response, or `unknown` for 2xx rows recorded before outcomes existed.
    pub fn stats_outcomes(
        &self,
        from: i64,
        to: i64,
        bucket_ms: i64,
        dim: Dim,
    ) -> rusqlite::Result<Vec<TallyRow>> {
        let (key, label) = usage_dim_sql(dim);
        let sql = format!(
            "SELECT (ts - ?1) / ?3 AS b, {key} AS k, MAX({label}), \
             COALESCE(stream_outcome, CASE WHEN status = 0 THEN 'network_error' WHEN status BETWEEN 200 AND 299 THEN 'unknown' ELSE 'http_' || status END) AS o, \
             COUNT(*) FROM usage_records WHERE ts >= ?1 AND ts < ?2 GROUP BY b, k, o"
        );
        self.tally(&sql, from, to, bucket_ms)
    }

    /// Per-bucket upstream event counts by kind.
    pub fn stats_events(
        &self,
        from: i64,
        to: i64,
        bucket_ms: i64,
        dim: Dim,
    ) -> rusqlite::Result<Vec<TallyRow>> {
        let (key, label) = match dim {
            Dim::Total => ("'*'", "'*'"),
            Dim::Model => ("COALESCE(model,'(unknown)')", "COALESCE(model,'(unknown)')"),
            Dim::Account => (
                "COALESCE(account_id,'(unknown)')",
                "COALESCE(account_label,account_id,'(unknown)')",
            ),
        };
        let sql = format!(
            "SELECT (ts - ?1) / ?3 AS b, {key} AS k, MAX({label}), kind, COUNT(*) \
             FROM upstream_events WHERE ts >= ?1 AND ts < ?2 GROUP BY b, k, kind"
        );
        self.tally(&sql, from, to, bucket_ms)
    }

    fn tally(
        &self,
        sql: &str,
        from: i64,
        to: i64,
        bucket_ms: i64,
    ) -> rusqlite::Result<Vec<TallyRow>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(sql)?;
        let rows = stmt.query_map(params![from, to, bucket_ms], |r| {
            Ok(TallyRow {
                bucket: r.get(0)?,
                key: r.get(1)?,
                label: r.get(2)?,
                name: r.get(3)?,
                count: r.get(4)?,
            })
        })?;
        rows.collect()
    }

    /// Lifetimes of attempts overlapping `[from, to)`; attempts started more than
    /// `stats::MAX_ATTEMPT_MS` before `from` are not considered.
    pub fn stats_intervals(&self, from: i64, to: i64, dim: Dim) -> rusqlite::Result<Vec<Interval>> {
        let (key, _) = usage_dim_sql(dim);
        let sql = format!(
            "SELECT {key}, ts, ts + MAX(duration_ms, 1) FROM usage_records \
             WHERE ts >= ?1 AND ts < ?2 AND duration_ms IS NOT NULL AND ts + MAX(duration_ms, 1) > ?3"
        );
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![from - stats::MAX_ATTEMPT_MS, to, from], |r| {
            Ok(Interval {
                key: r.get(0)?,
                start: r.get(1)?,
                end: r.get(2)?,
            })
        })?;
        rows.collect()
    }

    /// @cc [owner:ghuntley,label:accounting] stats-series-raw
    /// `stats_series` MUST aggregate exactly the usage rows and upstream events with `ts` in
    /// `[from, to)` into `bucket_ms` buckets per `dim` key, with concurrency from attempt lifetimes.
    pub fn stats_series(
        &self,
        from: i64,
        to: i64,
        bucket_ms: i64,
        dim: Dim,
    ) -> rusqlite::Result<Vec<Series>> {
        let counts = self.stats_counts(from, to, bucket_ms, dim)?;
        let outcomes = self.stats_outcomes(from, to, bucket_ms, dim)?;
        let events = self.stats_events(from, to, bucket_ms, dim)?;
        let intervals = self.stats_intervals(from, to, dim)?;
        Ok(stats::assemble(
            from, to, bucket_ms, &counts, &outcomes, &events, &intervals,
        ))
    }

    /// Exact distinct request and session counts per `dim` key over `[from, to)`.
    pub fn stats_distinct(
        &self,
        from: i64,
        to: i64,
        dim: Dim,
    ) -> rusqlite::Result<Vec<(String, i64, i64)>> {
        let (key, _) = usage_dim_sql(dim);
        let sql = format!(
            "SELECT {key} AS k, COUNT(DISTINCT request_id), COUNT(DISTINCT cache_key) FROM usage_records \
             WHERE ts >= ?1 AND ts < ?2 GROUP BY k"
        );
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![from, to], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
        rows.collect()
    }

    /// Stored finished-day rollups for days in `[from_day, to_day)`: `(day, key, label, totals)`.
    pub fn stats_daily(
        &self,
        from_day: i64,
        to_day: i64,
        dim: Dim,
    ) -> rusqlite::Result<Vec<(i64, String, String, Totals)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT day, key, label, totals FROM stats_daily WHERE day >= ?1 AND day < ?2 AND dim = ?3 ORDER BY day, key",
        )?;
        let rows = stmt.query_map(params![from_day, to_day, dim.as_str()], |r| {
            let totals: String = r.get(3)?;
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                serde_json::from_str(&totals).unwrap_or_default(),
            ))
        })?;
        rows.collect()
    }

    /// Days in `[from_day, to_day)` that already have a stored rollup.
    pub fn stats_rolled_days(
        &self,
        from_day: i64,
        to_day: i64,
    ) -> rusqlite::Result<std::collections::BTreeSet<i64>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT day FROM stats_daily WHERE day >= ?1 AND day < ?2 AND dim = 'total'",
        )?;
        let rows = stmt.query_map(params![from_day, to_day], |r| r.get(0))?;
        rows.collect()
    }

    /// @cc [owner:ghuntley,label:accounting] stats-daily-rollup
    /// `rollup_stats_day` MUST store, for every dim, exactly what `stats_series` reports for that UTC
    /// day as one bucket, and MUST always store a `total` row (zeros when idle) so the day is marked
    /// rolled up.
    pub fn rollup_stats_day(&self, day: i64, now_ms: i64) -> rusqlite::Result<()> {
        let from = day * stats::DAY_MS;
        let to = from + stats::DAY_MS;
        let mut rows: Vec<(Dim, String, String, Totals)> = Vec::new();
        for dim in Dim::all() {
            for series in self.stats_series(from, to, stats::DAY_MS, dim)? {
                let totals = series
                    .buckets
                    .into_iter()
                    .next()
                    .map(|b| b.totals)
                    .unwrap_or_default();
                rows.push((dim, series.key, series.label, totals));
            }
        }
        if !rows.iter().any(|(dim, ..)| *dim == Dim::Total) {
            rows.push((
                Dim::Total,
                stats::TOTAL_KEY.into(),
                stats::TOTAL_KEY.into(),
                Totals::default(),
            ));
        }
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute("DELETE FROM stats_daily WHERE day = ?1", params![day])?;
        for (dim, key, label, totals) in rows {
            tx.execute(
                "INSERT INTO stats_daily (day, dim, key, label, totals, computed_at) VALUES (?1,?2,?3,?4,?5,?6)",
                params![day, dim.as_str(), key, label, serde_json::to_string(&totals).unwrap_or_default(), now_ms],
            )?;
        }
        tx.commit()
    }

    /// Earliest `ts` across usage rows and upstream events.
    pub fn stats_earliest_ts(&self) -> rusqlite::Result<Option<i64>> {
        self.conn.lock().unwrap().query_row(
            "SELECT MIN(t) FROM (SELECT MIN(ts) AS t FROM usage_records UNION ALL SELECT MIN(ts) FROM upstream_events)",
            [],
            |r| r.get(0),
        )
    }

    /// @cc [owner:ghuntley,label:persistence] stats-retention
    /// `prune_stats` MUST delete every usage row and upstream event with `ts < cutoff_ms` and every
    /// daily rollup for a day ending at or before `cutoff_ms`, in bounded batches so request writes
    /// are not blocked for the whole prune, and MUST NOT delete anything newer.
    pub fn prune_stats(&self, cutoff_ms: i64) -> rusqlite::Result<usize> {
        let mut deleted = 0;
        for table in ["usage_records", "upstream_events"] {
            loop {
                let n = self.conn.lock().unwrap().execute(
                    &format!("DELETE FROM {table} WHERE id IN (SELECT id FROM {table} WHERE ts < ?1 LIMIT 5000)"),
                    params![cutoff_ms],
                )?;
                deleted += n;
                if n == 0 {
                    break;
                }
            }
        }
        deleted += self.conn.lock().unwrap().execute(
            "DELETE FROM stats_daily WHERE (day + 1) * ?1 <= ?2",
            params![stats::DAY_MS, cutoff_ms],
        )?;
        Ok(deleted)
    }

    pub fn open(path: &Path) -> rusqlite::Result<Self> {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let conn = Connection::open(path)?;
        conn.execute_batch(schema())?;
        migrate(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    #[allow(dead_code)]
    pub fn in_memory() -> rusqlite::Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(schema())?;
        migrate(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn list_accounts(&self) -> Vec<Account> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT id, backend, label, refresh_token, access_token, expires_at, account_id,
                        residency, enterprise_url, status, reset_at, created_at, updated_at,
                        token_refreshed_at, plan, disabled_reason
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
                    plan: row.get(14)?,
                    disabled_reason: row.get(15)?,
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
                        token_refreshed_at, plan, disabled_reason
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
                plan: row.get(14)?,
                disabled_reason: row.get(15)?,
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
                                   residency, enterprise_url, plan, status, reset_at, created_at, updated_at,
                                   token_refreshed_at, disabled_reason)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)
             ON CONFLICT(id) DO UPDATE SET
                label=?3, refresh_token=?4, access_token=?5, expires_at=?6, account_id=?7,
                residency=?8, enterprise_url=?9, plan=?10, status=?11, reset_at=?12, updated_at=?14,
                token_refreshed_at=?15, disabled_reason=?16",
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
                a.plan,
                status_str(a.status),
                a.reset_at,
                a.created_at,
                a.updated_at,
                a.token_refreshed_at,
                a.disabled_reason,
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
        conn.execute(
            "DELETE FROM reset_attempts WHERE account_id = ?1",
            params![id],
        )
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
        conn.execute(
            "DELETE FROM reset_attempts WHERE account_id = ?1",
            params![account_id],
        )
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

    /// Sets or clears why underclass disabled the account; `None` marks the status as the
    /// operator's, which the plan gate never reverts.
    pub fn set_disabled_reason(&self, id: &str, reason: Option<&str>) {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE accounts SET disabled_reason = ?2 WHERE id = ?1",
            params![id, reason],
        )
        .expect("update disabled reason");
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
            params![
                id,
                refresh_token,
                access_token,
                expires_at,
                chatgpt_account_id,
                residency,
                now
            ],
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
        tx.execute(
            "DELETE FROM catalog WHERE backend = ?1",
            params![backend.as_str()],
        )
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
        conn.query_row(
            "SELECT value FROM config WHERE key = ?1",
            params![key],
            |r| r.get(0),
        )
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
        let path =
            std::env::temp_dir().join(format!("underclass-legacy-{}.db", uuid::Uuid::now_v7()));
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
        let account = store
            .get_account("legacy")
            .expect("legacy row must survive migration");
        assert_eq!(account.label, "owner@example.test");
        assert_eq!(account.expires_at, 999);
        assert_eq!(account.reset_at, 4242);
        assert_eq!(
            account.token_refreshed_at, 0,
            "new column must default to never-rotated"
        );

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
            store
                .insert_usage(&UsageRecord {
                    id: 0,
                    request_id: format!("request-{ts}"),
                    ts,
                    endpoint: "/v1/responses".into(),
                    backend: "codex".into(),
                    model: "model".into(),
                    account_id: account.into(),
                    account_label: account.into(),
                    cache_key: Some("secret-session".into()),
                    status: 200,
                    input_tokens: input,
                    output_tokens: output,
                    ..Default::default()
                })
                .unwrap();
        }
        let bins = store.monitor_minute_bins(120_000).unwrap();
        assert_eq!(bins.len(), 60);
        assert_eq!(&bins[57..], &[1, 2, 1]);
        let totals = store.monitor_account_totals(60_000, 120_000).unwrap();
        assert_eq!(totals.len(), 1);
        assert_eq!(
            (
                totals[0].requests,
                totals[0].unknown_requests,
                totals[0].input_tokens,
                totals[0].output_tokens
            ),
            (2, 1, 10, 4)
        );
    }

    #[test]
    fn usage_survives_reopen_and_splits_unknown_from_measured() {
        let path =
            std::env::temp_dir().join(format!("underclass-usage-{}.db", uuid::Uuid::now_v7()));
        {
            let store = Store::open(&path).unwrap();
            for (key, input) in [
                (Some("session-a"), Some(12)),
                (Some("session-a"), None),
                (Some("session-b"), Some(5)),
            ] {
                store
                    .insert_usage(&UsageRecord {
                        id: 0,
                        request_id: uuid::Uuid::now_v7().to_string(),
                        ts: 100,
                        endpoint: "/v1/responses".into(),
                        backend: "codex".into(),
                        model: "model-a".into(),
                        account_id: "account-a".into(),
                        account_label: "alice".into(),
                        cache_key: key.map(str::to_string),
                        status: 200,
                        input_tokens: input,
                        output_tokens: input.map(|n| n / 2),
                        ..Default::default()
                    })
                    .unwrap();
            }
        }
        let store = Store::open(&path).unwrap();
        let query = UsageQuery {
            from_ms: Some(0),
            group_by: Some("model,account_id,cache_key".into()),
            cache_key: Some("session-a".into()),
            ..Default::default()
        };
        let group = &store.usage_summary(&query).unwrap()[0];
        assert_eq!(
            (
                group.requests,
                group.measured_requests,
                group.unknown_requests,
                group.input_tokens,
                group.output_tokens
            ),
            (2, 1, 1, 12, 6)
        );
        assert_eq!(group.account_label.as_deref(), Some("alice"));
        assert_eq!(store.usage_records(&query).unwrap().len(), 2);
        let page = store
            .usage_summary(&UsageQuery {
                from_ms: Some(0),
                group_by: Some("cache_key".into()),
                limit: Some(1),
                offset: Some(1),
                ..Default::default()
            })
            .unwrap();
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
        for row in [
            binding("expired", 1),
            binding("old", 100),
            binding("new", 200),
        ] {
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
