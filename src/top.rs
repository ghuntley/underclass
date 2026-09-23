//! Read-only terminal monitor for a running underclass server.
use crate::config::Config;
use crate::models::{AccountStatus, BackendId};
use crate::monitor::{MonitorAccount, MonitorSnapshot};
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Sparkline, Table, TableState, Wrap};
use std::io::IsTerminal;
use std::sync::mpsc;
use std::time::{Duration, Instant};

const BACKGROUND: Color = Color::Rgb(12, 16, 26);
const PANEL: Color = Color::Rgb(20, 27, 40);
const BORDER: Color = Color::Rgb(57, 70, 91);
const TEXT: Color = Color::Rgb(222, 231, 242);
const MUTED: Color = Color::Rgb(139, 154, 176);
const CYAN: Color = Color::Rgb(92, 216, 229);
const PURPLE: Color = Color::Rgb(185, 138, 245);
const GREEN: Color = Color::Rgb(111, 217, 151);
const YELLOW: Color = Color::Rgb(246, 196, 101);
const RED: Color = Color::Rgb(245, 111, 125);

fn panel() -> Block<'static> {
    Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(BORDER))
        .title_style(Style::default().fg(CYAN).add_modifier(Modifier::BOLD))
        .style(Style::default().fg(TEXT).bg(PANEL))
}

struct TerminalGuard;
impl Drop for TerminalGuard {
    fn drop(&mut self) {
        ratatui::restore();
    }
}

enum Update {
    Snapshot(MonitorSnapshot),
    Error(String),
}

/// @cc [owner:ghuntley,label:cli] top-observes-only
/// `run` MUST obtain dashboard data only through authenticated GET requests, MUST leave the local
/// SQLite database untouched, and MUST restore terminal state on every normal exit or error.
pub fn run(url_override: Option<String>) -> Result<(), String> {
    if !std::io::stdout().is_terminal() || !std::io::stdin().is_terminal() {
        return Err("underclass top requires an interactive terminal".into());
    }
    let config = Config::load();
    let url = match url_override.as_deref() {
        Some(url) => url.trim_end_matches('/').to_string(),
        None => {
            let bind = config
                .bind
                .replace("0.0.0.0", "127.0.0.1")
                .replace("[::]", "[::1]");
            format!("http://{bind}")
        }
    };
    let parsed = reqwest::Url::parse(&url).map_err(|_| "invalid server URL")?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || parsed.username() != ""
        || parsed.password().is_some()
        || !matches!(parsed.path(), "" | "/")
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(
            "server URL must be an HTTP(S) origin without credentials, path, or query".into(),
        );
    }
    let token = if url_override.is_some() {
        std::env::var("UNDERCLASS_UI_TOKEN")
            .ok()
            .filter(|s| !s.is_empty())
    } else {
        config
            .ui_token
            .clone()
            .filter(|s| !s.is_empty())
            .or_else(|| local_token(&config))
    }
    .ok_or("admin token unavailable; start underclass serve or set UNDERCLASS_UI_TOKEN")?;
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(2))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| format!("HTTP client: {e}"))?;
    let (tx, rx) = mpsc::channel();
    let worker_url = format!("{url}/admin/api/monitor");
    let worker = std::thread::spawn(move || {
        loop {
            let started = Instant::now();
            let update = match client.get(&worker_url).bearer_auth(&token).send() {
                Ok(response) if response.status().is_success() => {
                    match response.json::<MonitorSnapshot>() {
                        Ok(snapshot) => Update::Snapshot(snapshot),
                        Err(_) => Update::Error("invalid monitor response".into()),
                    }
                }
                Ok(response) if response.status() == reqwest::StatusCode::UNAUTHORIZED => {
                    Update::Error("admin token rejected (401)".into())
                }
                Ok(response) => Update::Error(format!("server returned {}", response.status())),
                Err(_) => Update::Error("server unavailable; retrying".into()),
            };
            if tx.send(update).is_err() {
                break;
            }
            let pause = Duration::from_secs(1).saturating_sub(started.elapsed());
            if !pause.is_zero() {
                std::thread::sleep(pause);
            }
        }
    });
    let mut terminal = ratatui::init();
    let _guard = TerminalGuard;
    let mut app = App::default();
    let result = loop {
        while let Ok(update) = rx.try_recv() {
            match update {
                Update::Snapshot(snapshot) => {
                    app.error = None;
                    app.snapshot = Some(snapshot);
                    app.last_seen = Some(Instant::now());
                }
                Update::Error(error) => app.error = Some(error),
            }
        }
        if let Err(error) = terminal.draw(|frame| draw(frame, &mut app)) {
            break Err(format!("terminal draw: {error}"));
        }
        match event::poll(Duration::from_millis(100)) {
            Ok(true) => match event::read() {
                Ok(Event::Key(key)) if key.kind == KeyEventKind::Press => match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => break Ok(()),
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        break Ok(());
                    }
                    KeyCode::Down | KeyCode::Char('j') => app.down(),
                    KeyCode::Up | KeyCode::Char('k') => app.up(),
                    KeyCode::PageDown => app.page(10),
                    KeyCode::PageUp => app.page(-10),
                    KeyCode::Home => app.selected = 0,
                    KeyCode::End => app.selected = app.account_count().saturating_sub(1),
                    _ => {}
                },
                Ok(_) => {}
                Err(error) => break Err(format!("terminal input: {error}")),
            },
            Ok(false) => {}
            Err(error) => break Err(format!("terminal poll: {error}")),
        }
    };
    drop(terminal);
    drop(rx);
    let _ = worker.join();
    result
}

fn local_token(config: &Config) -> Option<String> {
    let conn = rusqlite::Connection::open_with_flags(
        config.db_path(),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .ok()?;
    conn.query_row(
        "SELECT value FROM config WHERE key = 'ui_token'",
        [],
        |row| row.get(0),
    )
    .ok()
}

#[derive(Default)]
struct App {
    snapshot: Option<MonitorSnapshot>,
    error: Option<String>,
    last_seen: Option<Instant>,
    selected: usize,
}

impl App {
    fn account_count(&self) -> usize {
        self.snapshot.as_ref().map_or(0, |s| s.accounts.len())
    }
    fn down(&mut self) {
        self.selected = (self.selected + 1).min(self.account_count().saturating_sub(1));
    }
    fn up(&mut self) {
        self.selected = self.selected.saturating_sub(1);
    }
    fn page(&mut self, delta: isize) {
        self.selected = self
            .selected
            .saturating_add_signed(delta)
            .min(self.account_count().saturating_sub(1));
    }
}

fn draw(frame: &mut ratatui::Frame<'_>, app: &mut App) {
    let area = frame.area();
    frame.render_widget(
        Block::default().style(Style::default().bg(BACKGROUND)),
        area,
    );
    let compact = area.width < 100;
    let short = area.height < 25;
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(4),
            Constraint::Length(if short { 4 } else { 5 }),
            Constraint::Min(5),
            Constraint::Length(if short { 0 } else { 7 }),
            Constraint::Length(1),
        ])
        .split(area);
    let status = if let Some(error) = &app.error {
        format!("DISCONNECTED · {error}")
    } else if let Some(seen) = app.last_seen {
        format!("LIVE · updated {}s ago", seen.elapsed().as_secs())
    } else {
        "CONNECTING".to_string()
    };
    let header = Paragraph::new(Line::from(vec![
        Span::styled(
            " UNDERCLASS ",
            Style::default()
                .fg(BACKGROUND)
                .bg(CYAN)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("  pool monitor  ·  ", Style::default().fg(MUTED)),
        Span::styled(
            status,
            Style::default()
                .fg(if app.error.is_some() { RED } else { GREEN })
                .add_modifier(Modifier::BOLD),
        ),
    ]))
    .block(panel().title(" underclass top "));
    frame.render_widget(header, sections[0]);
    if let Some(snapshot) = &app.snapshot {
        let inflight: u32 = snapshot.accounts.iter().map(|a| a.inflight).sum();
        let healthy = snapshot
            .accounts
            .iter()
            .filter(|a| a.status == AccountStatus::Healthy)
            .count();
        let summary = vec![
            Line::from(vec![
                Span::styled(" LIVE ", Style::default().fg(MUTED)),
                Span::styled(
                    format!("{inflight:>3}"),
                    Style::default().fg(CYAN).add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    format!(
                        "   HEALTHY {healthy}/{}   LAST 60s ",
                        snapshot.accounts.len()
                    ),
                    Style::default().fg(MUTED),
                ),
                Span::styled(
                    snapshot.last_minute.attempts.to_string(),
                    Style::default().fg(PURPLE).add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    format!(
                        " attempts · {} in / {} out",
                        number(snapshot.last_minute.input_tokens),
                        number(snapshot.last_minute.output_tokens)
                    ),
                    Style::default().fg(TEXT),
                ),
            ]),
            Line::from(vec![
                Span::styled(" MONTH ", Style::default().fg(MUTED)),
                Span::styled(
                    format!("{:>6}", snapshot.month.attempts),
                    Style::default().fg(PURPLE).add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    format!(
                        " attempts · {} in / {} out · ",
                        number(snapshot.month.input_tokens),
                        number(snapshot.month.output_tokens)
                    ),
                    Style::default().fg(TEXT),
                ),
                Span::styled(
                    format!("{} unknown", snapshot.month.unknown),
                    Style::default().fg(if snapshot.month.unknown > 0 {
                        YELLOW
                    } else {
                        MUTED
                    }),
                ),
            ]),
        ];
        frame.render_widget(
            Paragraph::new(summary)
                .block(panel().title(" inference "))
                .wrap(Wrap { trim: true }),
            sections[1],
        );
        let data: Vec<u64> = snapshot
            .minute_bins
            .iter()
            .map(|&n| n.max(0) as u64)
            .collect();
        frame.render_widget(
            Sparkline::default()
                .block(panel().title(" attempts / minute · last 60 minutes "))
                .data(&data)
                .style(Style::default().fg(PURPLE).bg(PANEL)),
            sections[2],
        );
        if !short {
            draw_recent(frame, sections[4], snapshot);
        }
        draw_accounts(frame, sections[3], app, compact);
    } else {
        frame.render_widget(
            Paragraph::new("Waiting for the server…").block(panel().title(" accounts ")),
            sections[3],
        );
    }
    let help = if compact {
        " q quit  ↑/↓ scroll  PgUp/PgDn page  ·  counts: upstream attempts"
    } else {
        " q quit  ↑/↓ or j/k scroll  PgUp/PgDn page  ·  upstream attempts; unknown tokens excluded from totals"
    };
    frame.render_widget(
        Paragraph::new(help).style(Style::default().fg(MUTED).bg(BACKGROUND)),
        sections[5],
    );
}

fn draw_accounts(
    frame: &mut ratatui::Frame<'_>,
    area: ratatui::layout::Rect,
    app: &mut App,
    compact: bool,
) {
    let snapshot = app.snapshot.as_ref().unwrap();
    app.selected = app.selected.min(snapshot.accounts.len().saturating_sub(1));
    let rows: Vec<Row<'_>> = snapshot
        .accounts
        .iter()
        .enumerate()
        .map(|(index, account)| {
            let status = match account.status {
                AccountStatus::Healthy => "healthy".to_string(),
                AccountStatus::Cooling => {
                    format!("cooling {}", time_until(account.reset_at, snapshot.now_ms))
                }
                AccountStatus::AuthError => "auth error".to_string(),
                AccountStatus::Disabled => "disabled".to_string(),
            };
            let cells = if compact {
                vec![
                    Cell::from(display_text(&account.label)),
                    Cell::from(status).style(Style::default().fg(status_color(account.status))),
                    Cell::from(account.inflight.to_string()).style(Style::default().fg(CYAN)),
                    Cell::from(account.sticky_sessions.to_string())
                        .style(Style::default().fg(PURPLE)),
                    Cell::from(quota(account, snapshot.now_ms))
                        .style(Style::default().fg(quota_color(account, snapshot.now_ms))),
                ]
            } else {
                vec![
                    Cell::from(display_text(&account.label)),
                    Cell::from(account.backend.as_str()),
                    Cell::from(status).style(Style::default().fg(status_color(account.status))),
                    Cell::from(account.inflight.to_string()).style(Style::default().fg(CYAN)),
                    Cell::from(account.sticky_sessions.to_string())
                        .style(Style::default().fg(PURPLE)),
                    Cell::from(quota(account, snapshot.now_ms))
                        .style(Style::default().fg(quota_color(account, snapshot.now_ms))),
                    Cell::from(credits(account)).style(
                        Style::default().fg(
                            if account
                                .quota
                                .as_ref()
                                .is_some_and(|q| q.available_resets > 0)
                            {
                                CYAN
                            } else {
                                MUTED
                            },
                        ),
                    ),
                    Cell::from(format!(
                        "{} · {} / {}",
                        account.month.attempts,
                        number(account.month.input_tokens),
                        number(account.month.output_tokens)
                    ))
                    .style(Style::default().fg(MUTED)),
                ]
            };
            Row::new(cells).style(Style::default().fg(TEXT).bg(if index % 2 == 0 {
                PANEL
            } else {
                Color::Rgb(25, 34, 49)
            }))
        })
        .collect();
    let (headers, widths): (Vec<&str>, Vec<Constraint>) = if compact {
        (
            vec!["ACCOUNT", "STATE", "LIVE", "SESS", "QUOTA LEFT"],
            vec![
                Constraint::Percentage(30),
                Constraint::Percentage(22),
                Constraint::Length(5),
                Constraint::Length(5),
                Constraint::Min(16),
            ],
        )
    } else {
        (
            vec![
                "ACCOUNT",
                "BACKEND",
                "STATE",
                "LIVE",
                "SESS",
                "QUOTA LEFT",
                "RESETS",
                "MONTH ATT · IN/OUT",
            ],
            vec![
                Constraint::Percentage(20),
                Constraint::Length(9),
                Constraint::Percentage(16),
                Constraint::Length(5),
                Constraint::Length(5),
                Constraint::Percentage(25),
                Constraint::Length(7),
                Constraint::Min(15),
            ],
        )
    };
    let table = Table::new(rows, widths)
        .header(Row::new(headers).style(Style::default().fg(CYAN).add_modifier(Modifier::BOLD)))
        .block(panel().title(format!(" accounts · {} ", snapshot.accounts.len())))
        .row_highlight_style(
            Style::default()
                .bg(Color::Rgb(48, 61, 84))
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("▶ ");
    let mut state = TableState::default();
    if !snapshot.accounts.is_empty() {
        state.select(Some(app.selected));
    }
    let parts = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(2)])
        .split(area);
    frame.render_stateful_widget(table, parts[0], &mut state);
    let detail = snapshot.accounts.get(app.selected).map_or_else(
        || "No accounts configured".to_string(),
        |a| account_detail(a, snapshot.now_ms),
    );
    frame.render_widget(
        Paragraph::new(detail)
            .style(Style::default().fg(MUTED).bg(PANEL))
            .block(
                Block::default()
                    .borders(Borders::TOP)
                    .border_style(Style::default().fg(BORDER)),
            )
            .wrap(Wrap { trim: true }),
        parts[1],
    );
}

fn account_detail(account: &MonitorAccount, now_ms: i64) -> String {
    let mut parts = vec![format!(
        "{}: {} month attempts · {} unknown",
        display_text(&account.label),
        account.month.attempts,
        account.month.unknown
    )];
    if let Some(snapshot) = &account.quota {
        parts.push(format!(
            "quota checked {} ago",
            time_until(now_ms, snapshot.fetched_at_ms)
        ));
        if let Some(limit) = &snapshot.rate_limit {
            for (name, window) in [
                ("primary", &limit.primary_window),
                ("secondary", &limit.secondary_window),
            ] {
                if let Some(window) = window {
                    let reset = window
                        .reset_at
                        .and_then(|value| value.checked_mul(1000))
                        .map_or("reset unknown".to_string(), |time| {
                            format!("renews in {}", time_until(time, now_ms))
                        });
                    let left = (100.0 - window.used_percent).clamp(0.0, 100.0);
                    parts.push(format!(
                        "{name} {:.0}% used / {left:.0}% left · {reset}",
                        window.used_percent
                    ));
                }
            }
        }
    }
    parts.join("  ·  ")
}

fn draw_recent(
    frame: &mut ratatui::Frame<'_>,
    area: ratatui::layout::Rect,
    snapshot: &MonitorSnapshot,
) {
    let rows: Vec<Row<'_>> = snapshot
        .recent
        .iter()
        .take(5)
        .enumerate()
        .map(|(index, request)| {
            let status_color = if request.status >= 500 {
                RED
            } else if request.status >= 400 {
                YELLOW
            } else {
                GREEN
            };
            Row::new(vec![
                Cell::from(
                    chrono::DateTime::from_timestamp_millis(request.ts)
                        .map_or("?".into(), |t| t.format("%H:%M:%S").to_string()),
                )
                .style(Style::default().fg(MUTED)),
                Cell::from(display_text(&request.model)),
                Cell::from(request.backend.clone().unwrap_or_else(|| "—".into()))
                    .style(Style::default().fg(PURPLE)),
                Cell::from(
                    request
                        .label
                        .as_deref()
                        .map_or_else(|| "—".into(), display_text),
                ),
                Cell::from(request.status.to_string()).style(
                    Style::default()
                        .fg(status_color)
                        .add_modifier(Modifier::BOLD),
                ),
                Cell::from(format!("{} ms", request.duration_ms)).style(Style::default().fg(MUTED)),
            ])
            .style(Style::default().fg(TEXT).bg(if index % 2 == 0 {
                PANEL
            } else {
                Color::Rgb(25, 34, 49)
            }))
        })
        .collect();
    let table = Table::new(
        rows,
        [
            Constraint::Length(9),
            Constraint::Percentage(26),
            Constraint::Length(9),
            Constraint::Percentage(30),
            Constraint::Length(6),
            Constraint::Min(8),
        ],
    )
    .block(panel().title(" recent outcomes "));
    frame.render_widget(table, area);
}

fn quota(account: &MonitorAccount, now_ms: i64) -> String {
    if account.backend == BackendId::Copilot {
        return "unknown".into();
    }
    let Some(snapshot) = &account.quota else {
        return "waiting…".into();
    };
    let Some(limit) = &snapshot.rate_limit else {
        return "unavailable".into();
    };
    let mut windows = Vec::new();
    for window in [&limit.primary_window, &limit.secondary_window]
        .into_iter()
        .flatten()
    {
        let length = window.limit_window_seconds.map_or("?".into(), |s| {
            if s >= 86_400 {
                format!("{}d", s / 86_400)
            } else {
                format!("{}h", s / 3_600)
            }
        });
        let left = (100.0 - window.used_percent).clamp(0.0, 100.0);
        windows.push(format!("{length} {left:.0}%"));
    }
    if windows.is_empty() {
        "unavailable".into()
    } else {
        let mut result = windows.join(" · ");
        if now_ms.saturating_sub(snapshot.fetched_at_ms) > 180_000 {
            result.push_str(" · stale");
        }
        result
    }
}

fn credits(account: &MonitorAccount) -> String {
    account
        .quota
        .as_ref()
        .map_or("—".into(), |q| q.available_resets.to_string())
}

/// @cc [owner:ghuntley,label:cli] quota-color-severity
/// Fresh Codex quota MUST use the least remaining window: green above 30%, amber above 10% through
/// 30%, red at 10% or less or when exhausted. Missing or stale quota MUST use a muted color.
fn quota_color(account: &MonitorAccount, now_ms: i64) -> Color {
    if account.backend != BackendId::Codex {
        return MUTED;
    }
    let Some(snapshot) = &account.quota else {
        return MUTED;
    };
    if now_ms.saturating_sub(snapshot.fetched_at_ms) > 180_000 {
        return MUTED;
    }
    let Some(limit) = &snapshot.rate_limit else {
        return MUTED;
    };
    let remaining = [
        limit.primary_window.as_ref(),
        limit.secondary_window.as_ref(),
    ]
    .into_iter()
    .flatten()
    .map(|window| 100.0 - window.used_percent)
    .reduce(f64::min);
    if limit.allowed == Some(false) || limit.limit_reached == Some(true) {
        return RED;
    }
    match remaining {
        Some(value) if value <= 10.0 => RED,
        Some(value) if value <= 30.0 => YELLOW,
        Some(_) => GREEN,
        None => MUTED,
    }
}

fn status_color(status: AccountStatus) -> Color {
    match status {
        AccountStatus::Healthy => GREEN,
        AccountStatus::Cooling => YELLOW,
        AccountStatus::AuthError => RED,
        AccountStatus::Disabled => MUTED,
    }
}

fn time_until(deadline: i64, now: i64) -> String {
    let seconds = (deadline.saturating_sub(now) / 1000).max(0);
    if seconds >= 3600 {
        format!("{}h{}m", seconds / 3600, seconds % 3600 / 60)
    } else if seconds >= 60 {
        format!("{}m", seconds / 60)
    } else {
        format!("{}s", seconds)
    }
}

fn number(value: i64) -> String {
    if value >= 1_000_000 {
        format!("{:.1}M", value as f64 / 1_000_000.0)
    } else if value >= 1_000 {
        format!("{:.1}k", value as f64 / 1_000.0)
    } else {
        value.to_string()
    }
}

fn display_text(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::monitor::{Counters, MonitorSnapshot};
    use crate::resets::{RateLimit, UsageSnapshot, Window};
    use ratatui::backend::TestBackend;

    #[test]
    fn quota_palette_tracks_lowest_window_and_staleness() {
        let mut account = MonitorAccount {
            id: "a".into(),
            label: "alice".into(),
            backend: BackendId::Codex,
            status: AccountStatus::Healthy,
            reset_at: 0,
            inflight: 0,
            sticky_sessions: 0,
            month: Counters::default(),
            quota: Some(UsageSnapshot {
                rate_limit: Some(RateLimit {
                    allowed: Some(true),
                    limit_reached: Some(false),
                    primary_window: Some(Window {
                        used_percent: 20.0,
                        reset_at: None,
                        limit_window_seconds: None,
                    }),
                    secondary_window: Some(Window {
                        used_percent: 75.0,
                        reset_at: None,
                        limit_window_seconds: None,
                    }),
                }),
                available_resets: 0,
                credit_expirations: vec![],
                fetched_at_ms: 1_000,
            }),
        };
        assert_eq!(quota_color(&account, 1_000), YELLOW);
        account
            .quota
            .as_mut()
            .unwrap()
            .rate_limit
            .as_mut()
            .unwrap()
            .secondary_window
            .as_mut()
            .unwrap()
            .used_percent = 95.0;
        assert_eq!(quota_color(&account, 1_000), RED);
        assert_eq!(quota_color(&account, 181_001), MUTED);
        account.backend = BackendId::Copilot;
        assert_eq!(quota_color(&account, 1_000), MUTED);
    }

    #[test]
    fn dashboard_renders_at_standard_and_small_sizes() {
        for (width, height) in [(120, 30), (80, 24), (40, 12)] {
            let mut terminal = ratatui::Terminal::new(TestBackend::new(width, height)).unwrap();
            let mut app = App {
                snapshot: Some(MonitorSnapshot {
                    now_ms: 120_000,
                    month_start_ms: 0,
                    accounts: vec![],
                    month: Counters::default(),
                    last_minute: Counters::default(),
                    minute_bins: vec![0; 60],
                    recent: vec![],
                }),
                ..Default::default()
            };
            terminal.draw(|frame| draw(frame, &mut app)).unwrap();
        }
    }

    #[test]
    fn control_characters_are_removed_from_display() {
        assert_eq!(display_text("alice\x1b[31m\n"), "alice [31m ");
    }
}
