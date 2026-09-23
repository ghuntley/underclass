use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct FileConfig {
    pub bind: Option<String>,
    pub proxy_key: Option<String>,
    pub ui_token: Option<String>,
    pub codex_cooldown_secs: Option<u64>,
    pub copilot_cooldown_secs: Option<u64>,
    pub copilot_cooldown_ms: Option<u64>,
    pub codex_cooldown_ms: Option<u64>,
    pub auto_codex_resets: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub bind: String,
    pub proxy_key: Option<String>,
    pub ui_token: Option<String>,
    pub codex_cooldown_ms: i64,
    pub auto_codex_resets: bool,
    pub copilot_cooldown_ms: i64,
    pub data_dir: PathBuf,
    #[allow(dead_code)]
    pub config_dir: PathBuf,
}

pub const DEFAULT_COOLDOWN_MS: i64 = 30 * 60 * 1000;
pub const SYSTEM_MONITOR_SOCKET: &str = "/run/underclass/monitor.sock";

fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/"))
}

pub fn default_data_dir() -> PathBuf {
    home_dir().join(".local/share/underclass")
}

pub fn default_config_dir() -> PathBuf {
    home_dir().join(".config/underclass")
}

impl Config {
    pub fn load() -> Self {
        let config_dir = std::env::var_os("UNDERCLASS_CONFIG_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(default_config_dir);
        let data_dir = std::env::var_os("UNDERCLASS_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(default_data_dir);

        let mut file_cfg = FileConfig::default();
        let config_path = config_dir.join("config.toml");
        if let Ok(text) = std::fs::read_to_string(&config_path) {
            if let Ok(parsed) = toml::from_str::<FileConfig>(&text) {
                file_cfg = parsed;
            } else {
                eprintln!(
                    "warning: could not parse {}, using defaults",
                    config_path.display()
                );
            }
        }

        let bind = std::env::var("UNDERCLASS_BIND")
            .ok()
            .or(file_cfg.bind)
            .unwrap_or_else(|| "127.0.0.1:8080".to_string());
        let proxy_key = std::env::var("UNDERCLASS_PROXY_KEY")
            .ok()
            .or(file_cfg.proxy_key);
        let ui_token = std::env::var("UNDERCLASS_UI_TOKEN").ok().or(file_cfg.ui_token);
        let codex_cooldown_ms = file_cfg
            .codex_cooldown_ms
            .or(file_cfg.codex_cooldown_secs.map(|s| s * 1000))
            .unwrap_or(DEFAULT_COOLDOWN_MS as u64) as i64;
        let copilot_cooldown_ms = file_cfg
            .copilot_cooldown_ms
            .or(file_cfg.copilot_cooldown_secs.map(|s| s * 1000))
            .unwrap_or(DEFAULT_COOLDOWN_MS as u64) as i64;

        let auto_codex_resets = std::env::var("UNDERCLASS_AUTO_CODEX_RESETS")
            .ok()
            .and_then(|value| value.parse::<bool>().ok())
            .or(file_cfg.auto_codex_resets)
            .unwrap_or(true);

        Self {
            bind,
            proxy_key,
            ui_token,
            codex_cooldown_ms,
            auto_codex_resets,
            copilot_cooldown_ms,
            data_dir,
            config_dir,
        }
    }

    pub fn db_path(&self) -> PathBuf {
        self.data_dir.join("pool.db")
    }

    /// @cc [owner:ghuntley,label:cli] monitor-socket-location
    /// The server MUST honor a nonempty `UNDERCLASS_MONITOR_SOCKET` and otherwise place its local
    /// monitor socket beside the SQLite database.
    pub fn monitor_socket_path(&self) -> PathBuf {
        std::env::var_os("UNDERCLASS_MONITOR_SOCKET")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| self.data_dir.join("monitor.sock"))
    }
}
