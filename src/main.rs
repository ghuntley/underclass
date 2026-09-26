use underclass::provider::BackendMap;
use underclass::{cli, codex, config, copilot, flows, logging, models, monitor, pool, proxy, store, tokens, top, ui};

use clap::{Parser, Subcommand};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::Path;

/// How often the scheduled rotation loop re-evaluates which refresh tokens are due. The rotation
/// window itself is `tokens::ROTATION_INTERVAL_MS`; this only bounds how late a due token can be,
/// so a coarse tick keeps the 24h cadence cheap without pinning a timer to the exact deadline.
const ROTATION_TICK: std::time::Duration = std::time::Duration::from_secs(900);

#[derive(Parser)]
#[command(name = "underclass", about = "pooled multi-subscription codex/copilot proxy for opencode")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
    #[arg(long, value_enum, default_value = "auto", global = true)]
    log_format: logging::LogFormat,
}

#[derive(Subcommand)]
enum Command {
    /// run the proxy server
    Serve {
        #[arg(long)]
        bind: Option<String>,
    },
    /// configure opencode to use this pool
    Connect {
        #[clap(flatten)]
        args: cli::ConnectArgs,
    },
    /// watch live pool activity in a read-only terminal dashboard
    Top {
        #[arg(long, help = "server URL (requires UNDERCLASS_UI_TOKEN)")]
        url: Option<String>,
    },
}

fn main() {
    let cli = Cli::parse();
    if !matches!(cli.command, Some(Command::Top { .. })) {
        logging::init(cli.log_format);
    }
    match cli.command {
        Some(Command::Connect { args }) => {
            let cfg = config::Config::load();
            let store = store::Store::open(&cfg.db_path()).expect("open pool db");
            if let Err(e) = cli::connect(&args, &store) {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
        Some(Command::Serve { bind }) => serve(bind).expect("server failed"),
        Some(Command::Top { url }) => {
            if let Err(e) = top::run(url) {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
        None => serve(None).expect("server failed"),
    }
}

fn serve(bind_override: Option<String>) -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async_serve(bind_override))
}

/// @cc [owner:ghuntley,label:security] keys-minted-once
/// The proxy API key and admin UI token MUST be minted on first run, persisted to the store, and
/// reused on every subsequent start; an explicitly configured key/token MUST take precedence over
/// minted ones. Key and token values MUST NOT be written to diagnostics.
async fn async_serve(bind_override: Option<String>) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = config::Config::load();
    let store = Arc::new(store::Store::open(&cfg.db_path())?);
    store.prune_bindings(
        models::now_ms(),
        pool::BINDING_TTL_MS,
        pool::DEFAULT_BINDING_CAP,
    );

    let proxy_key = match cfg.proxy_key.clone() {
        Some(key) if !key.is_empty() => Some(key),
        _ => match store.config_get("proxy_key") {
            Some(key) => Some(key),
            None => {
                let key = format!("sk-underclass-{}", crate::models::new_id().replace('-', ""));
                store.config_set("proxy_key", &key);
                Some(key)
            }
        },
    };

    let ui_token = match cfg.ui_token.clone() {
        Some(token) if !token.is_empty() => token,
        _ => match store.config_get("ui_token") {
            Some(token) => token,
            None => {
                let token = crate::models::new_id().replace('-', "");
                store.config_set("ui_token", &token);
                token
            }
        },
    };

    seed_catalog(&store, models::BackendId::Codex, codex::default_catalog());
    seed_catalog(&store, models::BackendId::Copilot, copilot::fallback_catalog());

    let core = pool::PoolCore::new(&store);
    let pool = Arc::new(Mutex::new(core));

    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .build()?;

    let tokens = Arc::new(tokens::TokenManager::new(store.clone(), client.clone()));

    let mut backends: BackendMap = HashMap::new();
    backends.insert(
        models::BackendId::Codex,
        Arc::new(codex::CodexBackend {
            cooldown_ms: cfg.codex_cooldown_ms,
        }),
    );
    backends.insert(
        models::BackendId::Copilot,
        Arc::new(copilot::CopilotBackend {
            cooldown_ms: cfg.copilot_cooldown_ms,
        }),
    );
    let backends = Arc::new(backends);

    let state = Arc::new(proxy::AppState {
        store: store.clone(),
        pool: pool.clone(),
        tokens: tokens.clone(),
        backends: backends.clone(),
        client: client.clone(),
        logs: Arc::new(Mutex::new(std::collections::VecDeque::new())),
        flows: flows::FlowRegistry::default(),
        proxy_key,
        ui_token: ui_token.clone(),
        resets: Arc::new(underclass::resets::ResetManager::new(
            client.clone(),
            std::env::var("UNDERCLASS_CODEX_USAGE_BASE")
                .unwrap_or_else(|_| "https://chatgpt.com/backend-api".to_string()),
            cfg.auto_codex_resets,
        )),
        stream_usage_unsupported: Mutex::new(Default::default()),
    });

    {
        let store = store.clone();
        let state = state.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                store.prune_bindings(
                    models::now_ms(),
                    pool::BINDING_TTL_MS,
                    pool::DEFAULT_BINDING_CAP,
                );
                state.pool.lock().unwrap().sync_from_store(&store);
            }
        });
    }

    refresh_copilot_catalogs_on_boot(&state).await;
    refresh_identities_on_boot(&state).await;
    {
        let state = state.clone();
        tokio::spawn(async move {
            loop {
                rotate_due_tokens(&state).await;
                tokio::time::sleep(ROTATION_TICK).await;
            }
        });
    }
    {
        let state = state.clone();
        tokio::spawn(async move {
            loop {
                for account in state.store.list_accounts() {
                    state.resets.poll_account(&account, &state.tokens, &state.pool, &state.store).await;
                }
                tokio::time::sleep(std::time::Duration::from_secs(120)).await;
            }
        });
    }

    let bind_addr = bind_override.unwrap_or_else(|| cfg.bind.clone());
    let v1 = axum::Router::new()
        .route("/models", axum::routing::get(proxy::models))
        .route("/responses", axum::routing::post(proxy::infer))
        .route("/chat/completions", axum::routing::post(proxy::infer))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            proxy::require_proxy_key,
        ))
        .with_state(state.clone());

    // The local socket serves only the read-only monitor snapshot. Network admin routes
    // retain their token middleware.
    let local_app = axum::Router::new()
        .route("/monitor", axum::routing::get(monitor::snapshot))
        .with_state(state.clone());

    let app = axum::Router::new()
        .route("/admin/api/state", axum::routing::get(ui::state))
        .route("/admin/api/usage", axum::routing::get(ui::usage_summary))
        .route("/admin/api/usage/requests", axum::routing::get(ui::usage_requests))
        .route("/admin/api/monitor", axum::routing::get(monitor::snapshot))
        .route("/admin/api/flows", axum::routing::post(ui::start_flow))
        .route("/admin/api/flows/{id}", axum::routing::get(ui::flow_status))
        .route(
            "/admin/api/accounts/{id}",
            axum::routing::delete(ui::delete_account),
        )
        .route(
            "/admin/api/accounts/{id}/disable",
            axum::routing::post(ui::disable_account),
        )
        .route(
            "/admin/api/accounts/{id}/enable",
            axum::routing::post(ui::enable_account),
        )
        .route(
            "/admin/api/accounts/{id}/relogin",
            axum::routing::post(ui::relogin_account),
        )
        .route(
            "/admin/api/catalog/{backend}",
            axum::routing::get(ui::get_catalog).put(ui::put_catalog),
        )
        .route(
            "/admin/api/catalog/refresh-copilot",
            axum::routing::post(ui::refresh_copilot_catalog),
        )
        .route("/admin/api/client-key", axum::routing::get(ui::client_key))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            ui::require_ui_token,
        ))
        .route("/", axum::routing::get(ui::index))
        .nest("/v1", v1)
        .layer(axum::middleware::from_fn(underclass::correlation::middleware))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    let monitor_socket = bind_monitor_socket(&cfg.monitor_socket_path())?;
    println!("underclass listening on http://{bind_addr}");
    println!("web ui: http://{bind_addr}/");
    tokio::try_join!(axum::serve(listener, app), axum::serve(monitor_socket, local_app))?;
    Ok(())
}

/// @cc [owner:ghuntley,label:security] local-monitor-socket
/// The local monitor socket MUST be a Unix socket with mode 0666 so local users can read its
/// limited monitor endpoint. A stale socket MAY be replaced only when no server accepts it;
/// non-socket paths and live sockets MUST remain untouched.
fn bind_monitor_socket(path: &Path) -> std::io::Result<tokio::net::UnixListener> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let listener = match tokio::net::UnixListener::bind(path) {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
            let metadata = path.symlink_metadata()?;
            if !metadata.file_type().is_socket() {
                return Err(error);
            }
            match std::os::unix::net::UnixStream::connect(path) {
                Err(connect_error) if connect_error.kind() == std::io::ErrorKind::ConnectionRefused => {
                    std::fs::remove_file(path)?;
                    tokio::net::UnixListener::bind(path)?
                }
                _ => return Err(error),
            }
        }
        Err(error) => return Err(error),
    };
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o666))?;
    Ok(listener)
}

#[cfg(test)]
mod monitor_socket_tests {
    use super::bind_monitor_socket;

    #[tokio::test]
    async fn keeps_live_socket_and_recovers_stale_socket() {
        let dir = std::env::temp_dir().join(format!("underclass-monitor-{}", uuid::Uuid::new_v4()));
        let path = dir.join("monitor.sock");
        let first = bind_monitor_socket(&path).unwrap();
        assert!(bind_monitor_socket(&path).is_err());
        drop(first);
        let second = bind_monitor_socket(&path).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o666);
        drop(second);
        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir(dir).unwrap();
    }

    use std::os::unix::fs::PermissionsExt;
}

fn seed_catalog(store: &store::Store, backend: models::BackendId, defaults: Vec<models::ModelInfo>) {
    if store.catalog(backend).is_empty() {
        store.set_catalog(backend, &defaults);
    }
}

async fn refresh_copilot_catalogs_on_boot(state: &Arc<proxy::AppState>) {
    let accounts: Vec<models::Account> = state
        .store
        .list_accounts()
        .into_iter()
        .filter(|a| a.backend == models::BackendId::Copilot)
        .collect();
    for account in accounts {
        let Some(token) = account.refresh_token.clone() else {
            continue;
        };
        if let Ok(catalog) = copilot::fetch_catalog(&state.client, &account, &token).await {
            if !catalog.is_empty() {
                state.store.set_catalog(models::BackendId::Copilot, &catalog);
                let ids: Vec<String> = catalog.iter().map(|m| m.id.clone()).collect();
                state.pool.lock().unwrap().set_catalog(models::BackendId::Copilot, ids);
            }
        }
    }
}

fn is_generic_label(label: &str) -> bool {
    matches!(label, "chatgpt" | "github") || label.trim().is_empty()
}

/// @cc [owner:ghuntley,label:auth] scheduled-rotation-never-bricks-account
/// `rotate_due_tokens` MUST call `force_refresh` for every account reported due by
/// `tokens::rotation_due`, and MUST NOT change any account's status, pool state, or cooldown for any
/// outcome, including failure. A failed rotation MUST be logged and left for a later tick to retry
/// rather than reported as `auth_error`, so a transient issuer outage cannot remove a working
/// subscription from the pool.
async fn rotate_due_tokens(state: &Arc<proxy::AppState>) {
    let now = models::now_ms();
    for account in state.store.list_accounts() {
        if !tokens::rotation_due(&account, now) {
            continue;
        }
        match state.tokens.force_refresh(&account.id).await {
            Ok(_) => logging::log_token_rotation(&account.id, &account.label, "rotated", "scheduled"),
            Err(e) => logging::log_token_rotation(
                &account.id,
                &account.label,
                "rotation_failed",
                &e.to_string(),
            ),
        }
    }
}

async fn refresh_identities_on_boot(state: &Arc<proxy::AppState>) {
    let accounts: Vec<models::Account> = state
        .store
        .list_accounts()
        .into_iter()
        .filter(|a| is_generic_label(&a.label))
        .collect();
    for account in accounts {
        let identity = match account.backend {
            models::BackendId::Codex => {
                let token = match state.tokens.access_token(&account).await {
                    Ok(token) => token,
                    Err(_) => continue,
                };
                codex::identity_from_token(&token)
            }
            models::BackendId::Copilot => {
                let Some(token) = account.refresh_token.clone() else {
                    continue;
                };
                copilot::fetch_github_identity(&state.client, &token).await
            }
        };
        let Some(identity) = identity else {
            continue;
        };
        let mut updated = account.clone();
        updated.label = identity;
        updated.updated_at = models::now_ms();
        state.store.upsert_account(&updated);
        state.pool.lock().unwrap().sync_account(updated);
        tracing::info!(account = %account.id, "account.identity_refreshed");
    }
}
