# underclass

**A local proxy that pools multiple ChatGPT/Codex, GitHub Copilot, and Claude Code subscriptions behind one endpoint.**

```
                       ┌────────────────────────────────────┐
                       │            underclass              │
                       │                                    │
 opencode ────────────►│  /v1/responses       (OpenAI)      │────► chatgpt.com
 (any OpenAI-compatible│  /v1/chat/completions              │      (N Codex subs,
  client)              │  /v1/models                        │       OAuth device flow)
                       │                                    │────► api.githubcopilot.com
 Claude Code ─────────►│  /v1/messages        (Anthropic)    │      (M Copilot subs,
 (ANTHROPIC_BASE_URL)  │                                    │       GitHub device flow)
 web UI ◄─────────────►│  sticky sessions · health pool     │────► api.anthropic.com
 (accounts, catalog,   │  fail-fast saturation · tracing    │      (K Claude Code subs,
  live request feed)   │                                    │       OAuth paste-back)
                       └────────────────────────────────────┘
```

One subscription runs out of quota? It leaves rotation until its window resets — and comes back on its own. Sessions stay pinned to one subscription so upstream prompt caches stay warm. When *everything* is exhausted, the proxy fails fast with the earliest reset time instead of hanging.

---

## Why

Subscription-based model access has a per-account quota. One account is a ceiling; twenty accounts are a pool. underclass turns a pile of personal subscriptions into a single durable endpoint that behaves like one well-provisioned provider:

- **No client changes** — the surface is plain `/v1/*`; point opencode (or anything OpenAI-compatible) at it.
- **No quota whiplash** — exhausted accounts cool down and recover automatically; clients never see account churn.
- **No cache waste** — sessions are sticky, so the upstream prompt cache keeps working across turns.

## Quick start

```sh
UNDERCLASS_PROXY_KEY="$(openssl rand -hex 32)" \
UNDERCLASS_UI_TOKEN="$(openssl rand -hex 32)" \
cargo run -- serve
```

```
underclass listening on http://127.0.0.1:8080
web ui: http://127.0.0.1:8080/
```

Keep the generated values in a password manager or runtime secret file; underclass
never writes them to diagnostics. Open the web UI and paste the value supplied as
`UNDERCLASS_UI_TOKEN`.

1. Open the web UI and paste the configured admin token.
2. Click **Add account** → pick *ChatGPT / Codex*, *GitHub Copilot*, or *Claude Code (Anthropic)* → follow the flow. The account is labeled automatically with the account's email or username.
3. Repeat for every subscription you want in the pool.
4. Point opencode at the pool:

```sh
cargo run -- connect
opencode --provider underclass --model underclass/gpt-5.5
```

`underclass connect` writes the provider block and credentials into your global opencode config (`~/.config/opencode/opencode.json{,c}` + `auth.json`), idempotently and with backups. Re-run it any time; `--remove` undoes it.

5. Point Claude Code at the same pool (it speaks the Anthropic surface):

```sh
export ANTHROPIC_BASE_URL=http://127.0.0.1:8080
export CLAUDE_CODE_OAUTH_TOKEN=<the underclass proxy key>
```

`CLAUDE_CODE_OAUTH_TOKEN` is what the official client uses when an auth-injecting proxy sits in front of it: the client keeps its Claude Code request shape and sends the proxy key as its bearer token, and underclass replaces that with the pooled subscription credential.

## How routing works

- **Sticky sessions.** Requests carrying `prompt_cache_key` / `promptCacheKey` (opencode sends the session ID when configured with `setCacheKey: true`) always land on the same subscription. Bindings live for 24h, survive restarts, and rebind preferentially within the same backend when an account cools.
- **Health pool.** A quota response moves an account to *cooling* until the upstream deadline or a per-backend default. Codex uses the later valid deadline from its usage-limit error and `retry-after`, and its account usage poll can extend cooldown to a later exhausted-window reset or readmit an account when upstream reports it allowed. 401s trigger one token refresh + retry, then the account needs re-login. Cooling accounts stay configured and return to rotation automatically.
- **Codex usage and banked resets.** The admin page shows per-account usage windows and available reset credits. When a live request finds every enabled ChatGPT/Codex account unavailable, underclass can redeem one banked reset, choosing the account that would otherwise wait longest for natural quota recovery. This is enabled by default; set `auto_codex_resets = false` in `~/.config/underclass/config.toml` or `UNDERCLASS_AUTO_CODEX_RESETS=false` to disable automatic redemption. To spend a credit by hand, click **Use reset** on the account row in the admin page or run `underclass reset <email-or-id>` against the running server (it uses the same admin token resolution as `underclass top`; pass `--url` plus `UNDERCLASS_UI_TOKEN` for a remote server). The manual path redeems exactly one credit for that account, refuses when upstream reports the account is not rate limited or a reset already redeemed for this outage has not restored usage, and `--force` overrides both refusals. To refresh credentials on demand (for example after an outage), click **Refresh token** on the account row or run `underclass refresh <email-or-id>` (or `underclass refresh --all`). A successful refresh returns an `auth_error` account to rotation; a failed one changes nothing, so the account still needs **Re-login** (see [ADR 0026](docs/adr/0026-manual-token-refresh.md)). Claude Code accounts have banked credits too; redeeming one is always manual, because it spends your own subscription.
- **Token accounting.** Each upstream attempt is recorded in SQLite with its model, account, prompt cache key, and reported input/output tokens. Missing counts remain marked unknown. The admin page shows current-month totals and filters; historical rows remain queryable after account removal.
- **Stream outcomes and statistics.** A 2xx stream can still fail upstream (e.g. `response.failed` with `server_is_overloaded`, which Codex shows as "Selected model is at capacity"). underclass records how every body ended (`completed`, `failed`, `incomplete`, `truncated`, `upstream_error`, `client_closed`), the sanitized error code, and the attempt duration. It logs `stream.finished` at warn for failures, and records capacity, policy (`cyber_policy`), quota, and saturation events. `underclass stats` and the admin page's Statistics panel report requests, sessions, tokens, peak and mean concurrency, outcomes, and events over up to two years. See [ADR 0021](docs/adr/0021-stream-outcome-and-capacity-statistics.md).
- **Fail fast.** If every account eligible for the requested model is cooling, the proxy answers `429` + `Retry-After` = earliest reset. No queuing.
- **Flat pool.** Codex and Copilot accounts compete by least-in-flight, filtered by per-backend model catalogs. Unknown model IDs pass through to Codex so new models work without proxy changes.
- **Two surfaces, one pool.** `/v1/responses` and `/v1/chat/completions` speak OpenAI; `/v1/messages` speaks the Anthropic Messages API. A request is only ever routed to a backend that serves its surface, so an Anthropic request never lands on a Codex account and the other way round. See [ADR 0024](docs/adr/0024-surface-partitioned-routing.md).
- **Claude Code subscriptions.** The Anthropic surface serves the official Claude Code client only: the request must carry `x-app: cli`, a `claude-cli/` user agent, the `claude-code-20250219` beta, and a Claude Code identity block, and underclass forwards the client's headers and body verbatim, swapping only the credential. Anthropic's subscription quota is window-based (5-hour session, 7-day weekly, and a weekly window scoped to one model family), and underclass reads all of them from the account's own usage API, so an exhausted account leaves rotation exactly when its window resets — and a limit that only affects one model family leaves that account serving its other models. The admin page shows the plan, the three windows, and the banked reset credits; **Use reset** redeems exactly one of those credits when you ask for it (never automatically, because a credit is your own subscription). See [ADR 0023](docs/adr/0023-claude-code-subscription-backend.md).
- **Failover only before output.** A stream that fails with an upstream capacity error before any output is retried on another account. Once output starts — or, on the Anthropic surface, once the upstream has accepted the request — upstream errors pass through, so half-finished turns are never re-sent silently.

Design decisions and their trade-offs live in [`docs/adr/`](docs/adr) — start with [ADR 0010](docs/adr/0010-unprefixed-routing-flat-pool.md) for the routing model.

## Configuration

Optional `~/.config/underclass/config.toml`:

| key | default | meaning |
|---|---|---|
| `bind` | `127.0.0.1:8080` | listen address |
| `proxy_key` | minted on first run | bearer key clients must send to `/v1/*` |
| `ui_token` | minted on first run | admin token for the web UI + `/admin/api/*` |
| `codex_cooldown_secs` | `1800` | cooling window when upstream omits `retry-after` |
| `copilot_cooldown_secs` | `1800` | same, for Copilot |
| `claude_cooldown_secs` | `1800` | same, for Claude Code accounts |
| `stats_retention_days` | `730` | how long usage rows, upstream events, and daily stats rollups are kept |

Environment overrides: `UNDERCLASS_BIND`, `UNDERCLASS_PROXY_KEY`, `UNDERCLASS_UI_TOKEN`, `UNDERCLASS_MONITOR_SOCKET` (local socket path), `UNDERCLASS_STATS_RETENTION_DAYS`. For testing against a mock upstream: `UNDERCLASS_CODEX_UPSTREAM`, `UNDERCLASS_COPILOT_UPSTREAM`, `UNDERCLASS_CLAUDE_UPSTREAM` (default to the real endpoints).

State (credentials, sticky bindings, model catalog, minted keys) lives in `~/.local/share/underclass/pool.db`. Delete it to start fresh.

## CLI

```
underclass serve [--bind ADDR]
underclass top [--url HTTP_OR_HTTPS_ORIGIN]
underclass stats [--since 24h|7d|90d|2y] [--bucket minute|hour|day] [--by total|model|account] [--json] [--url ORIGIN]
underclass stats import-codex [--dir DIR]... [--dry-run] [--url ORIGIN]
underclass connect [--base-url URL] [--api-key KEY] [--model MODEL]
                   [--project] [--no-default-model] [--dry-run] [--remove]
```

`stats` prints a UTC table with these columns: requests, sessions, tokens in/out, peak/mean concurrency, failed streams, and overloaded / policy / quota / cooling / saturated events. It uses the same admin token resolution as `top`. `stats import-codex` backfills capacity, quota, retry-exhaustion, and policy errors (such as `cyber_policy`) from `$CODEX_HOME/sessions` rollout logs. It sends only timestamps, kinds, codes, models, and dedupe keys, and is safe to re-run.

`connect` targets the global opencode config by default; `--project` writes `./.opencode/opencode.json` instead. `--dry-run` prints the merged documents without writing.

Run `underclass top` (or `utop` from the Nix package) in a second terminal while `underclass serve` is running for a continuously updating, read-only pool monitor. It shows live in-flight requests, a 60-minute attempt chart, current UTC month token accounting, account status and sticky sessions, quota windows and banked resets, and recent outcomes. Each account has a PLAN column showing the plan the upstream reports (the ChatGPT plan for Codex accounts, `pro`/`max`/`team`/`enterprise` for Claude Code accounts); missing plan metadata shows unknown and Copilot shows —. Plan metadata updates with token rotation. Account states and quota levels are color coded and also labeled in text. Press `q` to quit; use arrow keys, `j`/`k`, or Page Up/Down to scroll accounts. The dashboard reconnects if the server becomes unavailable. Locally it uses a Unix socket without a token; the NixOS service exposes `/run/underclass/monitor.sock` to local users. The socket serves only the monitor snapshot, which includes account labels and usage. If no socket is available, `top` can use the local admin token from `UNDERCLASS_UI_TOKEN`, config, or the existing database. For another server, pass `--url` and set `UNDERCLASS_UI_TOKEN` in the environment. The browser UI still requires its admin token. Copilot quota availability is shown as unknown because underclass has no Copilot quota snapshot. Counts are upstream attempts; token totals omit unknown usage.

![utop showing a five-account Codex pool: live inference counts, a 60-minute attempts chart, per-account state, quota left and resets, and recent request outcomes](docs/images/utop.png)

## Endpoints

| route | auth | purpose |
|---|---|---|
| `POST /v1/responses` | proxy key | Responses API, streamed through to the pool |
| `POST /v1/chat/completions` | proxy key | Chat Completions, same |
| `POST /v1/messages` | proxy key | Anthropic Messages API (Claude Code clients) |
| `POST /v1/messages/count_tokens` | proxy key | Anthropic token counting, same |
| `POST /messages` | proxy key | Messages API alias, for clients whose base URL already ends in `/v1` |
| `GET /v1/models` | proxy key | union catalog with merged limits |
| `GET /` | none | web UI |
| `GET /admin/api/state` | admin token | accounts, catalog, last 200 requests |
| `GET /admin/api/usage` | admin token | token totals grouped by `group_by=model,account_id,cache_key` (any subset) |
| `GET /admin/api/usage/requests` | admin token | paginated per-attempt accounting rows |
| `GET /admin/api/monitor` | admin token | compact read-only live monitor snapshot |
| `GET /admin/api/stats` | admin token | bucketed requests, sessions, tokens, concurrency, outcomes, events (`since`, `bucket`, `by`) |
| `POST /admin/api/events/import` | admin token | idempotent upstream-event backfill (used by `stats import-codex`) |
| `POST /admin/api/flows` | admin token | start a device-flow onboarding |
| `GET /admin/api/flows/{id}` | admin token | poll an onboarding flow |
| `POST /admin/api/flows/{id}/code` | admin token | finish a Claude Code paste-back flow |
| `POST /admin/api/accounts/{id}/enable|disable|relogin` | admin token | account controls |
| `DELETE /admin/api/accounts/{id}` | admin token | remove from pool |
| `GET|PUT /admin/api/catalog/{backend}` | admin token | inspect/edit model catalog |
| `GET /admin/api/client-key` | admin token | retrieve the proxy key for `connect` |

Every response carries `x-request-id`; logs are JSON (`RUST_LOG` filters, `--log-format json|pretty`) and each request logs the account (label) that served it.

Accounting queries accept `from_ms` (inclusive), `to_ms` (exclusive), and exact `model`, `account_id`, or `cache_key` filters. Use `missing_key=true` to select requests without a key. Grouped summaries and request details accept `limit` (1–1000, default 100) and `offset`; summary responses also include unpaginated `totals`. With no time bounds, queries default to the current UTC month; provide `from_ms=0` to query all history. Each summary includes measured and unknown request counts. Token totals sum reported values only; they are lower bounds when unknown requests exist. See [ADR 0014](docs/adr/0014-per-attempt-token-accounting.md).

### Correlation with preflight

Clients send a UUIDv7 as `x-request-id`. Underclass preserves that ID in its response, routing/retry logs, and request history. Search the same `request_id` across the proxy chain to trace pool routing. Clients such as preflight must adopt UUIDv7 to retain correlation.

Underclass accepts one hyphenated RFC9562 UUIDv7 and normalizes it to lowercase. Missing, duplicate, invalid, nil, UUIDv4, and other-version IDs are replaced before authentication or logging. Every handled response receives the resulting UUIDv7, including 401s, 429s, model discovery, and unmatched routes. Provider responses cannot replace it. Correlation IDs convey no authorization or proof that another proxy inspected a request. See [ADR 0027](docs/adr/0027-uuidv7-identities-and-secret-randomness.md).

New account, flow, and redemption identifiers also use UUIDv7. Existing account identifiers, pending redemption IDs, stored credentials, and history remain unchanged. New proxy/admin secrets and OAuth PKCE material use independent operating-system randomness, never UUID bytes.

An exhausted chain of proven `server_is_overloaded` Responses stream failures may carry `x-underclass-retry-safe: pre-output-overload`. Underclass issues this only when every attempt failed before output and every observed event supports that conclusion. Generic failures, malformed/unknown events, output, transport loss, and other error codes prevent certification. Saved upstream error bodies carry no independent retry authority. The certificate requires a trusted authenticated connection to Underclass; it does not authorize unbounded retries or replay of ambiguous requests. See [ADR 0028](docs/adr/0028-proven-overload-retry-certificate.md).

## Models

The catalog is data, not code: seeded with the Codex families (`gpt-5.4`, `gpt-5.4-mini`, `gpt-5.3-codex-spark`, `gpt-5.5`, `gpt-5.6-sol`, `gpt-5.6-terra`, `gpt-5.6-luna`, `gpt-6-astra`, `gpt-6-sol`), Copilot's live `/models` list, and the Claude families (`claude-haiku-5-5`, `claude-sonnet-5-5`, `claude-opus-5-5`, `claude-fable-5-1`, and the 5.x/4.x generations), refreshed live from Anthropic's `/v1/models` when a Claude account is added. Edit it in the UI or via the admin API; routing eligibility and the opencode model block follow it. Claude models are not part of the opencode provider block: the Anthropic surface is not OpenAI-shaped. See [ADR 0007](docs/adr/0007-config-driven-model-catalog.md).

## Nix flake

The repository is a Nix flake: it exposes the CLI as a package/app and the devenv development shell as `devShells.default`.

Run the proxy without installing:

```sh
nix run github:ghuntley/underclass -- serve
```

Install it into your profile:

```sh
nix profile install github:ghuntley/underclass
utop
```

The installed package puts both `underclass` and `utop` on your PATH. `utop` runs
`underclass top` and accepts the same `--url` option. You can also run it without
installing with `nix run github:ghuntley/underclass#utop`. Enabling the NixOS
service with its default package also adds both commands to the host PATH. `utop`
connects to the service's local monitor socket without a token.

Use the devenv shell (Rust toolchain, cargo) for development:

```sh
nix develop --no-pure-eval
cargo test
```

`--no-pure-eval` is required for the devenv shell (devenv inspects the working directory; this matches devenv's own flake template). The package and app outputs are pure — `nix run` and `nix profile install` need no flags.

Long-term devenv users can keep using `devenv shell` / `devenv test` directly — `nix develop` and `devenv shell` activate the same `devenv.nix`.

### NixOS module

Add underclass to your flake inputs and import its module:

```nix
{
  inputs.underclass.url = "github:ghuntley/underclass";

  outputs =
    { nixpkgs, underclass, ... }:
    {
      nixosConfigurations.my-host = nixpkgs.lib.nixosSystem {
        system = "x86_64-linux";
        modules = [
          underclass.nixosModules.default
          {
            services.underclass = {
              enable = true;
              bindAddress = "127.0.0.1:8080";
              environmentFile = "/run/secrets/underclass.env";
              settings = {
                codex_cooldown_secs = 1800;
                copilot_cooldown_secs = 1800;
              };
            };
          }
        ];
      };
    };
}
```

The runtime environment file can provide credentials without placing them in the Nix store:

```sh
UNDERCLASS_PROXY_KEY=sk-underclass-...
UNDERCLASS_UI_TOKEN=...
```

The service uses a dynamic user, persists its database in `/var/lib/underclass`, binds to localhost by default, and leaves the firewall closed. Set `services.underclass.openFirewall = true` only when intentionally binding beyond localhost.

The flake also exports `overlays.default`. Validate the module and its QEMU machine test with:

```sh
nix build .#checks.x86_64-linux.underclass-module
nix build .#checks.x86_64-linux.underclass-vm
```

Notes:

- The package builds from the committed `Cargo.lock`; dependency versions are pinned there.
- `nix build` skips `cargo test` because the property tests compile the Hegel engine as a build step, which needs network access that the Nix sandbox denies. CI runs the full suite via devenv (see `.github/workflows/ci.yml`).

## Security

- Access/refresh tokens, authorization headers, and prompt bodies are **never** logged.
- Account labels (email/username) appear in logs and the UI by design; raw UUID account IDs are truncated.
- `auth.json` written by `connect` uses `0600`. Never commit `pool.db` or `*.bak`.
- The proxy binds to localhost by default; put it behind a tunnel only if you understand the exposure.
- OAuth tokens rotate: every Codex refresh persists the new refresh token immediately, and so does every Claude Code refresh.
- **Anthropic's terms restrict subscription OAuth.** As of February 2026 Anthropic states that subscription OAuth is intended exclusively for Claude Code and Claude.ai, that third parties may not collect, store, or intermediate those credentials, and that using them in any other product or service violates the Consumer Terms of Service. Anthropic enforces this server-side and can invalidate a credential without notice. underclass's Claude Code support is therefore local-only, serves the official client exclusively, and stores the credential in the pool database. See [ADR 0023](docs/adr/0023-claude-code-subscription-backend.md) for the measured behaviour and the full risk statement.

## Development

```sh
cargo build
cargo test        # 124 unit tests + 24 Hegel property tests + e2e suite
```

Testing is two-tier: plain unit tests for exact behavior (headers, merges, redaction), and [Hegel](https://hegel.dev) property tests over the pure pool core — stickiness stability, health-state invariants, saturation minimums, TTL/cap bounds. The core (`src/pool.rs`, `src/health.rs`) is synchronous with an injected clock; async lives only at the edges. New backends implement the `provider::Backend` trait and register — nothing else changes.

Agent conventions and the ADR policy are in [`AGENTS.md`](AGENTS.md). Architecture decision records: [`docs/adr/`](docs/adr).

## License

[MIT](LICENSE)

## Project layout

```
src/
  main.rs      bootstrap, router, background tasks
  config.rs    TOML + env config
  models.rs    domain types (Account, status, catalog, log entries)
  store.rs     SQLite persistence (accounts, bindings, catalog, config)
  pool.rs      pure pool core: stickiness, eligibility, selection
  health.rs    quota classification, retry-after parsing
  provider.rs  Backend trait
  codex.rs     ChatGPT/Codex backend (device flow, refresh, headers)
  copilot.rs   GitHub Copilot backend (device flow, catalog, headers)
  claude.rs    Claude Code backend (PKCE paste-back flow, Anthropic surface)
  tokens.rs    single-flight token refresh
  proxy.rs     /v1/* handlers, failover, streaming
  usage.rs     token + stream-outcome tap over upstream bodies
  stats.rs     pure stats core: concurrency sweep, bucketing, rollup merge
  stats_api.rs stats query, event import, retention/rollup maintenance
  stats_cli.rs `stats` verb and Codex rollout import
  logging.rs   structured logs, correlation IDs, redaction
  ui.rs        admin API
  ui.html      embedded single-page web UI
  cli.rs       `connect` verb, JSONC-safe opencode config merge
tests/
  properties.rs  Hegel property tests over the pure core
  e2e.rs         full-pool story against a mock upstream
```
