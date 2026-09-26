# AGENTS.md

Guidance for AI coding agents (and humans) working in this repository.

## What this is

`underclass` is a local LLM proxy that pools multiple ChatGPT/Codex and GitHub Copilot subscriptions behind one OpenAI-compatible endpoint (`/v1/responses`, `/v1/chat/completions`, `/v1/models`). It keeps sessions sticky per subscription via the prompt cache key, takes quota-exhausted subscriptions out of rotation until their window resets, and fails fast with `429` + earliest reset time when the whole pool is exhausted. Architecture decisions live in `docs/adr/` - read `docs/adr/0010-unprefixed-routing-flat-pool.md` first for the core model.

## Commands

```sh
cargo build                 # build
cargo test                  # unit tests + Hegel property tests (both must pass)
cargo run -- serve          # run the proxy (default 127.0.0.1:8080)
cargo run -- connect        # configure opencode to use the pool (global config)
cargo run -- connect --dry-run
cargo run -- connect --remove
```

## Testing policy (enforced)

1. **Unit tests** (`#[test]`, in-module) cover exact behavior: header wiring, URL rewriting, JSONC merge, redaction, retry-after parsing.
2. **Property tests** use [Hegel](https://hegel.dev) (`hegeltest`, pinned) in `tests/properties.rs`. Invariants over the pure pool core: stickiness stability, health state machine, saturation minimums, TTL/cap bounds. New pure-logic modules must ship properties.
3. **No mutation testing.** Considered in ADR 0002 and rejected; do not add cargo-mutants.

## Architecture rules

- **Pure sync core, async at the edges.** `pool::PoolCore` and `health` are synchronous structs with an injected clock (`now_ms` passed in). No tokio, no I/O inside the core - property tests depend on this. Async code lives in axum handlers, reqwest calls, and device-flow pollers.
- **Backends implement the `provider::Backend` trait** (`rewrite_url`, `inject_headers`, `classify`, `default_cooldown_ms`). Adding a provider = new trait impl + registration; never special-case a backend in the proxy path. Model-catalog eligibility and the flat pool are backend-agnostic.
- **Failover is pre-first-byte only.** Once a response stream starts, errors pass through to the client.
- **Write-through persistence.** Token rotations, health transitions, new bindings, and catalog edits are persisted to SQLite immediately (ADR 0005).

## Security invariants

- Never log or emit: access/refresh tokens, authorization headers, request bodies, full prompts.
- Account identity in logs and the request ring buffer uses the account **label** (email/username) in full - the operator asked for unredacted labels. Raw UUID account IDs are truncated to 8 chars (`logging.rs` is the only place that formats identity).
- `auth.json` writes use `0600`. Never commit credentials or `pool.db`.
- The proxy API key and admin UI token are minted on first run and stored in the DB; they gate `/v1/*` and `/admin/api/*` respectively.

## Code contracts

Use the `code-contracts` skill for every code change and code review. Follow its contract discovery, writing, and enforcement procedures before submitting commits or pull requests.

- Repo-wide rules live in [`CONTRACTS`](CONTRACTS); declaration contracts use `/// @cc` doc comments on the narrowest relevant function.
- `owner` is the account's GitHub username (`ghuntley`); never add `notify` unless explicitly requested.
- Validate syntax and duplicate IDs with `cc-check format` (npm `@spolu/cc-check`); there is no automated semantic enforcement — verify prose against the implementation for every touched contract.

## ADR policy

- Every non-trivial architectural decision gets a MADR record in `docs/adr/NNNN-title.md` (Status / Context / Decision / Consequences).
- Superseding a decision: mark the old ADR `Superseded by NNNN`, write the new one. Never delete ADRs.
- Index: 0001 Rust/Axum - 0002 Hegel PBT, no mutation testing - 0003 prompt-cache-key stickiness - 0004 health state machine + fail-fast - 0005 SQLite store - 0006 tracing/correlation IDs - 0007 config-driven catalog - 0008 JSONC-safe merge - 0009 device-code onboarding - 0010 unprefixed routing, flat pool - 0011 GitHub Copilot backend - 0018 recoverable credential rejection.

## Config files agents may edit

`AGENTS.md`, `README.md`, `docs/adr/*`, `Cargo.toml`, `flake.nix`, `.github/*`, everything under `src/` and `tests/`. Do not touch `devenv.nix`/`devenv.lock` unless the task explicitly requires it, and never commit `*.bak`, `pool.db`, or `.hegel/` artifacts.
