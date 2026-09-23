# 0004 - Health State Machine with Fail-Fast Saturation

## Status

Superseded by 0017

## Context

Subscriptions run out of quota at unpredictable times. The pool must remove exhausted subscriptions from rotation without losing their configuration, restore them when quota resets, and behave predictably when everything is exhausted.

Observed upstream signals (from the opencode source and Copilot behavior):
- Codex quota errors: HTTP 429 and/or bodies containing `usage_limit_reached`, `insufficient_quota`, `usage_not_included`, `FreeUsageLimitError`; reset time from `retry-after` / `retry-after-ms` headers.
- Copilot quota errors: HTTP 429, `retry-after` when present.
- 401/403 indicate credential problems, not quota.

## Decision

Each account is a small state machine: `Healthy | Cooling(until) | AuthError | Disabled`.

- Quota-class errors move the account to `Cooling(until = retry-after or backend default, 30 min)`. A cooling account stays configured but is never selected. A background sweep (and every selection) returns it to `Healthy` once `until` passes.
- 401 triggers a single forced token refresh and one same-account retry; a second 401 (or a failed refresh) moves the account to `AuthError`, which requires re-login through the web UI.
- Disabled is operator-controlled (UI/CLI).
- 5xx/network errors are transient: no state change, request fails over to another account.
- **Fail fast at saturation**: when every account eligible for the requested model is cooling, the proxy returns `429` immediately with `Retry-After` set to the earliest reset time in the pool. Requests are not queued.

## Consequences

- Clients get an honest, machine-parseable signal (with a concrete retry time) instead of unbounded queuing.
- Failover only happens before the first upstream byte; once a stream starts, mid-stream errors pass through to the client.
- The default cooldown window is configurable per backend (`codex_cooldown_secs` / `copilot_cooldown_secs`) for upstreams that omit `retry-after`.
