# 0019 - Scheduled Refresh Token Rotation

## Status

Accepted

## Context

Underclass had no scheduled token rotation. Refresh was entirely reactive:

- `TokenManager::access_token` refreshes only once the access token is within
  `PROACTIVE_REFRESH_MARGIN_MS` (5 minutes) of `expires_at`.
- `attempt_account` forces one refresh when the upstream rejects the credential (ADR 0018).
- `refresh_identities_on_boot` resolves generic account labels, but routes through
  `access_token`, so it only rotates a token that is already near expiry.

Codex access tokens are issued with a 6.6-10 day lifetime, so the stored refresh token sat
untouched for over a week at a time. Measured across the four production accounts, the interval
between rotations was 10.00, 10.00, 7.70, and 6.59 days. Copilot is unaffected: its GitHub OAuth
token does not expire and `force_refresh` returns it verbatim, so there is nothing to rotate.

A refresh token that is never exercised is a liability: it is the credential of last resort if an
access token is invalidated outside the proxy's request path, and ADR 0018 showed how damaging it is
to discover a problem only when the pool is already saturated.

There was also nowhere to record when a rotation last happened. `updated_at` could not be reused:
`update_account_status` bumps it too, so a status transition would silently shift the rotation
deadline.

## Decision

Rotate Codex refresh tokens on a fixed 24 hour cadence, measured from a dedicated column.

- `accounts.token_refreshed_at` records the instant of the last successful token write. It is
  persisted by `update_tokens` in the same statement and the same instant as `updated_at`, and is
  also set by both Codex device-flow completion paths, which are token issuances.
- `tokens::rotation_due` is a pure predicate over the account and an injected clock, matching the
  injected-time style of `pool.rs` and `health.rs`. Only Codex accounts that actually hold a refresh
  token are ever due, so an account mid-onboarding is skipped instead of raising a
  missing-credential error.
- `rotate_due_tokens` runs on a 15 minute tick and calls the existing single-flight
  `force_refresh`. The interval is a hardcoded constant, not configuration.
- **A failed rotation never changes account state.** It is logged and retried on a later tick. This
  is the load-bearing part of the decision: routing scheduled-rotation failures through
  `report_outcome(AuthFailed)` would reintroduce exactly the dead end ADR 0018 closed, this time
  driven by a transient issuer outage instead of a single 403.

Adding the column required an explicit migration. `Store` only ever ran
`CREATE TABLE IF NOT EXISTS`, which silently leaves an already-created table alone, so `migrate`
inspects `PRAGMA table_info` and issues `ALTER TABLE ... ADD COLUMN` when the column is absent.
Consulting the pragma rather than matching SQLite's "duplicate column name" error keeps re-opening
idempotent without depending on message wording.

## Consequences

- Codex refresh tokens are exercised daily instead of every 6.6-10 days. Copilot is untouched.
- The first run against an existing store rotates every Codex account once, because
  `token_refreshed_at` defaults to 0 and its age is therefore the entire elapsed epoch. That is a
  one-time burst of one refresh per account and establishes the baseline.
- Rotation now invalidates the prior refresh token roughly 365 times a year per account instead of
  about four. `force_refresh` persists write-through and is single-flight locked per account, so the
  window in which a crash could strand an account remains sub-millisecond, but it is crossed more
  often. This is the accepted cost of a shorter interval.
- Scheduled rotation is invisible to pool state by construction, so quota, stickiness, and the ADR
  0004 health machine are unaffected.
- The interval is a compile-time constant. Changing it requires a rebuild and redeploy.
