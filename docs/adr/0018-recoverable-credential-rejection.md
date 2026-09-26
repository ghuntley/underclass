# 0018 - Recoverable Credential Rejection

## Status

Accepted

## Context

ADR 0004 made `AuthError` a terminal pool state: `select` never returns it, so an account that
reaches it receives no further traffic and cannot recover on its own. ADR 0013 added automatic
Codex banked resets, which redeem a rate-limit credit on an account while a live request is
saturated.

In production on 2026-09-25, two healthy Codex accounts (`codex00`, `codex02`) were stranded in
`AuthError` within three seconds of a successful redemption on that same account, while their
access tokens were still valid for another ten days and their weekly quota sat at 22% and 0% used.
Both later accounts, plus two genuinely quota-exhausted accounts, left the pool with no eligible
member and every request failing fast with `429`.

Two defects combined to produce the dead end:

1. `attempt_account` refreshed the token and retried only on `401`. Codex answers a
   redemption-triggered credential rejection with `403`, which fell straight through to
   classification. `health::classify` maps `401 | 403` to `AuthFailed`, so the account was marked
   `AuthError` having made no recovery attempt at all.
2. `POST /admin/api/accounts/{id}/enable` only mapped `Disabled` to `Healthy` and re-wrote the
   existing status otherwise. The documented operator recovery lever was a silent no-op on
   `AuthError`. The same handler also passed `reset_at = 0` unconditionally, so enabling a
   `Cooling` account silently discarded an upstream quota deadline — the account stayed `Cooling`
   with a past deadline, which `PoolCore::sweep` readmitted on the next selection.

## Decision

Treat `401` and `403` as the same credential-rejection signal, and give `enable` a defined,
enumerable meaning.

- A `401` **or** `403` triggers exactly one forced token refresh and one retry per account per
  request. `AuthError` is therefore only reached once a rejection has survived a refresh that
  returned a usable token, which is the same bar ADR 0004 intended for `401`.
- The signal stays in shared proxy code. `health::classify` keeps mapping `401 | 403` to
  `AuthFailed` unchanged, so the pure classification core and its contract are untouched and no
  backend is special-cased by name.
- `enable_account` sets `Healthy` and clears `reset_at` for `Disabled` and `AuthError`. For
  `Cooling` it preserves both the status and the existing deadline, because that deadline is
  upstream quota state rather than an operator action; `PoolCore::sweep` readmits the account when
  the window resets. An already `Healthy` account stays `Healthy` with `reset_at` cleared.
- The admin UI offers **Enable** alongside **Re-login** for `auth_error` rows, since a refresh
  usually clears the condition and a full device flow should not be the only route.

`Cooling` remains excluded from operator override on purpose: re-enabling a quota-exhausted
account only produces a `429` and an immediate return to `Cooling`.

## Consequences

- A transient or redemption-triggered `403` no longer costs a subscription. Accounts return to
  rotation on the same request via the existing refresh path.
- `AuthError` now means "a rejection survived a token refresh", which is the useful and
  recoverable definition, and the UI has a working manual override for the genuinely dead case.
- Failover cost is unchanged: the refresh-and-retry path consumes one of the attempts already
  bounded by the configured account count.
- Copilot accounts get the same `403` handling for free. Their refresh is a no-op that returns the
  stored token, so a `403` there is retried once and then escalates — the same shape as Codex,
  with no backend-specific code.
- Operators can no longer use `enable` to force a quota-exhausted account back into rotation.
  Quota state is now only cleared by the upstream window elapsing, by a banked reset
  (ADR 0013), or by editing the store directly.
