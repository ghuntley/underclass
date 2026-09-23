# 0017 - Codex Quota Deadline Reconciliation

## Status

Accepted

## Context

ADR 0004 cools an account until Retry-After or a configured fallback. Codex also returns an
absolute `error.resets_at` in `usage_limit_reached` responses, and its account usage endpoint
reports exhausted windows. A fallback can expire before the upstream window resets. The existing
usage poll moved healthy accounts into cooling but did not extend an already cooling account.

## Decision

Keep the account health states and fail-fast behavior from ADR 0004. Codex classifies structured
usage-limit errors in its backend implementation. For a quota error, use the later valid future
deadline from `error.resets_at` (Unix seconds) and Retry-After; use the configured fallback when
neither gives a future deadline. Treat overload as transient. Other backends retain the generic
status and Retry-After classification.

The account-matched Codex usage poll can move a healthy account into cooling when it reports a
blocked, exhausted quota window. It can extend a cooling deadline to a later verified natural
recovery, but cannot shorten one. An explicit `allowed: true` readmits a cooling account. Missing
or failed usage results leave health untouched. Persist each health transition immediately. The
scope is the ordinary account-wide usage limit; model-specific limits need separate routing data.

## Consequences

- A Codex account remains out of rotation through a known upstream reset time, even when the
  fallback would have expired sooner.
- The dashboard continues to show usage windows and the account's persisted cooldown deadline.
- Accounts without a usable upstream deadline retain the configured fallback behavior.
