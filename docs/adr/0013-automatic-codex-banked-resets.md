# 0013 - Automatic Redemption of Codex Banked Resets

## Status

Accepted

## Context

Codex may grant a finite number of banked quota resets. A reset refreshes eligible usage windows, while the proxy's existing `reset_at` is only a cooling deadline and may be a fallback estimate. Spending a reset on an account that will recover soon wastes more potential service time than spending it on one blocked for days. The flat request pool also contains Copilot, but only ChatGPT/Codex accounts can redeem these credits.

## Decision

- Automatic redemption is enabled by default and can be disabled with `auto_codex_resets = false` or `UNDERCLASS_AUTO_CODEX_RESETS=false`.
- On a live request, when no enabled Codex account is healthy, read fresh usage for cooling Codex accounts. A banked reset is considered only when upstream says ordinary usage is blocked and an exhausted quota window has a known future reset time.
- Choose the account with the **latest natural return to usable service**. When multiple exhausted windows block one account, the latest of their reset times determines its return. Break ties by earliest credit expiry, then account ID. Do not redeem because a credit is expiring or because one healthy account is busy.
- Serialize decisions for one proxy process and persist the chosen credit and idempotency key before redemption. Retry an uncertain attempt with the same key. Confirm upstream usage is allowed before readmitting the account; otherwise retain its cooling state. A completed redemption that has not restored usage must not consume another credit for the same outage.
- Poll usage for every connected Codex account and show its windows, reset credits, expiry dates, and freshness in the admin UI. Usage reads may update health when upstream explicitly permits or blocks ordinary usage. Keep Copilot routing and the existing 429 response when no redemption restores service.

## Consequences

- An exhausted Codex pool can recover during a live request, including when Copilot remains available.
- Requests may take longer while fresh usage and redemption are checked. A 30-second check interval limits repeated upstream reads after an unsuccessful decision.
- The policy optimizes recoverable service time using upstream window timestamps; it does not estimate an unreported quota window or infer concurrency capacity from in-flight count.
- Reset credit metadata and idempotency keys are stored in SQLite. Structured logs record decisions and outcomes without tokens, credit IDs, or upstream bodies.
