# 0014 - Per-Attempt Token Accounting

## Status

Accepted

## Context

The request ring buffer is transient and records no token counts. Operators need historical input
and output usage by model, account, and prompt cache key. Codex Responses events and Copilot chat
completions can report usage, but usage is optional and may be lost when a stream ends early.

## Decision

Persist one SQLite row per upstream attempt, including the request's model, selected account ID
and label, full cache key when supplied, endpoint, status, and nullable input/output counts.
Inspect response bytes with a bounded parser while forwarding them unchanged. Sum only reported
counts and expose missing counts separately. Retain rows indefinitely, including after account
deletion. The admin API supports time filters, exact dimension filters, grouping, and paginated
details; the dashboard defaults to the current UTC month.

For Copilot streamed chat completions, ask for final usage unless the client chose a setting.
If Copilot rejects the added option before streaming, retry once without it and stop adding the
option for that backend process. Codex Responses completion events need no added option.

## Consequences

- The ledger begins at deployment; previous requests cannot be backfilled.
- Aborted or usage-free attempts remain visible with unknown counts, so totals are lower bounds.
- Full cache keys are retained in the local SQLite database and returned only through the
  admin-token-protected API.
- Storage grows with request volume until the operator removes the database.
