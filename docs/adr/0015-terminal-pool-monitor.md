# 0015 - Read-only terminal pool monitor

## Status

Superseded by 0016

## Context

The web UI exposes account health, in-flight counts, Codex quota snapshots, and historical token accounting, but operators need a continuously updating terminal view alongside their running proxy. A second process cannot read live in-flight counts or in-memory quota snapshots from SQLite alone.

## Decision

Add `underclass top` as a separate, read-only client of a running server. A compact admin-token-protected `GET /admin/api/monitor` response combines live pool state and Codex quota snapshots with bounded recent outcomes and SQLite aggregate usage. The CLI polls once per second and keeps its terminal responsive during HTTP calls. It reads the local admin token from configuration or the existing database in read-only mode. A URL override requires an explicit environment token.

The monitor reports upstream attempts, including retries, and marks absent token usage as unknown. Copilot quota availability is unknown until a reliable source is integrated. The response excludes credentials, request bodies, and prompt cache keys.

## Consequences

- The monitor requires a running server and the admin token; it does not alter routing or accounts.
- Traffic history starts when per-attempt accounting was deployed, and token totals can be lower bounds.
- Existing admin access also grants monitoring access; no new credential is introduced.
