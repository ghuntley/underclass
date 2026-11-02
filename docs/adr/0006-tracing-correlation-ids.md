# 0006 - Structured Logging with Correlation IDs

## Status

Accepted

## Context

When a pooled request misbehaves, the operator needs to answer "which subscription served this request, and why" across log lines. Free-text logging makes that greppy and fragile.

## Decision

Use the `tracing` ecosystem:

- `tracing-subscriber` with JSON output by default (pretty when attached to a TTY; `--log-format json|pretty` to override). Filter via `RUST_LOG`, default `info`.
- Validated request-ID middleware assigns or propagates `x-request-id`; the ID is echoed on every handled response. [ADR 0027](0027-uuidv7-identities-and-secret-randomness.md) specifies the current UUIDv7 protocol, superseding ADR 0012.
- Each request logs structured events with fields: `request.selected` (request_id, model, decision, backend, account), `request.completed` (request_id, status, duration_ms), `request.saturated` (request_id, retry_after_ms relative wait, until_ms absolute reset), and account lifecycle events (`account.cooling`, `account.auth_error`).
- Redaction rules live in `logging.rs`: access/refresh tokens, authorization headers, and prompt bodies are never logged; account IDs are truncated to 8 characters.

## Consequences

- Every log line within a request identifies the pool account used.
- The in-memory ring buffer (last 200 requests) in the web UI reuses the same `RequestLogEntry` structs, so UI and logs cannot disagree.
- Strict redaction means some debugging (e.g. malformed token content) requires exporting state out-of-band rather than reading logs.
