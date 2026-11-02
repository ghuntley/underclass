# 0012 — Validated request correlation with preflight

## Status
Superseded by [0027](0027-uuidv7-identities-and-secret-randomness.md).

## Context
Preflight and underclass need a common identity for inspection, routing, retries,
errors, and request history. The previous request-ID layers accepted arbitrary
client header values, and their ordering could omit IDs from some error responses.

## Decision
Use `x-request-id` as the sole shared correlation header and `request_id` in logs.
Preflight originates a fresh UUIDv4. Underclass preserves a single hyphenated
RFC4122 UUIDv4, normalizing its case, and generates a fresh UUIDv4 for absent,
duplicate, malformed, nil, or other-version values. Validation precedes logging
and authentication; arbitrary client text never becomes the correlation field.

One middleware supplies the tower RequestId extension used by existing routing
and request-history code, creates a request span, and sets the final response ID.
This applies to authentication failures, model discovery, unmatched routes, and
inference errors as well as successful streams. Pool failover keeps the same ID.

## Consequences
Both services and the request ring buffer can be searched using one identifier.
Correlation is not authentication or evidence that preflight inspected a request.
The header is scoped to the proxy chain; provider headers cannot overwrite it.
This refines the assignment/propagation mechanism in ADR 0006 without changing
the account identity or credential-redaction policies.
