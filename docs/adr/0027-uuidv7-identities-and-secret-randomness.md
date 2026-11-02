# 0027 - UUIDv7 Identities and Independent Secret Randomness

## Status

Accepted. Supersedes [0012](0012-cross-proxy-request-correlation.md).

## Context

Marshall and Ziggy identify inference attempts with UUIDv7. Underclass previously
replaced these IDs because ADR 0012 accepted only UUIDv4, breaking correlation at
the proxy boundary. Account and OAuth-flow identifiers also used UUIDv4. The
same generator minted bearer keys, and Claude PKCE material concatenated UUID
bytes. Replacing those uses mechanically with timestamp-bearing UUIDv7 would
reduce secret unpredictability.

## Decision

Generate only UUIDv7 for new request, account, flow, and redemption identities.
Accept exactly one canonical hyphenated RFC9562 UUIDv7 request header, retaining
case normalization and replacement of malformed, duplicate, nil and unsupported
versions before authentication or logging. Preserve the same identity in handler
extensions, response headers, request-start/finish logs, and every internal pool
attempt. Emit request.started before authentication or routing so an interrupted
request still has receipt evidence.
Upstream headers cannot replace it; correlation never grants authority.

Remove the UUID crate's v4 generation feature, including the unused tower-http
request-ID generator that enabled it transitively. A local validated RequestId
extension retains the existing handler boundary. Preserve all existing stored
identifiers and pending redemption keys as opaque values. Do not migrate account
IDs, history, external provider IDs or pending idempotency keys. Exact account
lookup and unambiguous prefix lookup continue to accept legacy IDs.

Use the existing `rand` dependency's operating-system CSPRNG for secret material:
32 independent random bytes for each newly minted bearer secret, 48 for the PKCE
verifier and 32 for its independent state, encoded with unpadded base64url. PKCE
continues to use S256. An entropy failure aborts minting or flow creation; there
is no weaker fallback. Existing configured/stored bearer secrets keep their
precedence and values. Secret bytes remain outside diagnostics.

Use full UUIDv7 values for temporary fixture paths. Their timestamp prefixes are
not suitable as independently unique short names.

## Consequences and verification

Old clients sending UUIDv4 receive newly generated UUIDv7 request IDs and must
upgrade to preserve correlation. UUIDv7 account prefixes may be ambiguous more
often; the CLI already refuses ambiguous prefixes and accepts full IDs or unique
labels. Existing eight-character account log abbreviations remain diagnostic
hints rather than unique identifiers; full request IDs carry request correlation.

Correlation integration tests cover preservation and fallback on successful,
unauthorized, and unmatched routes, including duplicate headers and provider-ID
replacement attempts. Tests cover new UUIDv7 account IDs, legacy account and
redemption lookup, credential persistence, PKCE challenge/encoding, and the Unix
socket fixture. CSPRNG assurance comes from the typed OS entropy API and source
review, not statistical claims from generated samples. No dependency package was
added or upgraded.
