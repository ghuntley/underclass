# 0028 - Proven Overload Retry Certificate

## Status

Accepted. Supplements [0022](0022-failover-on-overload-before-output.md).

## Context

A caller may see a Responses stream fail under HTTP 200 after Underclass has
exhausted its own account failover. Error text alone does not prove that another
inference is safe: an earlier or later attempt may have lost transport after
performing work. In particular, a saved overload response can be replayed after
a later generic failure that supplies no response body. The saved body cannot
certify the entire request.

## Decision

Only the final response constructor may issue
`x-underclass-retry-safe: pre-output-overload`. It requires at least one attempt,
with every attempt ending in the exact raw `server_is_overloaded` failure on a
Responses stream before any observed model output. The header is not copied from
clients, providers or saved replay responses.

Evidence accumulates monotonically. Any generic `Failover`, including absent
credentials, authentication refresh, HTTP error, network loss, or an internal
request-shape retry, permanently disqualifies the chain. A later exact overload
cannot repair it. Other capacity codes remain eligible for the existing bounded
internal pool failover but do not qualify for this certificate.

The stream proof accepts only parsed, understood preamble events and the exact
failure. Output, embedded response output, positive output usage, malformed or
oversized data, unknown events, mismatched event names, other terminal outcomes,
and incomplete trailing events invalidate proof. Output/acceptance indicators
cannot be cleared by subsequent events. Successful responses, live streams,
quota failures and admission failures never receive a certificate.

Callers must validate the trusted Underclass origin, exact certificate, failure
category, and their own retry budget. A retry uses a fresh attempt identity;
uncertain/lost requests must not be replayed merely because an old overload body
was returned. This is evidence from a trusted proxy, not a cryptographic signature
or permission for unlimited retry.

## Consequences and verification

The certificate is intentionally conservative: unknown future provider events
may reduce retries without weakening safety. Existing response status/body and
account failover policy remain intact. No provider credentials or database state
are shared by this protocol.

Hegel properties exercise chunk boundaries, malformed/unknown/output-bearing
prefixes, exact raw error codes, and monotone chain disqualification. Unit tests
exercise certificate finalization and stale/spoofed-header removal. HTTP integration
tests cover exhausted all-overload chains, successful failover, malformed streams,
policy/quota/other capacity failures, missing credentials, forged client/provider
headers, and an overload followed by a real connection failure. Local tests do
not establish the caller's admission/retry implementation or production routing;
those remain consumer acceptance obligations.

The existing Anthropic acceptance fence also applies when `message_start` and
an overload arrive in the same transport chunk: the accepted request cannot be
replayed. A regression test exercises that ordering.

Recorded local validation on 2026-11-02, inside `devenv shell`: `cargo test
--locked` passed 182 tests (134 library tests including Hegel, 2 binary tests,
1 correlation integration test, 14 HTTP integration tests, and 31 property or
invariant tests). `cargo build --locked --release`, `cargo fmt --check`, and
`cargo check --offline` passed. `cc-check format` passed and declaration discovery
found the new contracts; this validates syntax, with semantic review performed
against the implementation. The resolved UUID feature tree contains v7 and no
v4 feature. No production deployment or NixOS machine test was performed by this
upstream change; the consuming fleet verifies its packaged integration separately.
