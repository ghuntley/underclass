# 0022 - Failover on Upstream Overload Before Output

## Status

Accepted. Replaces the "failover only before the first upstream byte" rule, which ADR 0004 introduced and the `pre-first-byte-failover` contract in `CONTRACTS` carried. ADR 0004 is otherwise superseded by 0017. The contract is now `pre-output-failover`.

## Context

Codex users still see "Selected model is at capacity. Please try a different model." The text is Codex's own rendering of an upstream overload error. Underclass does not generate it. Upstream answers `200`, sends `response.created`, usually waits 2-30 seconds, then sends a stream `error` event (`{"type":"error","error":{"code":"server_is_overloaded",...}}`) or `response.failed` with the same code. Under that rule a `200` counts as started, so the proxy passed the failure straight through.

The statistics from ADR 0021 supplied the evidence the follow-up decision needed:

- In the 48 hours to 2026-10-20, 266 of 271 overload failures came before any output (`saw_output = false`). The client had received only the stream preamble. 265 of those 266 arrived within 60 seconds of the request.
- Failures depend on the account, even within one hour. On 2026-10-19 03:00 UTC, one account failed 18 of 226 requests while another served 875 with none. Retrying on a different account is likely to succeed.
- Under ADR 0003 stickiness, Codex's own retry goes back to the same bound account, so it keeps failing until the user gives up.

ADR 0021 also mis-recorded these failures. It took the error code from a stream `error` event's top-level `type` (`"error"`) before the nested `error.code`. As a result they counted as `upstream_failed / error` instead of `overloaded / server_is_overloaded`.

## Decision

- The rule moves from "before the first upstream byte" to "before any byte is sent to the client". For streamed 2xx responses the proxy holds the response, headers included, and buffers upstream chunks. It stops holding at the first model-output event (`*.delta`, `response.output_item.added`, chat choices), a terminal event, the end of the stream, 60 seconds, or 1 MiB, whichever comes first. Then it sends the buffer followed by the rest of the stream, unchanged.
- If the first terminal event is a failure with a capacity code (`is_overload_code`: `overload`, `capacity`, `slow_down`) and no output came before it, the attempt is discarded. It is recorded as a `failed` usage row and an `overloaded` upstream event with detail `stream_failover`. The account is excluded for the rest of this request and the next account is tried. The sticky key rebinds to the account that serves the request, so the session's next turn goes there.
- Exclusion uses a new pure `PoolCore::select_excluding`. It does not change account health. An overload is not quota exhaustion, and the account stays eligible for other requests.
- If no account is left, the proxy replays the held bytes of the last overloaded attempt with its original status, so the client sees exactly what upstream sent. It does not attempt a banked reset or return a 429 in that case.
- Policy, quota, and other failure codes are never retried this way: retrying a deterministic refusal elsewhere wastes quota. Failures after output always pass through.
- A stream `error` event's code is taken from its top-level `code`, then the nested `error.code`/`error.type`, and only then the event `type`.

## Consequences

- Most capacity failures become a few seconds of extra latency on another account instead of a user-visible error.
- Streamed responses now reach the client at the first output event, not at the first upstream byte. For a client this only moves when the response headers arrive; the first token comes no later. A client whose response-header timeout is under 60 seconds could time out while a response is held. Such a client should raise that timeout.
- A request can use up to one attempt per account, as before (`failover-bounded-by-accounts`). Each discarded attempt costs upstream work but produces no output tokens.
- Stats now record these failures as `overloaded`. Rows recorded before this change keep `error_code = "error"`.

## Implementation correction, 2026-11-02

A live Responses request returned valid SSE under `Content-Type: application/json`.
The usage parser already treated an explicit `stream: true` request as streaming,
but the proxy forwarded the incorrect header, causing a downstream client to
reject the completed response as invalid JSON. Successful responses now publish
`text/event-stream` whenever the existing request/header classifier selects
streaming. Both live forwarding and held overload replay share this correction;
non-stream JSON and HTTP errors retain their media type. Body bytes, request
identity, bounded failover and retry-certificate requirements are unchanged.

The HTTP integration regression exercises success and certified overload replay
with missing or incorrect upstream media types, plus ordinary JSON and HTTP
errors. In the pinned devenv shell on 2026-11-02, `cargo test --locked` passed all
183 tests (134 library, 2 binary, 1 correlation integration, 15 HTTP integration
and 31 property tests), including this regression. `cargo fmt --check`,
`cargo check --offline` and `cargo build --locked --release` also passed. Contract
syntax checks and semantic review confirmed that the existing pre-output failover,
backend independence and retry-certificate obligations remain unchanged.
