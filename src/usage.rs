use serde::{Deserialize, Serialize};
use serde_json::Value;

const MAX_CAPTURE_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenCounts {
    pub input_tokens: i64,
    pub output_tokens: i64,
}

/// The wire dialect of an upstream response body, which decides how token counts and stream
/// outcomes are read out of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UsageDialect {
    /// OpenAI Responses API: `response.completed` carries `usage`.
    Responses,
    /// OpenAI Chat Completions: `usage.prompt_tokens` / `usage.completion_tokens`.
    Chat,
    /// Anthropic Messages API: `message_start` carries input usage, `message_delta` output.
    Anthropic,
}

fn pair(value: &Value, input: &str, output: &str) -> Option<TokenCounts> {
    let input_tokens = value.get(input)?.as_i64()?;
    let output_tokens = value.get(output)?.as_i64()?;
    (input_tokens >= 0 && output_tokens >= 0).then_some(TokenCounts {
        input_tokens,
        output_tokens,
    })
}

pub fn usage_from_json(value: &Value, chat: bool) -> Option<TokenCounts> {
    let usage = value.get("usage")?;
    if chat {
        pair(usage, "prompt_tokens", "completion_tokens")
    } else {
        pair(usage, "input_tokens", "output_tokens")
    }
}

/// Reads token counts out of an upstream body in the given dialect.
pub fn usage_from_dialect(value: &Value, dialect: UsageDialect) -> Option<TokenCounts> {
    match dialect {
        UsageDialect::Chat => usage_from_json(value, true),
        UsageDialect::Responses => usage_from_json(value, false),
        UsageDialect::Anthropic => {
            let usage = value.get("usage")?;
            Some(TokenCounts {
                input_tokens: anthropic_input_tokens(usage)?,
                output_tokens: usage.get("output_tokens").and_then(Value::as_i64)?,
            })
        }
    }
}

/// How an upstream response body ended, as observed by the proxy while passing it through.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamOutcome {
    /// `response.completed`, chat `[DONE]`, or a successful non-stream body.
    Completed,
    /// `response.failed`, a stream `error` event, or an error object in the body.
    Failed,
    /// `response.incomplete`.
    Incomplete,
    /// Upstream closed the stream without a terminal event.
    Truncated,
    /// The upstream body errored mid-transfer.
    UpstreamError,
    /// The client went away before the stream ended.
    ClientClosed,
}

impl StreamOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Incomplete => "incomplete",
            Self::Truncated => "truncated",
            Self::UpstreamError => "upstream_error",
            Self::ClientClosed => "client_closed",
        }
    }
}

/// Why the proxy stopped reading an upstream body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamEnd {
    Eof,
    UpstreamError,
    Dropped,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamReport {
    pub outcome: StreamOutcome,
    pub error_code: Option<String>,
    /// Whether any model output reached the client before the stream ended.
    pub saw_output: bool,
}

const MAX_CODE_LEN: usize = 64;

/// @cc [owner:ghuntley,label:security] error-code-sanitized
/// `sanitize_code` MUST return only ASCII alphanumerics, `_`, `-`, and `.`, at most 64 bytes, and
/// `None` when nothing remains; upstream error codes pass through it before they are logged or
/// stored, so free-form upstream `message` text can never reach logs or the database.
pub fn sanitize_code(raw: &str) -> Option<String> {
    let code: String = raw
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
        .take(MAX_CODE_LEN)
        .collect();
    (!code.is_empty()).then_some(code)
}

fn raw_error_code(error: &Value) -> Option<&str> {
    error
        .get("code")
        .and_then(Value::as_str)
        .or_else(|| error.get("type").and_then(Value::as_str))
}

/// Extracts `code`, falling back to `type`, from an upstream error object.
pub fn error_code(error: &Value) -> Option<String> {
    error
        .get("code")
        .and_then(Value::as_str)
        .or_else(|| error.get("type").and_then(Value::as_str))
        .and_then(sanitize_code)
}

/// @cc [owner:ghuntley,label:accounting] stream-error-event-code
/// For a Responses stream `error` event, `stream_error_code` MUST prefer a top-level `code`, then
/// the nested `error` object's `code`/`type`, and MUST fall back to the event's own `type` only
/// when neither carries one, so `{"type":"error","error":{"code":"server_is_overloaded"}}` is
/// recorded as `server_is_overloaded`, not `error`.
fn stream_error_code(event: &Value) -> Option<String> {
    event
        .get("code")
        .and_then(Value::as_str)
        .and_then(sanitize_code)
        .or_else(|| event.get("error").and_then(error_code))
        .or_else(|| error_code(event))
}

/// Whether an upstream error code means the model is out of serving capacity.
pub fn is_overload_code(code: &str) -> bool {
    let code = code.to_ascii_lowercase();
    code == "slow_down" || code.contains("overload") || code.contains("capacity")
}

/// @cc [owner:ghuntley,label:accounting] upstream-event-kind
/// `event_kind` MUST map an upstream error code to exactly one stats kind: `overloaded` for
/// capacity codes, `policy_blocked` for policy/safety refusals (e.g. `cyber_policy`),
/// `quota_exhausted` for usage-limit codes, and `upstream_failed` otherwise.
pub fn event_kind(code: Option<&str>) -> &'static str {
    let Some(code) = code else {
        return "upstream_failed";
    };
    let lower = code.to_ascii_lowercase();
    if is_overload_code(&lower) {
        "overloaded"
    } else if lower.contains("policy")
        || lower.contains("content_filter")
        || lower.contains("safety")
    {
        "policy_blocked"
    } else if lower.contains("usage_limit")
        || lower.contains("quota")
        || lower.contains("usage_not_included")
    {
        "quota_exhausted"
    } else {
        "upstream_failed"
    }
}

/// @cc [owner:ghuntley,label:accounting] anthropic-input-usage-total
/// `anthropic_input_tokens` MUST return the sum of `input_tokens`,
/// `cache_creation_input_tokens`, and `cache_read_input_tokens` from an Anthropic `usage`
/// object, and MUST return `None` when `input_tokens` is absent or not a non-negative integer.
pub fn anthropic_input_tokens(usage: &Value) -> Option<i64> {
    let base = usage.get("input_tokens").and_then(Value::as_i64)?;
    if base < 0 {
        return None;
    }
    let extra = ["cache_creation_input_tokens", "cache_read_input_tokens"]
        .iter()
        .map(|key| usage.get(*key).and_then(Value::as_i64).unwrap_or(0))
        .sum::<i64>();
    Some(base + extra)
}

/// @cc [owner:ghuntley,label:accounting] usage-tap-bounded
/// The tap MUST retain at most one megabyte of upstream data while scanning and MUST count only
/// non-negative upstream-reported token pairs; absent, malformed, or incomplete usage is unknown.
pub struct UsageTap {
    stream: bool,
    dialect: UsageDialect,
    buffer: Vec<u8>,
    event_data: Vec<u8>,
    event_name: Option<String>,
    overflow: bool,
    counts: Option<TokenCounts>,
    /// Anthropic reports input usage in `message_start` and output usage in `message_delta`,
    /// so the two halves are tracked separately until both are known.
    anthropic_input: Option<i64>,
    anthropic_output: Option<i64>,
    terminal: Option<(StreamOutcome, Option<String>)>,
    saw_output: bool,
    /// The upstream accepted the request, so it must not be replayed on another account.
    committed: bool,
    /// Unlike accounting's sanitized classifications, retry authority requires an
    /// exact raw overload code and a fully understood prefix with no output.
    retry_prefix_proven: bool,
    retry_terminal_proven: bool,
}

impl UsageTap {
    pub fn new(stream: bool, dialect: UsageDialect) -> Self {
        Self {
            stream,
            dialect,
            buffer: Vec::new(),
            event_data: Vec::new(),
            event_name: None,
            overflow: false,
            counts: None,
            anthropic_input: None,
            anthropic_output: None,
            terminal: None,
            saw_output: false,
            committed: false,
            retry_prefix_proven: stream && dialect == UsageDialect::Responses,
            retry_terminal_proven: false,
        }
    }

    pub fn feed(&mut self, chunk: &[u8]) {
        if !self.stream {
            if self.overflow {
                return;
            }
            if self.buffer.len().saturating_add(chunk.len()) <= MAX_CAPTURE_BYTES {
                self.buffer.extend_from_slice(chunk);
            } else {
                self.overflow = true;
                self.buffer.clear();
            }
            return;
        }
        for &byte in chunk {
            if byte == b'\n' {
                let line = std::mem::take(&mut self.buffer);
                let line = line.strip_suffix(b"\r").unwrap_or(&line);
                if line.is_empty() {
                    self.finish_event();
                } else if !self.overflow && line.starts_with(b"event:") {
                    let name = line[6..].strip_prefix(b" ").unwrap_or(&line[6..]);
                    self.event_name = std::str::from_utf8(name)
                        .ok()
                        .filter(|name| name.len() <= MAX_CODE_LEN)
                        .map(str::to_string);
                } else if !self.overflow && line.starts_with(b"data:") {
                    let data = line[5..].strip_prefix(b" ").unwrap_or(&line[5..]);
                    let separator = usize::from(!self.event_data.is_empty());
                    if self
                        .event_data
                        .len()
                        .saturating_add(separator)
                        .saturating_add(data.len())
                        <= MAX_CAPTURE_BYTES
                    {
                        if separator != 0 {
                            self.event_data.push(b'\n');
                        }
                        self.event_data.extend_from_slice(data);
                    } else {
                        self.overflow = true;
                        self.event_data.clear();
                    }
                }
            } else if !self.overflow {
                if self.buffer.len().saturating_add(self.event_data.len()) < MAX_CAPTURE_BYTES {
                    self.buffer.push(byte);
                } else {
                    self.overflow = true;
                    self.buffer.clear();
                    self.buffer.push(b'!');
                }
            } else if self.buffer.is_empty() {
                self.buffer.push(b'!');
            }
        }
    }

    fn finish_event(&mut self) {
        let parsed = if self.overflow {
            None
        } else {
            serde_json::from_slice::<Value>(&self.event_data).ok()
        };
        self.observe_retry_evidence(parsed.as_ref());
        match self.dialect {
            UsageDialect::Chat => self.finish_chat_event(parsed),
            UsageDialect::Responses => self.finish_responses_event(parsed),
            UsageDialect::Anthropic => self.finish_anthropic_event(parsed),
        }
        self.event_data.clear();
        self.event_name = None;
        self.overflow = false;
    }

    fn observe_retry_evidence(&mut self, parsed: Option<&Value>) {
        if !self.retry_prefix_proven {
            return;
        }
        // Empty SSE keepalives have no data. Malformed/oversized or unknown data
        // may hide output, and that uncertainty cannot be repaired by a later error.
        if !self.overflow && self.event_data.is_empty() && self.event_name.is_none() {
            return;
        }
        let Some(value) = parsed.filter(|v| v.is_object()) else {
            self.retry_prefix_proven = false;
            return;
        };
        let kind = value.get("type").and_then(Value::as_str);
        if self.overflow
            || self.terminal.is_some()
            || self
                .event_name
                .as_deref()
                .is_some_and(|name| Some(name) != kind)
        {
            self.retry_prefix_proven = false;
            return;
        }
        let response = value.get("response");
        let output_present = [Some(value), response].into_iter().flatten().any(|body| {
            !body.is_object()
                || body
                    .get("output")
                    .is_some_and(|output| output.as_array().is_none_or(|items| !items.is_empty()))
                || body
                    .get("output_text")
                    .is_some_and(|text| text.as_str() != Some(""))
                || body.get("usage").is_some_and(|usage| {
                    !usage.is_null()
                        && (!usage.is_object()
                            || usage
                                .get("output_tokens")
                                .is_some_and(|count| count.as_u64() != Some(0)))
                })
                || ["delta", "choices", "tool_calls", "content"]
                    .iter()
                    .any(|key| body.get(*key).is_some())
        });
        if output_present {
            self.retry_prefix_proven = false;
            return;
        }
        match kind {
            Some("response.created" | "response.in_progress" | "response.queued") => {}
            Some("error") => {
                let raw_code = value
                    .get("code")
                    .and_then(Value::as_str)
                    .or_else(|| value.get("error").and_then(raw_error_code))
                    .or_else(|| raw_error_code(value));
                self.retry_terminal_proven = raw_code == Some("server_is_overloaded");
            }
            Some("response.failed") => {
                self.retry_terminal_proven = response
                    .and_then(|body| body.get("error"))
                    .and_then(raw_error_code)
                    == Some("server_is_overloaded");
            }
            _ => self.retry_prefix_proven = false,
        }
    }

    /// @cc [owner:ghuntley,label:proxy;security] exact-overload-output-proof
    /// Retry authority MUST require the exact raw server_is_overloaded code and
    /// a complete parsed Responses prefix containing no output. Malformed,
    /// oversized, unknown or output-bearing events permanently invalidate proof;
    /// partial trailing events, other dialects and committed responses never qualify.
    pub fn retry_safe_overload(&self) -> bool {
        self.retry_prefix_proven
            && self.retry_terminal_proven
            && !self.committed
            && !self.overflow
            && self.buffer.is_empty()
            && self.event_data.is_empty()
            && self.event_name.is_none()
            && self.overload_before_output() == Some("server_is_overloaded")
    }

    fn finish_chat_event(&mut self, parsed: Option<Value>) {
        if let Some(value) = &parsed
            && let Some(counts) = usage_from_json(value, true)
        {
            self.counts = Some(counts);
        }
        if !self.overflow && self.event_data.trim_ascii() == b"[DONE]" {
            self.set_terminal(StreamOutcome::Completed, None);
        } else if let Some(value) = &parsed {
            if let Some(error) = value.get("error").filter(|e| !e.is_null()) {
                self.set_terminal(StreamOutcome::Failed, error_code(error));
            } else if value
                .get("choices")
                .and_then(Value::as_array)
                .is_some_and(|choices| !choices.is_empty())
            {
                self.saw_output = true;
            }
        }
    }

    fn finish_responses_event(&mut self, parsed: Option<Value>) {
        if let Some(value) = &parsed
            && value.get("type").and_then(Value::as_str) == Some("response.completed")
            && let Some(counts) = value
                .get("response")
                .and_then(|response| usage_from_json(response, false))
        {
            self.counts = Some(counts);
        }
        // The SSE `event:` name survives even when an oversized data payload was dropped.
        let kind = parsed
            .as_ref()
            .and_then(|v| v.get("type"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| self.event_name.clone());
        let response = parsed.as_ref().and_then(|v| v.get("response"));
        match kind.as_deref() {
            Some("response.completed") => self.set_terminal(StreamOutcome::Completed, None),
            Some("response.failed") => {
                let code = response.and_then(|r| r.get("error")).and_then(error_code);
                self.set_terminal(StreamOutcome::Failed, code);
            }
            Some("response.incomplete") => {
                let reason = response
                    .and_then(|r| r.get("incomplete_details"))
                    .and_then(|d| d.get("reason"))
                    .and_then(Value::as_str)
                    .and_then(sanitize_code);
                self.set_terminal(StreamOutcome::Incomplete, reason);
            }
            Some("error") => {
                let code = parsed.as_ref().and_then(stream_error_code);
                self.set_terminal(StreamOutcome::Failed, code);
            }
            Some(kind) if kind == "response.output_item.added" || kind.ends_with(".delta") => {
                self.saw_output = true;
            }
            _ => {}
        }
    }

    /// @cc [owner:ghuntley,label:accounting] anthropic-stream-events
    /// `finish_anthropic_event` MUST read input usage from `message_start`, output usage from
    /// `message_delta`, MUST treat `message_stop` as a completed stream, MUST treat an `error`
    /// event as a failure carrying the error's sanitized code, MUST treat `content_block_delta`
    /// as model output, and MUST map a `max_tokens` stop reason to an incomplete outcome.
    fn finish_anthropic_event(&mut self, parsed: Option<Value>) {
        let kind = parsed
            .as_ref()
            .and_then(|v| v.get("type"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| self.event_name.clone());
        match kind.as_deref() {
            Some("message_start") => {
                // The upstream accepted the request: from here the stream is not replayable.
                self.committed = true;
                if let Some(usage) = parsed
                    .as_ref()
                    .and_then(|v| v.get("message"))
                    .and_then(|m| m.get("usage"))
                    && let Some(input) = anthropic_input_tokens(usage)
                {
                    self.anthropic_input = Some(input);
                }
            }
            Some("message_delta") => {
                if let Some(usage) = parsed.as_ref().and_then(|v| v.get("usage"))
                    && let Some(output) = usage.get("output_tokens").and_then(Value::as_i64)
                    && output >= 0
                {
                    self.anthropic_output = Some(output);
                }
                if let Some(reason) = parsed
                    .as_ref()
                    .and_then(|v| v.get("delta"))
                    .and_then(|d| d.get("stop_reason"))
                    .and_then(Value::as_str)
                {
                    match reason {
                        "end_turn" | "stop_sequence" | "tool_use" => {}
                        "max_tokens" => {
                            self.set_terminal(StreamOutcome::Incomplete, Some("max_tokens".into()))
                        }
                        "refusal" => {
                            self.set_terminal(StreamOutcome::Failed, Some("refusal".into()))
                        }
                        other => {
                            if let Some(code) = sanitize_code(other) {
                                self.set_terminal(StreamOutcome::Incomplete, Some(code));
                            }
                        }
                    }
                }
            }
            Some("message_stop") => self.set_terminal(StreamOutcome::Completed, None),
            Some("error") => {
                let code = parsed
                    .as_ref()
                    .and_then(|v| v.get("error"))
                    .and_then(error_code)
                    .or_else(|| {
                        parsed
                            .as_ref()
                            .and_then(|v| v.get("type"))
                            .and_then(Value::as_str)
                            .and_then(sanitize_code)
                    });
                self.set_terminal(StreamOutcome::Failed, code);
            }
            Some("content_block_delta") => self.saw_output = true,
            _ => {}
        }
    }

    fn set_terminal(&mut self, outcome: StreamOutcome, code: Option<String>) {
        if self.terminal.is_none() {
            self.terminal = Some((outcome, code));
        }
    }

    /// @cc [owner:ghuntley,label:accounting] stream-outcome-classified
    /// `report` MUST prefer the first terminal event seen in the body (`response.completed`,
    /// `response.failed`, `response.incomplete`, stream `error`, chat `[DONE]`) over how the
    /// transfer ended; without one, a streamed body MUST be `truncated` on EOF, `upstream_error` on a
    /// transport error, and `client_closed` when dropped. The result MUST NOT depend on how the body
    /// was split into chunks.
    pub fn report(&self, end: StreamEnd) -> StreamReport {
        let (outcome, error_code) = if let Some((outcome, code)) = &self.terminal {
            (*outcome, code.clone())
        } else {
            match end {
                StreamEnd::Eof if !self.stream => {
                    let error = (!self.overflow)
                        .then(|| serde_json::from_slice::<Value>(&self.buffer).ok())
                        .flatten()
                        .and_then(|v| v.get("error").filter(|e| !e.is_null()).cloned());
                    match error {
                        Some(error) => (StreamOutcome::Failed, error_code(&error)),
                        None => (StreamOutcome::Completed, None),
                    }
                }
                StreamEnd::Eof => (StreamOutcome::Truncated, None),
                StreamEnd::UpstreamError => (StreamOutcome::UpstreamError, None),
                StreamEnd::Dropped => (StreamOutcome::ClientClosed, None),
            }
        };
        StreamReport {
            outcome,
            error_code,
            saw_output: self.saw_output,
        }
    }

    /// Whether the body has shown model output, reached a terminal event, or the upstream has
    /// accepted the request.
    pub fn decided(&self) -> bool {
        self.saw_output || self.terminal.is_some() || self.committed
    }

    /// @cc [owner:ghuntley,label:proxy] overload-before-output
    /// `overload_before_output` MUST return the sanitized code only when the first terminal event
    /// is `failed` with a capacity code (`is_overload_code`) and no output event preceded it;
    /// otherwise, including for policy or quota codes, it MUST return `None`.
    pub fn overload_before_output(&self) -> Option<&str> {
        match &self.terminal {
            Some((StreamOutcome::Failed, Some(code)))
                if !self.saw_output && !self.committed && is_overload_code(code) =>
            {
                Some(code)
            }
            _ => None,
        }
    }

    pub fn counts(&self) -> Option<TokenCounts> {
        if self.stream {
            match self.dialect {
                UsageDialect::Anthropic => Some(TokenCounts {
                    input_tokens: self.anthropic_input?,
                    output_tokens: self.anthropic_output?,
                }),
                _ => self.counts,
            }
        } else if self.overflow {
            None
        } else {
            let value = serde_json::from_slice::<Value>(&self.buffer).ok()?;
            match self.dialect {
                UsageDialect::Anthropic => {
                    let usage = value.get("usage")?;
                    Some(TokenCounts {
                        input_tokens: anthropic_input_tokens(usage)?,
                        output_tokens: usage.get("output_tokens").and_then(Value::as_i64)?,
                    })
                }
                _ => usage_from_json(&value, self.dialect == UsageDialect::Chat),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn responses_stream_across_chunks() {
        let mut tap = UsageTap::new(true, UsageDialect::Responses);
        tap.feed(b"event: response.completed\r\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":12,");
        tap.feed(b"\"output_tokens\":4}}}\r\n\r\n");
        assert_eq!(
            tap.counts(),
            Some(TokenCounts {
                input_tokens: 12,
                output_tokens: 4
            })
        );
    }

    #[test]
    fn chat_ignores_null_and_negative_usage() {
        let mut tap = UsageTap::new(true, UsageDialect::Chat);
        tap.feed(b"data: {\"usage\":null}\n\ndata: {\"usage\":{\"prompt_tokens\":-1,\"completion_tokens\":2}}\n\n");
        assert_eq!(tap.counts(), None);
        tap.feed(b"data: {\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2}}\n\n");
        assert_eq!(
            tap.counts(),
            Some(TokenCounts {
                input_tokens: 5,
                output_tokens: 2
            })
        );
    }

    #[test]
    fn incomplete_stream_is_unknown() {
        let mut tap = UsageTap::new(true, UsageDialect::Responses);
        tap.feed(b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n");
        assert_eq!(tap.counts(), None);
    }

    #[test]
    fn overloaded_failure_before_output() {
        let mut tap = UsageTap::new(true, UsageDialect::Responses);
        tap.feed(b"event: response.created\ndata: {\"type\":\"response.created\"}\n\n");
        tap.feed(b"event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_is_overloaded\",\"message\":\"Selected model is at capacity.\"}}}\n\n");
        assert_eq!(
            tap.report(StreamEnd::Eof),
            StreamReport {
                outcome: StreamOutcome::Failed,
                error_code: Some("server_is_overloaded".into()),
                saw_output: false
            }
        );
    }

    #[test]
    fn stream_error_event_uses_nested_code() {
        let mut tap = UsageTap::new(true, UsageDialect::Responses);
        tap.feed(b"event: response.created\ndata: {\"type\":\"response.created\"}\n\n");
        assert!(!tap.decided());
        tap.feed(b"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"service_unavailable_error\",\"code\":\"server_is_overloaded\",\"message\":\"Selected model is at capacity.\"}}\n\n");
        assert!(tap.decided());
        assert_eq!(tap.overload_before_output(), Some("server_is_overloaded"));
        assert_eq!(
            tap.report(StreamEnd::Eof).error_code.as_deref(),
            Some("server_is_overloaded")
        );

        let mut flat = UsageTap::new(true, UsageDialect::Responses);
        flat.feed(b"data: {\"type\":\"error\",\"code\":\"slow_down\"}\n\n");
        assert_eq!(
            flat.report(StreamEnd::Eof).error_code.as_deref(),
            Some("slow_down")
        );

        let mut bare = UsageTap::new(true, UsageDialect::Responses);
        bare.feed(b"data: {\"type\":\"error\",\"message\":\"x\"}\n\n");
        assert_eq!(
            bare.report(StreamEnd::Eof).error_code.as_deref(),
            Some("error")
        );
        assert_eq!(bare.overload_before_output(), None);
    }

    #[test]
    fn overload_after_output_or_policy_is_not_retryable() {
        let mut tap = UsageTap::new(true, UsageDialect::Responses);
        tap.feed(b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n");
        assert!(tap.decided());
        tap.feed(b"data: {\"type\":\"error\",\"error\":{\"code\":\"server_is_overloaded\"}}\n\n");
        assert_eq!(tap.overload_before_output(), None);

        let mut policy = UsageTap::new(true, UsageDialect::Responses);
        policy.feed(b"data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"cyber_policy\"}}}\n\n");
        assert_eq!(policy.overload_before_output(), None);
    }

    #[test]
    fn failure_after_output_records_saw_output() {
        let mut tap = UsageTap::new(true, UsageDialect::Responses);
        tap.feed(b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n");
        tap.feed(b"data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"type\":\"server_error\"}}}\n\n");
        let report = tap.report(StreamEnd::Dropped);
        assert_eq!(report.outcome, StreamOutcome::Failed);
        assert_eq!(report.error_code.as_deref(), Some("server_error"));
        assert!(report.saw_output);
    }

    #[test]
    fn incomplete_reports_reason() {
        let mut tap = UsageTap::new(true, UsageDialect::Responses);
        tap.feed(b"data: {\"type\":\"response.incomplete\",\"response\":{\"incomplete_details\":{\"reason\":\"max_output_tokens\"}}}\n\n");
        let report = tap.report(StreamEnd::Eof);
        assert_eq!(report.outcome, StreamOutcome::Incomplete);
        assert_eq!(report.error_code.as_deref(), Some("max_output_tokens"));
    }

    #[test]
    fn missing_terminal_event_depends_on_how_stream_ended() {
        let mut tap = UsageTap::new(true, UsageDialect::Responses);
        tap.feed(b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n");
        assert_eq!(tap.report(StreamEnd::Eof).outcome, StreamOutcome::Truncated);
        assert_eq!(
            tap.report(StreamEnd::UpstreamError).outcome,
            StreamOutcome::UpstreamError
        );
        assert_eq!(
            tap.report(StreamEnd::Dropped).outcome,
            StreamOutcome::ClientClosed
        );
    }

    #[test]
    fn chat_done_and_chat_error() {
        let mut tap = UsageTap::new(true, UsageDialect::Chat);
        tap.feed(b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n");
        let report = tap.report(StreamEnd::Eof);
        assert_eq!(
            (report.outcome, report.saw_output),
            (StreamOutcome::Completed, true)
        );
        let mut tap = UsageTap::new(true, UsageDialect::Chat);
        tap.feed(b"data: {\"error\":{\"code\":\"model_overloaded\",\"message\":\"busy\"}}\n\n");
        assert_eq!(
            tap.report(StreamEnd::Eof).error_code.as_deref(),
            Some("model_overloaded")
        );
    }

    #[test]
    fn oversized_terminal_event_still_classified_by_event_name() {
        let mut tap = UsageTap::new(true, UsageDialect::Responses);
        tap.feed(b"event: response.completed\ndata: ");
        tap.feed(&vec![b'x'; MAX_CAPTURE_BYTES + 10]);
        tap.feed(b"\n\n");
        assert_eq!(tap.report(StreamEnd::Eof).outcome, StreamOutcome::Completed);
    }

    #[test]
    fn non_stream_bodies() {
        let mut tap = UsageTap::new(false, UsageDialect::Responses);
        tap.feed(b"{\"usage\":{\"input_tokens\":1,\"output_tokens\":2}}");
        assert_eq!(tap.report(StreamEnd::Eof).outcome, StreamOutcome::Completed);
        let mut tap = UsageTap::new(false, UsageDialect::Responses);
        tap.feed(b"{\"error\":{\"code\":\"server_is_overloaded\"}}");
        assert_eq!(
            tap.report(StreamEnd::Eof).error_code.as_deref(),
            Some("server_is_overloaded")
        );
    }

    #[test]
    fn codes_are_sanitized() {
        assert_eq!(
            sanitize_code("Selected model is at capacity."),
            Some("Selectedmodelisatcapacity.".into())
        );
        assert_eq!(sanitize_code("  "), None);
        assert_eq!(sanitize_code(&"a".repeat(100)).unwrap().len(), 64);
        assert!(is_overload_code("server_is_overloaded"));
        assert!(is_overload_code("server_overloaded"));
        assert!(is_overload_code("slow_down"));
        assert!(!is_overload_code("rate_limit_exceeded"));
        assert_eq!(event_kind(Some("server_is_overloaded")), "overloaded");
        assert_eq!(event_kind(Some("cyber_policy")), "policy_blocked");
        assert_eq!(event_kind(Some("usage_limit_reached")), "quota_exhausted");
        assert_eq!(event_kind(Some("server_error")), "upstream_failed");
        assert_eq!(event_kind(None), "upstream_failed");
    }

    #[test]
    fn oversized_json_does_not_parse_trailing_bytes_as_usage() {
        let mut tap = UsageTap::new(false, UsageDialect::Responses);
        tap.feed(&vec![b'x'; MAX_CAPTURE_BYTES + 1]);
        tap.feed(b"{\"usage\":{\"input_tokens\":1,\"output_tokens\":2}}");
        assert_eq!(tap.counts(), None);
    }

    #[test]
    fn anthropic_stream_joins_input_and_output_usage() {
        let mut tap = UsageTap::new(true, UsageDialect::Anthropic);
        tap.feed(b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":38,\"cache_creation_input_tokens\":2,\"cache_read_input_tokens\":10,\"output_tokens\":0}}}\n\n");
        tap.feed(b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n");
        tap.feed(b"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":13}}\n\n");
        tap.feed(b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n");
        assert_eq!(
            tap.counts(),
            Some(TokenCounts {
                input_tokens: 50,
                output_tokens: 13
            })
        );
        let report = tap.report(StreamEnd::Eof);
        assert_eq!(report.outcome, StreamOutcome::Completed);
        assert!(report.saw_output);
    }

    #[test]
    fn anthropic_overload_before_output_is_retryable() {
        let mut tap = UsageTap::new(true, UsageDialect::Anthropic);
        tap.feed(b"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n");
        assert_eq!(tap.overload_before_output(), Some("overloaded_error"));
        assert_eq!(
            tap.report(StreamEnd::Eof).error_code.as_deref(),
            Some("overloaded_error")
        );
    }

    #[test]
    fn anthropic_max_tokens_is_incomplete() {
        let mut tap = UsageTap::new(true, UsageDialect::Anthropic);
        tap.feed(b"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"max_tokens\"},\"usage\":{\"output_tokens\":5}}\n\n");
        let report = tap.report(StreamEnd::Eof);
        assert_eq!(report.outcome, StreamOutcome::Incomplete);
        assert_eq!(report.error_code.as_deref(), Some("max_tokens"));
    }

    #[test]
    fn anthropic_non_stream_body_reports_usage() {
        let mut tap = UsageTap::new(false, UsageDialect::Anthropic);
        tap.feed(b"{\"type\":\"message\",\"usage\":{\"input_tokens\":38,\"output_tokens\":13}}");
        assert_eq!(
            tap.counts(),
            Some(TokenCounts {
                input_tokens: 38,
                output_tokens: 13
            })
        );
        assert_eq!(tap.report(StreamEnd::Eof).outcome, StreamOutcome::Completed);
    }
}
