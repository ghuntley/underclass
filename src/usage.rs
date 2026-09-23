use serde::{Deserialize, Serialize};
use serde_json::Value;

const MAX_CAPTURE_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenCounts {
    pub input_tokens: i64,
    pub output_tokens: i64,
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

/// @cc [owner:ghuntley,label:accounting] usage-tap-bounded
/// The tap MUST retain at most one megabyte of upstream data while scanning and MUST count only
/// non-negative upstream-reported token pairs; absent, malformed, or incomplete usage is unknown.
pub struct UsageTap {
    stream: bool,
    chat: bool,
    buffer: Vec<u8>,
    event_data: Vec<u8>,
    overflow: bool,
    counts: Option<TokenCounts>,
}

impl UsageTap {
    pub fn new(stream: bool, chat: bool) -> Self {
        Self {
            stream,
            chat,
            buffer: Vec::new(),
            event_data: Vec::new(),
            overflow: false,
            counts: None,
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
        if !self.overflow {
            if let Ok(value) = serde_json::from_slice::<Value>(&self.event_data) {
                let counts = if self.chat {
                    usage_from_json(&value, true)
                } else if value.get("type").and_then(Value::as_str) == Some("response.completed") {
                    value
                        .get("response")
                        .and_then(|response| usage_from_json(response, false))
                } else {
                    None
                };
                if counts.is_some() {
                    self.counts = counts;
                }
            }
        }
        self.event_data.clear();
        self.overflow = false;
    }

    pub fn counts(&self) -> Option<TokenCounts> {
        if self.stream {
            self.counts
        } else if self.overflow {
            None
        } else {
            serde_json::from_slice::<Value>(&self.buffer)
                .ok()
                .and_then(|v| usage_from_json(&v, self.chat))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn responses_stream_across_chunks() {
        let mut tap = UsageTap::new(true, false);
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
        let mut tap = UsageTap::new(true, true);
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
        let mut tap = UsageTap::new(true, false);
        tap.feed(b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n");
        assert_eq!(tap.counts(), None);
    }

    #[test]
    fn oversized_json_does_not_parse_trailing_bytes_as_usage() {
        let mut tap = UsageTap::new(false, false);
        tap.feed(&vec![b'x'; MAX_CAPTURE_BYTES + 1]);
        tap.feed(b"{\"usage\":{\"input_tokens\":1,\"output_tokens\":2}}");
        assert_eq!(tap.counts(), None);
    }
}
