use crate::models::{BackendId, Outcome};

pub const RETRY_AFTER_MS_HEADER: &str = "retry-after-ms";
pub const RETRY_AFTER_HEADER: &str = "retry-after";

/// @cc [owner:ghuntley,label:pool] retry-after-absolute-deadline
/// `parse_retry_after_headers` MUST return the upstream deadline as an absolute epoch-millisecond
/// value (honoring `retry-after-ms`, then seconds-valued and HTTP-date `retry-after`), and MUST
/// return `None` when neither header is present or parseable.
pub fn parse_retry_after_headers(headers: &http::HeaderMap, now_ms: i64) -> Option<i64> {
    if let Some(v) = headers
        .get(RETRY_AFTER_MS_HEADER)
        .and_then(|v| v.to_str().ok())
    {
        if let Ok(ms) = v.trim().parse::<f64>() {
            if ms.is_finite() && ms >= 0.0 {
                return Some(now_ms + (ms as i64));
            }
        }
    }
    if let Some(v) = headers
        .get(RETRY_AFTER_HEADER)
        .and_then(|v| v.to_str().ok())
    {
        let trimmed = v.trim();
        if let Ok(secs) = trimmed.parse::<f64>() {
            if secs.is_finite() && secs >= 0.0 {
                return Some(now_ms + (secs * 1000.0) as i64);
            }
        }
        if let Ok(date) = httpdate::parse_http_date(trimmed) {
            if let Ok(delta) = date.duration_since(std::time::SystemTime::now()) {
                return Some(now_ms + delta.as_millis() as i64);
            }
        }
    }
    None
}

pub fn codex_quota_body(body: &str) -> bool {
    const NEEDLES: [&str; 7] = [
        "usage_limit_reached",
        "insufficient_quota",
        "usage_not_included",
        "FreeUsageLimitError",
        "rate_limit",
        "too_many_requests",
        // ChatGPT returns this account-specific capacity error when a selected model
        // cannot accept work from the subscription currently selected by the pool.
        "selected model is at capacity",
    ];
    let lower = body.to_ascii_lowercase();
    NEEDLES
        .iter()
        .any(|n| lower.contains(&n.to_ascii_lowercase()))
}

/// Capacity responses are distinct from generic provider failures, but still mean that this
/// subscription must leave rotation for a short period. Keep this list narrow: a bare occurrence
/// of `capacity` is not enough because model and prompt validation errors can mention capacity.
pub fn codex_capacity_body(body: &str) -> bool {
    const NEEDLES: [&str; 4] = [
        "selected model is at capacity",
        "model is at capacity",
        "model_at_capacity",
        "model_capacity_exceeded",
    ];
    let lower = body.to_ascii_lowercase();
    NEEDLES.iter().any(|needle| lower.contains(needle))
}

/// @cc [owner:ghuntley,label:pool] classify-quota-auth-transient
/// `classify` MUST return `QuotaExhausted` (with the `retry-after` deadline or, absent one,
/// `now_ms + default_cooldown_ms`) for status 429, Codex capacity responses, or any body
/// containing a quota-class error code; `AuthFailed` for 401/403 (absent a quota body);
/// `Transient` for 5xx; and `Ok` otherwise — for every backend.
pub fn classify(
    backend: BackendId,
    status: u16,
    body: &str,
    headers: &http::HeaderMap,
    default_cooldown_ms: i64,
    now_ms: i64,
) -> Outcome {
    let quota = |until: Option<i64>| Outcome::QuotaExhausted {
        until_ms: until.unwrap_or(now_ms + default_cooldown_ms),
    };
    if codex_quota_body(body) {
        return quota(parse_retry_after_headers(headers, now_ms));
    }
    match backend {
        BackendId::Codex => {
            if status == 429 && codex_capacity_body(body) {
                return quota(parse_retry_after_headers(headers, now_ms));
            }
            match status {
                429 => quota(parse_retry_after_headers(headers, now_ms)),
                401 | 403 => Outcome::AuthFailed,
                s if s >= 500 => Outcome::Transient,
                _ => Outcome::Ok,
            }
        }
        BackendId::Copilot => match status {
            429 => quota(parse_retry_after_headers(headers, now_ms)),
            401 | 403 => Outcome::AuthFailed,
            s if s >= 500 => Outcome::Transient,
            _ => Outcome::Ok,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> http::HeaderMap {
        let mut m = http::HeaderMap::new();
        for (k, v) in pairs {
            m.insert(
                http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                http::HeaderValue::from_str(v).unwrap(),
            );
        }
        m
    }

    #[test]
    fn retry_after_ms_is_absolute() {
        let h = headers(&[("retry-after-ms", "1500.5")]);
        let now = 10_000;
        assert_eq!(parse_retry_after_headers(&h, now), Some(11_500));
    }

    #[test]
    fn retry_after_seconds() {
        let h = headers(&[("retry-after", "30")]);
        assert_eq!(parse_retry_after_headers(&h, 1_000), Some(31_000));
    }

    #[test]
    fn no_headers_returns_none() {
        let h = headers(&[]);
        assert_eq!(parse_retry_after_headers(&h, 1_000), None);
    }

    #[test]
    fn codex_bodies_classify_quota_regardless_of_status() {
        let h = headers(&[]);
        for body in [
            "{\"error\":{\"code\":\"usage_not_included\"}}",
            "FreeUsageLimitError oops",
            "Error: Selected model is at capacity. Please try a different model.",
        ] {
            assert!(matches!(
                classify(BackendId::Codex, 400, body, &h, 60_000, 0),
                Outcome::QuotaExhausted { .. }
            ));
        }
    }

    #[test]
    fn codex_status_classes() {
        let h = headers(&[]);
        assert!(matches!(
            classify(BackendId::Codex, 429, "{}", &h, 60_000, 0),
            Outcome::QuotaExhausted { .. }
        ));
        assert!(matches!(
            classify(BackendId::Codex, 401, "", &h, 1, 0),
            Outcome::AuthFailed
        ));
        assert!(matches!(
            classify(BackendId::Codex, 403, "", &h, 1, 0),
            Outcome::AuthFailed
        ));
        assert!(matches!(
            classify(BackendId::Codex, 502, "", &h, 1, 0),
            Outcome::Transient
        ));
        assert!(matches!(
            classify(BackendId::Codex, 200, "", &h, 1, 0),
            Outcome::Ok
        ));
    }

    #[test]
    fn codex_capacity_429_cools_with_retry_deadline() {
        let h = headers(&[("retry-after", "45")]);
        match classify(
            BackendId::Codex,
            429,
            "Selected model is at capacity. Please try a different model.",
            &h,
            1_800_000,
            5_000,
        ) {
            Outcome::QuotaExhausted { until_ms } => assert_eq!(until_ms, 50_000),
            other => panic!("expected quota, got {other:?}"),
        }
    }

    #[test]
    fn generic_capacity_text_does_not_cool() {
        let h = headers(&[]);
        assert!(matches!(
            classify(
                BackendId::Codex,
                400,
                "context capacity is invalid",
                &h,
                60_000,
                0
            ),
            Outcome::Ok
        ));
    }

    #[test]
    fn copilot_429_uses_default_cooldown_when_no_header() {
        let h = headers(&[]);
        match classify(BackendId::Copilot, 429, "", &h, 1_800_000, 5_000) {
            Outcome::QuotaExhausted { until_ms } => assert_eq!(until_ms, 1_805_000),
            other => panic!("expected quota, got {other:?}"),
        }
    }
}
