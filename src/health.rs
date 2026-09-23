use crate::models::Outcome;

pub const RETRY_AFTER_MS_HEADER: &str = "retry-after-ms";
pub const RETRY_AFTER_HEADER: &str = "retry-after";

/// @cc [owner:ghuntley,label:pool] retry-after-absolute-deadline
/// `parse_retry_after_headers` MUST return the upstream deadline as an absolute epoch-millisecond
/// value (honoring `retry-after-ms`, then seconds-valued and HTTP-date `retry-after`), and MUST
/// return `None` when neither header supplies a usable deadline. HTTP dates in the past MUST
/// be ignored using the injected `now_ms` clock.
pub fn parse_retry_after_headers(headers: &http::HeaderMap, now_ms: i64) -> Option<i64> {
    if let Some(v) = headers
        .get(RETRY_AFTER_MS_HEADER)
        .and_then(|v| v.to_str().ok())
    {
        if let Ok(ms) = v.trim().parse::<f64>() {
            if ms.is_finite() && ms >= 0.0 {
                return Some(now_ms.saturating_add(ms as i64));
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
                return Some(now_ms.saturating_add((secs * 1000.0) as i64));
            }
        }
        if let Ok(date) = httpdate::parse_http_date(trimmed) {
            if let Ok(delta) = date.duration_since(std::time::UNIX_EPOCH) {
                let deadline = delta.as_millis().min(i64::MAX as u128) as i64;
                if deadline > now_ms {
                    return Some(deadline);
                }
            }
        }
    }
    None
}

/// @cc [owner:ghuntley,label:pool] classify-quota-auth-transient
/// `classify` MUST map status 429 to `QuotaExhausted` using the Retry-After deadline or the
/// configured fallback, 401/403 to `AuthFailed`, 5xx to `Transient`, and other statuses to `Ok`.
/// Backend-specific body signals MUST be handled by the backend implementation.
pub fn classify(
    status: u16,
    headers: &http::HeaderMap,
    default_cooldown_ms: i64,
    now_ms: i64,
) -> Outcome {
    let quota = |until: Option<i64>| Outcome::QuotaExhausted {
        until_ms: until.unwrap_or(now_ms.saturating_add(default_cooldown_ms)),
    };
    match status {
        429 => quota(parse_retry_after_headers(headers, now_ms)),
        401 | 403 => Outcome::AuthFailed,
        s if s >= 500 => Outcome::Transient,
        _ => Outcome::Ok,
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
    fn status_classes() {
        let h = headers(&[]);
        assert!(matches!(
            classify(429, &h, 60_000, 0),
            Outcome::QuotaExhausted { .. }
        ));
        assert!(matches!(classify(401, &h, 1, 0), Outcome::AuthFailed));
        assert!(matches!(classify(403, &h, 1, 0), Outcome::AuthFailed));
        assert!(matches!(classify(502, &h, 1, 0), Outcome::Transient));
        assert!(matches!(classify(200, &h, 1, 0), Outcome::Ok));
    }

    #[test]
    fn copilot_429_uses_default_cooldown_when_no_header() {
        let h = headers(&[]);
        match classify(429, &h, 1_800_000, 5_000) {
            Outcome::QuotaExhausted { until_ms } => assert_eq!(until_ms, 1_805_000),
            other => panic!("expected quota, got {other:?}"),
        }
    }

    #[test]
    fn http_date_uses_injected_clock() {
        let h = headers(&[("retry-after", "Thu, 01 Jan 1970 00:02:00 GMT")]);
        assert_eq!(parse_retry_after_headers(&h, 10_000), Some(120_000));
        assert_eq!(parse_retry_after_headers(&h, 130_000), None);
    }
}
