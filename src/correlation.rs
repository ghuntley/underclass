//! Validated request identity shared across the preflight/underclass boundary.
use axum::{
    extract::Request,
    http::{HeaderMap, HeaderValue},
    middleware::Next,
    response::Response,
};
use tracing::Instrument;
use uuid::Uuid;

/// The validated correlation header, installed only by this module's middleware.
#[derive(Clone, Debug)]
pub struct RequestId(HeaderValue);

impl RequestId {
    pub fn header_value(&self) -> &HeaderValue {
        &self.0
    }
}

/// @cc [owner:ghuntley,label:security] bounded-correlation-input
/// Only one hyphenated RFC9562 UUIDv7 header MAY be accepted as request identity.
/// Invalid, duplicate, nil, or other-version inputs MUST NOT be logged or echoed.
pub fn incoming_id(headers: &HeaderMap) -> Option<Uuid> {
    let mut values = headers.get_all("x-request-id").iter();
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    canonical_id(value.to_str().ok()?)
}
fn canonical_id(value: &str) -> Option<Uuid> {
    if value.len() != 36 {
        return None;
    }
    let id = Uuid::parse_str(value).ok()?;
    (id.get_version() == Some(uuid::Version::SortRand)
        && id.get_variant() == uuid::Variant::RFC4122
        && id.to_string().eq_ignore_ascii_case(value))
    .then_some(id)
}

/// @cc [owner:ghuntley,label:logging] shared-request-identity
/// A valid incoming request ID MUST be preserved through logs, handler extensions,
/// and every response. Otherwise a UUIDv7 MUST be minted before authentication.
/// request.started MUST record that identity before dispatch or authentication.
pub async fn middleware(mut request: Request, next: Next) -> Response {
    let id = incoming_id(request.headers()).unwrap_or_else(Uuid::now_v7);
    let value = HeaderValue::from_str(&id.to_string()).expect("UUID is a valid header");
    request.headers_mut().insert("x-request-id", value.clone());
    request.extensions_mut().insert(RequestId(value.clone()));
    let start = std::time::Instant::now();
    tracing::info!(request_id=%id, "request.started");
    let mut response = next
        .run(request)
        .instrument(tracing::info_span!("request",request_id=%id))
        .await;
    response.headers_mut().insert("x-request-id", value);
    tracing::info!(request_id=%id,status=response.status().as_u16(),duration_ms=start.elapsed().as_millis() as u64,"request.finished");
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use hegel::{TestCase, generators as gs};
    #[hegel::test]
    fn arbitrary_inputs_cannot_escape_the_uuid_contract(tc: TestCase) {
        let text = tc.draw(gs::text());
        if let Some(id) = canonical_id(&text) {
            assert_eq!(text.len(), 36);
            assert_eq!(id.get_version(), Some(uuid::Version::SortRand));
            assert_eq!(id.get_variant(), uuid::Variant::RFC4122);
            assert!(id.to_string().eq_ignore_ascii_case(&text));
        }
    }
    #[hegel::test]
    fn uuid_headers_roundtrip_but_duplicates_do_not(tc: TestCase) {
        let duplicate = tc.draw(gs::integers::<u8>().min_value(0).max_value(1)) == 1;
        let id = Uuid::now_v7();
        let mut headers = HeaderMap::new();
        let value = HeaderValue::from_str(&id.to_string()).unwrap();
        headers.insert("x-request-id", value.clone());
        if duplicate {
            headers.append("x-request-id", value);
        }
        assert_eq!(
            incoming_id(&headers),
            if duplicate { None } else { Some(id) }
        );
    }
    #[test]
    fn malformed_and_non_v7_ids_are_not_accepted() {
        for value in [
            "",
            "client-controlled-text",
            "00000000-0000-0000-0000-000000000000",
            "550e8400-e29b-41d4-a716-446655440000",
            "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
            "urn:uuid:550e8400-e29b-41d4-a716-446655440000",
        ] {
            assert!(canonical_id(value).is_none());
        }
    }
}
