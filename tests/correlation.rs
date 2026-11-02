use axum::{
    Router,
    body::Body,
    extract::{Extension, Request},
    http::{HeaderMap, StatusCode},
    middleware,
    routing::get,
};
use std::sync::{Arc, Mutex};
use underclass::correlation;
use underclass::correlation::RequestId;

#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CapturedLogs {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
    type Writer = Self;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[tokio::test]
async fn shared_identity_survives_handlers_and_error_responses() {
    let logs = CapturedLogs::default();
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .json()
            .with_ansi(false)
            .with_writer(logs.clone())
            .finish(),
    )
    .unwrap();
    let app = Router::new()
        .route(
            "/ok",
            get(
                |Extension(id): Extension<RequestId>, request: Request| async move {
                    assert_eq!(id.header_value(), &request.headers()["x-request-id"]);
                    let mut response = axum::response::Response::new(Body::from("ok"));
                    response.headers_mut().insert(
                        "x-request-id",
                        "provider-id-must-not-replace-correlation".parse().unwrap(),
                    );
                    response
                },
            ),
        )
        .route("/denied", get(|| async { StatusCode::UNAUTHORIZED }))
        .layer(middleware::from_fn(correlation::middleware));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = reqwest::Client::new();
    let supplied = uuid::Uuid::now_v7().to_string();
    for (path, status) in [("/ok", 200), ("/denied", 401), ("/missing", 404)] {
        let response = client
            .get(format!("http://{address}{path}"))
            .header("x-request-id", &supplied)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), status);
        assert_eq!(response.headers()["x-request-id"], supplied);
    }
    for malformed in [
        "client-controlled-text",
        "00000000-0000-0000-0000-000000000000",
        "550e8400-e29b-41d4-a716-446655440000",
        "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
    ] {
        let response = client
            .get(format!("http://{address}/denied"))
            .header("x-request-id", malformed)
            .send()
            .await
            .unwrap();
        let id = response.headers()["x-request-id"].to_str().unwrap();
        assert_ne!(id, malformed);
        assert_eq!(
            uuid::Uuid::parse_str(id).unwrap().get_version(),
            Some(uuid::Version::SortRand)
        );
    }
    let mut headers = HeaderMap::new();
    headers.append("x-request-id", supplied.parse().unwrap());
    headers.append("x-request-id", supplied.parse().unwrap());
    let response = client
        .get(format!("http://{address}/ok"))
        .headers(headers)
        .send()
        .await
        .unwrap();
    assert_ne!(response.headers()["x-request-id"], supplied);
    let response = client
        .get(format!("http://{address}/missing"))
        .send()
        .await
        .unwrap();
    assert!(correlation::incoming_id(response.headers()).is_some());
    let log_bytes = logs.0.lock().unwrap().clone();
    let records: Vec<serde_json::Value> = log_bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect();
    let starts: Vec<&serde_json::Value> = records
        .iter()
        .filter(|event| event["fields"]["message"] == "request.started")
        .collect();
    let finishes: Vec<&serde_json::Value> = records
        .iter()
        .filter(|event| event["fields"]["message"] == "request.finished")
        .collect();
    assert_eq!(starts.len(), 9);
    assert_eq!(finishes.len(), starts.len());
    assert_eq!(
        starts
            .iter()
            .filter(|event| event["fields"]["request_id"] == supplied)
            .count(),
        3
    );
    for started in starts {
        let id = started["fields"]["request_id"].as_str().unwrap();
        assert_eq!(
            uuid::Uuid::parse_str(id).unwrap().get_version(),
            Some(uuid::Version::SortRand)
        );
        assert!(
            finishes
                .iter()
                .any(|event| event["fields"]["request_id"] == id)
        );
    }
    assert!(
        !String::from_utf8(log_bytes)
            .unwrap()
            .contains("client-controlled-text")
    );
    task.abort();
}
