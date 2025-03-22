use warp::Filter;
use tokio::sync::mpsc;
use tracing::{info, error};
use std::sync::Arc;
use crate::models::log_entry::LogEntry;
use bytes::Bytes;
use serde_json;

pub async fn run_ingest_api(sender: Arc<mpsc::Sender<LogEntry>>) {
    let ingest_route = warp::post()
        .and(warp::path("ingest"))
        .and(warp::body::bytes())
        .and(with_sender(sender))
        .and_then(handle_ingest);

    info!("HTTP Ingestion API running on 0.0.0.0:8080/ingest");
    warp::serve(ingest_route).run(([0, 0, 0, 0], 8080)).await;
}

fn with_sender(sender: Arc<mpsc::Sender<LogEntry>>) -> impl warp::Filter<Extract = (Arc<mpsc::Sender<LogEntry>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || sender.clone())
}

async fn handle_ingest(body: Bytes, sender: Arc<mpsc::Sender<LogEntry>>) -> Result<impl warp::Reply, warp::Rejection> {
    // Zero-copy: Bytes wraps the underlying data.
    let body_str = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(_) => {
            error!("Invalid UTF-8 in HTTP body");
            return Ok(warp::reply::with_status("Invalid UTF-8", warp::http::StatusCode::BAD_REQUEST));
        }
    };

    info!("Received HTTP ingestion payload: {}", body_str);

    // Try parsing as an array first, then as a single log.
    let logs: Vec<LogEntry> = if let Ok(parsed) = serde_json::from_str::<Vec<LogEntry>>(body_str) {
        parsed
    } else if let Ok(single) = serde_json::from_str::<LogEntry>(body_str) {
        vec![single]
    } else {
        error!("Failed to parse JSON for LogEntry");
        return Ok(warp::reply::with_status("Bad Request", warp::http::StatusCode::BAD_REQUEST));
    };

    for log in logs {
        if let Err(_) = sender.send(log).await {
            error!("Log queue is full, dropping HTTP log");
            return Ok(warp::reply::with_status("Queue Full", warp::http::StatusCode::SERVICE_UNAVAILABLE));
        }
    }
    Ok(warp::reply::with_status("Logs Ingested", warp::http::StatusCode::OK))
}
