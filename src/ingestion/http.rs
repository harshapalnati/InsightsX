use warp::Filter;
use tokio::sync::mpsc;
use tracing::{info, error};
use std::sync::Arc;
use crate::models::log_entry::LogEntry;
use bytes::Bytes;
use simd_json::serde::from_slice;

/// Sets up a filter that injects a shared sender.
fn with_sender(sender: Arc<mpsc::Sender<LogEntry>>)
    -> impl warp::Filter<Extract = (Arc<mpsc::Sender<LogEntry>>,), Error = std::convert::Infallible> + Clone
{
    warp::any().map(move || sender.clone())
}

/// Ingestion endpoint handler. Parses the request body as JSON and sets the server timestamp.
async fn handle_ingest(body: Bytes, sender: Arc<mpsc::Sender<LogEntry>>)
    -> Result<impl warp::Reply, warp::Rejection>
{
    let mut input = body.to_vec();

    // Peek the first non-whitespace byte to determine if input is array or object
    let first_char = input.iter().find(|&&b| !b.is_ascii_whitespace());

    match first_char {
        Some(b'[') => {
            match from_slice::<Vec<LogEntry>>(&mut input) {
                Ok(logs) => {
                    for log in logs {
                        if let Err(e) = sender.send(log).await {
                            error!("Failed to send log: {}", e);
                        }
                    }
                    Ok(warp::reply::with_status("Batch Received", warp::http::StatusCode::OK))
                }
                Err(e) => {
                    error!("Failed to parse batch: {}", e);
                    Ok(warp::reply::with_status("Invalid batch", warp::http::StatusCode::BAD_REQUEST))
                }
            }
        }
        Some(b'{') => {
            match from_slice::<LogEntry>(&mut input) {
                Ok(log) => {
                    if let Err(e) = sender.send(log).await {
                        error!("Failed to send log: {}", e);
                    }
                    Ok(warp::reply::with_status("Log Received", warp::http::StatusCode::OK))
                }
                Err(e) => {
                    error!("Failed to parse log: {}", e);
                    Ok(warp::reply::with_status("Invalid log", warp::http::StatusCode::BAD_REQUEST))
                }
            }
        }
        _ => Ok(warp::reply::with_status("Empty or unknown input", warp::http::StatusCode::BAD_REQUEST))
    }
}

pub fn http_filter(sender: Arc<mpsc::Sender<LogEntry>>) -> impl warp::Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path("logs")
        .and(warp::post())
        .and(warp::body::bytes())
        .and(with_sender(sender))
        .and_then(handle_ingest)
}
