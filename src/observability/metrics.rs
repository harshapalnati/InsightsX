use prometheus::{Encoder, TextEncoder, register_counter, Counter};
use warp::Filter;
use lazy_static::lazy_static;

lazy_static! {
    pub static ref MESSAGE_COUNTER: Counter = register_counter!(
        "messages_processed_total",
        "Total number of processed messages"
    ).unwrap();
}

/// Serves a `/metrics` HTTP endpoint on port 9898 for Prometheus to scrape.
pub async fn serve_metrics() {
    let metrics_route = warp::path("metrics").map(|| {
        let encoder = TextEncoder::new();
        let metric_families = prometheus::gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        warp::reply::with_header(buffer, "Content-Type", encoder.format_type())
    });
    warp::serve(metrics_route).run(([0, 0, 0, 0], 9898)).await;
}
