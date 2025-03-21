// src/main.rs

pub mod kafka;
pub mod config;
pub mod grpc;
pub mod models;
pub mod observability;

use tokio::signal;
use tokio::task;
use tracing::{info, error};
use crate::kafka::consumer::KafkaConsumer;
use observability::tracing as ob_tracing;
use observability::metrics as ob_metrics;

#[tokio::main]
async fn main() {
    // Initialize distributed tracing
    ob_tracing::init_tracing();
    info!("🟢 Tracing initialized");

    // Spawn the metrics server on port 9898 in its own task
    tokio::spawn(async {
        ob_metrics::serve_metrics().await;
    });
    info!("🟢 Metrics server started at http://0.0.0.0:9898/metrics");

    let config = config::AppConfig::from_env();
    info!("🚀 InsightsX gRPC server running at: {}", config.grpc_address);

    // Start gRPC server asynchronously
    let grpc_task = tokio::spawn(async move {
        if let Err(e) = grpc::server::start_grpc_server(&config.grpc_address).await {
            error!("❌ Failed to start gRPC server: {}", e);
        }
    });

    // Start Kafka Consumer in a separate blocking task.
    // For each consumed message, we increment a Prometheus counter.
    let kafka_task = task::spawn_blocking(move || {
        let mut kafka_consumer = KafkaConsumer::new(
            vec![config.kafka_brokers.clone()],
            &config.kafka_topic,
        );
        kafka_consumer.consume(|msg| {
            // Increment the messages processed counter
            ob_metrics::MESSAGE_COUNTER.inc();
            info!("✅ Consumed message: {:?}", msg);
        });
    });

    // Graceful shutdown handling: wait for a shutdown signal or task exit.
    tokio::select! {
        _ = grpc_task => error!("⚠️ gRPC server exited unexpectedly"),
        _ = kafka_task => error!("⚠️ Kafka consumer exited unexpectedly"),
        _ = signal::ctrl_c() => {
            info!("🛑 Shutdown signal received. Exiting...");
        }
    }
}
