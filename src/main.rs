// src/main.rs

pub mod config;
pub mod grpc;
pub mod kafka;
pub mod models;
pub mod observability;
pub mod ingestion;
pub mod storage;

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, error};

use crate::models::log_entry::{LogEntry, LogLevel};
use crate::storage::clickhouse::LogStorage;
use crate::kafka::producer::KafkaProducer;

#[tokio::main]
async fn main() {
    // Initialize tracing and metrics
    observability::tracing::init_tracing();
    info!("Tracing initialized");

    tokio::spawn(async {
        observability::metrics::serve_metrics().await;
    });
    info!("Metrics server running on http://0.0.0.0:9898/metrics");

    // Load config
    dotenv::dotenv().ok();
    let config = config::AppConfig::from_env();

    // Initialize ClickHouse
    info!("🔄 Initializing ClickHouse storage...");
    let storage = Arc::new(LogStorage::new(
        &config.clickhouse_url,
        &config.clickhouse_database,
        &config.clickhouse_password,
    ));

    if let Err(e) = storage.init_table().await {
        error!("❌ Failed to initialize ClickHouse table: {:?}", e);
    } else {
        info!("✅ ClickHouse table initialized successfully!");
    }

    // Manual test insert
    let test_log = LogEntry::new_info("manual_test", "Manual test message");
    match storage.insert_log(&test_log).await {
        Ok(_) => info!("Manual insert succeeded"),
        Err(e) => error!("Manual insert failed: {:?}", e),
    }

    // Log ingestion channel
    let (tx, mut rx) = mpsc::channel::<LogEntry>(10_000);
    let tx = Arc::new(tx);

    // Start ingestion listeners
    let tcp_tx = tx.clone();
    tokio::spawn(async move {
        ingestion::tcp::start_tcp_server("0.0.0.0:6000", tcp_tx).await;
    });
    
    let udp_tx = tx.clone();
    tokio::spawn(async move {
        ingestion::udp::start_udp_listener("0.0.0.0:6001", udp_tx).await;
    });
    
    let syslog_tx = tx.clone();
    tokio::spawn(async move {
        ingestion::syslog::start_syslog_listener("0.0.0.0:6002", syslog_tx).await;
    });
    

    let http_tx = tx.clone();
tokio::spawn(async move {
    let routes = ingestion::http::http_filter(http_tx);
    warp::serve(routes).run(([0, 0, 0, 0], 8080)).await;
});

    info!("✅ HTTP ingestion API started on port 8080");

    // Kafka producer
    let kafka_producer = KafkaProducer::new(
        &config.kafka_brokers,
        &config.kafka_topic,
        50, // batch size
        Duration::from_secs(1), // flush interval
    );
    
    info!("✅ Kafka producer initialized for topic: {}", config.kafka_topic);

    // Kafka forwarder
    tokio::spawn(async move {
        info!("🚀 Forwarding logs to Kafka...");
        while let Some(log) = rx.recv().await {
            if let Ok(payload) = rmp_serde::to_vec(&log) {
                kafka_producer.send(payload).await;
            }
        }
        kafka_producer.flush().await;
    });

    // Kafka consumer → ClickHouse
    let storage_clone = Arc::clone(&storage);
    let brokers = config.kafka_brokers.clone();
    let topic = config.kafka_topic.clone();
    tokio::spawn(async move {
        info!("🔄 Starting Kafka consumer for ClickHouse...");
        kafka::consumer::start_kafka_consumer(&brokers, &topic, storage_clone).await;
    });

    // gRPC server
    let grpc_addr = config.grpc_address.clone();
    tokio::spawn(async move {
        if let Err(e) = grpc::server::start_grpc_server(&grpc_addr).await {
            error!("❌ gRPC server failed: {}", e);
        }
    });

    info!("✅ Unified Ingestion Pipeline is up and running!");
    tokio::signal::ctrl_c().await.expect("Ctrl+C handler failed");
    info!("⚠️ Shutdown signal received. Cleaning up...");
}
