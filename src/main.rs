pub mod config;
pub mod grpc;
pub mod kafka;
pub mod models;
pub mod observability;
pub mod ingestion;
pub mod storage;

use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;
use models::log_entry::LogEntry;
use storage::clickhouse::LogStorage;

#[tokio::main]
async fn main() {
    // Initialize tracing and metrics
    observability::tracing::init_tracing();
    info!("Tracing initialized");

    tokio::spawn(async {
        observability::metrics::serve_metrics().await;
    });
    info!("Metrics server running on http://0.0.0.0:9898/metrics");

    // Load app configuration
    let config = config::AppConfig::from_env();

    // Initialize ClickHouse Cloud storage
    // Note that the URL includes https:// to enable secure connection
    info!("🔄 Initializing ClickHouse storage...");
    let storage = Arc::new(LogStorage::new(
        "https://i0zbyfvwa7.us-east-1.aws.clickhouse.cloud:8443", 
        "default",
        "yq3ELB.ONGWE_",
    ));
   
    if let Err(e) = storage.init_table().await {
        eprintln!("❌ Failed to initialize ClickHouse table: {:?}", e);
    } else {
        info!("✅ ClickHouse table initialized successfully!");
    }

    // Create centralized log channel
    let (tx, rx) = mpsc::channel::<LogEntry>(10_000);
    let tx = Arc::new(tx);

    // Ingestion protocol handlers
    let tcp_tx = tx.clone();
    tokio::spawn(async move {
        ingestion::tcp::start_tcp_server("0.0.0.0:6000", tcp_tx).await;
    });
    info!("✅ TCP ingestion server started on 0.0.0.0:6000");

    let udp_tx = tx.clone();
    tokio::spawn(async move {
        ingestion::udp::start_udp_listener("0.0.0.0:6001", udp_tx).await;
    });
    info!("✅ UDP ingestion listener started on 0.0.0.0:6001");

    let syslog_tx = tx.clone();
    tokio::spawn(async move {
        ingestion::syslog::start_syslog_listener("0.0.0.0:6002", syslog_tx).await;
    });
    info!("✅ Syslog ingestion listener started on 0.0.0.0:6002");

    let http_tx = tx.clone();
    tokio::spawn(async move {
        ingestion::http::run_ingest_api(http_tx).await;
    });
    info!("✅ HTTP ingestion API started");

    // Kafka producer
    let mut kafka_producer = kafka::producer::KafkaProducer::new(
        &config.kafka_brokers,
        &config.kafka_topic,
        20,
    );
    info!("✅ Kafka producer initialized for topic: {}", config.kafka_topic);

    // Log stream -> Kafka
    let mut rx = rx;
    tokio::spawn(async move {
        info!("🚀 Starting log stream to Kafka pipeline");
        while let Some(log) = rx.recv().await {
            if let Ok(payload) = rmp_serde::to_vec(&log) {
                kafka_producer.send(&payload).await;
            }
        }
        kafka_producer.flush().await;
    });

    // Kafka -> ClickHouse
    let kafka_brokers = config.kafka_brokers.clone();
    let kafka_topic = config.kafka_topic.clone();
    let storage_clone = storage.clone();
    tokio::spawn(async move {
        info!("🔄 Starting Kafka consumer for ClickHouse ingestion");
        kafka::consumer::start_kafka_consumer(kafka_brokers, kafka_topic, storage_clone).await;
    });

    // gRPC server
    let grpc_addr = config.grpc_address.clone();
    tokio::spawn(async move {
        info!("🔄 Starting gRPC server on {}", grpc_addr);
        if let Err(e) = grpc::server::start_grpc_server(&grpc_addr).await {
            tracing::error!("❌ gRPC server failed: {}", e);
        }
    });

    info!("✅ Unified Ingestion Pipeline is up and running!");
    tokio::signal::ctrl_c().await.expect("Ctrl+C handler failed");
    info!("⚠️ Shutdown signal received. Cleaning up...");
}