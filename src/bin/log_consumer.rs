use insightsx::config::AppConfig;
use insightsx::kafka::consumer::start_kafka_consumer;
use insightsx::storage::clickhouse::LogStorage;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let config = AppConfig::from_env();

    let storage = Arc::new(LogStorage::new(
        &config.clickhouse_url,
        &config.clickhouse_database,
        &config.clickhouse_password,
    ));

    start_kafka_consumer(&config.kafka_brokers, &config.kafka_topic, storage).await;
}
