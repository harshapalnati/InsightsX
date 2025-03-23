use tonic::{transport::Server, Request, Response, Status, Streaming};
use std::net::SocketAddr;
use tracing::{info, error};
use tokio::time::{self, Duration};
use crate::kafka::producer::KafkaProducer;
use rmp_serde;
use crate::models::log_entry::{LogEntry, LogLevel};

pub mod logs {
    tonic::include_proto!("logs");
}

use logs::{Ack, LogEntry as GrpcLogEntry};
use logs::log_service_server::{LogService, LogServiceServer};

#[derive(Debug, Default)]
pub struct MyLogService;

#[tonic::async_trait]
impl LogService for MyLogService {
    async fn stream_logs(
        &self,
        request: Request<Streaming<GrpcLogEntry>>,
    ) -> Result<Response<Ack>, Status> {
        // Load configuration.
        let config = crate::config::AppConfig::from_env();

        // Create a Kafka producer with a batch size of 10.
        let mut kafka_producer = KafkaProducer::new(&config.kafka_brokers, &config.kafka_topic, 10);

        // Get the incoming stream of gRPC log entries.
        let mut stream = request.into_inner();
        // Set up a periodic flush interval.
        let mut flush_interval = time::interval(Duration::from_millis(100));

        loop {
            tokio::select! {
                maybe_msg = stream.message() => {
                    match maybe_msg {
                        Ok(Some(grpc_log)) => {
                            // Map the gRPC log message to our internal LogEntry.
                            // Assume grpc_log.timestamp exists and is i64. If not, use current time for client_timestamp.
                            let client_ts = grpc_log.timestamp; // if not available, replace with LogEntry::current_timestamp()
                            
                            let log_entry = LogEntry {
                                source: grpc_log.source,
                                level: match grpc_log.level.as_str() {
                                    "INFO"  => LogLevel::INFO,
                                    "WARN"  => LogLevel::WARN,
                                    "ERROR" => LogLevel::ERROR,
                                    "DEBUG" => LogLevel::DEBUG,
                                    _ => LogLevel::INFO,
                                },
                                message: grpc_log.message,
                                client_timestamp: client_ts,
                                server_timestamp: Some(LogEntry::current_timestamp()),
                                trace_id: Some("trace-1234".into()),   // Replace with dynamic value as needed.
                                span_id: Some("span-5678".into()),       // Replace with dynamic value as needed.
                                service: Some("insightsx-service".into()),
                                metadata: None,
                                log_type: Some("grpc".into()),
                            };

                            // Serialize the log entry using MessagePack.
                            let serialized_log = rmp_serde::to_vec(&log_entry)
                                .map_err(|e| Status::internal(format!("Serialization error: {}", e)))?;
                            
                            // Compress the serialized log.
                            let compressed_log = compress_data(&serialized_log);
                            
                            // Asynchronously send the compressed log.
                            kafka_producer.send(&compressed_log).await;
                        },
                        Ok(None) => break, // End-of-stream.
                        Err(e) => {
                            return Err(Status::internal(format!("Error receiving message: {}", e)));
                        },
                    }
                },
                _ = flush_interval.tick() => {
                    // Flush pending Kafka messages periodically.
                    kafka_producer.flush().await;
                },
            }
        }
        // Final flush in case any messages remain.
        kafka_producer.flush().await;
        Ok(Response::new(Ack { success: true, message: "Logs sent successfully".into() }))
    }
}

/// Compresses data using Snappy compression.
fn compress_data(data: &[u8]) -> Vec<u8> {
    let mut encoder = snap::raw::Encoder::new();
    encoder.compress_vec(data).expect("Compression failed")
}

/// Starts the gRPC server on the given address.
pub async fn start_grpc_server(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let socket_addr: SocketAddr = addr.parse().expect("Invalid address format");
    info!("Starting gRPC server on {}", socket_addr);

    let service = LogServiceServer::new(MyLogService::default());

    Server::builder()
        .add_service(service)
        .serve(socket_addr)
        .await
        .map_err(|e| {
            error!("Failed to start gRPC server: {}", e);
            Box::new(e) as Box<dyn std::error::Error>
        })
}
