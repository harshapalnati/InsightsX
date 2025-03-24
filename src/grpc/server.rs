// src/grpc/server.rs
use tonic::{transport::Server, Request, Response, Status, Streaming};
use std::net::SocketAddr;
use std::time::Duration;
use tracing::{info, error};
use tokio::time;

use crate::config::AppConfig;
use crate::kafka::producer::KafkaProducer;
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
        let config = AppConfig::from_env();

        let kafka_producer = KafkaProducer::new(
            &config.kafka_brokers,
            &config.kafka_topic,
            50,
            Duration::from_secs(1),
        );

        let mut stream = request.into_inner();
        let mut flush_interval = time::interval(Duration::from_millis(100));

        loop {
            tokio::select! {
                maybe_msg = stream.message() => {
                    match maybe_msg {
                        Ok(Some(grpc_log)) => {
                            let log_entry = LogEntry {
                                source: grpc_log.source,
                                level: match grpc_log.level.as_str() {
                                    "INFO" => LogLevel::INFO,
                                    "WARN" => LogLevel::WARN,
                                    "ERROR" => LogLevel::ERROR,
                                    "DEBUG" => LogLevel::DEBUG,
                                    _ => LogLevel::INFO,
                                },
                                message: grpc_log.message,
                                client_timestamp: grpc_log.timestamp,
                                server_timestamp: Some(LogEntry::current_timestamp()),
                                trace_id: Some("trace-1234".into()),
                                span_id: Some("span-5678".into()),
                                service: Some("insightsx-service".into()),
                                metadata: None,
                                log_type: Some("grpc".into()),
                            };

                            let serialized_log = rmp_serde::to_vec(&log_entry)
                                .map_err(|e| Status::internal(format!("Serialization error: {}", e)))?;

                            let compressed_log = compress_data(&serialized_log);

                            kafka_producer.send(compressed_log).await;
                        }
                        Ok(None) => break, // End-of-stream
                        Err(e) => return Err(Status::internal(format!("Stream error: {}", e))),
                    }
                },
                _ = flush_interval.tick() => {
                    kafka_producer.flush().await;
                },
            }
        }

        kafka_producer.flush().await;
        Ok(Response::new(Ack {
            success: true,
            message: "Logs sent successfully".into(),
        }))
    }
}

fn compress_data(data: &[u8]) -> Vec<u8> {
    let mut encoder = snap::raw::Encoder::new();
    encoder.compress_vec(data).expect("Compression failed")
}

pub async fn start_grpc_server(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let socket_addr: SocketAddr = addr.parse()?;
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