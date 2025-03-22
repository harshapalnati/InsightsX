use reqwest::Client;
use tokio::net::{TcpStream, UdpSocket};
use tokio::io::AsyncWriteExt;
use futures::stream::{self, StreamExt};
use serde_json::json;
use chrono::Utc;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::Instant;

#[tokio::main]
async fn main() {
    let total_messages = 1_000_000;
    let endpoints = 4;
    let per_endpoint = total_messages / endpoints;
    let concurrency = 1000;
    let start = Instant::now();

    // HTTP benchmark task.
    let http_client = Client::new();
    let http_task = tokio::spawn(async move {
        stream::iter(0..per_endpoint)
            .for_each_concurrent(concurrency, |i| {
                let client = http_client.clone();
                async move {
                    let payload = json!({
                        "log": {
                            "source": format!("benchmark_http_{}", i),
                            "level": "INFO",
                            "message": format!("HTTP Test log message {}", i),
                            "timestamp": Utc::now().timestamp_millis(),
                            "trace_id": null,
                            "span_id": null,
                            "service": "benchmark_http",
                            "metadata": {},
                            "log_type": "http"
                        }
                    });
                    if let Err(e) = client.post("http://localhost:8080/ingest")
                        .json(&payload)
                        .send()
                        .await
                    {
                        eprintln!("HTTP error on message {}: {}", i, e);
                    }
                }
            })
            .await;
    });

    // UDP benchmark task.
    let udp_task = tokio::spawn(async move {
        let socket = UdpSocket::bind("0.0.0.0:0").await.expect("failed to bind UDP");
        let socket = Arc::new(socket);
        let addr: SocketAddr = "127.0.0.1:6001".parse().unwrap();
        stream::iter(0..per_endpoint)
            .for_each_concurrent(concurrency, |i| {
                let socket = socket.clone();
                async move {
                    let payload = json!({
                        "source": format!("benchmark_udp_{}", i),
                        "level": "INFO",
                        "message": format!("UDP Test log message {}", i),
                        "timestamp": Utc::now().timestamp_millis(),
                        "trace_id": null,
                        "span_id": null,
                        "service": "benchmark_udp",
                        "metadata": {},
                        "log_type": "udp"
                    });
                    let data = payload.to_string();
                    if let Err(e) = socket.send_to(data.as_bytes(), addr).await {
                        eprintln!("UDP error on message {}: {}", i, e);
                    }
                }
            })
            .await;
    });

    // TCP benchmark task.
    let tcp_task = tokio::spawn(async move {
        let mut stream = TcpStream::connect("127.0.0.1:6000").await.expect("failed to connect TCP");
        stream.set_nodelay(true).expect("failed to set_nodelay");
        for i in 0..per_endpoint {
            let payload = json!({
                "source": format!("benchmark_tcp_{}", i),
                "level": "INFO",
                "message": format!("TCP Test log message {}", i),
                "timestamp": Utc::now().timestamp_millis(),
                "trace_id": null,
                "span_id": null,
                "service": "benchmark_tcp",
                "metadata": {},
                "log_type": "tcp"
            });
            let data = payload.to_string() + "\n";
            if let Err(e) = stream.write_all(data.as_bytes()).await {
                eprintln!("TCP error on message {}: {}", i, e);
            }
        }
    });

    // Syslog benchmark task (using UDP on port 6002).
    let syslog_task = tokio::spawn(async move {
        let socket = UdpSocket::bind("0.0.0.0:0").await.expect("failed to bind syslog UDP");
        let socket = Arc::new(socket);
        let addr: SocketAddr = "127.0.0.1:6002".parse().unwrap();
        stream::iter(0..per_endpoint)
            .for_each_concurrent(concurrency, |i| {
                let socket = socket.clone();
                async move {
                    let payload = json!({
                        "source": format!("benchmark_syslog_{}", i),
                        "level": "INFO",
                        "message": format!("Syslog Test log message {}", i),
                        "timestamp": Utc::now().timestamp_millis(),
                        "trace_id": null,
                        "span_id": null,
                        "service": "benchmark_syslog",
                        "metadata": {},
                        "log_type": "syslog"
                    });
                    let data = payload.to_string();
                    if let Err(e) = socket.send_to(data.as_bytes(), addr).await {
                        eprintln!("Syslog error on message {}: {}", i, e);
                    }
                }
            })
            .await;
    });

    // Wait for all tasks to complete.
    let _ = tokio::join!(http_task, udp_task, tcp_task, syslog_task);

    let duration = start.elapsed();
    println!("Benchmark completed: Sent {} messages per endpoint in {:?}", per_endpoint, duration);
}
