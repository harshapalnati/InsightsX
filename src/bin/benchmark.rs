use reqwest::Client;
use tokio::net::{TcpStream, UdpSocket};
use tokio::io::AsyncWriteExt;
use futures::stream::{self, StreamExt};
use serde_json::json;
use chrono::Utc;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::time::Instant;
use std::env;

#[tokio::main]
async fn main() {
    // Read endpoint configuration from environment variables.
    // For HTTP, if not set, default to "http://localhost:8080/ingest"
    let http_endpoint = env::var("HTTP_ENDPOINT").unwrap_or_else(|_| "http://localhost:8080/ingest".to_string());
    // For UDP, default to "127.0.0.1:6001"
    let udp_endpoint = env::var("UDP_ENDPOINT").unwrap_or_else(|_| "127.0.0.1:6001".to_string());
    // For TCP, default to "127.0.0.1:6000"
    let tcp_endpoint = env::var("TCP_ENDPOINT").unwrap_or_else(|_| "127.0.0.1:6000".to_string());
    // For Syslog (UDP), default to "127.0.0.1:6002"
    let syslog_endpoint = env::var("SYSLOG_ENDPOINT").unwrap_or_else(|_| "127.0.0.1:6002".to_string());

    // For testing, we'll send 100 messages total.
    let total_messages = 100;
    let concurrency = 10;
    let start = Instant::now();

    // Atomic counters for successes and errors.
    let http_success = Arc::new(AtomicUsize::new(0));
    let http_error = Arc::new(AtomicUsize::new(0));

    let udp_success = Arc::new(AtomicUsize::new(0));
    let udp_error = Arc::new(AtomicUsize::new(0));

    let tcp_success = Arc::new(AtomicUsize::new(0));
    let tcp_error = Arc::new(AtomicUsize::new(0));

    let syslog_success = Arc::new(AtomicUsize::new(0));
    let syslog_error = Arc::new(AtomicUsize::new(0));

    // HTTP benchmark task.
    let http_client = Client::new();
    let http_success_clone = http_success.clone();
    let http_error_clone = http_error.clone();
    let http_ep = http_endpoint.clone();
    let http_task = tokio::spawn(async move {
        stream::iter(0..total_messages)
            .for_each_concurrent(concurrency, |i| {
                let client = http_client.clone();
                let success = http_success_clone.clone();
                let error = http_error_clone.clone();
                let ep = http_ep.clone();
                async move {
                    // Build a payload matching your working manual request.
                    // Here we use a 13-digit millisecond timestamp as in your Postman test.
                    let payload = json!({
                        "source": "test",
                        "level": "INFO",
                        "message": "Test message from Postman",
                        "client_timestamp": Utc::now().timestamp_millis(),
                        "trace_id": null,
                        "span_id": null,
                        "service": "test-service",
                        "metadata": {
                            "app": "test-app",
                            "environment": "development",
                            "version": "1.0.0"
                        },
                        "log_type": "application"
                    });
                    match client.post(&ep)
                        .json(&payload)
                        .send()
                        .await
                    {
                        Ok(resp) if resp.status().is_success() => {
                            success.fetch_add(1, Ordering::Relaxed);
                            ()
                        },
                        Ok(resp) => {
                            eprintln!("HTTP error on message {}: HTTP {}", i, resp.status());
                            if resp.status() == reqwest::StatusCode::BAD_REQUEST {
                                if let Ok(text) = resp.text().await {
                                    eprintln!("Payload: {}", payload);
                                    eprintln!("Response body: {}", text);
                                }
                            }
                            error.fetch_add(1, Ordering::Relaxed);
                            ()
                        },
                        Err(e) => {
                            eprintln!("HTTP error on message {}: {}", i, e);
                            error.fetch_add(1, Ordering::Relaxed);
                            ()
                        }
                    }
                }
            })
            .await;
    });

    // UDP benchmark task.
    let udp_success_clone = udp_success.clone();
    let udp_error_clone = udp_error.clone();
    let udp_ep = udp_endpoint.clone();
    let udp_task = tokio::spawn(async move {
        // Resolve UDP endpoint address.
        let addr: SocketAddr = udp_ep.parse().expect("Invalid UDP endpoint address");
        let socket = UdpSocket::bind("0.0.0.0:0").await.expect("failed to bind UDP");
        let socket = Arc::new(socket);
        stream::iter(0..total_messages)
            .for_each_concurrent(concurrency, |i| {
                let socket = socket.clone();
                let success = udp_success_clone.clone();
                let error = udp_error_clone.clone();
                async move {
                    let payload = json!({
                        "source": "test",
                        "level": "INFO",
                        "message": "Test message from Postman",
                        "client_timestamp": Utc::now().timestamp_millis(),
                        "trace_id": null,
                        "span_id": null,
                        "service": "test-service",
                        "metadata": {
                            "app": "test-app",
                            "environment": "development",
                            "version": "1.0.0"
                        },
                        "log_type": "application"
                    });
                    let data = payload.to_string();
                    match socket.send_to(data.as_bytes(), addr).await {
                        Ok(_) => {
                            success.fetch_add(1, Ordering::Relaxed);
                            ()
                        },
                        Err(e) => {
                            eprintln!("UDP error on message {}: {}", i, e);
                            error.fetch_add(1, Ordering::Relaxed);
                            ()
                        }
                    }
                }
            })
            .await;
    });

    // TCP benchmark task.
    let tcp_success_clone = tcp_success.clone();
    let tcp_error_clone = tcp_error.clone();
    let tcp_ep = tcp_endpoint.clone();
    let tcp_task = tokio::spawn(async move {
        let mut stream = TcpStream::connect(&tcp_ep)
            .await
            .expect("failed to connect TCP");
        stream.set_nodelay(true).expect("failed to set_nodelay");
        for i in 0..total_messages {
            let payload = json!({
                "source": "test",
                "level": "INFO",
                "message": "Test message from Postman",
                "client_timestamp": Utc::now().timestamp_millis(),
                "trace_id": null,
                "span_id": null,
                "service": "test-service",
                "metadata": {
                    "app": "test-app",
                    "environment": "development",
                    "version": "1.0.0"
                },
                "log_type": "application"
            });
            let data = payload.to_string() + "\n";
            if let Err(e) = stream.write_all(data.as_bytes()).await {
                eprintln!("TCP error on message {}: {}", i, e);
                tcp_error_clone.fetch_add(1, Ordering::Relaxed);
            } else {
                tcp_success_clone.fetch_add(1, Ordering::Relaxed);
            }
        }
    });

    // Syslog benchmark task (UDP).
    let syslog_success_clone = syslog_success.clone();
    let syslog_error_clone = syslog_error.clone();
    let syslog_ep = syslog_endpoint.clone();
    let syslog_task = tokio::spawn(async move {
        let addr: SocketAddr = syslog_ep.parse().expect("Invalid syslog endpoint address");
        let socket = UdpSocket::bind("0.0.0.0:0").await.expect("failed to bind syslog UDP");
        let socket = Arc::new(socket);
        stream::iter(0..total_messages)
            .for_each_concurrent(concurrency, |i| {
                let socket = socket.clone();
                let success = syslog_success_clone.clone();
                let error = syslog_error_clone.clone();
                async move {
                    let payload = json!({
                        "source": "test",
                        "level": "INFO",
                        "message": "Test message from Postman",
                        "client_timestamp": Utc::now().timestamp_millis(),
                        "trace_id": null,
                        "span_id": null,
                        "service": "test-service",
                        "metadata": {
                            "app": "test-app",
                            "environment": "development",
                            "version": "1.0.0"
                        },
                        "log_type": "application"
                    });
                    let data = payload.to_string();
                    match socket.send_to(data.as_bytes(), addr).await {
                        Ok(_) => {
                            success.fetch_add(1, Ordering::Relaxed);
                            ()
                        },
                        Err(e) => {
                            eprintln!("Syslog error on message {}: {}", i, e);
                            error.fetch_add(1, Ordering::Relaxed);
                            ()
                        }
                    }
                }
            })
            .await;
    });

    // Wait for all tasks to complete.
    let _ = tokio::join!(http_task, udp_task, tcp_task, syslog_task);

    let duration = start.elapsed();

    println!("Benchmark completed in {:?}", duration);
    println!("Messages per protocol: {}", total_messages);
    println!();
    println!("HTTP -> Success: {}, Errors: {}", http_success.load(Ordering::Relaxed), http_error.load(Ordering::Relaxed));
    println!("UDP -> Success: {}, Errors: {}", udp_success.load(Ordering::Relaxed), udp_error.load(Ordering::Relaxed));
    println!("TCP -> Success: {}, Errors: {}", tcp_success.load(Ordering::Relaxed), tcp_error.load(Ordering::Relaxed));
    println!("Syslog -> Success: {}, Errors: {}", syslog_success.load(Ordering::Relaxed), syslog_error.load(Ordering::Relaxed));

    let total_success = http_success.load(Ordering::Relaxed)
        + udp_success.load(Ordering::Relaxed)
        + tcp_success.load(Ordering::Relaxed)
        + syslog_success.load(Ordering::Relaxed);
    println!("Total successful messages sent: {}", total_success);
    println!("Throughput: {:.2} messages/second", (total_messages * 4) as f64 / duration.as_secs_f64());
}
