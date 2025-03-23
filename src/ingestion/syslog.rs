use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tracing::{info, error, warn};
use std::sync::Arc;
use std::str;
use std::net::SocketAddr;
use chrono::Utc;
use crate::models::log_entry::{LogEntry, LogLevel};

pub async fn start_syslog_listener(addr: &str, sender: Arc<mpsc::Sender<LogEntry>>) {
    let socket = UdpSocket::bind(addr)
        .await
        .expect("Failed to bind Syslog UDP socket");
    let mut buf = vec![0u8; 2048];
    info!("Syslog Listener running on {}", addr);

    loop {
        match socket.recv_from(&mut buf).await {
            Ok((size, src)) => {
                let raw_msg = match str::from_utf8(&buf[..size]) {
                    Ok(s) => s,
                    Err(_) => "<invalid UTF-8>",
                };
                if let Some(log) = parse_syslog(raw_msg, src) {
                    if sender.send(log).await.is_err() {
                        error!("Syslog queue is full, dropping log from {}", src);
                    }
                }
            }
            Err(e) => error!("Syslog receive error: {}", e),
        }
    }
}

fn parse_syslog(msg: &str, src: SocketAddr) -> Option<LogEntry> {
    // Split the incoming syslog message into at most 3 parts:
    let parts: Vec<&str> = msg.splitn(3, ' ').collect();
    if parts.len() < 3 {
        warn!("Invalid syslog format from {}: {}", src, msg);
        return None;
    }
    let level = match parts[0] {
        "INFO" => LogLevel::INFO,
        "WARN" => LogLevel::WARN,
        "ERROR" => LogLevel::ERROR,
        "DEBUG" => LogLevel::DEBUG,
        _ => LogLevel::INFO,
    };
    // Set both client and server timestamps to the current time.
    let now = Utc::now().timestamp_millis();
    Some(LogEntry {
        log_type: Some("syslog".into()),
        source: format!("syslog:{}", src),
        level,
        message: parts[2].to_string(),
        client_timestamp: now,
        server_timestamp: Some(now),
        trace_id: None,
        span_id: None,
        service: None,
        metadata: None,
    })
}
