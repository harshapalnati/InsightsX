use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tracing::{info, error};
use std::sync::Arc;
use crate::models::log_entry::LogEntry;
use serde_json;

pub async fn start_udp_listener(addr: &str, sender: Arc<mpsc::Sender<LogEntry>>) {
    let socket = UdpSocket::bind(addr).await.expect("Failed to bind UDP socket");
    let mut buf = vec![0u8; 1024];
    info!("UDP Log Listener running on {}", addr);

    loop {
        match socket.recv_from(&mut buf).await {
            Ok((size, _src)) => {
                let data = &buf[..size];
                match std::str::from_utf8(data) {
                    Ok(text) => {
                        match serde_json::from_str::<LogEntry>(text) {
                            Ok(log) => {
                                if sender.send(log).await.is_err() {
                                    error!("UDP log queue is full, dropping log");
                                }
                            }
                            Err(e) => error!("Failed to parse UDP log: {} | data: {}", e, text),
                        }
                    }
                    Err(_) => error!("Invalid UTF-8 data received on UDP"),
                }
            }
            Err(e) => error!("UDP receive error: {}", e),
        }
    }
}
