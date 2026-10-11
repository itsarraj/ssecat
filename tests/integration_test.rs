use std::net::SocketAddr;
use std::time::Duration;

use reqwest::Method;
use ssecat::{ClientConfig, StreamClient};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

async fn spawn_sse_mock_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                // Read HTTP request header
                let mut buf = [0u8; 1024];
                let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut buf).await;

                // Send HTTP 200 OK response with chunked SSE
                let header = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n";
                let _ = stream.write_all(header.as_bytes()).await;

                let event1 = "event: init\nid: 1\ndata: {\"status\":\"ready\"}\n\n";
                let _ = stream.write_all(event1.as_bytes()).await;
                tokio::time::sleep(Duration::from_millis(50)).await;

                let event2 = "event: token\nid: 2\ndata: {\"delta\":{\"text\":\"Hello \"}}\n\n";
                let _ = stream.write_all(event2.as_bytes()).await;
                tokio::time::sleep(Duration::from_millis(50)).await;

                let event3 = "event: token\nid: 3\ndata: {\"delta\":{\"text\":\"World!\"}}\n\n";
                let _ = stream.write_all(event3.as_bytes()).await;

                let _ = stream.flush().await;
            });
        }
    });

    addr
}

#[tokio::test]
async fn test_sse_streaming_and_metrics() {
    let addr = spawn_sse_mock_server().await;
    let url = format!("http://{addr}/events");

    let config = ClientConfig {
        url,
        method: Method::GET,
        headers: Vec::new(),
        body: None,
        event_filter: None,
        extract_path: None,
        data_only: true,
        show_metrics: false,
        reconnect: false,
        max_reconnects: 0,
    };

    let client = StreamClient::new(config);
    let metrics = client.stream().await.unwrap();

    assert_eq!(metrics.event_count, 3);
    assert!(metrics.byte_count > 0);
    assert!(metrics.first_event_time.is_some());
    assert!(!metrics.inter_event_delays.is_empty());

    let summary = metrics.compute_summary();
    assert_eq!(summary.total_events, 3);
    assert!(summary.ttft_ms.unwrap() >= 0.0);
    assert!(summary.itl_mean_ms.unwrap() >= 30.0);
}

#[tokio::test]
async fn test_sse_event_filtering() {
    let addr = spawn_sse_mock_server().await;
    let url = format!("http://{addr}/events");

    let config = ClientConfig {
        url,
        method: Method::GET,
        headers: Vec::new(),
        body: None,
        event_filter: Some("token".to_string()),
        extract_path: None,
        data_only: true,
        show_metrics: false,
        reconnect: false,
        max_reconnects: 0,
    };

    let client = StreamClient::new(config);
    let metrics = client.stream().await.unwrap();

    // Only the 2 "token" events should be processed
    assert_eq!(metrics.event_count, 2);
}

#[tokio::test]
async fn test_sse_json_extraction() {
    let addr = spawn_sse_mock_server().await;
    let url = format!("http://{addr}/events");

    let config = ClientConfig {
        url,
        method: Method::GET,
        headers: Vec::new(),
        body: None,
        event_filter: Some("token".to_string()),
        extract_path: Some("delta.text".to_string()),
        data_only: false,
        show_metrics: false,
        reconnect: false,
        max_reconnects: 0,
    };

    let client = StreamClient::new(config);
    let metrics = client.stream().await.unwrap();

    assert_eq!(metrics.event_count, 2);
}
