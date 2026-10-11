use std::fs;
use std::path::PathBuf;

use clap::Parser;
use reqwest::Method;
use ssecat::{ClientConfig, StreamClient};

#[derive(Parser, Debug)]
#[command(
    name = "ssecat",
    about = "Server-Sent Events (SSE) streaming inspector, JSON harvester, and LLM token latency benchmark",
    version
)]
struct Args {
    /// URL of the SSE streaming endpoint (e.g. https://api.openai.com/v1/chat/completions)
    url: String,

    /// HTTP request method
    #[arg(short = 'X', long, default_value = "GET")]
    method: String,

    /// Custom HTTP header (can be used multiple times, e.g. -H "Authorization: Bearer ...")
    #[arg(short = 'H', long)]
    header: Vec<String>,

    /// Request body payload (e.g. JSON payload for POST endpoints)
    #[arg(short = 'd', long)]
    data: Option<String>,

    /// Read request body from a file
    #[arg(long)]
    data_file: Option<PathBuf>,

    /// Filter events by event type name (e.g. --event message_delta)
    #[arg(long)]
    event: Option<String>,

    /// Extract a specific JSON dot-path from data payloads (e.g. choices.0.delta.content or delta.text)
    #[arg(short = 'e', long)]
    extract: Option<String>,

    /// Output only the raw data field without event or id framing
    #[arg(long)]
    data_only: bool,

    /// Show streaming performance metrics (TTFT, inter-token arrival latency, throughput)
    #[arg(short = 'm', long)]
    metrics: bool,

    /// Auto-reconnect on stream drop with Last-Event-ID tracking
    #[arg(long)]
    reconnect: bool,

    /// Maximum reconnect attempts
    #[arg(long, default_value_t = 5)]
    max_reconnects: usize,

    /// Output final metrics as JSON
    #[arg(long)]
    json: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let method = match args.method.to_uppercase().as_str() {
        "POST" => Method::POST,
        "PUT" => Method::PUT,
        "DELETE" => Method::DELETE,
        "HEAD" => Method::HEAD,
        "OPTIONS" => Method::OPTIONS,
        _ => Method::GET,
    };

    let mut headers = Vec::new();
    for h in args.header {
        if let Some((k, v)) = h.split_once(':') {
            headers.push((k.trim().to_string(), v.trim().to_string()));
        }
    }

    let body = if let Some(ref p) = args.data_file {
        Some(fs::read_to_string(p)?)
    } else {
        args.data
    };

    let show_metrics = args.metrics || args.json;

    let config = ClientConfig {
        url: args.url,
        method,
        headers,
        body,
        event_filter: args.event,
        extract_path: args.extract,
        data_only: args.data_only,
        show_metrics: show_metrics && !args.json,
        reconnect: args.reconnect,
        max_reconnects: args.max_reconnects,
    };

    let client = StreamClient::new(config);
    let metrics = client.stream().await?;

    if args.json {
        let summary = metrics.compute_summary();
        println!("{}", serde_json::to_string_pretty(&summary)?);
    }

    Ok(())
}
