use std::io::{self, Write};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use colored::*;
use futures_util::StreamExt;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, ACCEPT, CACHE_CONTROL};
use reqwest::{Client, Method};

use crate::extractor::extract_json_field;
use crate::metrics::StreamMetrics;
use crate::parser::{Event, EventParser};

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub url: String,
    pub method: Method,
    pub headers: Vec<(String, String)>,
    pub body: Option<String>,
    pub event_filter: Option<String>,
    pub extract_path: Option<String>,
    pub data_only: bool,
    pub show_metrics: bool,
    pub reconnect: bool,
    pub max_reconnects: usize,
}

pub struct StreamClient {
    config: ClientConfig,
    http_client: Client,
}

impl StreamClient {
    pub fn new(config: ClientConfig) -> Self {
        let http_client = Client::builder()
            .timeout(Duration::from_secs(300))
            .build()
            .unwrap_or_default();
        Self {
            config,
            http_client,
        }
    }

    pub async fn stream(&self) -> Result<StreamMetrics> {
        let mut last_event_id: Option<String> = None;
        let mut reconnect_count: usize = 0;
        let mut metrics = StreamMetrics::new();
        let mut stdout = io::stdout();

        loop {
            let mut headers = HeaderMap::new();
            headers.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
            headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));

            if let Some(ref id) = last_event_id {
                if let Ok(val) = HeaderValue::from_str(id) {
                    headers.insert(HeaderName::from_static("last-event-id"), val);
                }
            }

            for (k, v) in &self.config.headers {
                if let (Ok(hk), Ok(hv)) = (
                    HeaderName::from_bytes(k.as_bytes()),
                    HeaderValue::from_str(v),
                ) {
                    headers.insert(hk, hv);
                }
            }

            let mut req = self
                .http_client
                .request(self.config.method.clone(), &self.config.url)
                .headers(headers);

            if let Some(ref body) = self.config.body {
                req = req
                    .header("content-type", "application/json")
                    .body(body.clone());
            }

            let response = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    if self.config.reconnect && reconnect_count < self.config.max_reconnects {
                        reconnect_count += 1;
                        eprintln!(
                            "{}",
                            format!("[ssecat] Connection failed ({e}). Reconnecting ({reconnect_count}/{})...", self.config.max_reconnects).yellow()
                        );
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    } else {
                        return Err(e).context("Failed to send request");
                    }
                }
            };

            let status = response.status();
            if !status.is_success() {
                let err_body = response.text().await.unwrap_or_default();
                bail!("Server returned HTTP {} error: {}", status, err_body);
            }

            let mut stream = response.bytes_stream();
            let mut parser = EventParser::new();
            let mut retry_ms: u64 = 1000;

            let mut stream_interrupted = false;

            while let Some(chunk_result) = stream.next().await {
                match chunk_result {
                    Ok(chunk) => {
                        let text = String::from_utf8_lossy(&chunk);
                        let events = parser.push_chunk(&text);

                        for ev in events {
                            if let Some(r) = ev.retry {
                                retry_ms = r;
                            }
                            if let Some(ref id) = ev.id {
                                last_event_id = Some(id.clone());
                            }

                            // Filter by event type if specified
                            if let Some(ref target_event) = self.config.event_filter {
                                if ev.event.as_deref() != Some(target_event) {
                                    continue;
                                }
                            }

                            metrics.record_event(ev.data.len());
                            self.output_event(&ev, &mut stdout)?;
                        }
                    }
                    Err(e) => {
                        eprintln!("{}", format!("[ssecat] Stream error: {e}").red());
                        stream_interrupted = true;
                        break;
                    }
                }
            }

            // Process any remaining event in buffer
            for ev in parser.finish() {
                metrics.record_event(ev.data.len());
                self.output_event(&ev, &mut stdout)?;
            }

            if stream_interrupted && self.config.reconnect && reconnect_count < self.config.max_reconnects {
                reconnect_count += 1;
                eprintln!(
                    "{}",
                    format!("[ssecat] Stream dropped. Reconnecting in {}ms ({reconnect_count}/{})...", retry_ms, self.config.max_reconnects).yellow()
                );
                tokio::time::sleep(Duration::from_millis(retry_ms)).await;
                continue;
            }

            break;
        }

        if self.config.show_metrics {
            metrics.print_summary();
        }

        Ok(metrics)
    }

    fn output_event(&self, ev: &Event, stdout: &mut io::Stdout) -> Result<()> {
        if let Some(ref path) = self.config.extract_path {
            if let Some(extracted) = extract_json_field(&ev.data, path) {
                print!("{extracted}");
                let _ = stdout.flush();
            }
        } else if self.config.data_only {
            println!("{}", ev.data);
        } else {
            // Pretty mode
            let event_tag = match &ev.event {
                Some(name) => format!("event: {}", name).bold().cyan(),
                None => "event: message".bold().blue(),
            };

            let id_tag = match &ev.id {
                Some(id) => format!(" [id: {id}]").yellow(),
                None => "".normal(),
            };

            println!("{event_tag}{id_tag}");
            for line in ev.data.lines() {
                println!("  data: {}", line);
            }
            println!();
        }

        Ok(())
    }
}
