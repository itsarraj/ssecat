use std::time::{Duration, Instant};
use colored::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct StreamMetrics {
    pub start_time: Instant,
    pub first_event_time: Option<Instant>,
    pub last_event_time: Option<Instant>,
    pub event_count: usize,
    pub byte_count: usize,
    pub inter_event_delays: Vec<Duration>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSummary {
    pub total_events: usize,
    pub total_bytes: usize,
    pub total_duration_secs: f64,
    pub ttft_ms: Option<f64>,
    pub events_per_sec: f64,
    pub bytes_per_sec: f64,
    pub itl_min_ms: Option<f64>,
    pub itl_mean_ms: Option<f64>,
    pub itl_p50_ms: Option<f64>,
    pub itl_p90_ms: Option<f64>,
    pub itl_p99_ms: Option<f64>,
    pub itl_max_ms: Option<f64>,
}

impl StreamMetrics {
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            first_event_time: None,
            last_event_time: None,
            event_count: 0,
            byte_count: 0,
            inter_event_delays: Vec::new(),
        }
    }

    pub fn record_event(&mut self, payload_bytes: usize) {
        let now = Instant::now();
        self.event_count += 1;
        self.byte_count += payload_bytes;

        if self.first_event_time.is_none() {
            self.first_event_time = Some(now);
        } else if let Some(last) = self.last_event_time {
            self.inter_event_delays.push(now.saturating_duration_since(last));
        }

        self.last_event_time = Some(now);
    }

    pub fn compute_summary(&self) -> MetricsSummary {
        let total_duration = self.start_time.elapsed().as_secs_f64();
        let ttft_ms = self
            .first_event_time
            .map(|t| t.saturating_duration_since(self.start_time).as_secs_f64() * 1000.0);

        let events_per_sec = if total_duration > 0.0 {
            self.event_count as f64 / total_duration
        } else {
            0.0
        };

        let bytes_per_sec = if total_duration > 0.0 {
            self.byte_count as f64 / total_duration
        } else {
            0.0
        };

        let (itl_min_ms, itl_mean_ms, itl_p50_ms, itl_p90_ms, itl_p99_ms, itl_max_ms) =
            if self.inter_event_delays.is_empty() {
                (None, None, None, None, None, None)
            } else {
                let mut ms: Vec<f64> = self
                    .inter_event_delays
                    .iter()
                    .map(|d| d.as_secs_f64() * 1000.0)
                    .collect();
                ms.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let n = ms.len();
                let min = ms[0];
                let max = ms[n - 1];
                let sum: f64 = ms.iter().sum();
                let mean = sum / (n as f64);

                let p50 = ms[(n as f64 * 0.50).min((n - 1) as f64) as usize];
                let p90 = ms[(n as f64 * 0.90).min((n - 1) as f64) as usize];
                let p99 = ms[(n as f64 * 0.99).min((n - 1) as f64) as usize];

                (Some(min), Some(mean), Some(p50), Some(p90), Some(p99), Some(max))
            };

        MetricsSummary {
            total_events: self.event_count,
            total_bytes: self.byte_count,
            total_duration_secs: total_duration,
            ttft_ms,
            events_per_sec,
            bytes_per_sec,
            itl_min_ms,
            itl_mean_ms,
            itl_p50_ms,
            itl_p90_ms,
            itl_p99_ms,
            itl_max_ms,
        }
    }

    pub fn print_summary(&self) {
        let summary = self.compute_summary();
        eprintln!("\n{}", "--- Streaming Metrics & Latency ---".bold().cyan());
        if let Some(ttft) = summary.ttft_ms {
            eprintln!("  Time to First Event (TTFT): {:>8.2} ms", ttft);
        }
        eprintln!("  Total Events / Chunks:      {:>8}", summary.total_events);
        eprintln!("  Total Bytes Streamed:       {:>8} B", summary.total_bytes);
        eprintln!("  Total Stream Duration:      {:>8.2} s", summary.total_duration_secs);
        eprintln!("  Throughput:                 {:>8.1} events/s ({:.2} KB/s)",
            summary.events_per_sec, summary.bytes_per_sec / 1024.0);

        if let (Some(min), Some(mean), Some(p50), Some(p90), Some(p99), Some(max)) = (
            summary.itl_min_ms,
            summary.itl_mean_ms,
            summary.itl_p50_ms,
            summary.itl_p90_ms,
            summary.itl_p99_ms,
            summary.itl_max_ms,
        ) {
            eprintln!("\n{}", "--- Inter-Event Arrival Latency (ITL) ---".bold().yellow());
            eprintln!("  Min:  {:>8.2} ms", min);
            eprintln!("  p50:  {:>8.2} ms", p50);
            eprintln!("  Mean: {:>8.2} ms", mean);
            eprintln!("  p90:  {:>8.2} ms", p90);
            eprintln!("  p99:  {:>8.2} ms", p99);
            eprintln!("  Max:  {:>8.2} ms", max);
        }
        eprintln!("{}\n", "-----------------------------------".bold().cyan());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_recording() {
        let mut m = StreamMetrics::new();
        m.record_event(50);
        assert_eq!(m.event_count, 1);
        assert_eq!(m.byte_count, 50);
        assert!(m.first_event_time.is_some());

        let summary = m.compute_summary();
        assert_eq!(summary.total_events, 1);
        assert_eq!(summary.total_bytes, 50);
        assert!(summary.ttft_ms.is_some());
    }
}
