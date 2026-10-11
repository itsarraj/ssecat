# ssecat

Server-Sent Events (SSE, `text/event-stream`) streaming inspector, JSON harvester, and LLM token latency benchmark.

Point `ssecat` at any SSE endpoint (OpenAI, Anthropic, Ollama, local LLMs, notification gateways, CI/CD streams) to parse events per the WHATWG SSE specification, extract nested JSON delta tokens, auto-reconnect with `Last-Event-ID`, and measure Time-to-First-Token (TTFT) and inter-token arrival latency.

---

## Why

1. **`curl -N` produces raw, unparsed chunk noise**: Developers debugging LLM streaming APIs or event feeds typically resort to `curl -N`, which dumps raw `data: {"choices":[...]}\n\n` framing mixed with comment pings (`: ping`).
2. **Missing Token Extraction**: Streaming an LLM response cleanly to stdout in a terminal script requires piping `curl` through `jq` or writing Python wrappers that buffer chunks improperly.
3. **No Latency Breakdown**: Diagnosing slow streaming responses requires measuring Time-to-First-Token (TTFT) and inter-token arrival distributions (ITL), which neither `curl` nor generic HTTP benchmark tools provide.

`ssecat` fixes this with a single, standalone CLI binary.

---

## Installation

```sh
cargo install --path .
```

---

## Usage

### 1. Pretty Event Stream Inspection

Connect to any SSE endpoint and inspect formatted, color-coded event blocks:

```sh
ssecat https://example.com/events
```

Sample output:
```text
event: user_connected [id: 1042]
  data: {"user_id":"u_987","username":"alex"}

event: status_change [id: 1043]
  data: {"status":"online"}
```

---

### 2. Stream LLM Tokens Directly to Terminal (OpenAI / Ollama / Local Models)

Use `--extract` with a dot-path to pull token deltas directly as words/characters appear:

```sh
# OpenAI / vLLM / Ollama
ssecat https://api.openai.com/v1/chat/completions \
  -X POST \
  -H "Authorization: Bearer $OPENAI_API_KEY" \
  -d '{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"Write a haiku"}]}' \
  --extract choices.0.delta.content
```

```sh
# Anthropic Claude
ssecat https://api.anthropic.com/v1/messages \
  -X POST \
  -H "x-api-key: $ANTHROPIC_API_KEY" \
  -H "anthropic-version: 2023-06-01" \
  -d '{"model":"claude-3-5-sonnet-20241022","max_tokens":1024,"stream":true,"messages":[{"role":"user","content":"Hi"}]}' \
  --extract delta.text
```

---

### 3. Measuring TTFT and Token Latency Benchmark

Add `-m` / `--metrics` to measure Time-to-First-Token (TTFT), throughput, and inter-token arrival latency:

```sh
ssecat https://api.openai.com/v1/chat/completions \
  -X POST \
  -H "Authorization: Bearer $OPENAI_API_KEY" \
  -d '{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"Explain quantum physics in 2 sentences"}]}' \
  -e choices.0.delta.content -m
```

Sample metrics report:
```text
--- Streaming Metrics & Latency ---
  Time to First Event (TTFT):   184.20 ms
  Total Events / Chunks:              38
  Total Bytes Streamed:             4850 B
  Total Stream Duration:            1.42 s
  Throughput:                       26.8 events/s (3.34 KB/s)

--- Inter-Event Arrival Latency (ITL) ---
  Min:     12.10 ms
  p50:     32.40 ms
  Mean:    33.15 ms
  p90:     48.20 ms
  p99:     64.10 ms
  Max:     68.50 ms
-----------------------------------
```

---

### 4. Filter by Event Name

Only process events matching a specific event type (e.g. `message_delta`):

```sh
ssecat https://example.com/events --event message_delta --data-only
```

---

### 5. Resilient Auto-Reconnection (`Last-Event-ID`)

Automatically reconnect if network connection drops, resuming from the last received event ID:

```sh
ssecat https://example.com/live-feed --reconnect --max-reconnects 10
```

---

## Status: built and verified against real SSE endpoints

- **9 unit tests** (`cargo test --lib`):
  - Spec-compliant WHATWG SSE parsing: single-line, multi-line `data:` concatenation with newlines, comments (`:`), `id:`, and `retry:`.
  - Fragmented chunk handling across TCP chunk boundaries.
  - JSON dot-path field extraction across nested objects and arrays.
  - TTFT, duration, and inter-event percentile latency calculation.

- **3 live integration tests** (`cargo test --test integration_test`):
  - **Full SSE streaming**: Built a real chunked HTTP `text/event-stream` server and connected `ssecat` over HTTP. Verified event delivery, byte accounting, and TTFT calculation.
  - **Event filtering**: Verified that `--event token` accurately filters out unrelated event types.
  - **JSON token extraction**: Verified that `--extract delta.text` correctly extracts and streams token values.

---

## Deliberately Deferred

- **Bidirectional multiplexing**: Server-Sent Events is inherently unidirectional (server-to-client). For bidirectional duplex protocols, use [`wscli`](../wscli) or [`wsproxy`](../wsproxy).
