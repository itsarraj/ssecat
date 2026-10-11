#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub event: Option<String>,
    pub data: String,
    pub id: Option<String>,
    pub retry: Option<u64>,
}

#[derive(Default)]
pub struct EventParser {
    buffer: String,
    current_event: Option<String>,
    current_data: String,
    current_id: Option<String>,
    current_retry: Option<u64>,
}

impl EventParser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_chunk(&mut self, chunk: &str) -> Vec<Event> {
        self.buffer.push_str(chunk);
        let mut events = Vec::new();

        while let Some(line_end) = self.find_line_end() {
            let line: String = self.buffer.drain(..line_end.0).collect();
            // Discard newline characters (\n, \r\n, \r)
            self.buffer.drain(..line_end.1);

            if line.is_empty() {
                // Blank line -> dispatch event
                if let Some(ev) = self.dispatch_event() {
                    events.push(ev);
                }
            } else if line.starts_with(':') {
                // Comment, ignored
            } else if let Some((field, value)) = self.parse_field(&line) {
                match field {
                    "event" => {
                        self.current_event = Some(value.to_string());
                    }
                    "data" => {
                        if !self.current_data.is_empty() {
                            self.current_data.push('\n');
                        }
                        self.current_data.push_str(value);
                    }
                    "id" => {
                        if !value.contains('\0') {
                            self.current_id = Some(value.to_string());
                        }
                    }
                    "retry" => {
                        if let Ok(ms) = value.parse::<u64>() {
                            self.current_retry = Some(ms);
                        }
                    }
                    _ => {}
                }
            }
        }

        events
    }

    pub fn finish(&mut self) -> Vec<Event> {
        let mut events = Vec::new();
        if !self.buffer.is_empty() {
            let line = std::mem::take(&mut self.buffer);
            if !line.starts_with(':') {
                if let Some((field, value)) = self.parse_field(&line) {
                    if field == "data" {
                        if !self.current_data.is_empty() {
                            self.current_data.push('\n');
                        }
                        self.current_data.push_str(value);
                    }
                }
            }
        }
        if let Some(ev) = self.dispatch_event() {
            events.push(ev);
        }
        events
    }

    fn find_line_end(&self) -> Option<(usize, usize)> {
        // Returns (line_len, delimiter_len)
        let bytes = self.buffer.as_bytes();
        for (i, &b) in bytes.iter().enumerate() {
            if b == b'\n' {
                return Some((i, 1));
            }
            if b == b'\r' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\n' {
                    return Some((i, 2));
                }
                return Some((i, 1));
            }
        }
        None
    }

    fn parse_field<'a>(&self, line: &'a str) -> Option<(&'a str, &'a str)> {
        if let Some(idx) = line.find(':') {
            let field = &line[..idx];
            let mut value = &line[idx + 1..];
            if value.starts_with(' ') {
                value = &value[1..];
            }
            Some((field, value))
        } else {
            Some((line, ""))
        }
    }

    fn dispatch_event(&mut self) -> Option<Event> {
        if self.current_data.is_empty() && self.current_event.is_none() {
            return None;
        }

        let event = self.current_event.take();
        let data = std::mem::take(&mut self.current_data);
        let id = self.current_id.clone();
        let retry = self.current_retry;

        Some(Event {
            event,
            data,
            id,
            retry,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_event() {
        let mut parser = EventParser::new();
        let events = parser.push_chunk("data: hello world\n\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "hello world");
        assert_eq!(events[0].event, None);
    }

    #[test]
    fn test_multiline_data() {
        let mut parser = EventParser::new();
        let events = parser.push_chunk("data: line 1\ndata: line 2\n\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "line 1\nline 2");
    }

    #[test]
    fn test_event_with_id_and_type() {
        let mut parser = EventParser::new();
        let raw = "id: 101\nevent: delta\ndata: {\"token\":\"foo\"}\n\n";
        let events = parser.push_chunk(raw);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id.as_deref(), Some("101"));
        assert_eq!(events[0].event.as_deref(), Some("delta"));
        assert_eq!(events[0].data, "{\"token\":\"foo\"}");
    }

    #[test]
    fn test_comments_and_whitespace() {
        let mut parser = EventParser::new();
        let raw = ": this is a ping\r\ndata: actual data\r\n\r\n";
        let events = parser.push_chunk(raw);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "actual data");
    }

    #[test]
    fn test_fragmented_chunks() {
        let mut parser = EventParser::new();
        assert_eq!(parser.push_chunk("da"), vec![]);
        assert_eq!(parser.push_chunk("ta: par"), vec![]);
        assert_eq!(parser.push_chunk("tial\n"), vec![]);
        let events = parser.push_chunk("\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "partial");
    }
}
