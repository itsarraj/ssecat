use serde_json::Value;

pub fn extract_json_field(json_str: &str, path: &str) -> Option<String> {
    let root: Value = serde_json::from_str(json_str).ok()?;
    let mut current = &root;

    for part in path.split('.') {
        if part.is_empty() {
            continue;
        }

        match current {
            Value::Object(map) => {
                current = map.get(part)?;
            }
            Value::Array(arr) => {
                let idx: usize = part.parse().ok()?;
                current = arr.get(idx)?;
            }
            _ => return None,
        }
    }

    match current {
        Value::String(s) => Some(s.clone()),
        Value::Null => None,
        other => Some(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_openai_delta() {
        let json = r#"{
            "id": "chatcmpl-123",
            "choices": [
                {
                    "index": 0,
                    "delta": {
                        "content": "Hello world!"
                    }
                }
            ]
        }"#;

        let content = extract_json_field(json, "choices.0.delta.content");
        assert_eq!(content, Some("Hello world!".to_string()));
    }

    #[test]
    fn test_extract_anthropic_delta() {
        let json = r#"{
            "type": "content_block_delta",
            "delta": {
                "type": "text_delta",
                "text": "Antigravity"
            }
        }"#;

        let text = extract_json_field(json, "delta.text");
        assert_eq!(text, Some("Antigravity".to_string()));
    }

    #[test]
    fn test_extract_non_existent_path() {
        let json = r#"{"name": "test"}"#;
        assert_eq!(extract_json_field(json, "invalid.path"), None);
    }
}
