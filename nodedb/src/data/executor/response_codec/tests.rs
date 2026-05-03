use super::*;

#[test]
fn encode_vector_hits() {
    let hits = vec![
        VectorSearchHit {
            id: 1,
            distance: 0.5,
            doc_id: None,
            body: None,
        },
        VectorSearchHit {
            id: 2,
            distance: 0.8,
            doc_id: None,
            body: None,
        },
    ];
    let bytes = encode(&hits).unwrap();
    assert!(!bytes.is_empty());

    let json = decode_payload_to_json(&bytes);
    assert!(json.contains("\"id\""));
    assert!(json.contains("\"distance\""));
}

#[test]
fn encode_count_msg() {
    let bytes = encode_count("inserted", 42).unwrap();
    let json = decode_payload_to_json(&bytes);
    assert!(json.contains("\"inserted\""));
    assert!(json.contains("42"));
}

#[test]
fn json_passthrough() {
    let json_str = r#"[{"id":1}]"#;
    let result = decode_payload_to_json(json_str.as_bytes());
    assert_eq!(result, json_str);
}

#[test]
fn msgpack_to_json_roundtrip() {
    let value = serde_json::json!({"key": "value", "num": 42});
    let msgpack = nodedb_types::json_to_msgpack(&value).unwrap();
    let json = decode_payload_to_json(&msgpack);
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed["key"], "value");
    assert_eq!(parsed["num"], 42);
}

#[test]
fn raw_document_rows_roundtrip() {
    let doc1 = serde_json::json!({"name": "alice", "age": 30});
    let doc2 = serde_json::json!({"name": "bob", "age": 25});
    let msgpack1 = nodedb_types::json_to_msgpack(&doc1).unwrap();
    let msgpack2 = nodedb_types::json_to_msgpack(&doc2).unwrap();

    let rows = vec![
        ("doc1".to_string(), msgpack1),
        ("doc2".to_string(), msgpack2),
    ];

    let encoded = encode_raw_document_rows(&rows).unwrap();
    let json = decode_payload_to_json(&encoded);
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed.len(), 2);
    assert_eq!(parsed[0]["id"], "doc1");
    assert_eq!(parsed[0]["data"]["name"], "alice");
    assert_eq!(parsed[0]["data"]["age"], 30);
    assert_eq!(parsed[1]["id"], "doc2");
    assert_eq!(parsed[1]["data"]["name"], "bob");
}

#[test]
fn raw_document_rows_empty() {
    let rows: Vec<(String, Vec<u8>)> = vec![];
    let encoded = encode_raw_document_rows(&rows).unwrap();
    let json = decode_payload_to_json(&encoded);
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
    assert!(parsed.is_empty());
}

#[test]
fn decode_raw_scan_to_docs_accepts_plain_rows() {
    let rows = vec![serde_json::json!({"avg_amount": 43.598})];
    let encoded = encode_json_vec(&rows).unwrap();

    let decoded = decode_raw_scan_to_docs(&encoded);

    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].0, "");
    let decoded_json = decode_payload_to_json(&decoded[0].1);
    let parsed: serde_json::Value = serde_json::from_str(&decoded_json).unwrap();
    assert_eq!(parsed["avg_amount"], 43.598);
}

#[test]
fn decode_raw_scan_to_docs_handles_mixed_arrays() {
    let wrapped_doc = serde_json::json!({"name": "alice"});
    let wrapped_rows = vec![(
        "doc1".to_string(),
        nodedb_types::json_to_msgpack(&wrapped_doc).unwrap(),
    )];
    let wrapped = encode_raw_document_rows(&wrapped_rows).unwrap();

    let plain_rows = vec![serde_json::json!({"avg_amount": 43.598})];
    let plain = encode_json_vec(&plain_rows).unwrap();

    let mut combined = wrapped;
    combined.extend_from_slice(&plain);

    let decoded = decode_raw_scan_to_docs(&combined);

    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0].0, "doc1");
    assert_eq!(decode_payload_to_json(&decoded[0].1), r#"{"name":"alice"}"#);
    assert_eq!(decoded[1].0, "");
    let parsed: serde_json::Value =
        serde_json::from_str(&decode_payload_to_json(&decoded[1].1)).unwrap();
    assert_eq!(parsed["avg_amount"], 43.598);
}
