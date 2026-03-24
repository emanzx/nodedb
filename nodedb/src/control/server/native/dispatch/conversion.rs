//! Shared conversion helpers for native protocol dispatch.

use nodedb_types::Value;
use nodedb_types::protocol::NativeResponse;

/// Convert a crate-level error into a NativeResponse.
pub(crate) fn error_to_native(seq: u64, e: &crate::Error) -> NativeResponse {
    let (code, message) = match e {
        crate::Error::BadRequest { detail } => ("42601", detail.clone()),
        crate::Error::RejectedAuthz { resource, .. } => ("42501", resource.clone()),
        crate::Error::DeadlineExceeded { .. } => ("57014", "query cancelled due to timeout".into()),
        crate::Error::CollectionNotFound { collection, .. } => {
            ("42P01", format!("collection '{collection}' not found"))
        }
        other => ("XX000", format!("{other}")),
    };
    NativeResponse::error(seq, code, message)
}

/// Parse a JSON string (from the Data Plane) into proper columns and rows.
///
/// The Data Plane returns JSON in several formats:
/// - Array of objects: `[{"id":"1","name":"Alice"}, ...]` → extract keys as columns
/// - Single object: `{"id":"1","name":"Alice"}` → one row
/// - Scalar/string: just wrap as a single "result" column
pub(crate) fn parse_json_to_columns_rows(json_text: &str) -> (Vec<String>, Vec<Vec<Value>>) {
    if let Ok(val) = serde_json::from_str::<serde_json::Value>(json_text) {
        match val {
            serde_json::Value::Array(arr) if !arr.is_empty() => {
                // Array of objects: extract columns from first object's keys.
                if let Some(first) = arr.first().and_then(|v| v.as_object()) {
                    let columns: Vec<String> = first.keys().cloned().collect();
                    let mut rows = Vec::with_capacity(arr.len());
                    for item in &arr {
                        if let Some(obj) = item.as_object() {
                            let row: Vec<Value> = columns
                                .iter()
                                .map(|col| {
                                    obj.get(col)
                                        .map(json_value_to_nodedb)
                                        .unwrap_or(Value::Null)
                                })
                                .collect();
                            rows.push(row);
                        }
                    }
                    return (columns, rows);
                }
                // Array of non-objects: single "value" column.
                let rows: Vec<Vec<Value>> =
                    arr.iter().map(|v| vec![json_value_to_nodedb(v)]).collect();
                return (vec!["value".into()], rows);
            }
            serde_json::Value::Object(obj) => {
                // Single object: keys are columns, values are one row.
                let columns: Vec<String> = obj.keys().cloned().collect();
                let row: Vec<Value> = columns
                    .iter()
                    .map(|col| {
                        obj.get(col)
                            .map(json_value_to_nodedb)
                            .unwrap_or(Value::Null)
                    })
                    .collect();
                return (columns, vec![row]);
            }
            _ => {}
        }
    }

    // Fallback: raw text in a single column.
    (
        vec!["result".into()],
        vec![vec![Value::String(json_text.to_string())]],
    )
}

fn json_value_to_nodedb(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else {
                Value::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(arr) => {
            let inner: Vec<Value> = arr.iter().map(json_value_to_nodedb).collect();
            Value::Array(inner)
        }
        serde_json::Value::Object(_) => {
            // Nested objects: serialize back to JSON string for display.
            Value::String(serde_json::to_string(v).unwrap_or_default())
        }
    }
}
