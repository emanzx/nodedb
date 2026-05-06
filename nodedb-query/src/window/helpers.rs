// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers for window-function evaluation.

use std::collections::HashMap;

/// Group row indices by partition key, preserving first-seen partition order.
pub(super) fn build_partitions(
    rows: &[(String, serde_json::Value)],
    partition_by: &[String],
) -> Vec<Vec<usize>> {
    if partition_by.is_empty() {
        return vec![(0..rows.len()).collect()];
    }

    let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
    let mut order = Vec::new();

    for (i, (_id, doc)) in rows.iter().enumerate() {
        // Partition key uses JSON serialization: the string literal "null" and a missing
        // field both produce "null" here, but a JSON string value "null" serializes as
        // "\"null\"" and is therefore distinct from a missing field. This is intentional.
        let key: String = partition_by
            .iter()
            .map(|col| {
                doc.get(col)
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "null".to_string())
            })
            .collect::<Vec<_>>()
            .join("\x00");
        let entry = groups.entry(key.clone()).or_default();
        if entry.is_empty() {
            order.push(key);
        }
        entry.push(i);
    }

    order.iter().filter_map(|k| groups.remove(k)).collect()
}

pub(super) fn set_window_col(row: &mut serde_json::Value, alias: &str, val: serde_json::Value) {
    if let serde_json::Value::Object(map) = row {
        map.insert(alias.to_string(), val);
    }
}

pub(super) fn get_field(doc: &serde_json::Value, field: &str) -> serde_json::Value {
    doc.get(field).cloned().unwrap_or(serde_json::Value::Null)
}

pub(super) fn as_f64(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// Returns true when row at index `b` has the same ORDER BY key as row at
/// index `a` (used by peer-aware ranking like RANK and PERCENT_RANK).
pub(super) fn order_keys_equal(
    rows: &[(String, serde_json::Value)],
    a: usize,
    b: usize,
    order_by: &[(String, bool)],
) -> bool {
    order_by
        .iter()
        .all(|(col, _)| get_field(&rows[a].1, col) == get_field(&rows[b].1, col))
}
