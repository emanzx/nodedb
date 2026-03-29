//! Shared helper functions for the evaluator.

use super::super::ast::Grouping;
use super::super::types::*;

pub fn negate_value(val: Value, ts: i64) -> Value {
    match val {
        Value::Scalar(v, _) => Value::Scalar(-v, ts),
        Value::Vector(samples) => Value::Vector(
            samples
                .into_iter()
                .map(|s| InstantSample {
                    value: -s.value,
                    ..s
                })
                .collect(),
        ),
        other => other,
    }
}

pub fn labels_key(labels: &Labels) -> String {
    let mut parts: Vec<String> = labels.iter().map(|(k, v)| format!("{k}={v}")).collect();
    parts.sort();
    parts.join(",")
}

/// Match key for binary operations (excludes `__name__`).
pub fn match_key(labels: &Labels) -> String {
    let mut parts: Vec<String> = labels
        .iter()
        .filter(|(k, _)| k.as_str() != "__name__")
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    parts.sort();
    parts.join(",")
}

pub fn group_key(labels: &Labels, grouping: &Grouping) -> String {
    match grouping {
        Grouping::None => String::new(),
        Grouping::By(cols) => {
            let mut parts: Vec<String> = cols
                .iter()
                .filter_map(|c| labels.get(c).map(|v| format!("{c}={v}")))
                .collect();
            parts.sort();
            parts.join(",")
        }
        Grouping::Without(cols) => {
            let exclude: std::collections::HashSet<&str> =
                cols.iter().map(|s| s.as_str()).collect();
            let mut parts: Vec<String> = labels
                .iter()
                .filter(|(k, _)| !exclude.contains(k.as_str()) && k.as_str() != "__name__")
                .map(|(k, v)| format!("{k}={v}"))
                .collect();
            parts.sort();
            parts.join(",")
        }
    }
}

pub fn group_labels(labels: &Labels, grouping: &Grouping) -> Labels {
    match grouping {
        Grouping::None => Labels::new(),
        Grouping::By(cols) => {
            let mut result = Labels::new();
            for c in cols {
                if let Some(v) = labels.get(c) {
                    result.insert(c.clone(), v.clone());
                }
            }
            result
        }
        Grouping::Without(cols) => {
            let exclude: std::collections::HashSet<&str> =
                cols.iter().map(|s| s.as_str()).collect();
            labels
                .iter()
                .filter(|(k, _)| !exclude.contains(k.as_str()) && k.as_str() != "__name__")
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        }
    }
}
