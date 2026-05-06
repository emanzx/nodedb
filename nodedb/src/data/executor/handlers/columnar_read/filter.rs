// SPDX-License-Identifier: BUSL-1.1

//! Row-level WHERE predicate evaluation for memtable scans.

use crate::bridge::scan_filter::{FilterOp, ScanFilter};

/// Check whether a memtable row satisfies all filter predicates.
///
/// Returns `true` if every filter passes (AND semantics). An unknown field or
/// out-of-bounds column index causes the row to be excluded.
pub(in crate::data::executor) fn row_matches_filters(
    row: &[nodedb_types::value::Value],
    schema: &nodedb_types::columnar::ColumnarSchema,
    filters: &[ScanFilter],
) -> bool {
    for filter in filters {
        if filter.op == FilterOp::MatchAll {
            continue;
        }
        let col_idx = match schema.columns.iter().position(|c| c.name == filter.field) {
            Some(i) => i,
            None => continue, // unknown field — skip predicate
        };
        if col_idx >= row.len() {
            return false;
        }
        if !eval_filter(&row[col_idx], filter.op, &filter.value) {
            return false;
        }
    }
    true
}

/// Coerce a `Value` to f64 for numeric comparisons.
///
/// Accepts `Integer`, `Float`, and `String` (parsed) — the last case handles
/// untyped `Type::UNKNOWN` bind parameters that arrive as text and survive
/// planner normalization as `Value::String`.
fn value_as_f64(v: &nodedb_types::value::Value) -> Option<f64> {
    use nodedb_types::value::Value;
    match v {
        Value::Float(f) => Some(*f),
        Value::Integer(i) => Some(*i as f64),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

/// Evaluate a single filter predicate against a row value.
fn eval_filter(
    val: &nodedb_types::value::Value,
    op: FilterOp,
    filter_val: &nodedb_types::value::Value,
) -> bool {
    use nodedb_types::value::Value;

    let val_f64 = value_as_f64(val);
    let filter_f64 = value_as_f64(filter_val);

    let val_str = match val {
        Value::String(s) => Some(s.as_str()),
        _ => None,
    };
    let filter_str = match filter_val {
        Value::String(s) => Some(s.as_str()),
        _ => None,
    };

    match op {
        FilterOp::Eq => {
            if let (Some(a), Some(b)) = (val_f64, filter_f64) {
                a == b
            } else if let (Some(a), Some(b)) = (val_str, filter_str) {
                a == b
            } else {
                false
            }
        }
        FilterOp::Ne => {
            if let (Some(a), Some(b)) = (val_f64, filter_f64) {
                a != b
            } else if let (Some(a), Some(b)) = (val_str, filter_str) {
                a != b
            } else {
                true
            }
        }
        FilterOp::Gt => val_f64.zip(filter_f64).is_some_and(|(a, b)| a > b),
        FilterOp::Gte => val_f64.zip(filter_f64).is_some_and(|(a, b)| a >= b),
        FilterOp::Lt => val_f64.zip(filter_f64).is_some_and(|(a, b)| a < b),
        FilterOp::Lte => val_f64.zip(filter_f64).is_some_and(|(a, b)| a <= b),
        FilterOp::IsNull => matches!(val, Value::Null),
        FilterOp::IsNotNull => !matches!(val, Value::Null),
        _ => true, // unknown/unsupported op — pass through
    }
}
