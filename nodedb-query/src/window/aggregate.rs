//! Aggregate functions used as windows: sum, count, avg, min, max,
//! first_value, last_value.

use crate::expr::SqlExpr;

use super::helpers::{as_f64, get_field, set_window_col};
use super::spec::{FrameBound, WindowFuncSpec};

pub(super) fn apply_aggregate_window(
    rows: &mut [(String, serde_json::Value)],
    indices: &[usize],
    spec: &WindowFuncSpec,
) {
    let field = spec
        .args
        .first()
        .and_then(|e| match e {
            SqlExpr::Column(c) => Some(c.as_str()),
            _ => None,
        })
        .unwrap_or("*");

    let use_running = spec.frame.mode == "range"
        && matches!(spec.frame.start, FrameBound::UnboundedPreceding)
        && matches!(spec.frame.end, FrameBound::CurrentRow);

    if use_running {
        running_aggregate(rows, indices, spec, field);
    } else {
        full_partition_aggregate(rows, indices, spec, field);
    }
}

fn running_aggregate(
    rows: &mut [(String, serde_json::Value)],
    indices: &[usize],
    spec: &WindowFuncSpec,
    field: &str,
) {
    let mut running_sum = 0.0f64;
    let mut running_count = 0u64;
    let mut running_min: Option<f64> = None;
    let mut running_max: Option<f64> = None;

    for (pos, &i) in indices.iter().enumerate() {
        let val = get_field(&rows[i].1, field);
        if let Some(n) = as_f64(&val) {
            running_sum += n;
            running_count += 1;
            running_min = Some(running_min.map_or(n, |m: f64| m.min(n)));
            running_max = Some(running_max.map_or(n, |m: f64| m.max(n)));
        } else if spec.func_name == "count" {
            running_count += 1;
        }

        let result = match spec.func_name.as_str() {
            "sum" => serde_json::json!(running_sum),
            "count" => serde_json::json!(running_count),
            "avg" => {
                if running_count > 0 {
                    serde_json::json!(running_sum / running_count as f64)
                } else {
                    serde_json::Value::Null
                }
            }
            "min" => running_min
                .map(|m| serde_json::json!(m))
                .unwrap_or(serde_json::Value::Null),
            "max" => running_max
                .map(|m| serde_json::json!(m))
                .unwrap_or(serde_json::Value::Null),
            "first_value" => get_field(&rows[indices[0]].1, field),
            "last_value" => get_field(&rows[indices[pos]].1, field),
            _ => serde_json::Value::Null,
        };
        set_window_col(&mut rows[i].1, &spec.alias, result);
    }
}

fn full_partition_aggregate(
    rows: &mut [(String, serde_json::Value)],
    indices: &[usize],
    spec: &WindowFuncSpec,
    field: &str,
) {
    let values: Vec<f64> = indices
        .iter()
        .filter_map(|&i| as_f64(&get_field(&rows[i].1, field)))
        .collect();

    let rt = crate::simd_agg::ts_runtime();
    let result = match spec.func_name.as_str() {
        "sum" => serde_json::json!((rt.sum_f64)(&values)),
        "count" => serde_json::json!(indices.len()),
        "avg" => {
            if values.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::json!((rt.sum_f64)(&values) / values.len() as f64)
            }
        }
        "min" => {
            if values.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::json!((rt.min_f64)(&values))
            }
        }
        "max" => {
            if values.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::json!((rt.max_f64)(&values))
            }
        }
        "first_value" => indices
            .first()
            .map(|&i| get_field(&rows[i].1, field))
            .unwrap_or(serde_json::Value::Null),
        "last_value" => indices
            .last()
            .map(|&i| get_field(&rows[i].1, field))
            .unwrap_or(serde_json::Value::Null),
        _ => serde_json::Value::Null,
    };

    for &i in indices {
        set_window_col(&mut rows[i].1, &spec.alias, result.clone());
    }
}
