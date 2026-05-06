// SPDX-License-Identifier: BUSL-1.1

//! Offset window functions: lag, lead, nth_value.

use crate::expr::SqlExpr;

use super::helpers::{get_field, set_window_col};
use super::spec::WindowFuncSpec;

fn col_arg(spec: &WindowFuncSpec, idx: usize) -> &str {
    spec.args
        .get(idx)
        .and_then(|e| match e {
            SqlExpr::Column(c) => Some(c.as_str()),
            _ => None,
        })
        .unwrap_or("*")
}

fn usize_arg(spec: &WindowFuncSpec, idx: usize, default: usize) -> usize {
    spec.args
        .get(idx)
        .and_then(|e| match e {
            SqlExpr::Literal(v) => v.as_f64().map(|n| n as usize),
            _ => None,
        })
        .unwrap_or(default)
}

fn default_arg(spec: &WindowFuncSpec, idx: usize) -> serde_json::Value {
    spec.args
        .get(idx)
        .and_then(|e| match e {
            SqlExpr::Literal(v) => Some(serde_json::Value::from(v.clone())),
            _ => None,
        })
        .unwrap_or(serde_json::Value::Null)
}

pub(super) fn apply_lag(
    rows: &mut [(String, serde_json::Value)],
    indices: &[usize],
    spec: &WindowFuncSpec,
) {
    let field = col_arg(spec, 0);
    let offset = usize_arg(spec, 1, 1);
    let default = default_arg(spec, 2);

    for (pos, &i) in indices.iter().enumerate() {
        let val = if pos >= offset {
            get_field(&rows[indices[pos - offset]].1, field)
        } else {
            default.clone()
        };
        set_window_col(&mut rows[i].1, &spec.alias, val);
    }
}

pub(super) fn apply_lead(
    rows: &mut [(String, serde_json::Value)],
    indices: &[usize],
    spec: &WindowFuncSpec,
) {
    let field = col_arg(spec, 0);
    let offset = usize_arg(spec, 1, 1);
    let default = default_arg(spec, 2);

    for (pos, &i) in indices.iter().enumerate() {
        let val = if pos + offset < indices.len() {
            get_field(&rows[indices[pos + offset]].1, field)
        } else {
            default.clone()
        };
        set_window_col(&mut rows[i].1, &spec.alias, val);
    }
}

/// PostgreSQL `nth_value(expr, n)` — value of `expr` at the n'th row of the
/// window frame, NULL if the frame doesn't yet contain n rows. Default frame
/// is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, so the first n-1
/// rows of each partition return NULL and rows from the n'th onward return
/// the value of `expr` at the n'th row.
pub(super) fn apply_nth_value(
    rows: &mut [(String, serde_json::Value)],
    indices: &[usize],
    spec: &WindowFuncSpec,
) {
    let field = col_arg(spec, 0);
    let n = usize_arg(spec, 1, 1).max(1);

    for (pos, &i) in indices.iter().enumerate() {
        let val = if pos + 1 >= n {
            get_field(&rows[indices[n - 1]].1, field)
        } else {
            serde_json::Value::Null
        };
        set_window_col(&mut rows[i].1, &spec.alias, val);
    }
}
