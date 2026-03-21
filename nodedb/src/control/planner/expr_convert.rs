//! DataFusion `Expr` to Rust value conversion utilities.
//!
//! Standalone functions that convert DataFusion expression types into plain
//! Rust/JSON values. Used by both the filter extractor and insert/update
//! plan handlers.

use datafusion::prelude::*;

/// Extract a usize from an Expr (for OFFSET values).
pub(super) fn expr_to_usize(expr: &Expr) -> crate::Result<usize> {
    match expr {
        Expr::Literal(lit, _) => {
            let s = lit.to_string();
            s.parse::<usize>().map_err(|_| crate::Error::PlanError {
                detail: format!("expected integer for OFFSET, got: {s}"),
            })
        }
        _ => Err(crate::Error::PlanError {
            detail: format!("expected literal for OFFSET, got: {expr}"),
        }),
    }
}

/// Convert an expression to a string value (for document IDs).
pub(super) fn expr_to_string(expr: &Expr) -> String {
    match expr {
        Expr::Literal(lit, _) => {
            let s = lit.to_string();
            s.trim_matches('\'').trim_matches('"').to_string()
        }
        _ => format!("{expr}"),
    }
}

/// Convert an expression to a JSON value (for document fields).
pub(super) fn expr_to_json_value(expr: &Expr) -> serde_json::Value {
    match expr {
        Expr::Literal(lit, _) => {
            let s = lit.to_string();
            // Try parsing as number first.
            if let Ok(n) = s.parse::<i64>() {
                return serde_json::Value::Number(n.into());
            }
            if let Ok(n) = s.parse::<f64>()
                && let Some(num) = serde_json::Number::from_f64(n)
            {
                return serde_json::Value::Number(num);
            }
            if s == "true" {
                return serde_json::Value::Bool(true);
            }
            if s == "false" {
                return serde_json::Value::Bool(false);
            }
            if s == "NULL" || s == "null" {
                return serde_json::Value::Null;
            }
            // String value — strip quotes.
            serde_json::Value::String(s.trim_matches('\'').trim_matches('"').to_string())
        }
        _ => serde_json::Value::String(format!("{expr}")),
    }
}
