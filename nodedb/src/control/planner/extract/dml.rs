//! DML extraction: INSERT values, UPDATE assignments, point targets.

use datafusion::logical_expr::{LogicalPlan, Operator};
use datafusion::prelude::*;

use super::super::expr_convert::{expr_to_json_value, expr_to_string};

/// Extract SET field assignments from an UPDATE DML input plan.
///
/// DataFusion represents UPDATE SET as a projection with assignment expressions.
/// Returns `Vec<(field_name, json_value_bytes)>`.
pub(in crate::control::planner) fn extract_update_assignments(
    plan: &LogicalPlan,
) -> crate::Result<Vec<(String, Vec<u8>)>> {
    match plan {
        LogicalPlan::Projection(proj) => {
            let mut updates = Vec::new();
            let schema = proj.schema.fields();
            for (i, expr) in proj.expr.iter().enumerate() {
                let field_name = if i < schema.len() {
                    schema[i].name().clone()
                } else {
                    continue;
                };
                if field_name == "id" || field_name == "document_id" {
                    continue;
                }
                let value = expr_to_json_value(expr);
                let value_bytes =
                    serde_json::to_vec(&value).map_err(|e| crate::Error::PlanError {
                        detail: format!("failed to serialize update value: {e}"),
                    })?;
                updates.push((field_name, value_bytes));
            }
            Ok(updates)
        }
        _ => {
            // DataFusion may wrap UPDATE in various ways. Return empty if we can't parse.
            Ok(Vec::new())
        }
    }
}

/// Collect document IDs from equality predicates (`id = 'value'` OR `id = 'value2'`).
pub(in crate::control::planner) fn collect_eq_ids(expr: &Expr, ids: &mut Vec<String>) {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            let (col_name, value) = match (&*binary.left, &*binary.right) {
                (Expr::Column(col), Expr::Literal(lit, _)) => (col.name.as_str(), lit.to_string()),
                (Expr::Literal(lit, _), Expr::Column(col)) => (col.name.as_str(), lit.to_string()),
                _ => return,
            };
            if col_name == "id" || col_name == "document_id" {
                ids.push(value.trim_matches('\'').trim_matches('"').to_string());
            }
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
            collect_eq_ids(&binary.left, ids);
            collect_eq_ids(&binary.right, ids);
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            collect_eq_ids(&binary.left, ids);
            collect_eq_ids(&binary.right, ids);
        }
        _ => {}
    }
}

/// Extract (document_id, value_bytes) pairs from an INSERT input plan.
///
/// DataFusion represents `INSERT INTO t VALUES (...)` as a projection of
/// literal values. The first column is the document ID; remaining columns
/// are serialized as a JSON object.
pub(in crate::control::planner) fn extract_insert_values(
    plan: &LogicalPlan,
) -> crate::Result<Vec<(String, Vec<u8>)>> {
    match plan {
        LogicalPlan::Values(values) => {
            let schema = values.schema.fields();
            let mut results = Vec::with_capacity(values.values.len());
            for row in &values.values {
                let doc_id = if let Some(first) = row.first() {
                    expr_to_string(first)
                } else {
                    continue;
                };
                let mut obj = serde_json::Map::new();
                for (i, expr) in row.iter().enumerate() {
                    let field_name = if i < schema.len() {
                        schema[i].name().clone()
                    } else {
                        format!("column{i}")
                    };
                    let val = expr_to_json_value(expr);
                    obj.insert(field_name, val);
                }
                let value_bytes =
                    serde_json::to_vec(&obj).map_err(|e| crate::Error::PlanError {
                        detail: format!("failed to serialize insert values: {e}"),
                    })?;
                results.push((doc_id, value_bytes));
            }
            Ok(results)
        }
        LogicalPlan::Projection(proj) => extract_insert_values(&proj.input),
        _ => Err(crate::Error::PlanError {
            detail: format!("unsupported INSERT input plan type: {}", plan.display()),
        }),
    }
}

/// Extract document IDs from a DML plan's `WHERE id = '...'` predicates.
///
/// Used by both DELETE and UPDATE to detect point operations (single-doc
/// targets) vs bulk operations (complex WHERE predicates).
pub(in crate::control::planner) fn extract_point_targets(
    plan: &LogicalPlan,
    _collection: &str,
) -> crate::Result<Vec<String>> {
    match plan {
        LogicalPlan::Filter(filter) => {
            let mut ids = Vec::new();
            collect_eq_ids(&filter.predicate, &mut ids);
            Ok(ids)
        }
        LogicalPlan::TableScan(_) => Err(crate::Error::PlanError {
            detail: "DELETE without WHERE clause is not supported. Use DROP COLLECTION to remove all data.".into(),
        }),
        _ => Err(crate::Error::PlanError {
            detail: format!("unsupported DELETE input plan: {}", plan.display()),
        }),
    }
}
