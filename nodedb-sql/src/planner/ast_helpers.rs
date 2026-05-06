// SPDX-License-Identifier: BUSL-1.1

//! Shared AST manipulation helpers for DML planners.

use sqlparser::ast;

use crate::error::Result;
use crate::parser::normalize::normalize_ident;
use crate::planner::select::convert_where_to_filters;
use crate::types::Filter;

/// Return `(table, column)` for a `table.col` compound identifier, or `None`.
pub fn qualified_ident_pair(expr: &ast::Expr) -> Option<(String, String)> {
    match expr {
        ast::Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            Some((normalize_ident(&parts[0]), normalize_ident(&parts[1])))
        }
        _ => None,
    }
}

/// Flatten a right-leaning AND expression tree into a list of conjuncts.
pub fn flatten_and_expr(expr: &ast::Expr, out: &mut Vec<ast::Expr>) {
    match expr {
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::And,
            right,
        } => {
            flatten_and_expr(left, out);
            flatten_and_expr(right, out);
        }
        other => out.push(other.clone()),
    }
}

/// Reassemble conjuncts into a right-leaning AND tree. Panics if empty.
pub fn rebuild_and_expr(mut conjuncts: Vec<ast::Expr>) -> ast::Expr {
    let last = conjuncts.pop().expect("non-empty conjuncts");
    conjuncts
        .into_iter()
        .rfold(last, |acc, next| ast::Expr::BinaryOp {
            left: Box::new(next),
            op: ast::BinaryOperator::And,
            right: Box::new(acc),
        })
}

/// Walk an expression and replace every `table.col` compound identifier where
/// `table == qualifier` with a bare `col` identifier. Lets target-side
/// predicates like `t.score > 15` be evaluated against documents that store
/// fields without a table qualifier.
pub fn strip_table_qualifier(expr: &ast::Expr, qualifier: &str) -> ast::Expr {
    match expr {
        ast::Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            if normalize_ident(&parts[0]) == qualifier {
                ast::Expr::Identifier(parts[1].clone())
            } else {
                expr.clone()
            }
        }
        ast::Expr::BinaryOp { left, op, right } => ast::Expr::BinaryOp {
            left: Box::new(strip_table_qualifier(left, qualifier)),
            op: op.clone(),
            right: Box::new(strip_table_qualifier(right, qualifier)),
        },
        ast::Expr::UnaryOp { op, expr: inner } => ast::Expr::UnaryOp {
            op: *op,
            expr: Box::new(strip_table_qualifier(inner, qualifier)),
        },
        ast::Expr::Nested(inner) => {
            ast::Expr::Nested(Box::new(strip_table_qualifier(inner, qualifier)))
        }
        ast::Expr::IsNull(inner) => {
            ast::Expr::IsNull(Box::new(strip_table_qualifier(inner, qualifier)))
        }
        ast::Expr::IsNotNull(inner) => {
            ast::Expr::IsNotNull(Box::new(strip_table_qualifier(inner, qualifier)))
        }
        other => other.clone(),
    }
}

/// Strip `qualifier.` from all compound identifiers in `expr`, then convert
/// the result to `Vec<Filter>` via `convert_where_to_filters`.
pub fn strip_and_convert_filters(
    conjuncts: Vec<ast::Expr>,
    qualifier: &str,
) -> Result<Vec<Filter>> {
    if conjuncts.is_empty() {
        return Ok(Vec::new());
    }
    let stripped: Vec<ast::Expr> = conjuncts
        .into_iter()
        .map(|c| strip_table_qualifier(&c, qualifier))
        .collect();
    let rebuilt = rebuild_and_expr(stripped);
    convert_where_to_filters(&rebuilt)
}
