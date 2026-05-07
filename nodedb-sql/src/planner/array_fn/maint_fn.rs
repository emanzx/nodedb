// SPDX-License-Identifier: Apache-2.0

//! Maintenance ARRAY_* functions: bare `SELECT ARRAY_FLUSH(name)` /
//! `SELECT ARRAY_COMPACT(name)` with no FROM clause.

use sqlparser::ast;

use super::helpers::{collect_args, require_array_name};
use crate::error::Result;
use crate::types::{SqlCatalog, SqlPlan};

/// Try to intercept a no-FROM `SELECT array_flush(name)` /
/// `SELECT array_compact(name)`. The single projection item must be
/// a bare function call carrying one string-literal argument.
pub fn try_plan_array_maint_fn(
    items: &[ast::SelectItem],
    catalog: &dyn SqlCatalog,
) -> Result<Option<SqlPlan>> {
    if items.len() != 1 {
        return Ok(None);
    }
    let func = match &items[0] {
        ast::SelectItem::UnnamedExpr(ast::Expr::Function(f))
        | ast::SelectItem::ExprWithAlias {
            expr: ast::Expr::Function(f),
            ..
        } => f,
        _ => return Ok(None),
    };
    let fn_name = crate::parser::normalize::normalize_object_name_checked(&func.name)?;
    let arg_exprs = match &func.args {
        ast::FunctionArguments::List(list) => collect_args(&list.args),
        _ => Vec::new(),
    };
    match fn_name.as_str() {
        "array_flush" => {
            let name = require_array_name(&arg_exprs, 0, "ARRAY_FLUSH", catalog)?;
            Ok(Some(SqlPlan::ArrayFlush { name }))
        }
        "array_compact" => {
            let name = require_array_name(&arg_exprs, 0, "ARRAY_COMPACT", catalog)?;
            Ok(Some(SqlPlan::ArrayCompact { name }))
        }
        _ => Ok(None),
    }
}
