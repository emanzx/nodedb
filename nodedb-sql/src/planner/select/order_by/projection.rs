//! SELECT-projection fallback for hybrid-search trigger detection.
//!
//! When `apply_order_by` left the plan as a `Scan` (no ORDER BY, or an
//! ORDER BY that did not match any search trigger), the `rrf_score(...)`
//! call may still appear directly in the SELECT projection. The canonical
//! shape `SELECT id, rrf_score(...) AS score FROM c WHERE ... LIMIT N`
//! requires this entry path because there is no ORDER BY clause to inspect.
//! Without it the score column resolves to NULL via scalar evaluation that
//! has no implementation.

use sqlparser::ast;

use super::super::helpers::extract_func_args;
use super::aliases::function_call_name;
use super::hybrid::{no_args_rrf_score_error, plan_hybrid_from_sort};
use crate::error::Result;
use crate::functions::registry::{FunctionRegistry, SearchTrigger};
use crate::parser::normalize::normalize_ident;
use crate::types::SqlPlan;

/// Try to fire a hybrid-search trigger from the SELECT projection alone.
pub(in crate::planner::select) fn try_hybrid_from_projection(
    plan: &SqlPlan,
    select_items: &[ast::SelectItem],
    functions: &FunctionRegistry,
) -> Result<Option<SqlPlan>> {
    let collection = match plan {
        SqlPlan::Scan { collection, .. } => collection.clone(),
        _ => return Ok(None),
    };
    for item in select_items {
        let (expr, alias) = match item {
            ast::SelectItem::ExprWithAlias { expr, alias } => (expr, Some(normalize_ident(alias))),
            ast::SelectItem::UnnamedExpr(expr) => (expr, None),
            _ => continue,
        };
        let ast::Expr::Function(func) = expr else {
            continue;
        };
        let name = function_call_name(expr).unwrap_or_default();
        if !matches!(functions.search_trigger(&name), SearchTrigger::HybridSearch) {
            continue;
        }
        let args = extract_func_args(func)?;
        if args.is_empty() {
            return Err(no_args_rrf_score_error());
        }
        return plan_hybrid_from_sort(&args, &collection, plan, alias.as_deref());
    }
    Ok(None)
}
