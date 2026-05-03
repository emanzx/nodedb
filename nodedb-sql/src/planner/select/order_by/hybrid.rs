//! Hybrid-search plan construction from `rrf_score(...)` calls.
//!
//! The function shape is `rrf_score(vector_distance(...), bm25_score(...),
//! k1?, k2?)`. Vector and text query components are extracted from the
//! first two argument expressions; `k1`/`k2` (RRF constants) default to
//! 60.0 each. `score_alias` carries the SELECT alias the response should
//! use for the RRF score column — without it, the executor falls back to
//! the fixed internal name `rrf_score`.

use sqlparser::ast;

use super::super::helpers::{
    extract_float, extract_float_array, extract_func_args, extract_string_literal,
};
use crate::error::{Result, SqlError};
use crate::types::SqlPlan;

/// Build a `SqlPlan::HybridSearch` from a `rrf_score(...)` call.
pub(super) fn plan_hybrid_from_sort(
    args: &[ast::Expr],
    collection: &str,
    plan: &SqlPlan,
    score_alias: Option<&str>,
) -> Result<Option<SqlPlan>> {
    if args.len() < 2 {
        // 0-args / 1-arg shapes have no concrete vector or text component to
        // fuse; we cannot synthesise meaningful search arguments. Surfacing
        // a typed error is louder than the previous behaviour, which
        // returned `Ok(None)` and let the column resolve to NULL via scalar
        // evaluation that has no implementation.
        return Err(no_args_rrf_score_error());
    }
    let vector = match &args[0] {
        ast::Expr::Function(f) => {
            let inner_args = extract_func_args(f)?;
            if inner_args.len() >= 2 {
                extract_float_array(&inner_args[1]).unwrap_or_default()
            } else {
                Vec::new()
            }
        }
        _ => Vec::new(),
    };
    let text = match &args[1] {
        ast::Expr::Function(f) => {
            let inner_args = extract_func_args(f)?;
            if inner_args.len() >= 2 {
                extract_string_literal(&inner_args[1]).unwrap_or_default()
            } else {
                String::new()
            }
        }
        _ => String::new(),
    };
    let k1 = args
        .get(2)
        .and_then(|e| extract_float(e).ok())
        .unwrap_or(60.0);
    let k2 = args
        .get(3)
        .and_then(|e| extract_float(e).ok())
        .unwrap_or(60.0);
    let limit = match plan {
        SqlPlan::Scan { limit, .. } => limit.unwrap_or(10),
        _ => 10,
    };
    let vector_weight = k2 as f32 / (k1 as f32 + k2 as f32);

    Ok(Some(SqlPlan::HybridSearch {
        collection: collection.into(),
        query_vector: vector,
        query_text: text,
        top_k: limit,
        ef_search: limit * 2,
        vector_weight,
        fuzzy: true,
        score_alias: score_alias.map(|s| s.to_string()),
    }))
}

/// Construct the typed error returned for `rrf_score()` with no arguments.
///
/// The previous behaviour was a silent fall-through to scalar evaluation
/// (which has no implementation), surfacing a NULL column with no signal.
/// This error makes the bad shape visible at parse time.
pub(super) fn no_args_rrf_score_error() -> SqlError {
    SqlError::InvalidFunction {
        detail: "rrf_score() requires at least vector_distance(...) and bm25_score(...) \
                 arguments; e.g. rrf_score(vector_distance(emb, ARRAY[...]), \
                 bm25_score(content, 'query'))"
            .into(),
    }
}
