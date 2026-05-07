// SPDX-License-Identifier: Apache-2.0

//! Types shared between the SQL planner and converters for MERGE plans.

use crate::types::filter::Filter;
use crate::types_expr::SqlExpr;

/// A single `WHEN ... THEN ...` arm within a `SqlPlan::Merge`.
#[derive(Debug, Clone)]
pub struct MergePlanClause {
    /// The kind of match this arm fires on.
    pub kind: MergeClauseKind,
    /// Optional extra predicate (the `AND <pred>` part), evaluated against
    /// the joined row pair after the match condition is satisfied.
    pub extra_predicate: Vec<Filter>,
    /// The action to apply when this arm fires.
    pub action: MergePlanAction,
}

/// Which rows trigger a WHEN arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeClauseKind {
    /// `WHEN MATCHED` — target row exists for this source row.
    Matched,
    /// `WHEN NOT MATCHED` / `WHEN NOT MATCHED BY TARGET` — source row has no
    /// corresponding target row.
    NotMatched,
    /// `WHEN NOT MATCHED BY SOURCE` — target row has no corresponding source row.
    NotMatchedBySource,
}

/// The action performed when a WHEN arm fires.
#[derive(Debug, Clone)]
pub enum MergePlanAction {
    /// `THEN UPDATE SET col = expr, ...`
    Update { assignments: Vec<(String, SqlExpr)> },
    /// `THEN DELETE`
    Delete,
    /// `THEN INSERT (cols) VALUES (exprs)`
    Insert {
        /// Column names in declaration order.
        columns: Vec<String>,
        /// Parallel value expressions (same length as `columns`).
        values: Vec<SqlExpr>,
    },
    /// `THEN DO NOTHING`
    DoNothing,
}
