// SPDX-License-Identifier: BUSL-1.1

//! ORDER BY processing and search-trigger detection.
//!
//! Splits responsibilities into focused modules:
//!
//! - `apply` — `apply_order_by` entry point: maps an ORDER BY clause to
//!   either sort keys on a scan or a search-shaped plan when the leading
//!   sort expression matches a `SearchTrigger`.
//! - `projection` — `try_hybrid_from_projection` fallback: when no ORDER BY
//!   drives the conversion, scan the SELECT list for a hybrid-search trigger.
//! - `aliases` — alias resolution between ORDER BY and SELECT projection.
//! - `triggers` — generic `SearchTrigger` → `SqlPlan` detection.
//! - `hybrid` — `rrf_score(...)` → `SqlPlan::HybridSearch` construction.
//! - `vector_join` — `vector_distance ⋈ ARRAY_SLICE` fusion target detection.

mod aliases;
mod apply;
mod hybrid;
mod projection;
mod triggers;
mod vector_join;

pub(super) use apply::apply_order_by;
pub(super) use projection::try_hybrid_from_projection;
