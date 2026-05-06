// SPDX-License-Identifier: BUSL-1.1

//! LATERAL subquery planning.
//!
//! Handles `FROM t, LATERAL (SELECT ...) x` and `JOIN LATERAL (...) ON true`
//! patterns by classifying the correlation and emitting the appropriate
//! `SqlPlan::LateralTopK` or `SqlPlan::LateralLoop` variant.

pub mod correlation;
pub mod plan;

pub use plan::plan_lateral_join;
