// SPDX-License-Identifier: BUSL-1.1

//! Document / sparse engine operations dispatched to the Data Plane.

pub mod merge_types;
pub mod op;
pub mod types;

pub use merge_types::{MergeActionOp, MergeClauseKind as MergeClauseKindOp, MergeClauseOp};
pub use op::DocumentOp;
pub use types::{
    BalancedDef, EnforcementOptions, GeneratedColumnSpec, MaterializedSumBinding, PeriodLockConfig,
    RegisteredIndex, RegisteredIndexState, ReturningColumns, ReturningItem, ReturningSpec,
    StorageMode, UpdateValue,
};
