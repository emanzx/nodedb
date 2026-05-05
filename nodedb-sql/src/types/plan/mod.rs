//! SqlPlan intermediate representation and supporting types.

mod merge_types;
mod row_types;
mod variants;
mod vector_opts;

pub use merge_types::{MergeClauseKind, MergePlanAction, MergePlanClause};
pub use row_types::{KvInsertIntent, VectorPrimaryRow};
pub use variants::{DistanceMetric, SqlPlan};
pub use vector_opts::{ArrayPrefilter, VectorAnnOptions, VectorQuantization};
