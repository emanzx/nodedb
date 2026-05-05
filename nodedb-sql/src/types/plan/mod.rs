//! SqlPlan intermediate representation and supporting types.

mod row_types;
mod variants;
mod vector_opts;

pub use row_types::{KvInsertIntent, VectorPrimaryRow};
pub use variants::{DistanceMetric, SqlPlan};
pub use vector_opts::{ArrayPrefilter, VectorAnnOptions, VectorQuantization};
