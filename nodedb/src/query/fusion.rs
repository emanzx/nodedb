//! RRF fusion — re-exported from the shared `nodedb-query` crate.
//!
//! Origin's internal code continues to use `crate::query::fusion::*` paths;
//! they now resolve to the shared implementation in `nodedb_query::fusion`.

pub use nodedb_query::fusion::*;
