// SPDX-License-Identifier: Apache-2.0

//! Supporting row and intent types for SqlPlan.

use crate::types_expr::SqlValue;

/// A single row for a vector-primary INSERT.
///
/// The surrogate is allocated by the Control Plane before the op reaches
/// the Data Plane; the Data Plane only stores the binding.
#[derive(Debug, Clone)]
pub struct VectorPrimaryRow {
    /// Global surrogate allocated by the Control Plane (`Surrogate::ZERO`
    /// is a sentinel meaning "not yet assigned").
    pub surrogate: nodedb_types::Surrogate,
    /// FP32 vector extracted from the vector-field column.
    pub vector: Vec<f32>,
    /// Payload fields (non-vector columns that may feed bitmap indexes).
    pub payload_fields: std::collections::HashMap<String, SqlValue>,
}

/// INSERT-vs-UPSERT intent carried on `SqlPlan::KvInsert`.
///
/// The KV engine's `KvOp::Put` is a Redis-SET-style upsert: write wins
/// unconditionally. SQL requires `INSERT` to raise `unique_violation`
/// on duplicate keys, so the plan must carry the caller's intent through
/// to the Data Plane where the hash-index existence probe happens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvInsertIntent {
    /// Plain `INSERT`: duplicate key raises `SQLSTATE 23505`.
    Insert,
    /// `INSERT ... ON CONFLICT DO NOTHING`: duplicate key is a no-op.
    InsertIfAbsent,
    /// `UPSERT` / `INSERT ... ON CONFLICT (key) DO UPDATE` / RESP `SET`:
    /// duplicate key overwrites. Also the shape used by the RESP SET path.
    Put,
}
