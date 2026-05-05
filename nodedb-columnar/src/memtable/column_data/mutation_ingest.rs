//! `push_ref` and `backfill_nulls` methods on `ColumnData`.
//!
//! Extracted from `mutation.rs` to keep that file under 500 lines.

use crate::error::ColumnarError;
use crate::memtable::IngestValue;

use super::types::ColumnData;

impl ColumnData {
    /// Append a borrowed value (zero-copy for strings). Used by `ingest_row_refs`.
    pub(crate) fn push_ref(
        &mut self,
        value: &IngestValue<'_>,
        col_name: &str,
    ) -> Result<(), ColumnarError> {
        match (self, value) {
            (Self::Int64 { values, valid }, IngestValue::Null) => {
                values.push(0);
                Self::push_valid(valid, false);
            }
            (Self::Float64 { values, valid }, IngestValue::Null) => {
                values.push(0.0);
                Self::push_valid(valid, false);
            }
            (Self::Bool { values, valid }, IngestValue::Null) => {
                values.push(false);
                Self::push_valid(valid, false);
            }
            (Self::Timestamp { values, valid }, IngestValue::Null) => {
                values.push(0);
                Self::push_valid(valid, false);
            }
            (Self::String { offsets, valid, .. }, IngestValue::Null) => {
                offsets.push(*offsets.last().unwrap_or(&0));
                Self::push_valid(valid, false);
            }
            (Self::DictEncoded { ids, valid, .. }, IngestValue::Null) => {
                ids.push(0);
                Self::push_valid(valid, false);
            }
            (Self::Int64 { values, valid }, IngestValue::Int64(v)) => {
                values.push(*v);
                Self::push_valid(valid, true);
            }
            (Self::Float64 { values, valid }, IngestValue::Float64(v)) => {
                values.push(*v);
                Self::push_valid(valid, true);
            }
            (Self::Float64 { values, valid }, IngestValue::Int64(v)) => {
                values.push(*v as f64);
                Self::push_valid(valid, true);
            }
            (Self::Bool { values, valid }, IngestValue::Bool(v)) => {
                values.push(*v);
                Self::push_valid(valid, true);
            }
            (Self::Timestamp { values, valid }, IngestValue::Timestamp(v)) => {
                values.push(*v);
                Self::push_valid(valid, true);
            }
            (Self::Timestamp { values, valid }, IngestValue::Int64(v)) => {
                values.push(*v);
                Self::push_valid(valid, true);
            }
            (
                Self::String {
                    data,
                    offsets,
                    valid,
                },
                IngestValue::Str(s),
            ) => {
                data.extend_from_slice(s.as_bytes());
                offsets.push(data.len() as u32);
                Self::push_valid(valid, true);
            }
            (
                Self::DictEncoded {
                    ids,
                    dictionary,
                    reverse,
                    valid,
                },
                IngestValue::Str(s),
            ) => {
                let id = if let Some(&existing) = reverse.get(*s) {
                    existing
                } else {
                    let new_id = dictionary.len() as u32;
                    dictionary.push((*s).to_string());
                    reverse.insert((*s).to_string(), new_id);
                    new_id
                };
                ids.push(id);
                Self::push_valid(valid, true);
            }
            (other, _) => {
                let type_name = match other {
                    Self::Int64 { .. } => "Int64",
                    Self::Float64 { .. } => "Float64",
                    Self::Bool { .. } => "Bool",
                    Self::Timestamp { .. } => "Timestamp",
                    Self::Decimal { .. } => "Decimal",
                    Self::Uuid { .. } => "Uuid",
                    Self::String { .. } => "String",
                    Self::Bytes { .. } => "Bytes",
                    Self::Geometry { .. } => "Geometry",
                    Self::Vector { .. } => "Vector",
                    Self::DictEncoded { .. } => "DictEncoded",
                };
                return Err(ColumnarError::TypeMismatch {
                    column: col_name.to_string(),
                    expected: type_name.to_string(),
                });
            }
        }
        Ok(())
    }

    /// Backfill a column with null/default values for existing rows.
    pub(crate) fn backfill_nulls(&mut self, count: usize) {
        match self {
            Self::Int64 { values, valid } => {
                values.extend(std::iter::repeat_n(0i64, count));
                if let Some(v) = valid {
                    v.extend(std::iter::repeat_n(false, count));
                }
            }
            Self::Float64 { values, valid } => {
                values.extend(std::iter::repeat_n(f64::NAN, count));
                if let Some(v) = valid {
                    v.extend(std::iter::repeat_n(false, count));
                }
            }
            Self::Bool { values, valid } => {
                values.extend(std::iter::repeat_n(false, count));
                if let Some(v) = valid {
                    v.extend(std::iter::repeat_n(false, count));
                }
            }
            Self::Timestamp { values, valid } => {
                values.extend(std::iter::repeat_n(0i64, count));
                if let Some(v) = valid {
                    v.extend(std::iter::repeat_n(false, count));
                }
            }
            Self::Decimal { values, valid } => {
                values.extend(std::iter::repeat_n([0u8; 16], count));
                if let Some(v) = valid {
                    v.extend(std::iter::repeat_n(false, count));
                }
            }
            Self::Uuid { values, valid } => {
                values.extend(std::iter::repeat_n([0u8; 16], count));
                if let Some(v) = valid {
                    v.extend(std::iter::repeat_n(false, count));
                }
            }
            Self::String { offsets, valid, .. } => {
                let last = *offsets.last().unwrap_or(&0);
                offsets.extend(std::iter::repeat_n(last, count));
                if let Some(v) = valid {
                    v.extend(std::iter::repeat_n(false, count));
                }
            }
            Self::Bytes { offsets, valid, .. } => {
                let last = *offsets.last().unwrap_or(&0);
                offsets.extend(std::iter::repeat_n(last, count));
                if let Some(v) = valid {
                    v.extend(std::iter::repeat_n(false, count));
                }
            }
            Self::Geometry { offsets, valid, .. } => {
                let last = *offsets.last().unwrap_or(&0);
                offsets.extend(std::iter::repeat_n(last, count));
                if let Some(v) = valid {
                    v.extend(std::iter::repeat_n(false, count));
                }
            }
            Self::Vector { data, dim, valid } => {
                data.extend(std::iter::repeat_n(0.0f32, *dim as usize * count));
                if let Some(v) = valid {
                    v.extend(std::iter::repeat_n(false, count));
                }
            }
            Self::DictEncoded { ids, valid, .. } => {
                ids.extend(std::iter::repeat_n(0u32, count));
                if let Some(v) = valid {
                    v.extend(std::iter::repeat_n(false, count));
                }
            }
        }
    }
}
