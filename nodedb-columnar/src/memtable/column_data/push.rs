// SPDX-License-Identifier: BUSL-1.1

//! Append operations on `ColumnData`: push owned values and push borrowed values.

use nodedb_types::value::Value;

use crate::error::ColumnarError;

use super::super::IngestValue;
use super::types::ColumnData;

impl ColumnData {
    /// Push a validity bit (if the column is nullable).
    #[inline(always)]
    pub(crate) fn push_valid(valid: &mut Option<Vec<bool>>, is_valid: bool) {
        if let Some(v) = valid {
            v.push(is_valid);
        }
    }

    /// Append a value. Returns error if type doesn't match.
    pub(crate) fn push(&mut self, value: &Value, col_name: &str) -> Result<(), ColumnarError> {
        match (self, value) {
            (Self::Int64 { values, valid }, Value::Null) => {
                values.push(0);
                Self::push_valid(valid, false);
            }
            (Self::Float64 { values, valid }, Value::Null) => {
                values.push(0.0);
                Self::push_valid(valid, false);
            }
            (Self::Bool { values, valid }, Value::Null) => {
                values.push(false);
                Self::push_valid(valid, false);
            }
            (Self::Timestamp { values, valid }, Value::Null) => {
                values.push(0);
                Self::push_valid(valid, false);
            }
            (Self::Decimal { values, valid }, Value::Null) => {
                values.push([0u8; 16]);
                Self::push_valid(valid, false);
            }
            (Self::Uuid { values, valid }, Value::Null) => {
                values.push([0u8; 16]);
                Self::push_valid(valid, false);
            }
            (Self::String { offsets, valid, .. }, Value::Null) => {
                offsets.push(*offsets.last().unwrap_or(&0));
                Self::push_valid(valid, false);
            }
            (Self::Bytes { offsets, valid, .. }, Value::Null) => {
                offsets.push(*offsets.last().unwrap_or(&0));
                Self::push_valid(valid, false);
            }
            (Self::Geometry { offsets, valid, .. }, Value::Null) => {
                offsets.push(*offsets.last().unwrap_or(&0));
                Self::push_valid(valid, false);
            }
            (Self::Vector { data, dim, valid }, Value::Null) => {
                data.extend(std::iter::repeat_n(0.0f32, *dim as usize));
                Self::push_valid(valid, false);
            }
            (Self::Int64 { values, valid }, Value::Integer(v)) => {
                values.push(*v);
                Self::push_valid(valid, true);
            }
            (Self::Float64 { values, valid }, Value::Float(v)) => {
                values.push(*v);
                Self::push_valid(valid, true);
            }
            (Self::Float64 { values, valid }, Value::Integer(v)) => {
                values.push(*v as f64);
                Self::push_valid(valid, true);
            }
            (Self::Bool { values, valid }, Value::Bool(v)) => {
                values.push(*v);
                Self::push_valid(valid, true);
            }
            (Self::Timestamp { values, valid }, Value::DateTime(dt))
            | (Self::Timestamp { values, valid }, Value::NaiveDateTime(dt)) => {
                values.push(dt.micros);
                Self::push_valid(valid, true);
            }
            (Self::Timestamp { values, valid }, Value::Integer(micros)) => {
                values.push(*micros);
                Self::push_valid(valid, true);
            }
            (Self::Decimal { values, valid }, Value::Decimal(d)) => {
                values.push(d.serialize());
                Self::push_valid(valid, true);
            }
            (Self::Uuid { values, valid }, Value::Uuid(s)) => {
                let bytes = uuid::Uuid::parse_str(s)
                    .map(|u| *u.as_bytes())
                    .unwrap_or([0u8; 16]);
                values.push(bytes);
                Self::push_valid(valid, true);
            }
            (
                Self::String {
                    data,
                    offsets,
                    valid,
                },
                Value::String(s),
            ) => {
                data.extend_from_slice(s.as_bytes());
                offsets.push(data.len() as u32);
                Self::push_valid(valid, true);
            }
            (
                Self::Bytes {
                    data,
                    offsets,
                    valid,
                },
                Value::Bytes(b),
            ) => {
                data.extend_from_slice(b);
                offsets.push(data.len() as u32);
                Self::push_valid(valid, true);
            }
            (
                Self::Geometry {
                    data,
                    offsets,
                    valid,
                },
                Value::Geometry(g),
            ) => {
                if let Ok(json) = sonic_rs::to_vec(g) {
                    data.extend_from_slice(&json);
                }
                offsets.push(data.len() as u32);
                Self::push_valid(valid, true);
            }
            (
                Self::Geometry {
                    data,
                    offsets,
                    valid,
                },
                Value::String(s),
            ) => {
                data.extend_from_slice(s.as_bytes());
                offsets.push(data.len() as u32);
                Self::push_valid(valid, true);
            }
            (Self::Vector { data, dim, valid }, Value::Array(arr)) => {
                let d = *dim as usize;
                for (i, v) in arr.iter().take(d).enumerate() {
                    let f = match v {
                        Value::Float(f) => *f as f32,
                        Value::Integer(n) => *n as f32,
                        _ => 0.0,
                    };
                    if i < d {
                        data.push(f);
                    }
                }
                for _ in arr.len()..d {
                    data.push(0.0);
                }
                Self::push_valid(valid, true);
            }
            (Self::DictEncoded { ids, valid, .. }, Value::Null) => {
                ids.push(0);
                Self::push_valid(valid, false);
            }
            (
                Self::DictEncoded {
                    ids,
                    dictionary,
                    reverse,
                    valid,
                },
                Value::String(s),
            ) => {
                let id = if let Some(&existing) = reverse.get(s.as_str()) {
                    existing
                } else {
                    let new_id = dictionary.len() as u32;
                    dictionary.push(s.clone());
                    reverse.insert(s.clone(), new_id);
                    new_id
                };
                ids.push(id);
                Self::push_valid(valid, true);
            }
            (other, val) => {
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
                let _ = val;
                return Err(ColumnarError::TypeMismatch {
                    column: col_name.to_string(),
                    expected: type_name.to_string(),
                });
            }
        }
        Ok(())
    }

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
}
