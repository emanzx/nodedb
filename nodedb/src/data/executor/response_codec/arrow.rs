// SPDX-License-Identifier: BUSL-1.1

//! Arrow IPC encoding for columnar transport.
//!
//! Converts `Vec<(doc_id, serde_json::Value)>` into an Arrow RecordBatch
//! serialized as IPC stream bytes. Schema is inferred from the first row.
//! The Control Plane receives native Arrow batches for DataFusion processing.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

/// Encode document rows as Arrow IPC bytes for columnar transport.
///
/// Returns `None` if rows are empty or schema inference fails.
pub fn encode_as_arrow_ipc(
    rows: &[(String, serde_json::Value)],
    projection: &[String],
) -> Option<Vec<u8>> {
    if rows.is_empty() {
        return None;
    }

    let first_obj = rows[0].1.as_object()?;
    let field_names: Vec<&str> = if projection.is_empty() {
        first_obj.keys().map(|k| k.as_str()).collect()
    } else {
        projection.iter().map(|s| s.as_str()).collect()
    };

    if field_names.is_empty() {
        return None;
    }

    let mut fields = vec![Field::new("id", DataType::Utf8, false)];
    for &name in &field_names {
        let dt = first_obj
            .get(name)
            .map(infer_type)
            .unwrap_or(DataType::Utf8);
        fields.push(Field::new(name, dt, true));
    }
    let schema = Arc::new(Schema::new(fields));

    let mut ids: Vec<String> = Vec::with_capacity(rows.len());
    let mut builders: Vec<ColBuilder> = field_names
        .iter()
        .map(|&name| {
            let dt = first_obj
                .get(name)
                .map(infer_type)
                .unwrap_or(DataType::Utf8);
            ColBuilder::new(dt, rows.len())
        })
        .collect();

    for (doc_id, data) in rows {
        ids.push(doc_id.clone());
        let obj = data.as_object();
        for (i, &name) in field_names.iter().enumerate() {
            match obj.and_then(|o| o.get(name)) {
                Some(v) => builders[i].push(v),
                None => builders[i].push_null(),
            }
        }
    }

    let mut arrays: Vec<ArrayRef> = vec![Arc::new(StringArray::from(ids))];
    for b in builders {
        arrays.push(b.finish());
    }

    let batch = RecordBatch::try_new(schema.clone(), arrays).ok()?;

    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema).ok()?;
        writer.write(&batch).ok()?;
        writer.finish().ok()?;
    }
    Some(buf)
}

fn infer_type(v: &serde_json::Value) -> DataType {
    match v {
        serde_json::Value::Number(n) if n.is_i64() => DataType::Int64,
        serde_json::Value::Number(_) => DataType::Float64,
        serde_json::Value::Bool(_) => DataType::Boolean,
        _ => DataType::Utf8,
    }
}

enum ColBuilder {
    Str(Vec<Option<String>>),
    I64(Vec<Option<i64>>),
    F64(Vec<Option<f64>>),
}

impl ColBuilder {
    fn new(dt: DataType, cap: usize) -> Self {
        match dt {
            DataType::Int64 => Self::I64(Vec::with_capacity(cap)),
            DataType::Float64 => Self::F64(Vec::with_capacity(cap)),
            _ => Self::Str(Vec::with_capacity(cap)),
        }
    }

    fn push(&mut self, v: &serde_json::Value) {
        match self {
            Self::Str(vec) => vec.push(Some(match v {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            })),
            Self::I64(vec) => vec.push(v.as_i64()),
            Self::F64(vec) => vec.push(v.as_f64()),
        }
    }

    fn push_null(&mut self) {
        match self {
            Self::Str(v) => v.push(None),
            Self::I64(v) => v.push(None),
            Self::F64(v) => v.push(None),
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Str(v) => Arc::new(StringArray::from(v)) as ArrayRef,
            Self::I64(v) => Arc::new(Int64Array::from(v)) as ArrayRef,
            Self::F64(v) => Arc::new(Float64Array::from(v)) as ArrayRef,
        }
    }
}
