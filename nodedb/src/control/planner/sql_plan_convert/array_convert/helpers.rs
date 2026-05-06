// SPDX-License-Identifier: BUSL-1.1

//! Shared coercion helpers for array coordinate and attribute literals.

use nodedb_array::schema::{ArraySchema, AttrType as EngineAttrType, DimType as EngineDimType};
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_sql::types_array::{ArrayAttrLiteral, ArrayCoordLiteral};

pub(super) fn coerce_coords(
    coords: &[ArrayCoordLiteral],
    schema: &ArraySchema,
) -> crate::Result<Vec<CoordValue>> {
    if coords.len() != schema.dims.len() {
        return Err(crate::Error::PlanError {
            detail: format!(
                "coord arity {} does not match dim count {}",
                coords.len(),
                schema.dims.len()
            ),
        });
    }
    let mut out = Vec::with_capacity(coords.len());
    for (i, c) in coords.iter().enumerate() {
        let dim = &schema.dims[i];
        let v = match (c, dim.dtype) {
            (ArrayCoordLiteral::Int64(n), EngineDimType::Int64) => CoordValue::Int64(*n),
            (ArrayCoordLiteral::Int64(n), EngineDimType::TimestampMs) => {
                CoordValue::TimestampMs(*n)
            }
            (ArrayCoordLiteral::Int64(n), EngineDimType::Float64) => CoordValue::Float64(*n as f64),
            (ArrayCoordLiteral::Float64(f), EngineDimType::Float64) => CoordValue::Float64(*f),
            (ArrayCoordLiteral::String(s), EngineDimType::String) => CoordValue::String(s.clone()),
            (got, want) => {
                return Err(crate::Error::PlanError {
                    detail: format!(
                        "coord literal for dim `{}`: got {got:?}, expected dim type {want:?}",
                        dim.name
                    ),
                });
            }
        };
        out.push(v);
    }
    Ok(out)
}

pub(super) fn coerce_attrs(
    attrs: &[ArrayAttrLiteral],
    schema: &ArraySchema,
) -> crate::Result<Vec<CellValue>> {
    if attrs.len() != schema.attrs.len() {
        return Err(crate::Error::PlanError {
            detail: format!(
                "attr arity {} does not match attr count {}",
                attrs.len(),
                schema.attrs.len()
            ),
        });
    }
    let mut out = Vec::with_capacity(attrs.len());
    for (i, a) in attrs.iter().enumerate() {
        let spec = &schema.attrs[i];
        let v = match (a, spec.dtype) {
            (ArrayAttrLiteral::Null, _) if spec.nullable => CellValue::Null,
            (ArrayAttrLiteral::Null, _) => {
                return Err(crate::Error::PlanError {
                    detail: format!("attr `{}` is NOT NULL", spec.name),
                });
            }
            (ArrayAttrLiteral::Int64(n), EngineAttrType::Int64) => CellValue::Int64(*n),
            (ArrayAttrLiteral::Int64(n), EngineAttrType::Float64) => CellValue::Float64(*n as f64),
            (ArrayAttrLiteral::Float64(f), EngineAttrType::Float64) => CellValue::Float64(*f),
            (ArrayAttrLiteral::String(s), EngineAttrType::String) => CellValue::String(s.clone()),
            (ArrayAttrLiteral::Bytes(b), EngineAttrType::Bytes) => CellValue::Bytes(b.clone()),
            (got, want) => {
                return Err(crate::Error::PlanError {
                    detail: format!(
                        "attr literal for `{}`: got {got:?}, expected attr type {want:?}",
                        spec.name
                    ),
                });
            }
        };
        out.push(v);
    }
    Ok(out)
}
