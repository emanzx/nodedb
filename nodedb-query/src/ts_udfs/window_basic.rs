//! `ts_delta`, `ts_interpolate`, `ts_lag`, `ts_lead`, `ts_rank` window UDFs.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, StringArray, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::Result as DfResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::function::WindowUDFFieldArgs;
use datafusion::logical_expr::{
    PartitionEvaluator, Signature, TypeSignature, Volatility, WindowUDFImpl,
};
use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;

use crate::ts_functions;

use super::helpers::{
    extract_f64_values, extract_timestamps_ns, f64_to_array, option_f64_to_array,
};

// ── ts_delta ─────────────────────────────────────────────────────────────

/// `ts_delta(value) OVER (ORDER BY time)` — consecutive-sample difference.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TsDeltaUdwf {
    signature: Signature,
}

impl TsDeltaUdwf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::Any(1)], Volatility::Immutable),
        }
    }
}

impl Default for TsDeltaUdwf {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowUDFImpl for TsDeltaUdwf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "ts_delta"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _args: PartitionEvaluatorArgs,
    ) -> DfResult<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(DeltaEvaluator))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> DfResult<FieldRef> {
        Ok(Arc::new(Field::new(
            field_args.name(),
            DataType::Float64,
            true,
        )))
    }
}

#[derive(Debug)]
struct DeltaEvaluator;

impl PartitionEvaluator for DeltaEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], _num_rows: usize) -> DfResult<ArrayRef> {
        let vals = extract_f64_values(&values[0])?;
        Ok(option_f64_to_array(ts_functions::ts_delta(&vals)))
    }
}

// ── ts_interpolate ───────────────────────────────────────────────────────

/// `ts_interpolate(value, time, method) OVER (ORDER BY time)` — gap fill.
///
/// `method` is a string: `'linear'`, `'prev'`/`'previous'`/`'locf'`,
/// `'next'`/`'nocb'`, or `'zero'`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TsInterpolateUdwf {
    signature: Signature,
}

impl TsInterpolateUdwf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::Any(3)], Volatility::Immutable),
        }
    }
}

impl Default for TsInterpolateUdwf {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowUDFImpl for TsInterpolateUdwf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "ts_interpolate"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _args: PartitionEvaluatorArgs,
    ) -> DfResult<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(InterpolateEvaluator))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> DfResult<FieldRef> {
        Ok(Arc::new(Field::new(
            field_args.name(),
            DataType::Float64,
            false,
        )))
    }
}

#[derive(Debug)]
struct InterpolateEvaluator;

impl PartitionEvaluator for InterpolateEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], _num_rows: usize) -> DfResult<ArrayRef> {
        let arr = &values[0];
        let f64_arr = arr.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
            DataFusionError::Internal("ts_interpolate: expected Float64 value column".into())
        })?;
        // Build Option<f64> vec respecting nulls.
        let opt_vals: Vec<Option<f64>> = (0..f64_arr.len())
            .map(|i| {
                if f64_arr.is_null(i) {
                    None
                } else {
                    Some(f64_arr.value(i))
                }
            })
            .collect();

        let ts = extract_timestamps_ns(&values[1])?;

        let method_str = extract_string_scalar(&values[2])?;
        let method = ts_functions::InterpolateMethod::parse(&method_str).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "ts_interpolate: unknown method '{method_str}'. \
                 Use 'linear', 'prev', 'next', or 'zero'"
            ))
        })?;

        let result = ts_functions::ts_interpolate(&opt_vals, &ts, method);
        Ok(f64_to_array(result))
    }
}

// ── ts_lag ───────────────────────────────────────────────────────────────

/// `ts_lag(value, offset) OVER (ORDER BY time)` — previous value.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TsLagUdwf {
    signature: Signature,
}

impl TsLagUdwf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(2), TypeSignature::Any(1)],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for TsLagUdwf {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowUDFImpl for TsLagUdwf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "ts_lag"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _args: PartitionEvaluatorArgs,
    ) -> DfResult<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(LagLeadEvaluator { is_lead: false }))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> DfResult<FieldRef> {
        Ok(Arc::new(Field::new(
            field_args.name(),
            DataType::Float64,
            true,
        )))
    }
}

// ── ts_lead ──────────────────────────────────────────────────────────────

/// `ts_lead(value, offset) OVER (ORDER BY time)` — next value.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TsLeadUdwf {
    signature: Signature,
}

impl TsLeadUdwf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(2), TypeSignature::Any(1)],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for TsLeadUdwf {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowUDFImpl for TsLeadUdwf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "ts_lead"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _args: PartitionEvaluatorArgs,
    ) -> DfResult<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(LagLeadEvaluator { is_lead: true }))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> DfResult<FieldRef> {
        Ok(Arc::new(Field::new(
            field_args.name(),
            DataType::Float64,
            true,
        )))
    }
}

#[derive(Debug)]
struct LagLeadEvaluator {
    is_lead: bool,
}

impl PartitionEvaluator for LagLeadEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> DfResult<ArrayRef> {
        let vals = extract_f64_values(&values[0])?;
        let offset = if values.len() > 1 {
            extract_usize_scalar(&values[1])?
        } else {
            1
        };

        let mut result: Vec<Option<f64>> = Vec::with_capacity(num_rows);
        for i in 0..num_rows {
            let src = if self.is_lead {
                i.checked_add(offset)
            } else {
                i.checked_sub(offset)
            };
            match src {
                Some(idx) if idx < vals.len() => result.push(Some(vals[idx])),
                _ => result.push(None),
            }
        }
        Ok(option_f64_to_array(result))
    }
}

// ── ts_rank ──────────────────────────────────────────────────────────────

/// `ts_rank(value) OVER (ORDER BY time)` — ordinal rank within partition.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TsRankUdwf {
    signature: Signature,
}

impl TsRankUdwf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::Any(1)], Volatility::Immutable),
        }
    }
}

impl Default for TsRankUdwf {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowUDFImpl for TsRankUdwf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "ts_rank"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _args: PartitionEvaluatorArgs,
    ) -> DfResult<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(RankEvaluator))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> DfResult<FieldRef> {
        Ok(Arc::new(Field::new(
            field_args.name(),
            DataType::Float64,
            false,
        )))
    }
}

#[derive(Debug)]
struct RankEvaluator;

impl PartitionEvaluator for RankEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> DfResult<ArrayRef> {
        let vals = extract_f64_values(&values[0])?;
        // Sort indices by value, assign ranks (1-based, ties share rank).
        let mut indices: Vec<usize> = (0..vals.len()).collect();
        indices.sort_by(|&a, &b| {
            vals[a]
                .partial_cmp(&vals[b])
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let mut ranks = vec![0.0_f64; num_rows];
        let mut rank = 1_usize;
        for (pos, &idx) in indices.iter().enumerate() {
            if pos > 0 && (vals[idx] - vals[indices[pos - 1]]).abs() > f64::EPSILON {
                rank = pos + 1;
            }
            ranks[idx] = rank as f64;
        }
        Ok(f64_to_array(ranks))
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────

fn extract_usize_scalar(arr: &ArrayRef) -> DfResult<usize> {
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>()
        && !a.is_empty()
    {
        return Ok(a.value(0) as usize);
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>()
        && !a.is_empty()
    {
        return Ok(a.value(0) as usize);
    }
    Err(DataFusionError::Internal("expected integer offset".into()))
}

fn extract_string_scalar(arr: &ArrayRef) -> DfResult<String> {
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>()
        && !a.is_empty()
        && !a.is_null(0)
    {
        return Ok(a.value(0).to_string());
    }
    Err(DataFusionError::Internal(
        "ts_interpolate: expected Utf8 method string".into(),
    ))
}
