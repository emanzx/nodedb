//! `ts_moving_avg` and `ts_ema` window UDFs.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::Result as DfResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::function::WindowUDFFieldArgs;
use datafusion::logical_expr::{
    PartitionEvaluator, Signature, TypeSignature, Volatility, WindowUDFImpl,
};
use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;

use crate::ts_functions;

use super::helpers::{extract_f64_values, f64_to_array, option_f64_to_array};

// ── ts_moving_avg ────────────────────────────────────────────────────────

/// `ts_moving_avg(value, window_size) OVER (ORDER BY time)` — simple moving average.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TsMovingAvgUdwf {
    signature: Signature,
}

impl TsMovingAvgUdwf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
        }
    }
}

impl Default for TsMovingAvgUdwf {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowUDFImpl for TsMovingAvgUdwf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "ts_moving_avg"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _args: PartitionEvaluatorArgs,
    ) -> DfResult<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(MovingAvgEvaluator))
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
struct MovingAvgEvaluator;

impl PartitionEvaluator for MovingAvgEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], _num_rows: usize) -> DfResult<ArrayRef> {
        let vals = extract_f64_values(&values[0])?;
        let window = extract_usize_scalar(&values[1])?;
        let result = ts_functions::ts_moving_avg(&vals, window);
        Ok(option_f64_to_array(result))
    }
}

// ── ts_ema ───────────────────────────────────────────────────────────────

/// `ts_ema(value, alpha) OVER (ORDER BY time)` — exponential moving average.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TsEmaUdwf {
    signature: Signature,
}

impl TsEmaUdwf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
        }
    }
}

impl Default for TsEmaUdwf {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowUDFImpl for TsEmaUdwf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "ts_ema"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _args: PartitionEvaluatorArgs,
    ) -> DfResult<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(EmaEvaluator))
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
struct EmaEvaluator;

impl PartitionEvaluator for EmaEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], _num_rows: usize) -> DfResult<ArrayRef> {
        let vals = extract_f64_values(&values[0])?;
        let alpha = extract_f64_scalar(&values[1])?;
        if alpha <= 0.0 || alpha > 1.0 {
            return Err(DataFusionError::Plan(
                "ts_ema: alpha must be in (0.0, 1.0]".into(),
            ));
        }
        let result = ts_functions::ts_ema(&vals, alpha);
        Ok(f64_to_array(result))
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────

/// Extract a scalar integer parameter broadcast across all rows.
///
/// Window function parameters like `window_size` come as a constant array
/// where every element has the same value.
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
    Err(DataFusionError::Internal(
        "ts_moving_avg: expected integer window_size".into(),
    ))
}

/// Extract a scalar Float64 parameter broadcast across all rows.
fn extract_f64_scalar(arr: &ArrayRef) -> DfResult<f64> {
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>()
        && !a.is_empty()
    {
        return Ok(a.value(0));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>()
        && !a.is_empty()
    {
        return Ok(a.value(0) as f64);
    }
    Err(DataFusionError::Internal(
        "ts_ema: expected Float64 alpha".into(),
    ))
}
