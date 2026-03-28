//! `ts_rate` and `ts_derivative` window UDFs.
//!
//! Both accept `(value_col, time_col)` and produce a per-second rate.
//! `ts_rate` handles counter resets; `ts_derivative` does not.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::function::WindowUDFFieldArgs;
use datafusion::logical_expr::{
    PartitionEvaluator, Signature, TypeSignature, Volatility, WindowUDFImpl,
};
use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;

use crate::ts_functions;

use super::helpers::{extract_f64_values, extract_timestamps_ns, option_f64_to_array};

// ── ts_rate ──────────────────────────────────────────────────────────────

/// `ts_rate(value, time) OVER (ORDER BY time)` — counter-aware per-second rate.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TsRateUdwf {
    signature: Signature,
}

impl TsRateUdwf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
        }
    }
}

impl Default for TsRateUdwf {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowUDFImpl for TsRateUdwf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "ts_rate"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _args: PartitionEvaluatorArgs,
    ) -> DfResult<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(RateEvaluator {
            counter_aware: true,
        }))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> DfResult<FieldRef> {
        Ok(Arc::new(Field::new(
            field_args.name(),
            DataType::Float64,
            true,
        )))
    }
}

// ── ts_derivative ────────────────────────────────────────────────────────

/// `ts_derivative(value, time) OVER (ORDER BY time)` — per-second rate of change.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TsDerivativeUdwf {
    signature: Signature,
}

impl TsDerivativeUdwf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
        }
    }
}

impl Default for TsDerivativeUdwf {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowUDFImpl for TsDerivativeUdwf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "ts_derivative"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _args: PartitionEvaluatorArgs,
    ) -> DfResult<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(RateEvaluator {
            counter_aware: false,
        }))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> DfResult<FieldRef> {
        Ok(Arc::new(Field::new(
            field_args.name(),
            DataType::Float64,
            true,
        )))
    }
}

// ── Shared evaluator ─────────────────────────────────────────────────────

#[derive(Debug)]
struct RateEvaluator {
    counter_aware: bool,
}

impl PartitionEvaluator for RateEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], _num_rows: usize) -> DfResult<ArrayRef> {
        let vals = extract_f64_values(&values[0])?;
        let ts = extract_timestamps_ns(&values[1])?;
        let result = if self.counter_aware {
            ts_functions::ts_rate(&vals, &ts)
        } else {
            ts_functions::ts_derivative(&vals, &ts)
        };
        Ok(option_f64_to_array(result))
    }
}
