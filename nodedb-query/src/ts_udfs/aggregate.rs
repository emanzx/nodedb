//! `ts_percentile`, `ts_stddev`, `ts_correlate` aggregate UDFs.

use std::any::Any;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DfResult, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, Signature, TypeSignature, Volatility,
};

use crate::ts_functions;

// ── ts_percentile ────────────────────────────────────────────────────────

/// `ts_percentile(value, p)` — exact percentile (p ∈ [0, 1]).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TsPercentileUdaf {
    signature: Signature,
}

impl TsPercentileUdaf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![
                    DataType::Float64,
                    DataType::Float64,
                ])],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for TsPercentileUdaf {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for TsPercentileUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "ts_percentile"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> DfResult<Box<dyn Accumulator>> {
        Ok(Box::new(PercentileAccum {
            inner: ts_functions::TsPercentileAccum::new(0.5),
            percentile: None,
        }))
    }
}

#[derive(Debug)]
struct PercentileAccum {
    inner: ts_functions::TsPercentileAccum,
    percentile: Option<f64>,
}

impl Accumulator for PercentileAccum {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let arr = values[0]
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Internal("ts_percentile: expected Float64".into()))?;

        // Extract percentile from second argument (constant across batch).
        if self.percentile.is_none() {
            let p_arr = values[1]
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("ts_percentile: expected Float64 percentile".into())
                })?;
            if !p_arr.is_empty() {
                let p = p_arr.value(0);
                self.percentile = Some(p);
                self.inner = ts_functions::TsPercentileAccum::new(p);
            }
        }

        for i in 0..arr.len() {
            if !arr.is_null(i) {
                self.inner.update(arr.value(i));
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        Ok(ScalarValue::Float64(self.inner.evaluate()))
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        // Serialize collected values + percentile target as a list.
        let mut state = self.inner.state();
        state.push(self.percentile.unwrap_or(0.5));
        let scalars: Vec<ScalarValue> = state
            .into_iter()
            .map(|v| ScalarValue::Float64(Some(v)))
            .collect();
        Ok(vec![ScalarValue::List(ScalarValue::new_list(
            &scalars,
            &DataType::Float64,
            true,
        ))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        use datafusion::arrow::array::ListArray;
        let list_arr = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("ts_percentile merge: expected List".into())
            })?;

        for i in 0..list_arr.len() {
            if list_arr.is_null(i) {
                continue;
            }
            let inner = list_arr.value(i);
            let f64_arr = inner
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("ts_percentile merge: expected Float64 list".into())
                })?;
            if f64_arr.is_empty() {
                continue;
            }
            // Last element is the percentile target.
            let p = f64_arr.value(f64_arr.len() - 1);
            if self.percentile.is_none() {
                self.percentile = Some(p);
                self.inner = ts_functions::TsPercentileAccum::new(p);
            }
            let data_end = f64_arr.len() - 1;
            for j in 0..data_end {
                if !f64_arr.is_null(j) {
                    self.inner.update(f64_arr.value(j));
                }
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        self.inner.size() + std::mem::size_of::<Option<f64>>()
    }
}

// ── ts_stddev ────────────────────────────────────────────────────────────

/// `ts_stddev(value)` — population standard deviation (Welford).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TsStddevUdaf {
    signature: Signature,
}

impl TsStddevUdaf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Float64])],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for TsStddevUdaf {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for TsStddevUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "ts_stddev"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> DfResult<Box<dyn Accumulator>> {
        Ok(Box::new(StddevAccum(ts_functions::TsStddevAccum::new())))
    }
}

#[derive(Debug)]
struct StddevAccum(ts_functions::TsStddevAccum);

impl Accumulator for StddevAccum {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let arr = values[0]
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Internal("ts_stddev: expected Float64".into()))?;
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                self.0.update(arr.value(i));
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        Ok(ScalarValue::Float64(self.0.evaluate_population()))
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        let [n, mean, m2] = self.0.state();
        Ok(vec![
            ScalarValue::Float64(Some(n)),
            ScalarValue::Float64(Some(mean)),
            ScalarValue::Float64(Some(m2)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        let n_arr = as_f64(&states[0])?;
        let mean_arr = as_f64(&states[1])?;
        let m2_arr = as_f64(&states[2])?;
        for i in 0..n_arr.len() {
            if !n_arr.is_null(i) {
                self.0
                    .merge_state(&[n_arr.value(i), mean_arr.value(i), m2_arr.value(i)]);
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        self.0.size()
    }
}

// ── ts_correlate ─────────────────────────────────────────────────────────

/// `ts_correlate(col_a, col_b)` — Pearson correlation coefficient.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TsCorrelateUdaf {
    signature: Signature,
}

impl TsCorrelateUdaf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![
                    DataType::Float64,
                    DataType::Float64,
                ])],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for TsCorrelateUdaf {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for TsCorrelateUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "ts_correlate"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> DfResult<Box<dyn Accumulator>> {
        Ok(Box::new(CorrelateAccum(
            ts_functions::TsCorrelationAccum::new(),
        )))
    }
}

#[derive(Debug)]
struct CorrelateAccum(ts_functions::TsCorrelationAccum);

impl Accumulator for CorrelateAccum {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let a = as_f64(&values[0])?;
        let b = as_f64(&values[1])?;
        for i in 0..a.len() {
            if !a.is_null(i) && !b.is_null(i) {
                self.0.update(a.value(i), b.value(i));
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        Ok(ScalarValue::Float64(self.0.evaluate()))
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        let s = self.0.state();
        Ok(s.iter().map(|&v| ScalarValue::Float64(Some(v))).collect())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        let arrays: Vec<&Float64Array> = states
            .iter()
            .map(|a| as_f64(a))
            .collect::<DfResult<Vec<_>>>()?;
        for i in 0..arrays[0].len() {
            if !arrays[0].is_null(i) {
                let s: [f64; 6] = std::array::from_fn(|j| arrays[j].value(i));
                self.0.merge_state(&s);
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        self.0.size()
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────

fn as_f64(arr: &ArrayRef) -> DfResult<&Float64Array> {
    arr.as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| DataFusionError::Internal("expected Float64 array".into()))
}
