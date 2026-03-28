//! Arrow array extraction helpers for timeseries UDFs.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use datafusion::common::Result as DfResult;
use datafusion::error::DataFusionError;

/// Extract an `ArrayRef` as `Vec<f64>`, handling Float64 and Int64 inputs.
pub fn extract_f64_values(arr: &ArrayRef) -> DfResult<Vec<f64>> {
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return Ok(a.values().iter().copied().collect());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return Ok(a.values().iter().map(|&v| v as f64).collect());
    }
    Err(DataFusionError::Internal(
        "ts_*: expected Float64 or Int64 array".into(),
    ))
}

/// Extract an `ArrayRef` as `Vec<i64>` of nanosecond timestamps.
///
/// Handles `Timestamp{Second,Millisecond,Microsecond,Nanosecond}` and raw `Int64`.
pub fn extract_timestamps_ns(arr: &ArrayRef) -> DfResult<Vec<i64>> {
    if let Some(a) = arr.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return Ok(a.values().iter().copied().collect());
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return Ok(a.values().iter().map(|&v| v * 1_000).collect());
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return Ok(a.values().iter().map(|&v| v * 1_000_000).collect());
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampSecondArray>() {
        return Ok(a.values().iter().map(|&v| v * 1_000_000_000).collect());
    }
    // Fall back to raw Int64 (assumed nanoseconds).
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return Ok(a.values().iter().copied().collect());
    }
    Err(DataFusionError::Internal(
        "ts_*: expected Timestamp or Int64 array for time column".into(),
    ))
}

/// Convert `Vec<Option<f64>>` to a nullable Float64 Arrow array.
pub fn option_f64_to_array(values: Vec<Option<f64>>) -> ArrayRef {
    Arc::new(Float64Array::from(values))
}

/// Convert `Vec<f64>` to a non-nullable Float64 Arrow array.
pub fn f64_to_array(values: Vec<f64>) -> ArrayRef {
    Arc::new(Float64Array::from(values))
}
