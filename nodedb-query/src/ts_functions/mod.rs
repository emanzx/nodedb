//! Timeseries SQL function kernels.
//!
//! Pure computation on `&[f64]` / `&[i64]` slices — no DataFusion, no Arrow,
//! no allocator dependency. Shared by Origin, Lite, and WASM.

pub mod correlation;
pub mod delta;
pub mod derivative;
pub mod ema;
pub mod interpolate;
pub mod moving_avg;
pub mod percentile;
pub mod rate;
pub mod stddev;

pub use correlation::{TsCorrelationAccum, ts_correlate};
pub use delta::ts_delta;
pub use derivative::ts_derivative;
pub use ema::ts_ema;
pub use interpolate::{InterpolateMethod, ts_interpolate};
pub use moving_avg::ts_moving_avg;
pub use percentile::{TsPercentileAccum, ts_percentile_exact};
pub use rate::ts_rate;
pub use stddev::{TsStddevAccum, ts_stddev};
