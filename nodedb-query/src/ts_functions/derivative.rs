//! Pure mathematical derivative (rate of change per second).
//!
//! Unlike `ts_rate`, this does NOT handle counter resets.
//! Suitable for gauge-type metrics (temperature, queue depth, etc.).

/// Compute the per-second rate of change between consecutive samples.
///
/// `dv / dt` where `dt` is in seconds. Returns `None` for the first
/// sample and for zero-duration intervals.
pub fn ts_derivative(values: &[f64], timestamps_ns: &[i64]) -> Vec<Option<f64>> {
    debug_assert_eq!(values.len(), timestamps_ns.len());
    let n = values.len();
    if n == 0 {
        return vec![];
    }

    let mut result = Vec::with_capacity(n);
    result.push(None);

    for i in 1..n {
        let dt_ns = timestamps_ns[i] - timestamps_ns[i - 1];
        if dt_ns <= 0 {
            result.push(None);
            continue;
        }
        let dt_secs = dt_ns as f64 / 1_000_000_000.0;
        result.push(Some((values[i] - values[i - 1]) / dt_secs));
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positive_slope() {
        let vals = [0.0, 10.0, 30.0];
        let ts = [0, 1_000_000_000, 2_000_000_000];
        let r = ts_derivative(&vals, &ts);
        assert!(r[0].is_none());
        assert!((r[1].unwrap() - 10.0).abs() < 1e-9);
        assert!((r[2].unwrap() - 20.0).abs() < 1e-9);
    }

    #[test]
    fn negative_slope() {
        // Unlike ts_rate, derivative preserves negative values.
        let vals = [100.0, 50.0, 20.0];
        let ts = [0, 1_000_000_000, 2_000_000_000];
        let r = ts_derivative(&vals, &ts);
        assert!((r[1].unwrap() - (-50.0)).abs() < 1e-9);
        assert!((r[2].unwrap() - (-30.0)).abs() < 1e-9);
    }

    #[test]
    fn sub_second() {
        let vals = [0.0, 5.0];
        let ts = [0, 500_000_000]; // 0.5s
        let r = ts_derivative(&vals, &ts);
        assert!((r[1].unwrap() - 10.0).abs() < 1e-9);
    }

    #[test]
    fn duplicate_timestamp() {
        let r = ts_derivative(&[0.0, 10.0], &[100, 100]);
        assert!(r[1].is_none());
    }

    #[test]
    fn empty() {
        assert!(ts_derivative(&[], &[]).is_empty());
    }
}
