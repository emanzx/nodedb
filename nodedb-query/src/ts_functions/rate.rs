//! Counter-aware per-second rate of increase.
//!
//! Mirrors Prometheus `rate()` semantics: detects monotonic counter resets
//! and extrapolates the increase across the reset boundary.

/// Compute the per-second rate of increase between consecutive samples.
///
/// Counter-reset aware: if `values[i] < values[i-1]`, assumes the counter
/// wrapped and treats `values[i]` as the total increase since the reset.
///
/// Returns `None` for the first sample (no predecessor) and for
/// zero-duration intervals (duplicate timestamps).
///
/// # Arguments
/// * `values` — monotonic counter values (pre-sorted by time)
/// * `timestamps_ns` — epoch nanoseconds, same length as `values`
pub fn ts_rate(values: &[f64], timestamps_ns: &[i64]) -> Vec<Option<f64>> {
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

        // Counter reset detection: value decreased → counter wrapped.
        let dv = if values[i] >= values[i - 1] {
            values[i] - values[i - 1]
        } else {
            // After reset the counter starts from zero, so the current
            // value *is* the total increase since reset.
            values[i]
        };

        result.push(Some(dv / dt_secs));
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotonic_increase() {
        let vals = [0.0, 10.0, 30.0, 60.0];
        let ts = [0, 1_000_000_000, 2_000_000_000, 3_000_000_000]; // 1s intervals
        let r = ts_rate(&vals, &ts);
        assert_eq!(r.len(), 4);
        assert!(r[0].is_none());
        assert!((r[1].unwrap() - 10.0).abs() < 1e-9);
        assert!((r[2].unwrap() - 20.0).abs() < 1e-9);
        assert!((r[3].unwrap() - 30.0).abs() < 1e-9);
    }

    #[test]
    fn counter_reset() {
        // Counter goes 100 → 5 (reset) → 15
        let vals = [100.0, 5.0, 15.0];
        let ts = [0, 1_000_000_000, 2_000_000_000];
        let r = ts_rate(&vals, &ts);
        assert!((r[1].unwrap() - 5.0).abs() < 1e-9); // 5 increase since reset
        assert!((r[2].unwrap() - 10.0).abs() < 1e-9); // normal diff
    }

    #[test]
    fn duplicate_timestamp() {
        let vals = [0.0, 10.0];
        let ts = [1_000_000_000, 1_000_000_000];
        let r = ts_rate(&vals, &ts);
        assert!(r[1].is_none());
    }

    #[test]
    fn empty() {
        assert!(ts_rate(&[], &[]).is_empty());
    }

    #[test]
    fn sub_second_interval() {
        let vals = [0.0, 5.0];
        let ts = [0, 500_000_000]; // 0.5s
        let r = ts_rate(&vals, &ts);
        assert!((r[1].unwrap() - 10.0).abs() < 1e-9); // 5 / 0.5 = 10/s
    }
}
