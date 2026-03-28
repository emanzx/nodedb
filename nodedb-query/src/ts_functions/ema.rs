//! Exponential Moving Average (EMA).

/// Compute an exponential moving average.
///
/// ```text
/// EMA[0] = values[0]
/// EMA[i] = α · values[i] + (1 − α) · EMA[i−1]
/// ```
///
/// # Panics
/// Panics (debug) if `alpha` is outside `(0.0, 1.0]`.
pub fn ts_ema(values: &[f64], alpha: f64) -> Vec<f64> {
    debug_assert!(alpha > 0.0 && alpha <= 1.0, "alpha must be in (0, 1]");
    let n = values.len();
    if n == 0 {
        return vec![];
    }
    let one_minus_alpha = 1.0 - alpha;
    let mut result = Vec::with_capacity(n);
    result.push(values[0]);

    for i in 1..n {
        let prev = result[i - 1];
        result.push(alpha.mul_add(values[i], one_minus_alpha * prev));
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alpha_1_equals_identity() {
        let vals = [1.0, 5.0, 3.0, 8.0];
        let r = ts_ema(&vals, 1.0);
        for (a, b) in r.iter().zip(vals.iter()) {
            assert!((a - b).abs() < 1e-12);
        }
    }

    #[test]
    fn alpha_half() {
        let r = ts_ema(&[10.0, 20.0, 30.0], 0.5);
        assert!((r[0] - 10.0).abs() < 1e-12);
        assert!((r[1] - 15.0).abs() < 1e-12); // 0.5*20 + 0.5*10
        assert!((r[2] - 22.5).abs() < 1e-12); // 0.5*30 + 0.5*15
    }

    #[test]
    fn small_alpha_smooths_heavily() {
        let r = ts_ema(&[100.0, 0.0, 0.0, 0.0], 0.1);
        assert!((r[0] - 100.0).abs() < 1e-12);
        assert!((r[1] - 90.0).abs() < 1e-12);
        assert!((r[2] - 81.0).abs() < 1e-12);
        assert!((r[3] - 72.9).abs() < 1e-10);
    }

    #[test]
    fn empty() {
        assert!(ts_ema(&[], 0.5).is_empty());
    }
}
