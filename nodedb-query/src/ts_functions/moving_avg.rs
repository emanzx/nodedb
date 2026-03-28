//! Simple Moving Average (SMA) with Kahan-compensated summation.

/// Compute a simple moving average over a sliding window.
///
/// Uses Kahan summation to minimise floating-point drift across
/// the running window. Returns `None` for positions where fewer
/// than `window` samples are available.
pub fn ts_moving_avg(values: &[f64], window: usize) -> Vec<Option<f64>> {
    let n = values.len();
    if n == 0 || window == 0 {
        return vec![None; n];
    }

    let mut result = Vec::with_capacity(n);
    let mut sum = 0.0_f64;
    let mut comp = 0.0_f64;

    for i in 0..n {
        // Kahan add incoming value.
        kahan_add(&mut sum, &mut comp, values[i]);

        if i + 1 < window {
            result.push(None);
        } else {
            if i >= window {
                // Kahan subtract the value leaving the window.
                kahan_add(&mut sum, &mut comp, -values[i - window]);
            }
            result.push(Some(sum / window as f64));
        }
    }
    result
}

/// Kahan compensated addition: adds `val` to `sum` with error tracked in `comp`.
#[inline]
fn kahan_add(sum: &mut f64, comp: &mut f64, val: f64) {
    let y = val - *comp;
    let t = *sum + y;
    *comp = (t - *sum) - y;
    *sum = t;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_3() {
        let r = ts_moving_avg(&[1.0, 2.0, 3.0, 4.0, 5.0], 3);
        assert!(r[0].is_none());
        assert!(r[1].is_none());
        assert!((r[2].unwrap() - 2.0).abs() < 1e-12); // (1+2+3)/3
        assert!((r[3].unwrap() - 3.0).abs() < 1e-12); // (2+3+4)/3
        assert!((r[4].unwrap() - 4.0).abs() < 1e-12); // (3+4+5)/3
    }

    #[test]
    fn window_1() {
        let r = ts_moving_avg(&[10.0, 20.0, 30.0], 1);
        assert!((r[0].unwrap() - 10.0).abs() < 1e-12);
        assert!((r[1].unwrap() - 20.0).abs() < 1e-12);
    }

    #[test]
    fn window_exceeds_length() {
        let r = ts_moving_avg(&[1.0, 2.0], 5);
        assert!(r.iter().all(|v| v.is_none()));
    }

    #[test]
    fn kahan_precision() {
        // Many small values where naive summation would drift.
        let vals: Vec<f64> = (0..10_000).map(|_| 0.1).collect();
        let r = ts_moving_avg(&vals, 10_000);
        let avg = r.last().unwrap().unwrap();
        assert!((avg - 0.1).abs() < 1e-10);
    }

    #[test]
    fn empty_and_zero_window() {
        assert!(ts_moving_avg(&[], 3).is_empty());
        assert!(ts_moving_avg(&[1.0], 0).iter().all(|v| v.is_none()));
    }
}
