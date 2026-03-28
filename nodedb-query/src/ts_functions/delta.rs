//! Consecutive-sample difference.

/// Compute the difference between each sample and its predecessor.
///
/// Returns `None` for the first sample.
pub fn ts_delta(values: &[f64]) -> Vec<Option<f64>> {
    let n = values.len();
    if n == 0 {
        return vec![];
    }
    let mut result = Vec::with_capacity(n);
    result.push(None);
    for i in 1..n {
        result.push(Some(values[i] - values[i - 1]));
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() {
        let r = ts_delta(&[1.0, 3.0, 7.0, 4.0]);
        assert!(r[0].is_none());
        assert!((r[1].unwrap() - 2.0).abs() < 1e-12);
        assert!((r[2].unwrap() - 4.0).abs() < 1e-12);
        assert!((r[3].unwrap() - (-3.0)).abs() < 1e-12);
    }

    #[test]
    fn single() {
        let r = ts_delta(&[42.0]);
        assert_eq!(r.len(), 1);
        assert!(r[0].is_none());
    }

    #[test]
    fn empty() {
        assert!(ts_delta(&[]).is_empty());
    }
}
