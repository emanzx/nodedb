//! Online Pearson correlation coefficient (Welford-style).

/// Online accumulator for Pearson correlation.
///
/// Uses the Welford/Knuth single-pass algorithm for numerical stability.
/// Computes `cov(X,Y) / (σ_X · σ_Y)` incrementally.
#[derive(Debug)]
pub struct TsCorrelationAccum {
    n: u64,
    mean_x: f64,
    mean_y: f64,
    m2_x: f64,
    m2_y: f64,
    co_moment: f64,
}

impl TsCorrelationAccum {
    pub fn new() -> Self {
        Self {
            n: 0,
            mean_x: 0.0,
            mean_y: 0.0,
            m2_x: 0.0,
            m2_y: 0.0,
            co_moment: 0.0,
        }
    }

    /// Add a single (x, y) observation.
    pub fn update(&mut self, x: f64, y: f64) {
        self.n += 1;
        let n = self.n as f64;
        let dx = x - self.mean_x;
        let dy = y - self.mean_y;
        self.mean_x += dx / n;
        self.mean_y += dy / n;
        // Use OLD dx with NEW mean_y for co-moment (Welford identity).
        let dy2 = y - self.mean_y;
        self.co_moment += dx * dy2;
        self.m2_x += dx * (x - self.mean_x);
        self.m2_y += dy * dy2;
    }

    /// Returns `None` if n < 2 or either variable has zero variance.
    pub fn evaluate(&self) -> Option<f64> {
        if self.n < 2 {
            return None;
        }
        let denom = self.m2_x * self.m2_y;
        if denom <= 0.0 {
            return None;
        }
        Some(self.co_moment / denom.sqrt())
    }

    /// Serialise state for partial-aggregate merging.
    pub fn state(&self) -> [f64; 6] {
        [
            self.n as f64,
            self.mean_x,
            self.mean_y,
            self.m2_x,
            self.m2_y,
            self.co_moment,
        ]
    }

    /// Merge another accumulator using Chan's parallel algorithm.
    pub fn merge(&mut self, other: &Self) {
        if other.n == 0 {
            return;
        }
        if self.n == 0 {
            *self = Self {
                n: other.n,
                mean_x: other.mean_x,
                mean_y: other.mean_y,
                m2_x: other.m2_x,
                m2_y: other.m2_y,
                co_moment: other.co_moment,
            };
            return;
        }
        let n_a = self.n as f64;
        let n_b = other.n as f64;
        let n_ab = n_a + n_b;

        let dx = other.mean_x - self.mean_x;
        let dy = other.mean_y - self.mean_y;

        self.co_moment += other.co_moment + dx * dy * n_a * n_b / n_ab;
        self.m2_x += other.m2_x + dx * dx * n_a * n_b / n_ab;
        self.m2_y += other.m2_y + dy * dy * n_a * n_b / n_ab;
        self.mean_x = (n_a * self.mean_x + n_b * other.mean_x) / n_ab;
        self.mean_y = (n_a * self.mean_y + n_b * other.mean_y) / n_ab;
        self.n += other.n;
    }

    /// Merge from serialised state array.
    pub fn merge_state(&mut self, state: &[f64; 6]) {
        let other = Self {
            n: state[0] as u64,
            mean_x: state[1],
            mean_y: state[2],
            m2_x: state[3],
            m2_y: state[4],
            co_moment: state[5],
        };
        self.merge(&other);
    }

    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl Default for TsCorrelationAccum {
    fn default() -> Self {
        Self::new()
    }
}

/// Batch computation: Pearson correlation between two equal-length slices.
///
/// Skips pairs where either value is NaN.
pub fn ts_correlate(a: &[f64], b: &[f64]) -> Option<f64> {
    let mut accum = TsCorrelationAccum::new();
    for (&x, &y) in a.iter().zip(b.iter()) {
        if !x.is_nan() && !y.is_nan() {
            accum.update(x, y);
        }
    }
    accum.evaluate()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn perfect_positive() {
        let r = ts_correlate(&[1.0, 2.0, 3.0, 4.0, 5.0], &[2.0, 4.0, 6.0, 8.0, 10.0]);
        assert!((r.unwrap() - 1.0).abs() < 1e-10);
    }

    #[test]
    fn perfect_negative() {
        let r = ts_correlate(&[1.0, 2.0, 3.0, 4.0, 5.0], &[10.0, 8.0, 6.0, 4.0, 2.0]);
        assert!((r.unwrap() - (-1.0)).abs() < 1e-10);
    }

    #[test]
    fn zero_variance() {
        let r = ts_correlate(&[5.0, 5.0, 5.0], &[1.0, 2.0, 3.0]);
        assert!(r.is_none());
    }

    #[test]
    fn too_few_samples() {
        assert!(ts_correlate(&[1.0], &[2.0]).is_none());
        assert!(ts_correlate(&[], &[]).is_none());
    }

    #[test]
    fn nan_skipped() {
        let r = ts_correlate(&[1.0, f64::NAN, 3.0, 4.0, 5.0], &[2.0, 4.0, 6.0, 8.0, 10.0]);
        // Only pairs (1,2), (3,6), (4,8), (5,10) used.
        assert!(r.unwrap() > 0.99);
    }

    #[test]
    fn merge_preserves_result() {
        let mut a = TsCorrelationAccum::new();
        for (&x, &y) in [1.0, 2.0, 3.0].iter().zip([2.0, 4.0, 6.0].iter()) {
            a.update(x, y);
        }
        let mut b = TsCorrelationAccum::new();
        for (&x, &y) in [4.0, 5.0].iter().zip([8.0, 10.0].iter()) {
            b.update(x, y);
        }
        a.merge(&b);
        assert!((a.evaluate().unwrap() - 1.0).abs() < 1e-10);
    }
}
