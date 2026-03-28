//! Online standard deviation (Welford's algorithm).

/// Online standard deviation accumulator.
///
/// Single-pass, numerically stable Welford algorithm.
#[derive(Debug)]
pub struct TsStddevAccum {
    n: u64,
    mean: f64,
    m2: f64,
}

impl TsStddevAccum {
    pub fn new() -> Self {
        Self {
            n: 0,
            mean: 0.0,
            m2: 0.0,
        }
    }

    pub fn update(&mut self, value: f64) {
        self.n += 1;
        let delta = value - self.mean;
        self.mean += delta / self.n as f64;
        let delta2 = value - self.mean;
        self.m2 += delta * delta2;
    }

    pub fn update_batch(&mut self, values: &[f64]) {
        for &v in values {
            if !v.is_nan() {
                self.update(v);
            }
        }
    }

    /// Population standard deviation (`σ`). Returns `None` if n < 2.
    pub fn evaluate_population(&self) -> Option<f64> {
        if self.n < 2 {
            return None;
        }
        Some((self.m2 / self.n as f64).sqrt())
    }

    /// Sample standard deviation (`s`). Returns `None` if n < 2.
    pub fn evaluate_sample(&self) -> Option<f64> {
        if self.n < 2 {
            return None;
        }
        Some((self.m2 / (self.n - 1) as f64).sqrt())
    }

    /// Serialise state for partial-aggregate merging.
    pub fn state(&self) -> [f64; 3] {
        [self.n as f64, self.mean, self.m2]
    }

    /// Merge another accumulator using Chan's parallel algorithm.
    pub fn merge(&mut self, other: &Self) {
        if other.n == 0 {
            return;
        }
        if self.n == 0 {
            self.n = other.n;
            self.mean = other.mean;
            self.m2 = other.m2;
            return;
        }
        let n_a = self.n as f64;
        let n_b = other.n as f64;
        let n_ab = n_a + n_b;
        let delta = other.mean - self.mean;

        self.m2 += other.m2 + delta * delta * n_a * n_b / n_ab;
        self.mean = (n_a * self.mean + n_b * other.mean) / n_ab;
        self.n += other.n;
    }

    /// Merge from serialised state.
    pub fn merge_state(&mut self, state: &[f64; 3]) {
        let other = Self {
            n: state[0] as u64,
            mean: state[1],
            m2: state[2],
        };
        self.merge(&other);
    }

    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl Default for TsStddevAccum {
    fn default() -> Self {
        Self::new()
    }
}

/// Batch population standard deviation. Skips NaN values.
pub fn ts_stddev(values: &[f64]) -> Option<f64> {
    let mut accum = TsStddevAccum::new();
    accum.update_batch(values);
    accum.evaluate_population()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_values() {
        // population stddev of [2, 4, 4, 4, 5, 5, 7, 9] = 2.0
        let vals = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        assert!((ts_stddev(&vals).unwrap() - 2.0).abs() < 1e-10);
    }

    #[test]
    fn constant_values() {
        assert!((ts_stddev(&[5.0, 5.0, 5.0, 5.0]).unwrap()).abs() < 1e-12);
    }

    #[test]
    fn sample_vs_population() {
        let mut acc = TsStddevAccum::new();
        acc.update_batch(&[2.0, 4.0]);
        let pop = acc.evaluate_population().unwrap();
        let samp = acc.evaluate_sample().unwrap();
        assert!(samp > pop); // sample stddev > population stddev for small n
    }

    #[test]
    fn too_few() {
        assert!(ts_stddev(&[1.0]).is_none());
        assert!(ts_stddev(&[]).is_none());
    }

    #[test]
    fn nan_skipped() {
        let r = ts_stddev(&[2.0, f64::NAN, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]);
        assert!(r.is_some());
    }

    #[test]
    fn merge_preserves_result() {
        let mut a = TsStddevAccum::new();
        a.update_batch(&[2.0, 4.0, 4.0, 4.0]);
        let mut b = TsStddevAccum::new();
        b.update_batch(&[5.0, 5.0, 7.0, 9.0]);
        a.merge(&b);
        assert!((a.evaluate_population().unwrap() - 2.0).abs() < 1e-10);
    }
}
