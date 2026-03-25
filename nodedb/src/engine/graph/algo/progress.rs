//! Progress reporting for long-running graph algorithms.
//!
//! Iterative algorithms (PageRank, Label Propagation, Louvain) emit
//! structured progress events via tracing spans. This enables:
//! - `GRAPH ALGO ... EXPLAIN` to show iteration stats
//! - Operational dashboards to track algorithm execution
//! - Timeout detection for algorithms that fail to converge

use std::time::Instant;

use super::params::GraphAlgorithm;

/// Progress snapshot for a single algorithm iteration.
#[derive(Debug, Clone)]
pub struct AlgoProgress {
    /// Which algorithm is running.
    pub algorithm: GraphAlgorithm,
    /// Current iteration number (1-indexed).
    pub iteration: usize,
    /// Maximum iterations configured.
    pub max_iterations: usize,
    /// Convergence metric (e.g., L1 norm of rank delta for PageRank).
    /// `None` for non-iterative algorithms.
    pub convergence_delta: Option<f64>,
    /// Convergence target (tolerance).
    pub tolerance: Option<f64>,
    /// Elapsed time since algorithm start.
    pub elapsed_ms: u64,
    /// Number of nodes processed in this iteration.
    pub nodes_processed: usize,
    /// Whether the algorithm has converged (delta < tolerance).
    pub converged: bool,
}

/// Reports progress for a running graph algorithm.
///
/// Created at algorithm start, updated per iteration. Emits structured
/// tracing events that flow through the TelemetryRing to the Control Plane.
///
/// Usage:
/// ```ignore
/// let mut reporter = ProgressReporter::new(GraphAlgorithm::PageRank, 20, Some(1e-7), node_count);
/// for iteration in 1..=max_iter {
///     // ... compute iteration ...
///     reporter.report_iteration(iteration, Some(delta));
///     if delta < tolerance { break; }
/// }
/// reporter.finish();
/// ```
pub struct ProgressReporter {
    algorithm: GraphAlgorithm,
    max_iterations: usize,
    tolerance: Option<f64>,
    node_count: usize,
    start: Instant,
    last_delta: Option<f64>,
}

impl ProgressReporter {
    /// Create a new progress reporter.
    pub fn new(
        algorithm: GraphAlgorithm,
        max_iterations: usize,
        tolerance: Option<f64>,
        node_count: usize,
    ) -> Self {
        tracing::info!(
            algorithm = algorithm.name(),
            max_iterations,
            node_count,
            "graph algorithm started"
        );
        Self {
            algorithm,
            max_iterations,
            tolerance,
            node_count,
            start: Instant::now(),
            last_delta: None,
        }
    }

    /// Report progress after completing an iteration.
    ///
    /// `convergence_delta`: the convergence metric for this iteration
    /// (e.g., L1 norm of rank change for PageRank). Pass `None` for
    /// non-convergence-tracked algorithms.
    pub fn report_iteration(&mut self, iteration: usize, convergence_delta: Option<f64>) {
        self.last_delta = convergence_delta;
        let elapsed_ms = self.start.elapsed().as_millis() as u64;
        let converged = match (convergence_delta, self.tolerance) {
            (Some(delta), Some(tol)) => delta < tol,
            _ => false,
        };

        tracing::debug!(
            algorithm = self.algorithm.name(),
            iteration,
            max_iterations = self.max_iterations,
            convergence_delta = convergence_delta.unwrap_or(0.0),
            elapsed_ms,
            converged,
            "graph algorithm iteration"
        );
    }

    /// Build a progress snapshot for the current state.
    pub fn snapshot(&self, iteration: usize) -> AlgoProgress {
        let converged = match (self.last_delta, self.tolerance) {
            (Some(delta), Some(tol)) => delta < tol,
            _ => false,
        };
        AlgoProgress {
            algorithm: self.algorithm,
            iteration,
            max_iterations: self.max_iterations,
            convergence_delta: self.last_delta,
            tolerance: self.tolerance,
            elapsed_ms: self.start.elapsed().as_millis() as u64,
            nodes_processed: self.node_count,
            converged,
        }
    }

    /// Report algorithm completion.
    pub fn finish(&self) {
        let elapsed_ms = self.start.elapsed().as_millis() as u64;
        let converged = match (self.last_delta, self.tolerance) {
            (Some(delta), Some(tol)) => delta < tol,
            _ => true, // non-iterative algorithms are always "converged"
        };
        tracing::info!(
            algorithm = self.algorithm.name(),
            elapsed_ms,
            converged,
            final_delta = self.last_delta.unwrap_or(0.0),
            node_count = self.node_count,
            "graph algorithm completed"
        );
    }

    /// Total elapsed time since start.
    pub fn elapsed_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_reporter_lifecycle() {
        let mut reporter = ProgressReporter::new(GraphAlgorithm::PageRank, 20, Some(1e-7), 1000);

        reporter.report_iteration(1, Some(0.5));
        reporter.report_iteration(2, Some(0.01));
        reporter.report_iteration(3, Some(1e-8));

        let snap = reporter.snapshot(3);
        assert!(snap.converged);
        assert_eq!(snap.algorithm, GraphAlgorithm::PageRank);
        assert_eq!(snap.max_iterations, 20);
        assert_eq!(snap.nodes_processed, 1000);

        reporter.finish();
    }

    #[test]
    fn progress_non_iterative() {
        let reporter = ProgressReporter::new(GraphAlgorithm::Wcc, 1, None, 500);

        let snap = reporter.snapshot(1);
        assert!(!snap.converged); // no delta tracked
        assert!(snap.convergence_delta.is_none());

        reporter.finish();
    }

    #[test]
    fn progress_not_converged() {
        let mut reporter = ProgressReporter::new(GraphAlgorithm::LabelPropagation, 10, None, 100);

        reporter.report_iteration(1, Some(50.0));
        let snap = reporter.snapshot(1);
        assert!(!snap.converged); // no tolerance set
    }

    #[test]
    fn elapsed_increases() {
        let reporter = ProgressReporter::new(GraphAlgorithm::Sssp, 1, None, 10);
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(reporter.elapsed_ms() >= 4);
    }
}
