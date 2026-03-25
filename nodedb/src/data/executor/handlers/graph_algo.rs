//! Graph algorithm dispatch handler.
//!
//! Routes `PhysicalPlan::GraphAlgo` to the appropriate algorithm
//! implementation in `engine::graph::algo::*`. Each algorithm runs
//! on the in-memory CSR index and returns JSON-serialized results.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::graph::algo::params::{AlgoParams, GraphAlgorithm};

impl CoreLoop {
    pub(in crate::data::executor) fn execute_graph_algo(
        &self,
        task: &ExecutionTask,
        algorithm: &GraphAlgorithm,
        params: &AlgoParams,
    ) -> Response {
        debug!(
            core = self.core_id,
            algorithm = algorithm.name(),
            collection = %params.collection,
            "graph algorithm dispatch"
        );

        // Validate source_node for SSSP.
        if *algorithm == GraphAlgorithm::Sssp && params.source_node.is_none() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "SSSP requires FROM '<source_node>'".into(),
                },
            );
        }

        // Dispatch to algorithm implementation.
        // Each algorithm operates on `self.csr` and returns JSON bytes.
        // Individual algorithms are wired here as they are implemented.
        warn!(
            algorithm = algorithm.name(),
            "graph algorithm not yet implemented"
        );
        let result: Result<Vec<u8>, crate::Error> = Err(crate::Error::BadRequest {
            detail: format!("graph algorithm '{}' not yet implemented", algorithm.name()),
        });

        match result {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }
}
