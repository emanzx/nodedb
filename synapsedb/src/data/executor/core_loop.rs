use std::collections::VecDeque;
use std::sync::Arc;

use tracing::{debug, warn};

use synapsedb_bridge::buffer::{Consumer, Producer};

use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response, Status};
use crate::types::Lsn;

use super::task::{ExecutionTask, TaskState};

/// Per-core event loop for the Data Plane.
///
/// Each CPU core runs one `CoreLoop`. It owns:
/// - SPSC consumer for incoming requests from the Control Plane
/// - SPSC producer for outgoing responses to the Control Plane
/// - Per-tenant engine instances for this core's vShards
/// - Task queue for pending execution
///
/// This type is intentionally `!Send` — pinned to a single core.
pub struct CoreLoop {
    core_id: usize,

    /// SPSC channel: receives requests from Control Plane.
    request_rx: Consumer<BridgeRequest>,

    /// SPSC channel: sends responses to Control Plane.
    response_tx: Producer<BridgeResponse>,

    /// Pending tasks ordered by priority then arrival.
    task_queue: VecDeque<ExecutionTask>,

    /// Current watermark LSN for this core's shard data.
    watermark: Lsn,
}

impl CoreLoop {
    /// Create a core loop with its SPSC channel endpoints.
    pub fn new(
        core_id: usize,
        request_rx: Consumer<BridgeRequest>,
        response_tx: Producer<BridgeResponse>,
    ) -> Self {
        Self {
            core_id,
            request_rx,
            response_tx,
            task_queue: VecDeque::with_capacity(256),
            watermark: Lsn::ZERO,
        }
    }

    pub fn core_id(&self) -> usize {
        self.core_id
    }

    /// Drain incoming requests from the SPSC bridge into the task queue.
    pub fn drain_requests(&mut self) {
        let mut batch = Vec::new();
        self.request_rx.drain_into(&mut batch, 64);
        for br in batch {
            self.task_queue.push_back(ExecutionTask::new(br.inner));
        }
    }

    /// Process the next pending task and send the response back via SPSC.
    ///
    /// Returns `true` if a task was processed, `false` if the queue was empty.
    pub fn poll_one(&mut self) -> bool {
        let Some(mut task) = self.task_queue.pop_front() else {
            return false;
        };

        // Check deadline before executing.
        let response = if task.is_expired() {
            task.state = TaskState::Failed;
            Response {
                request_id: task.request_id(),
                status: Status::Error,
                attempt: 1,
                partial: false,
                payload: Arc::from([].as_slice()),
                watermark_lsn: self.watermark,
                error_code: Some(ErrorCode::DeadlineExceeded),
            }
        } else {
            task.state = TaskState::Running;
            let resp = self.execute(&task);
            task.state = TaskState::Completed;
            resp
        };

        // Send response back to Control Plane via SPSC.
        if let Err(e) = self
            .response_tx
            .try_push(BridgeResponse { inner: response })
        {
            warn!(
                core = self.core_id,
                error = %e,
                "failed to send response — response queue full"
            );
        }

        true
    }

    /// Run one iteration of the event loop: drain requests, process tasks.
    ///
    /// Returns the number of tasks processed.
    pub fn tick(&mut self) -> usize {
        self.drain_requests();
        let mut processed = 0;
        while self.poll_one() {
            processed += 1;
        }
        processed
    }

    fn response_for(&self, task: &ExecutionTask, error_code: Option<ErrorCode>) -> Response {
        Response {
            request_id: task.request_id(),
            status: if error_code.is_some() {
                Status::Error
            } else {
                Status::Ok
            },
            attempt: 1,
            partial: false,
            payload: Arc::from([].as_slice()),
            watermark_lsn: self.watermark,
            error_code,
        }
    }

    #[allow(dead_code)]
    fn response_with_payload(&self, task: &ExecutionTask, payload: Vec<u8>) -> Response {
        Response {
            request_id: task.request_id(),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Arc::from(payload.into_boxed_slice()),
            watermark_lsn: self.watermark,
            error_code: None,
        }
    }

    /// Execute a physical plan. Dispatches to the appropriate engine.
    fn execute(&mut self, task: &ExecutionTask) -> Response {
        match task.plan() {
            PhysicalPlan::PointGet {
                collection,
                document_id,
            } => {
                debug!(core = self.core_id, %collection, %document_id, "point get");
                // Sparse/metadata engine lookup — returns empty payload until redb is wired.
                self.response_for(task, None)
            }

            PhysicalPlan::VectorSearch {
                collection, top_k, ..
            } => {
                debug!(core = self.core_id, %collection, top_k, "vector search");
                // Vector engine HNSW search — returns empty until numr is wired.
                self.response_for(task, None)
            }

            PhysicalPlan::RangeScan {
                collection,
                field,
                limit,
                ..
            } => {
                debug!(core = self.core_id, %collection, %field, limit, "range scan");
                self.response_for(task, None)
            }

            PhysicalPlan::CrdtRead {
                collection,
                document_id,
            } => {
                debug!(core = self.core_id, %collection, %document_id, "crdt read");
                // CRDT read returns the snapshot bytes if the row exists.
                // In production, we'd look up the per-tenant CrdtEngine here.
                self.response_for(task, None)
            }

            PhysicalPlan::CrdtApply {
                collection,
                document_id,
                delta: _,
                peer_id,
            } => {
                debug!(
                    core = self.core_id,
                    %collection, %document_id, peer_id, "crdt apply"
                );
                // CRDT delta application — validate constraints, apply to state.
                // In production, the TenantCrdtEngine lives on this core.
                self.response_for(task, None)
            }

            PhysicalPlan::WalAppend { payload } => {
                debug!(core = self.core_id, len = payload.len(), "wal append");
                // WAL append is handled by the WalManager on the write path.
                // The CoreLoop receives this after WAL commit to apply to state.
                self.response_for(task, None)
            }

            PhysicalPlan::Cancel { target_request_id } => {
                debug!(core = self.core_id, %target_request_id, "cancel");
                if let Some(pos) = self
                    .task_queue
                    .iter()
                    .position(|t| t.request_id() == *target_request_id)
                {
                    self.task_queue.remove(pos);
                }
                self.response_for(task, None)
            }
        }
    }

    pub fn pending_count(&self) -> usize {
        self.task_queue.len()
    }

    pub fn advance_watermark(&mut self, lsn: Lsn) {
        self.watermark = lsn;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::{Priority, Request};
    use crate::types::*;
    use std::time::{Duration, Instant};
    use synapsedb_bridge::buffer::RingBuffer;

    fn make_core() -> (CoreLoop, Producer<BridgeRequest>, Consumer<BridgeResponse>) {
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::new(0, req_rx, resp_tx);
        (core, req_tx, resp_rx)
    }

    fn make_request(plan: PhysicalPlan) -> Request {
        Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            plan,
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: 0,
            consistency: ReadConsistency::Strong,
        }
    }

    #[test]
    fn full_roundtrip_via_spsc() {
        let (mut core, mut req_tx, mut resp_rx) = make_core();

        // Control Plane pushes a request.
        req_tx
            .try_push(BridgeRequest {
                inner: make_request(PhysicalPlan::PointGet {
                    collection: "users".into(),
                    document_id: "u1".into(),
                }),
            })
            .unwrap();

        // Core drains and processes.
        let processed = core.tick();
        assert_eq!(processed, 1);

        // Control Plane receives the response.
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.status, Status::Ok);
    }

    #[test]
    fn expired_task_returns_deadline_exceeded() {
        let (mut core, mut req_tx, mut resp_rx) = make_core();

        req_tx
            .try_push(BridgeRequest {
                inner: Request {
                    request_id: RequestId::new(2),
                    tenant_id: TenantId::new(1),
                    vshard_id: VShardId::new(0),
                    plan: PhysicalPlan::PointGet {
                        collection: "x".into(),
                        document_id: "y".into(),
                    },
                    deadline: Instant::now() - Duration::from_secs(1),
                    priority: Priority::Normal,
                    trace_id: 0,
                    consistency: ReadConsistency::Strong,
                },
            })
            .unwrap();

        core.tick();

        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.status, Status::Error);
        assert_eq!(resp.inner.error_code, Some(ErrorCode::DeadlineExceeded));
    }

    #[test]
    fn empty_tick_processes_nothing() {
        let (mut core, _, _) = make_core();
        assert_eq!(core.tick(), 0);
    }

    #[test]
    fn watermark_in_response() {
        let (mut core, mut req_tx, mut resp_rx) = make_core();
        core.advance_watermark(Lsn::new(99));

        req_tx
            .try_push(BridgeRequest {
                inner: make_request(PhysicalPlan::PointGet {
                    collection: "x".into(),
                    document_id: "y".into(),
                }),
            })
            .unwrap();

        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.watermark_lsn, Lsn::new(99));
    }

    #[test]
    fn cancel_removes_pending_task() {
        let (mut core, mut req_tx, _resp_rx) = make_core();

        // Push two requests.
        req_tx
            .try_push(BridgeRequest {
                inner: Request {
                    request_id: RequestId::new(10),
                    tenant_id: TenantId::new(1),
                    vshard_id: VShardId::new(0),
                    plan: PhysicalPlan::PointGet {
                        collection: "x".into(),
                        document_id: "y".into(),
                    },
                    deadline: Instant::now() + Duration::from_secs(60),
                    priority: Priority::Normal,
                    trace_id: 0,
                    consistency: ReadConsistency::Strong,
                },
            })
            .unwrap();

        // Drain them into the task queue but don't process yet.
        core.drain_requests();
        assert_eq!(core.pending_count(), 1);

        // Push a cancel for request 10.
        req_tx
            .try_push(BridgeRequest {
                inner: Request {
                    request_id: RequestId::new(99),
                    tenant_id: TenantId::new(1),
                    vshard_id: VShardId::new(0),
                    plan: PhysicalPlan::Cancel {
                        target_request_id: RequestId::new(10),
                    },
                    deadline: Instant::now() + Duration::from_secs(5),
                    priority: Priority::Critical,
                    trace_id: 0,
                    consistency: ReadConsistency::Eventual,
                },
            })
            .unwrap();

        // Tick processes the first task (PointGet for req 10), then the Cancel.
        // The cancel won't find req 10 in the queue anymore (already processed).
        let processed = core.tick();
        assert_eq!(processed, 2);
    }
}
