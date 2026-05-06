// SPDX-License-Identifier: BUSL-1.1

//! `Scheduler` struct definition, constructor, and main run loop.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use tracing::info;

use nodedb_cluster::MultiRaft;
use nodedb_cluster::calvin::types::SequencedTxn;
use nodedb_cluster::calvin::{SEQUENCER_GROUP_ID, SequencerEntry};

use super::super::barrier::{PendingDependentBarrier, ReadResultEvent};
use super::super::config::SchedulerConfig;
use super::super::types::{BlockedTxn, PendingTxn};
use crate::bridge::envelope::Response;
use crate::control::cluster::calvin::scheduler::lock_manager::{LockManager, TxnId};
use crate::control::cluster::calvin::scheduler::metrics::SchedulerMetrics;
#[allow(unused_imports)]
use crate::control::cluster::calvin::scheduler::recovery::read_last_applied_epoch;
use crate::control::shutdown::ShutdownReceiver;
use crate::control::state::SharedState;
use crate::types::RequestId;

/// Outcome of an executor response bridge task.
///
/// `None` means the executor response channel was closed before a response
/// arrived (infra error).
pub(in crate::control::cluster::calvin::scheduler::driver::core) type CompletionItem =
    (TxnId, RequestId, Option<Response>);

/// The Calvin scheduler for one vshard.
///
/// Owns the in-memory lock table and orchestrates lock acquisition, dispatch,
/// and response handling for both static-set and dependent-read transactions.
///
/// `Send` — runs as a Tokio task on the Control Plane.
pub struct Scheduler {
    /// Vshard this scheduler is responsible for.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) vshard_id: u32,
    /// Incoming sequenced transactions from the sequencer fan-out.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) receiver:
        mpsc::Receiver<SequencedTxn>,
    /// Shared control-plane state used for dispatch, response tracking, WAL,
    /// and request-id allocation.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) shared: Arc<SharedState>,
    /// Handle to MultiRaft so completion acknowledgements can be proposed to
    /// the sequencer group.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) multi_raft:
        Arc<Mutex<MultiRaft>>,
    /// Deterministic lock manager for this vshard.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) lock_manager: LockManager,
    /// In-flight static/active transactions awaiting executor response.
    /// `BTreeMap` ensures deterministic iteration order.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) pending:
        BTreeMap<TxnId, PendingTxn>,
    /// Blocked transactions awaiting lock release.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) blocked:
        BTreeMap<TxnId, BlockedTxn>,
    /// Dependent-read barriers awaiting passive read results.
    /// `BTreeMap` for determinism.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) dependent_barrier:
        BTreeMap<TxnId, PendingDependentBarrier>,
    /// Channel receiving `CalvinReadResult` Raft apply events from the
    /// per-vshard data Raft apply loop. Bounded.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) read_result_rx:
        mpsc::Receiver<ReadResultEvent>,
    /// Highest epoch applied so far.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) last_applied_epoch: u64,
    /// Rebuild target epoch (from initial recovery scan).
    pub(in crate::control::cluster::calvin::scheduler::driver::core) rebuild_target_epoch: u64,
    /// Scheduler configuration.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) config: SchedulerConfig,
    /// Metrics.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) metrics: Arc<SchedulerMetrics>,
    /// Fan-in receiver for executor responses.
    ///
    /// Each dispatched transaction spawns a lightweight bridge task that
    /// awaits the per-request `mpsc::Receiver<Response>` and forwards the
    /// result here as a [`CompletionItem`]. The scheduler's `select!` loop
    /// includes this channel as a first-class arm so it wakes the moment
    /// any executor response is ready — no polling, no sleep.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) completion_rx:
        mpsc::Receiver<CompletionItem>,
    /// Sender half of the completion fan-in channel, cloned per dispatch.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) completion_tx:
        mpsc::Sender<CompletionItem>,
}

impl Scheduler {
    /// Construct a scheduler.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        vshard_id: u32,
        receiver: mpsc::Receiver<SequencedTxn>,
        shared: Arc<SharedState>,
        multi_raft: Arc<Mutex<MultiRaft>>,
        last_applied_epoch: u64,
        rebuild_target_epoch: u64,
        config: SchedulerConfig,
        metrics: Arc<SchedulerMetrics>,
        read_result_rx: mpsc::Receiver<ReadResultEvent>,
    ) -> Self {
        // Capacity: at most one completion per inflight txn. Use the incoming
        // channel capacity as a proxy for the max concurrent pending count.
        let completion_cap = config.channel_capacity;
        let (completion_tx, completion_rx) = mpsc::channel(completion_cap);

        Self {
            vshard_id,
            receiver,
            shared,
            multi_raft,
            lock_manager: LockManager::new(),
            pending: BTreeMap::new(),
            blocked: BTreeMap::new(),
            dependent_barrier: BTreeMap::new(),
            read_result_rx,
            last_applied_epoch,
            rebuild_target_epoch,
            config,
            metrics,
            completion_rx,
            completion_tx,
        }
    }

    /// Whether the scheduler has caught up to the rebuild target epoch.
    pub fn is_caught_up(&self) -> bool {
        self.last_applied_epoch >= self.rebuild_target_epoch
    }

    /// Spawn a bridge task that awaits a single executor response and forwards
    /// it to the scheduler's fan-in completion channel.
    ///
    /// The bridge task is cancel-safe: it holds only a cloned sender and the
    /// per-request receiver. Dropping the scheduler's `completion_rx` causes
    /// the bridge's `send` to fail silently, which is fine on shutdown.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn spawn_response_bridge(
        &self,
        txn_id: TxnId,
        request_id: RequestId,
        mut response_rx: mpsc::Receiver<Response>,
    ) {
        let tx = self.completion_tx.clone();
        tokio::spawn(async move {
            let result = response_rx.recv().await;
            // Ignore send error: scheduler has shut down.
            let _ = tx.send((txn_id, request_id, result)).await;
        });
    }

    /// Run the scheduler event loop until shutdown is signaled.
    pub async fn run(mut self, mut shutdown: ShutdownReceiver) {
        info!(
            vshard_id = self.vshard_id,
            last_applied_epoch = self.last_applied_epoch,
            rebuild_target_epoch = self.rebuild_target_epoch,
            "calvin scheduler starting"
        );

        loop {
            self.check_dependent_barrier_timeouts();

            tokio::select! {
                biased;

                _ = shutdown.wait_cancelled() => {
                    info!(vshard_id = self.vshard_id, "calvin scheduler shutting down");
                    break;
                }

                maybe_completion = self.completion_rx.recv() => {
                    if let Some((txn_id, request_id, resp_opt)) = maybe_completion {
                        self.handle_completion(txn_id, request_id, resp_opt);
                    }
                }

                maybe_event = self.read_result_rx.recv() => {
                    if let Some(event) = maybe_event {
                        self.handle_read_result(event);
                    }
                }

                maybe_txn = self.receiver.recv() => {
                    match maybe_txn {
                        Some(txn) => self.process_new_txn(txn),
                        None => {
                            info!(
                                vshard_id = self.vshard_id,
                                "calvin scheduler: receiver channel closed; exiting"
                            );
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Process a completed executor response (or disconnected channel).
    ///
    /// Called from the `completion_rx` arm of the main `select!` loop.
    fn handle_completion(
        &mut self,
        txn_id: TxnId,
        request_id: RequestId,
        resp_opt: Option<Response>,
    ) {
        let response = match resp_opt {
            Some(r) => r,
            None => {
                // Bridge task observed a closed channel before any response.
                tracing::warn!(
                    vshard_id = self.vshard_id,
                    request_id = request_id.as_u64(),
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    "calvin: executor response channel disconnected"
                );
                self.metrics.record_executor_error();
                self.metrics.record_infra_abort(
                    crate::control::cluster::calvin::scheduler::metrics::infra_abort_reason::IO_ERROR,
                );
                self.metrics.record_completed();
                self.on_txn_complete(txn_id);
                return;
            }
        };

        let elapsed_ms = self
            .pending
            .get(&txn_id)
            .map(|p| p.dispatch_time.elapsed().as_millis() as u64)
            .unwrap_or(0);
        self.metrics.record_executor_txn_duration_ms(elapsed_ms);

        // OLLP mismatch: the active executor detected predicate drift and returned
        // OllpRetryRequired without writing. Retry via the orchestrator's backoff
        // and circuit-breaker path if the pending txn carries predicate class info.
        if response.status == crate::bridge::envelope::Status::Error
            && response.error_code == Some(crate::bridge::envelope::ErrorCode::OllpRetryRequired)
        {
            self.schedule_ollp_retry(txn_id);
            return;
        }

        if response.status == crate::bridge::envelope::Status::Ok {
            if let Err(e) = self.shared.wal.append_calvin_applied(
                crate::types::VShardId::new(self.vshard_id),
                txn_id.epoch,
                txn_id.position,
            ) {
                tracing::error!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    error = %e,
                    "calvin: failed to write CalvinApplied WAL record"
                );
            }
            let ack = SequencerEntry::CompletionAck {
                epoch: txn_id.epoch,
                position: txn_id.position,
                vshard_id: self.vshard_id,
            };
            match zerompk::to_msgpack_vec(&ack) {
                Ok(bytes) => {
                    if let Err(e) = self
                        .multi_raft
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .propose_to_group(SEQUENCER_GROUP_ID, bytes)
                    {
                        tracing::warn!(
                            vshard_id = self.vshard_id,
                            epoch = txn_id.epoch,
                            position = txn_id.position,
                            error = %e,
                            "calvin: failed to propose completion ack"
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        vshard_id = self.vshard_id,
                        epoch = txn_id.epoch,
                        position = txn_id.position,
                        error = %e,
                        "calvin: failed to encode completion ack"
                    );
                }
            }
        } else {
            tracing::warn!(
                vshard_id = self.vshard_id,
                epoch = txn_id.epoch,
                position = txn_id.position,
                "calvin: executor response was not Ok; locks NOT released (shard degraded)"
            );
            self.metrics.record_executor_error();
            self.metrics.record_infra_abort(
                crate::control::cluster::calvin::scheduler::metrics::infra_abort_reason::IO_ERROR,
            );
        }

        self.metrics.record_completed();
        self.on_txn_complete(txn_id);
    }

    /// Allocate a fresh request ID for a dispatch.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn next_request_id(
        &self,
    ) -> RequestId {
        self.shared.next_request_id()
    }

    /// Handle an OLLP retry required response.
    ///
    /// Looks up the pending transaction's predicate class hash and retry count.
    /// If below the configured maximum, increments the count, spawns an async
    /// task to run the orchestrator backoff + preexec + re-submission, and
    /// re-queues the txn via the inbox. On exhaustion, releases locks and
    /// records an abort.
    ///
    /// This method is synchronous (called from the `select!` loop body). The
    /// async orchestrator backoff is off-loaded to a spawned task that posts
    /// back to the completion fan-in channel on success or sends an error txn.
    fn schedule_ollp_retry(&mut self, txn_id: TxnId) {
        let pending = match self.pending.remove(&txn_id) {
            Some(p) => p,
            None => {
                tracing::warn!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    "ollp retry: pending txn not found — ignoring"
                );
                return;
            }
        };

        let predicate_class_hash = match pending.predicate_class_hash {
            Some(h) => h,
            None => {
                // No predicate class recorded — treat as hard failure.
                tracing::warn!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    "ollp retry: no predicate_class_hash on pending txn — aborting"
                );
                self.metrics.record_executor_error();
                self.metrics.record_infra_abort(
                    crate::control::cluster::calvin::scheduler::metrics::infra_abort_reason::IO_ERROR,
                );
                self.metrics.record_completed();
                self.on_txn_complete(txn_id);
                return;
            }
        };

        let max_retries = self
            .shared
            .ollp_orchestrator
            .get()
            .map(|o| o.ollp_max_retries() as u32)
            .unwrap_or(5);

        if pending.retry_count >= max_retries {
            tracing::warn!(
                vshard_id = self.vshard_id,
                epoch = txn_id.epoch,
                position = txn_id.position,
                retry_count = pending.retry_count,
                "ollp retry exhausted — aborting txn"
            );
            self.metrics.record_executor_error();
            self.metrics.record_infra_abort(
                crate::control::cluster::calvin::scheduler::metrics::infra_abort_reason::IO_ERROR,
            );
            self.metrics.record_completed();
            // Release locks for this transaction even on abort.
            self.on_txn_complete(txn_id);
            return;
        }

        let retry_count = pending.retry_count;

        // Spawn an async task to run orchestrator backoff then re-submit.
        // The task uses the shared state's inbox to re-queue the txn.
        let shared = Arc::clone(&self.shared);
        let completion_tx = self.completion_tx.clone();
        let keys = pending.keys.clone();

        tracing::debug!(
            vshard_id = self.vshard_id,
            epoch = txn_id.epoch,
            position = txn_id.position,
            retry_count,
            "ollp retry: scheduling async retry task"
        );

        tokio::spawn(async move {
            // Apply adaptive backoff + circuit-breaker bookkeeping.
            if let Some(orc) = shared.ollp_orchestrator.get() {
                orc.on_retry_required(predicate_class_hash, retry_count)
                    .await;
            }

            // Re-submit the txn to the inbox. The sequencer will re-sequence
            // and fan-out to the scheduler again with the same TxClass but a
            // new epoch/position.
            //
            // NOTE: The plan bytes in the original TxClass still carry
            // ollp_predicted_surrogates from the first attempt. The active
            // executor will re-verify against the (now potentially updated)
            // engine state on re-admission. If the predicate has changed
            // again, another OllpRetryRequired is returned and this path
            // fires again — up to max_retries.
            if let Some(inbox) = shared.sequencer_inbox.get() {
                let txn_class = pending.txn.tx_class.clone();
                if let Err(e) = inbox.submit(txn_class) {
                    tracing::warn!(
                        error = %e,
                        "ollp retry: inbox submit failed after backoff"
                    );
                }
                // The response will arrive via the normal scheduler path
                // (new epoch/position). The old txn_id is already removed
                // from pending; the new one will be inserted on dispatch.
            } else {
                // No inbox — signal completion as error via the fan-in channel.
                let _ = completion_tx.send((txn_id, pending.request_id, None)).await;
            }

            // Suppress unused variable warning for keys — kept for future
            // lock re-acquisition on retry.
            let _ = keys;
        });
    }
}
