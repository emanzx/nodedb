//! `SyncDelegate` implementation — bridges the sync transport to NodeDbLite's engines.

use crate::storage::engine::StorageEngine;

use super::core::NodeDbLite;

#[cfg(not(target_arch = "wasm32"))]
impl<S: StorageEngine> crate::sync::SyncDelegate for NodeDbLite<S> {
    fn pending_deltas(&self) -> Vec<crate::engine::crdt::engine::PendingDelta> {
        self.pending_crdt_deltas().unwrap_or_default()
    }

    fn acknowledge(&self, mutation_id: u64) {
        if let Err(e) = self.acknowledge_deltas(mutation_id) {
            tracing::warn!(mutation_id, error = %e, "SyncDelegate: acknowledge failed");
        }
    }

    fn reject(&self, mutation_id: u64) {
        if let Err(e) = self.reject_delta(mutation_id) {
            tracing::warn!(mutation_id, error = %e, "SyncDelegate: reject failed");
        }
    }

    fn reject_with_policy(
        &self,
        mutation_id: u64,
        hint: &nodedb_types::sync::compensation::CompensationHint,
    ) {
        use super::lock_ext::LockExt;

        let mut crdt = self.crdt.lock_or_recover();
        match crdt.reject_delta_with_policy(mutation_id, hint) {
            Some(nodedb_crdt::PolicyResolution::AutoResolved(action)) => {
                tracing::info!(
                    mutation_id,
                    action = ?action,
                    "SyncDelegate: delta auto-resolved by policy"
                );
            }
            Some(nodedb_crdt::PolicyResolution::Deferred {
                retry_after_ms,
                attempt,
            }) => {
                tracing::info!(
                    mutation_id,
                    retry_after_ms,
                    attempt,
                    "SyncDelegate: delta deferred for retry"
                );
            }
            Some(nodedb_crdt::PolicyResolution::Escalate) => {
                tracing::warn!(mutation_id, "SyncDelegate: delta escalated to DLQ (policy)");
            }
            Some(nodedb_crdt::PolicyResolution::WebhookRequired { webhook_url, .. }) => {
                tracing::warn!(
                    mutation_id,
                    webhook_url,
                    "SyncDelegate: delta requires webhook (not supported on Lite)"
                );
                // Fallback: treat as escalate.
                let _ = crdt.reject_delta(mutation_id);
            }
            None => {
                tracing::debug!(
                    mutation_id,
                    "SyncDelegate: reject_with_policy — delta not found"
                );
            }
        }
    }

    fn import_remote(&self, data: &[u8]) {
        if let Err(e) = self.import_remote_deltas(data) {
            tracing::warn!(error = %e, "SyncDelegate: import_remote failed");
        }
    }
}
