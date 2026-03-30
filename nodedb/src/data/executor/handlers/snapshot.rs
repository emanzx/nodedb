//! Tenant snapshot handlers: export and import Data Plane state.
//!
//! Used by BACKUP TENANT / RESTORE TENANT DDL commands.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantDataSnapshot;

impl CoreLoop {
    /// Create a snapshot of a tenant's data from the sparse engine.
    ///
    /// Returns MessagePack-serialized `TenantDataSnapshot`.
    pub(in crate::data::executor) fn execute_create_tenant_snapshot(
        &mut self,
        task: &ExecutionTask,
        tenant_id: u32,
    ) -> Response {
        tracing::info!(core = self.core_id, tenant_id, "creating tenant snapshot");

        let documents = match self.sparse.scan_all_for_tenant(tenant_id) {
            Ok(docs) => docs,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("snapshot scan failed: {e}"),
                    },
                );
            }
        };

        let indexes = match self.sparse.scan_indexes_for_tenant(tenant_id) {
            Ok(idx) => idx,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("index scan failed: {e}"),
                    },
                );
            }
        };

        tracing::info!(
            tenant_id,
            documents = documents.len(),
            indexes = indexes.len(),
            "tenant snapshot created"
        );

        let snapshot = TenantDataSnapshot { documents, indexes };
        let payload = match rmp_serde::to_vec(&snapshot) {
            Ok(p) => p,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("snapshot serialization failed: {e}"),
                    },
                );
            }
        };
        self.response_with_payload(task, payload)
    }

    /// Restore a tenant's data to the sparse engine from a snapshot.
    ///
    /// Receives MessagePack-serialized documents and indexes and writes
    /// them to the sparse engine.
    pub(in crate::data::executor) fn execute_restore_tenant_snapshot(
        &mut self,
        task: &ExecutionTask,
        tenant_id: u32,
        documents_bytes: &[u8],
        indexes_bytes: &[u8],
    ) -> Response {
        tracing::info!(core = self.core_id, tenant_id, "restoring tenant snapshot");

        // Deserialize documents.
        let documents: Vec<(String, Vec<u8>)> = match rmp_serde::from_slice(documents_bytes) {
            Ok(d) => d,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("document deserialization failed: {e}"),
                    },
                );
            }
        };
        let mut docs_written = 0u64;
        for (key, value) in &documents {
            // Key is in sparse engine format: "tenant:collection\0doc_id".
            // Use raw put to write directly.
            if let Err(e) = self.sparse.put_raw(key, value) {
                tracing::warn!(key, error = %e, "failed to restore document");
                continue;
            }
            docs_written += 1;
        }

        // Deserialize and restore indexes.
        let indexes: Vec<(String, Vec<u8>)> = match rmp_serde::from_slice(indexes_bytes) {
            Ok(i) => i,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("index deserialization failed: {e}"),
                    },
                );
            }
        };
        let mut indexes_written = 0u64;
        for (key, value) in &indexes {
            if let Err(e) = self.sparse.put_index_raw(key, value) {
                tracing::warn!(key, error = %e, "failed to restore index");
                continue;
            }
            indexes_written += 1;
        }

        tracing::info!(
            tenant_id,
            docs_written,
            indexes_written,
            "tenant snapshot restored"
        );

        let result = serde_json::json!({
            "documents_restored": docs_written,
            "indexes_restored": indexes_written,
            "tenant_id": tenant_id,
        });
        let payload = match serde_json::to_vec(&result) {
            Ok(p) => p,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("result serialization failed: {e}"),
                    },
                );
            }
        };
        self.response_with_payload(task, payload)
    }
}
