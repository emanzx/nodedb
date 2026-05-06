// SPDX-License-Identifier: BUSL-1.1

//! Descriptor lease acquisition and release methods for `SharedState`.

use std::sync::Arc;

use super::SharedState;

impl SharedState {
    /// Acquire (or re-confirm) a descriptor lease at the given
    /// version, valid for `duration` from now. This is the public
    /// API the planner and tests use to obtain a lease before reading
    /// a descriptor.
    ///
    /// Fast path returns immediately if a non-expired lease at the
    /// requested version (or higher) is already held by this node.
    /// Slow path proposes a `MetadataEntry::DescriptorLeaseGrant`
    /// through the metadata raft group and blocks on the local
    /// applied watermark. Single-node fallback writes directly to
    /// the in-memory cache. See
    /// [`crate::control::lease::propose::acquire_lease`] for the
    /// full semantics.
    pub fn acquire_descriptor_lease(
        &self,
        descriptor_id: nodedb_cluster::DescriptorId,
        version: u64,
        duration: std::time::Duration,
    ) -> crate::Result<nodedb_cluster::DescriptorLease> {
        crate::control::lease::acquire_lease(self, descriptor_id, version, duration)
    }

    /// Release every lease this node currently holds against any
    /// of `descriptor_ids`. Used on `SIGTERM` drain and by tests.
    /// Empty input is a no-op.
    pub fn release_descriptor_leases(
        &self,
        descriptor_ids: Vec<nodedb_cluster::DescriptorId>,
    ) -> crate::Result<()> {
        crate::control::lease::release_leases(self, descriptor_ids)
    }

    /// Acquire the descriptor leases needed to execute a plan
    /// that reads the descriptors in `version_set`. Returns a
    /// [`crate::control::lease::QueryLeaseScope`] whose drop
    /// decrements each refcount and triggers a background
    /// release for any descriptor whose count hits zero.
    ///
    /// This is called by the pgwire handler AFTER planning
    /// (fresh or cache hit) and held through the query's
    /// execute phase. Multiple concurrent queries that share
    /// a descriptor all pay a single raft acquire (on the
    /// first-holder call) and a single raft release (when the
    /// last holder drops its scope).
    ///
    /// Errors while acquiring (drain in progress, NotLeader,
    /// etc.) are logged at warn and the affected descriptor is
    /// NOT added to the returned scope — the query proceeds
    /// without a lease on that one descriptor.
    pub fn acquire_plan_lease_scope(
        self: &Arc<Self>,
        version_set: &crate::control::planner::descriptor_set::DescriptorVersionSet,
    ) -> crate::control::lease::QueryLeaseScope {
        use crate::control::lease::{DEFAULT_LEASE_DURATION, QueryLeaseScope};
        if version_set.is_empty() {
            return QueryLeaseScope::empty();
        }
        let mut held_ids = Vec::with_capacity(version_set.len());
        for (id, version) in version_set.iter() {
            let count_after = self.lease_refcount.increment(id);
            held_ids.push(id.clone());
            if count_after == 1 {
                let acquire_result: crate::Result<nodedb_cluster::DescriptorLease> =
                    self.acquire_descriptor_lease(id.clone(), version, DEFAULT_LEASE_DURATION);
                if let Err(e) = acquire_result {
                    let msg = e.to_string();
                    if !msg.contains("drain in progress") {
                        tracing::warn!(
                            error = %msg,
                            descriptor = ?id,
                            version,
                            "acquire_plan_lease_scope: first-holder acquire failed"
                        );
                    }
                }
            }
        }
        QueryLeaseScope::new(held_ids, self)
    }

    /// Look up a single lease by `(descriptor_id, this_node_id)`,
    /// filtering expired records. Used by tests and by the planner
    /// to short-circuit when a fresh lease already exists. Returns
    /// `None` if absent or past expiry.
    pub fn lookup_lease_for_self(
        &self,
        descriptor_id: &nodedb_cluster::DescriptorId,
    ) -> Option<nodedb_cluster::DescriptorLease> {
        let now = self.hlc_clock.peek();
        let cache = self
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        cache
            .leases
            .get(&(descriptor_id.clone(), self.node_id))
            .filter(|l| l.expires_at > now)
            .cloned()
    }
}
