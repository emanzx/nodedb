// SPDX-License-Identifier: BUSL-1.1

use std::sync::{Arc, RwLock};

use crate::decommission::MetadataProposer;
use crate::error::Result;
use crate::metadata_group::migration_recovery::{
    RecoveryDecision, recover_in_flight_migrations as recovery_scan,
};
use crate::metadata_group::migration_state::SharedMigrationStateTable;
use crate::topology::ClusterTopology;

/// Scan in-flight migrations from the state table and either resume or
/// abort them. Called from coordinator startup after the metadata Raft
/// group is up but before the rebalancer spawns.
pub async fn recover_in_flight_migrations(
    migration_state: SharedMigrationStateTable,
    topology: Arc<RwLock<ClusterTopology>>,
    proposer: Arc<dyn MetadataProposer>,
    abort_timeout: std::time::Duration,
) -> Result<Vec<RecoveryDecision>> {
    {
        let mut guard = migration_state.lock().unwrap_or_else(|p| p.into_inner());
        guard.load_all()?;
    }
    recovery_scan(migration_state, topology, proposer, abort_timeout).await
}
