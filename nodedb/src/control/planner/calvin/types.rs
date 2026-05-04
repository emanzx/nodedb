//! Calvin dispatch classification types.

use std::collections::BTreeSet;

use crate::types::VShardId;

/// Classification of a task set by the number of distinct write vShards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchClass {
    /// All write tasks target one vshard (or there are no writes).
    SingleShard { vshard: VShardId },
    /// Write tasks span two or more vShards — requires Calvin or best-effort.
    /// `BTreeSet` mandatory for determinism contract.
    MultiShard { vshards: BTreeSet<u32> },
}

/// Outcome returned by `dispatch_calvin_or_fast`.
#[derive(Debug)]
pub enum DispatchOutcome {
    /// Dispatched via the single-shard fast path.
    SingleShard,
    /// Submitted to the Calvin sequencer (static path).
    CalvinStatic { inbox_seq: u64 },
    /// OLLP dependent-read path submitted successfully.
    CalvinDependent { inbox_seq: u64 },
    /// Best-effort non-atomic: each vshard dispatched independently.
    BestEffortNonAtomic,
}
