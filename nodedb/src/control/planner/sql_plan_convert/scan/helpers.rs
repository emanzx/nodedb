//! Tiny helpers shared across the scan/search converters.

use nodedb_sql::TemporalScope;

/// Project `TemporalScope::valid_time` into the wire field `valid_at_ms`.
///
/// `ValidTime::Range` is currently not threaded on the wire — a range form
/// requires a wire-type widening, tracked as a separate batch.
pub(super) fn valid_at_from_scope(t: &TemporalScope) -> Option<i64> {
    match t.valid_time {
        nodedb_sql::ValidTime::Any | nodedb_sql::ValidTime::Range(..) => None,
        nodedb_sql::ValidTime::At(ms) => Some(ms),
    }
}
