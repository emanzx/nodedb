// SPDX-License-Identifier: BUSL-1.1

//! Write-path coordinator for distributed array operations.
//!
//! Provides [`coord_put`], [`coord_put_partitioned`], and [`coord_delete`]
//! — functions that partition a flat cell/coord list by Hilbert tile and
//! fan writes to the owning vShards.

use std::sync::Arc;

use crate::circuit_breaker::CircuitBreaker;
use crate::error::{ClusterError, Result};

use super::super::partition::{partition_delete_coords, partition_put_cells};
use super::super::rpc::ShardRpcDispatch;
use super::super::scatter::{FanOutPartitionedParams, fan_out_partitioned};
use super::super::wire::{
    ArrayShardDeleteReq, ArrayShardDeleteResp, ArrayShardPutReq, ArrayShardPutResp,
};
use super::read::decode_resps;

/// Parameters for write-path coordinator entry points (partitioned fan-out).
pub struct ArrayWriteCoordParams {
    pub source_node: u64,
    pub timeout_ms: u64,
}

/// Forward pre-partitioned cell writes to the owning shards.
///
/// The caller groups cells by Hilbert prefix bucket using
/// `array_vshard_for_tile` and produces one `ArrayShardPutReq` per
/// target shard. This function dispatches each batch to its shard via
/// `fan_out_partitioned` and collects acknowledgements.
///
/// No cell payload is decoded inside this function — the coordinator
/// has no dependency on `nodedb-array`.
pub async fn coord_put_partitioned(
    params: &ArrayWriteCoordParams,
    per_shard: Vec<(u32, ArrayShardPutReq)>,
    dispatch: &Arc<dyn ShardRpcDispatch>,
    circuit_breaker: &Arc<CircuitBreaker>,
) -> Result<Vec<ArrayShardPutResp>> {
    if per_shard.is_empty() {
        return Ok(Vec::new());
    }

    let fo_params = FanOutPartitionedParams {
        timeout_ms: params.timeout_ms,
        source_node: params.source_node,
    };

    let encoded: Result<Vec<(u32, Vec<u8>)>> = per_shard
        .iter()
        .map(|(shard_id, req)| {
            zerompk::to_msgpack_vec(req)
                .map(|bytes| (*shard_id, bytes))
                .map_err(|e| ClusterError::Codec {
                    detail: format!("ArrayShardPutReq serialise (shard {shard_id}): {e}"),
                })
        })
        .collect();

    let raw = fan_out_partitioned(
        &fo_params,
        super::super::opcodes::ARRAY_SHARD_PUT_REQ,
        &encoded?,
        dispatch,
        circuit_breaker,
    )
    .await?;

    decode_resps::<ArrayShardPutResp>(&raw)
}

/// Partition a flat cell list by Hilbert tile and fan out to owning shards.
///
/// `cells` — each element is `(hilbert_prefix, zerompk-encoded single-cell bytes)`.
/// The Hilbert prefix is computed by the caller (the Control Plane planner) from
/// the cell's coord tuple and the array schema; this function does not decode
/// cell bytes.
///
/// `prefix_bits` — routing granularity (1–16) from the array catalog entry.
/// `wal_lsn` — WAL sequence number allocated by the Control Plane for this batch.
///
/// Atomicity is per-shard only: if cells span multiple shards each shard's write
/// is committed independently. A partial failure returns the first error encountered;
/// cells that were already committed to other shards are not rolled back.
pub async fn coord_put(
    params: &ArrayWriteCoordParams,
    array_id_msgpack: Vec<u8>,
    prefix_bits: u8,
    wal_lsn: u64,
    cells: &[(u64, Vec<u8>)],
    dispatch: &Arc<dyn ShardRpcDispatch>,
    circuit_breaker: &Arc<CircuitBreaker>,
) -> Result<Vec<ArrayShardPutResp>> {
    if cells.is_empty() {
        return Ok(Vec::new());
    }

    let buckets = partition_put_cells(cells, prefix_bits)?;

    let per_shard: Vec<(u32, ArrayShardPutReq)> = buckets
        .into_iter()
        .map(|b| {
            let req = ArrayShardPutReq {
                array_id_msgpack: array_id_msgpack.clone(),
                cells_msgpack: b.cells_msgpack,
                wal_lsn,
                representative_hilbert_prefix: b.representative_hilbert_prefix,
                prefix_bits,
            };
            (b.vshard_id, req)
        })
        .collect();

    coord_put_partitioned(params, per_shard, dispatch, circuit_breaker).await
}

/// Partition a flat coord list by Hilbert tile and fan delete requests to owning shards.
///
/// `coords` — each element is `(hilbert_prefix, zerompk-encoded single-coord bytes)`.
/// `prefix_bits` — routing granularity (1–16).
/// `wal_lsn` — WAL sequence number allocated by the Control Plane.
///
/// Atomicity is per-shard only (same contract as `coord_put`).
pub async fn coord_delete(
    params: &ArrayWriteCoordParams,
    array_id_msgpack: Vec<u8>,
    prefix_bits: u8,
    wal_lsn: u64,
    coords: &[(u64, Vec<u8>)],
    dispatch: &Arc<dyn ShardRpcDispatch>,
    circuit_breaker: &Arc<CircuitBreaker>,
) -> Result<Vec<ArrayShardDeleteResp>> {
    if coords.is_empty() {
        return Ok(Vec::new());
    }

    let buckets = partition_delete_coords(coords, prefix_bits)?;

    let fo_params = FanOutPartitionedParams {
        timeout_ms: params.timeout_ms,
        source_node: params.source_node,
    };

    let encoded: Result<Vec<(u32, Vec<u8>)>> = buckets
        .into_iter()
        .map(|b| {
            let req = ArrayShardDeleteReq {
                array_id_msgpack: array_id_msgpack.clone(),
                coords_msgpack: b.coords_msgpack,
                wal_lsn,
                representative_hilbert_prefix: b.representative_hilbert_prefix,
                prefix_bits,
            };
            zerompk::to_msgpack_vec(&req)
                .map(|bytes| (b.vshard_id, bytes))
                .map_err(|e| ClusterError::Codec {
                    detail: format!("ArrayShardDeleteReq serialise (shard {}): {e}", b.vshard_id),
                })
        })
        .collect();

    let raw = fan_out_partitioned(
        &fo_params,
        super::super::opcodes::ARRAY_SHARD_DELETE_REQ,
        &encoded?,
        dispatch,
        circuit_breaker,
    )
    .await?;

    decode_resps::<ArrayShardDeleteResp>(&raw)
}
