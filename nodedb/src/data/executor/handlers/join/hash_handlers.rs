// SPDX-License-Identifier: BUSL-1.1

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use nodedb_query::msgpack_scan;

use super::hash::{HashIndex, ProbeParams, probe_hash_index};
use super::params::{BroadcastJoinParams, HashJoinParams, InlineHashJoinParams};

impl CoreLoop {
    pub(in crate::data::executor) fn execute_hash_join(
        &mut self,
        p: HashJoinParams<'_>,
    ) -> Response {
        let HashJoinParams {
            join,
            tid,
            left_collection,
            right_collection,
            left_alias,
            right_alias,
            inline_left,
            inline_right,
            inline_left_bitmap,
            inline_right_bitmap,
        } = p;

        debug!(
            core = self.core_id,
            %left_collection,
            %right_collection,
            left_alias = left_alias.unwrap_or(""),
            right_alias = right_alias.unwrap_or(""),
            keys = join.on.len(),
            %join.join_type,
            inline = inline_left.is_some(),
            "hash join"
        );

        let scan_limit = (join.limit * 10).min(50000);

        let left_bm = inline_left_bitmap.map(|sub_plan| {
            crate::data::executor::dispatch::bitmap::hashjoin_inline::run_bitmap_subplan(
                self, join.task, sub_plan,
            )
        });

        let right_bm = inline_right_bitmap.map(|sub_plan| {
            crate::data::executor::dispatch::bitmap::hashjoin_inline::run_bitmap_subplan(
                self, join.task, sub_plan,
            )
        });

        let left_docs = if let Some(sub_plan) = inline_left {
            let sub_response = self.execute_plan(join.task, sub_plan);
            match crate::data::executor::response_codec::decode_response_to_docs(&sub_response) {
                Some(docs) => docs,
                None => return sub_response,
            }
        } else if let Some(bm) = left_bm {
            match crate::data::executor::dispatch::bitmap::hashjoin_inline::prefiltered_scan_plan(
                left_collection,
                scan_limit,
                bm,
            ) {
                Some(scan_plan) => {
                    let resp = self.execute_plan(join.task, &scan_plan);
                    crate::data::executor::response_codec::decode_response_to_docs(&resp)
                        .unwrap_or_default()
                }
                None => match self.scan_collection(tid, left_collection, scan_limit) {
                    Ok(d) => d,
                    Err(e) => {
                        return self.response_error(
                            join.task,
                            ErrorCode::Internal {
                                detail: e.to_string(),
                            },
                        );
                    }
                },
            }
        } else {
            match self.scan_collection(tid, left_collection, scan_limit) {
                Ok(d) => d,
                Err(e) => {
                    return self.response_error(
                        join.task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }
            }
        };

        let right_docs = if let Some(sub_plan) = inline_right {
            let sub_response = self.execute_plan(join.task, sub_plan);
            match crate::data::executor::response_codec::decode_response_to_docs(&sub_response) {
                Some(docs) => docs,
                None => return sub_response,
            }
        } else if let Some(bm) = right_bm {
            match crate::data::executor::dispatch::bitmap::hashjoin_inline::prefiltered_scan_plan(
                right_collection,
                scan_limit,
                bm,
            ) {
                Some(scan_plan) => {
                    let resp = self.execute_plan(join.task, &scan_plan);
                    crate::data::executor::response_codec::decode_response_to_docs(&resp)
                        .unwrap_or_default()
                }
                None => match self.scan_collection(tid, right_collection, scan_limit) {
                    Ok(d) => d,
                    Err(e) => {
                        return self.response_error(
                            join.task,
                            ErrorCode::Internal {
                                detail: e.to_string(),
                            },
                        );
                    }
                },
            }
        } else {
            match self.scan_collection(tid, right_collection, scan_limit) {
                Ok(d) => d,
                Err(e) => {
                    return self.response_error(
                        join.task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }
            }
        };

        let left_prefix = left_alias.unwrap_or(left_collection);
        let right_prefix = right_alias.unwrap_or(right_collection);

        let right_keys: Vec<&str> = join.on.iter().map(|(_, r)| r.as_str()).collect();
        let left_keys: Vec<&str> = join.on.iter().map(|(l, _)| l.as_str()).collect();

        let right_index = HashIndex::build(&right_docs, &right_keys);

        let mut results = probe_hash_index(&ProbeParams {
            probe_docs: &left_docs,
            index: &right_index,
            index_docs: &right_docs,
            probe_keys: &left_keys,
            join_type: join.join_type,
            limit: join.limit,
            probe_collection: left_prefix,
            index_collection: right_prefix,
            emit_unmatched_right: true,
        });

        join.filter_and_project(&mut results);

        let payload = super::super::super::response_codec::encode_binary_rows(&results);
        self.response_with_payload(join.task, payload)
    }

    pub(in crate::data::executor) fn execute_inline_hash_join(
        &mut self,
        p: InlineHashJoinParams<'_>,
    ) -> Response {
        let InlineHashJoinParams {
            join,
            left_data,
            right_data,
            right_alias,
        } = p;

        debug!(
            core = self.core_id,
            left_bytes = left_data.len(),
            right_bytes = right_data.len(),
            keys = join.on.len(),
            %join.join_type,
            "inline hash join"
        );

        let left_docs =
            match crate::data::executor::response_codec::decode_response_to_docs_from_bytes(
                left_data,
            ) {
                Some(d) => d,
                None => {
                    return self.response_with_payload(
                        join.task,
                        super::super::super::response_codec::encode_binary_rows(&[]),
                    );
                }
            };

        let right_docs = super::super::super::response_codec::decode_raw_scan_to_docs(right_data);

        let mut left_key_strs: Vec<String> = join.on.iter().map(|(l, _)| l.clone()).collect();
        if let Some((_, first_doc)) = left_docs.first() {
            for key in &mut left_key_strs {
                if msgpack_scan::extract_field(first_doc, 0, key).is_none() {
                    let suffix = format!(".{key}");
                    let mut resolved = None;
                    if let Some((count, mut pos)) = msgpack_scan::map_header(first_doc, 0) {
                        for _ in 0..count {
                            if let Some(field_name) = msgpack_scan::read_str(first_doc, pos)
                                && field_name.ends_with(&suffix)
                            {
                                resolved = Some(field_name.to_string());
                                break;
                            }
                            pos = match msgpack_scan::skip_value(first_doc, pos) {
                                Some(p) => p,
                                None => break,
                            };
                            pos = match msgpack_scan::skip_value(first_doc, pos) {
                                Some(p) => p,
                                None => break,
                            };
                        }
                    }
                    if let Some(resolved) = resolved {
                        *key = resolved;
                    }
                }
            }
        }
        let left_keys: Vec<&str> = left_key_strs.iter().map(|s| s.as_str()).collect();
        let right_keys: Vec<&str> = join.on.iter().map(|(_, r)| r.as_str()).collect();

        let right_index = HashIndex::build(&right_docs, &right_keys);

        let mut results = probe_hash_index(&ProbeParams {
            probe_docs: &left_docs,
            index: &right_index,
            index_docs: &right_docs,
            probe_keys: &left_keys,
            join_type: join.join_type,
            limit: join.limit,
            probe_collection: "",
            index_collection: right_alias.unwrap_or("inline_right"),
            emit_unmatched_right: true,
        });

        join.filter_and_project(&mut results);

        let payload = super::super::super::response_codec::encode_binary_rows(&results);
        self.response_with_payload(join.task, payload)
    }

    pub(in crate::data::executor) fn execute_broadcast_join(
        &mut self,
        p: BroadcastJoinParams<'_>,
    ) -> Response {
        let BroadcastJoinParams {
            join,
            tid,
            large_collection,
            small_collection,
            large_alias,
            small_alias,
            broadcast_data,
        } = p;

        debug!(
            core = self.core_id,
            %large_collection,
            %small_collection,
            large_alias = large_alias.unwrap_or(""),
            small_alias = small_alias.unwrap_or(""),
            broadcast_bytes = broadcast_data.len(),
            keys = join.on.len(),
            %join.join_type,
            "broadcast join"
        );

        let large_prefix = large_alias.unwrap_or(large_collection);
        let small_prefix = small_alias.unwrap_or(small_collection);

        let small_docs_raw: Vec<(String, Vec<u8>)> =
            super::super::super::response_codec::decode_raw_scan_to_docs(broadcast_data);

        tracing::warn!(
            core = self.core_id,
            small_count = small_docs_raw.len(),
            broadcast_len = broadcast_data.len(),
            "broadcast join: decoded small side"
        );

        let scan_limit = (join.limit * 10).min(50000);
        let large_docs = match self.scan_collection(tid, large_collection, scan_limit) {
            Ok(d) => d,
            Err(e) => {
                return self.response_error(
                    join.task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        let large_keys: Vec<&str> = join.on.iter().map(|(l, _)| l.as_str()).collect();
        let small_keys: Vec<&str> = join.on.iter().map(|(_, s)| s.as_str()).collect();

        let small_index = HashIndex::build(&small_docs_raw, &small_keys);

        let mut results = probe_hash_index(&ProbeParams {
            probe_docs: &large_docs,
            index: &small_index,
            index_docs: &small_docs_raw,
            probe_keys: &large_keys,
            join_type: join.join_type,
            limit: join.limit,
            probe_collection: large_prefix,
            index_collection: small_prefix,
            emit_unmatched_right: false,
        });

        join.filter_and_project(&mut results);

        let payload = super::super::super::response_codec::encode_binary_rows(&results);
        self.response_with_payload(join.task, payload)
    }
}
