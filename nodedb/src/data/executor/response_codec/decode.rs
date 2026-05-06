// SPDX-License-Identifier: BUSL-1.1

//! Payload→docs decoders for inline sub-plans (multi-way joins consuming an
//! inner-join Response, etc.).

use sonic_rs;

/// Decode a Response payload (msgpack array) back to `(doc_id, msgpack_bytes)` pairs.
///
/// Used for inline sub-plans in multi-way joins: the inner join produces a
/// Response, and the outer join needs its rows as input.
pub(in crate::data::executor) fn decode_response_to_docs(
    response: &crate::bridge::envelope::Response,
) -> Option<Vec<(String, Vec<u8>)>> {
    use nodedb_query::msgpack_scan;

    let payload = response.payload.as_ref();
    if payload.is_empty() {
        return None;
    }

    let (count, mut offset) = msgpack_scan::array_header(payload, 0)?;
    let mut docs = Vec::with_capacity(count);

    for _ in 0..count {
        let entry_start = offset;
        let id = msgpack_scan::extract_field(payload, offset, "id")
            .and_then(|(s, _e)| msgpack_scan::read_str(payload, s).map(|s| s.to_string()))
            .unwrap_or_default();
        let next = msgpack_scan::skip_value(payload, offset)?;
        let entry_bytes = payload[entry_start..next].to_vec();
        docs.push((id, entry_bytes));
        offset = next;
    }

    Some(docs)
}

/// Decode a payload (msgpack array or JSON array) to `(doc_id, msgpack_bytes)` pairs.
///
/// Used for inline hash join: decodes the result of an inner join back to doc pairs.
/// Handles both msgpack (from encode_binary_rows) and JSON (from broadcast_to_all_cores merge).
pub(in crate::data::executor) fn decode_response_to_docs_from_bytes(
    payload: &[u8],
) -> Option<Vec<(String, Vec<u8>)>> {
    use nodedb_query::msgpack_scan;

    if payload.is_empty() {
        return None;
    }

    if let Some((count, mut offset)) = msgpack_scan::array_header(payload, 0) {
        let mut docs = Vec::with_capacity(count);
        for _ in 0..count {
            let entry_start = offset;
            let id = msgpack_scan::extract_field(payload, offset, "id")
                .and_then(|(s, _e)| msgpack_scan::read_str(payload, s).map(|s| s.to_string()))
                .unwrap_or_default();
            let next = msgpack_scan::skip_value(payload, offset)?;
            let entry_bytes = payload[entry_start..next].to_vec();
            docs.push((id, entry_bytes));
            offset = next;
        }
        return Some(docs);
    }

    // JSON fallback (from broadcast_to_all_cores merge which returns JSON text).
    if payload.first() == Some(&b'[') {
        let arr: Vec<serde_json::Value> = sonic_rs::from_slice(payload).ok()?;
        let mut docs = Vec::with_capacity(arr.len());
        for val in &arr {
            let id = val
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let mp = nodedb_types::json_to_msgpack(val).unwrap_or_default();
            docs.push((id, mp));
        }
        return Some(docs);
    }

    None
}
