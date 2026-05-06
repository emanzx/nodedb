// SPDX-License-Identifier: BUSL-1.1

//! Generic encoders for Data Plane response payloads, plus the
//! `decode_payload_to_json` transcoder used at the Control Plane boundary.

/// Serialize a response payload as MessagePack bytes.
///
/// Drop-in replacement for `serde_json::to_vec(&value)` in handler code.
/// Returns MessagePack bytes that are 30-50% smaller and 2-3x faster to
/// produce than JSON.
pub(in crate::data::executor) fn encode<T: zerompk::ToMessagePack>(
    value: &T,
) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(value).map_err(|e| crate::Error::Codec {
        detail: format!("response serialization: {e}"),
    })
}

/// Encode a serde_json::Value payload as MessagePack bytes.
pub(in crate::data::executor) fn encode_json(value: &serde_json::Value) -> crate::Result<Vec<u8>> {
    nodedb_types::json_to_msgpack(value).map_err(|e| crate::Error::Codec {
        detail: format!("response serialization: {e}"),
    })
}

/// Encode any `Serialize` type as MessagePack bytes.
///
/// Serializes via serde to an intermediate `serde_json::Value`, then converts
/// to MessagePack. Use `encode()` for types that implement `ToMessagePack`
/// directly (faster, no intermediate).
pub(in crate::data::executor) fn encode_serde<T: serde::Serialize>(
    value: &T,
) -> crate::Result<Vec<u8>> {
    let json_value = serde_json::to_value(value).map_err(|e| crate::Error::Codec {
        detail: format!("serde serialization: {e}"),
    })?;
    encode_json(&json_value)
}

/// Encode a Vec of serde_json::Value as MessagePack bytes.
pub(in crate::data::executor) fn encode_json_vec(
    values: &[serde_json::Value],
) -> crate::Result<Vec<u8>> {
    let wrapped: Vec<nodedb_types::JsonValue> = values
        .iter()
        .map(|v| nodedb_types::JsonValue(v.clone()))
        .collect();
    zerompk::to_msgpack_vec(&wrapped).map_err(|e| crate::Error::Codec {
        detail: format!("response serialization: {e}"),
    })
}

/// Encode a slice of `nodedb_types::Value` as a msgpack array.
///
/// No JSON intermediary — values are serialized directly to standard msgpack.
pub(in crate::data::executor) fn encode_value_vec(
    values: &[nodedb_types::Value],
) -> crate::Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(values.len() * 64);
    let n = values.len();
    if n <= 15 {
        buf.push(0x90 | n as u8);
    } else if n <= 0xFFFF {
        buf.push(0xDC);
        buf.extend_from_slice(&(n as u16).to_be_bytes());
    } else {
        buf.push(0xDD);
        buf.extend_from_slice(&(n as u32).to_be_bytes());
    }
    for val in values {
        let encoded = nodedb_types::value_to_msgpack(val).map_err(|e| crate::Error::Codec {
            detail: format!("value serialization: {e}"),
        })?;
        buf.extend_from_slice(&encoded);
    }
    Ok(buf)
}

/// Encode a simple `{"key": count}` response (for insert confirmations).
pub(in crate::data::executor) fn encode_count(key: &str, count: usize) -> crate::Result<Vec<u8>> {
    let mut map = std::collections::BTreeMap::new();
    map.insert(key, count);
    zerompk::to_msgpack_vec(&map).map_err(|e| crate::Error::Codec {
        detail: format!("count response serialization: {e}"),
    })
}

/// Decode a MessagePack or JSON payload to a JSON string for pgwire/HTTP output.
///
/// Auto-detects format: if first byte indicates MessagePack, transcodes directly
/// to JSON text via streaming transcoder (no intermediate `serde_json::Value`).
/// If already JSON (starts with `[` or `{`), returns as-is.
pub fn decode_payload_to_json(payload: &[u8]) -> String {
    if payload.is_empty() {
        return String::new();
    }

    let first = payload[0];

    let is_likely_json = first == b'['
        || first == b'{'
        || first == b'"'
        || first.is_ascii_digit()
        || first == b't'
        || first == b'f'
        || first == b'n';

    if is_likely_json {
        return String::from_utf8_lossy(payload).into_owned();
    }

    nodedb_types::msgpack_to_json_string(payload)
        .unwrap_or_else(|_| String::from_utf8_lossy(payload).into_owned())
}
