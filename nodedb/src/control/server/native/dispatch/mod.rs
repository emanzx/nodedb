//! Per-opcode dispatch handlers for the native protocol.
//!
//! Each handler builds a `PhysicalPlan` (for Data Plane ops) or calls
//! `SharedState` methods directly (for DDL/session ops), reusing the
//! same infrastructure as the pgwire and HTTP endpoints.

mod direct_ops;
mod pgwire_bridge;
mod session_ops;
mod sql;
mod transaction;

use crate::control::planner::context::QueryContext;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::session::SessionStore;
use crate::control::state::SharedState;
use crate::types::{TenantId, VShardId};

// Re-export public handler functions.
pub(crate) use direct_ops::handle_direct_op;
pub(crate) use session_ops::{handle_reset, handle_set, handle_show};
pub(crate) use sql::handle_sql;
pub(crate) use transaction::{handle_begin, handle_commit, handle_rollback};

/// Dispatch context: holds references needed by all handlers.
pub(crate) struct DispatchCtx<'a> {
    pub state: &'a SharedState,
    pub identity: &'a AuthenticatedIdentity,
    pub query_ctx: &'a QueryContext,
    pub sessions: &'a SessionStore,
    pub peer_addr: &'a std::net::SocketAddr,
}

impl DispatchCtx<'_> {
    pub(super) fn tenant_id(&self) -> TenantId {
        self.identity.tenant_id
    }

    pub(super) fn vshard_for_key(&self, key: &str) -> VShardId {
        VShardId::from_key(key.as_bytes())
    }
}

// ─── Auth ──────────────────────────────────────────────────────────

pub(crate) fn handle_auth(
    state: &SharedState,
    auth_mode: &crate::config::auth::AuthMode,
    auth: &nodedb_types::protocol::AuthMethod,
    peer_addr: &str,
) -> crate::Result<AuthenticatedIdentity> {
    use nodedb_types::protocol::AuthMethod as ProtoAuth;

    let body = match auth {
        ProtoAuth::Trust { username } => {
            serde_json::json!({ "method": "trust", "username": username })
        }
        ProtoAuth::Password { username, password } => {
            serde_json::json!({ "method": "password", "username": username, "password": password })
        }
        ProtoAuth::ApiKey { token } => {
            serde_json::json!({ "method": "api_key", "token": token })
        }
    };

    super::super::session_auth::authenticate(state, auth_mode, &body, peer_addr)
}

// ─── Ping ──────────────────────────────────────────────────────────

pub(crate) fn handle_ping(seq: u64) -> nodedb_types::protocol::NativeResponse {
    nodedb_types::protocol::NativeResponse::status_row(seq, "PONG")
}

// ─── Conversion Helpers (shared across sub-modules) ────────────────

/// Parse a JSON string (from the Data Plane) into proper columns and rows.
///
/// The Data Plane returns JSON in several formats:
/// - Array of objects: `[{"id":"1","name":"Alice"}, ...]` → extract keys as columns
/// - Single object: `{"id":"1","name":"Alice"}` → one row
/// - Scalar/string: just wrap as a single "result" column
pub(super) fn parse_json_to_columns_rows(
    json_text: &str,
) -> (Vec<String>, Vec<Vec<nodedb_types::Value>>) {
    use nodedb_types::Value;

    if let Ok(val) = serde_json::from_str::<serde_json::Value>(json_text) {
        match val {
            serde_json::Value::Array(arr) if !arr.is_empty() => {
                // Array of objects: extract columns from first object's keys.
                if let Some(first) = arr.first().and_then(|v| v.as_object()) {
                    let columns: Vec<String> = first.keys().cloned().collect();
                    let mut rows = Vec::with_capacity(arr.len());
                    for item in &arr {
                        if let Some(obj) = item.as_object() {
                            let row: Vec<Value> = columns
                                .iter()
                                .map(|col| {
                                    obj.get(col)
                                        .map(json_value_to_nodedb)
                                        .unwrap_or(Value::Null)
                                })
                                .collect();
                            rows.push(row);
                        }
                    }
                    return (columns, rows);
                }
                // Array of non-objects: single "value" column.
                let rows: Vec<Vec<Value>> =
                    arr.iter().map(|v| vec![json_value_to_nodedb(v)]).collect();
                return (vec!["value".into()], rows);
            }
            serde_json::Value::Object(obj) => {
                // Single object: keys are columns, values are one row.
                let columns: Vec<String> = obj.keys().cloned().collect();
                let row: Vec<Value> = columns
                    .iter()
                    .map(|col| {
                        obj.get(col)
                            .map(json_value_to_nodedb)
                            .unwrap_or(Value::Null)
                    })
                    .collect();
                return (columns, vec![row]);
            }
            _ => {}
        }
    }

    // Fallback: raw text in a single column.
    (
        vec!["result".into()],
        vec![vec![nodedb_types::Value::String(json_text.to_string())]],
    )
}

fn json_value_to_nodedb(v: &serde_json::Value) -> nodedb_types::Value {
    match v {
        serde_json::Value::Null => nodedb_types::Value::Null,
        serde_json::Value::Bool(b) => nodedb_types::Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                nodedb_types::Value::Integer(i)
            } else {
                nodedb_types::Value::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => nodedb_types::Value::String(s.clone()),
        serde_json::Value::Array(arr) => {
            let inner: Vec<nodedb_types::Value> = arr.iter().map(json_value_to_nodedb).collect();
            nodedb_types::Value::Array(inner)
        }
        serde_json::Value::Object(_) => {
            // Nested objects: serialize back to JSON string for display.
            nodedb_types::Value::String(serde_json::to_string(v).unwrap_or_default())
        }
    }
}

pub(super) fn error_to_native(
    seq: u64,
    e: &crate::Error,
) -> nodedb_types::protocol::NativeResponse {
    let (code, message) = match e {
        crate::Error::BadRequest { detail } => ("42601", detail.clone()),
        crate::Error::RejectedAuthz { resource, .. } => ("42501", resource.clone()),
        crate::Error::DeadlineExceeded { .. } => ("57014", "query cancelled due to timeout".into()),
        crate::Error::CollectionNotFound { collection, .. } => {
            ("42P01", format!("collection '{collection}' not found"))
        }
        other => ("XX000", format!("{other}")),
    };
    nodedb_types::protocol::NativeResponse::error(seq, code, message)
}
