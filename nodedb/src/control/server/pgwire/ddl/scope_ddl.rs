//! Scope management DDL commands.
//!
//! ```sql
//! DEFINE SCOPE 'profile:read' AS READ ON user_profiles, READ ON user_settings
//! DEFINE SCOPE 'customer' AS INCLUDE 'profile:read', INCLUDE 'orders:write'
//! DROP SCOPE 'profile:read'
//! GRANT SCOPE 'pro:all' TO ORG 'acme'
//! GRANT SCOPE 'profile:read' TO USER 'user_42'
//! REVOKE SCOPE 'pro:all' FROM ORG 'acme'
//! SHOW SCOPES
//! SHOW SCOPE 'profile:read'
//! SHOW MY SCOPES
//! SHOW SCOPES FOR USER 'user_42'
//! SHOW SCOPES FOR ORG 'acme'
//! ```

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::{sqlstate_error, text_field};

/// DEFINE SCOPE '<name>' AS <perm> ON <coll> [, <perm> ON <coll>] [INCLUDE '<scope>']
pub fn define_scope(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: requires superuser",
        ));
    }
    // DEFINE SCOPE '<name>' AS ...
    if parts.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "syntax: DEFINE SCOPE '<name>' AS <perm> ON <coll> [, ...] [INCLUDE '<scope>']",
        ));
    }

    let scope_name = parts[2].trim_matches('\'');
    // Everything after "AS" is the definition.
    let as_idx = parts
        .iter()
        .position(|p| p.to_uppercase() == "AS")
        .ok_or_else(|| sqlstate_error("42601", "missing AS keyword"))?;

    let def_parts = &parts[as_idx + 1..];

    let mut grants = Vec::new();
    let mut includes = Vec::new();

    let mut i = 0;
    while i < def_parts.len() {
        let token = def_parts[i].to_uppercase();
        match token.as_str() {
            "INCLUDE" => {
                if i + 1 < def_parts.len() {
                    let inc = def_parts[i + 1].trim_matches('\'').trim_end_matches(',');
                    includes.push(inc.to_string());
                    i += 2;
                } else {
                    return Err(sqlstate_error("42601", "INCLUDE requires a scope name"));
                }
            }
            "READ" | "WRITE" | "CREATE" | "DROP" | "ALTER" | "ADMIN" => {
                // <perm> ON <collection>
                if i + 2 < def_parts.len() && def_parts[i + 1].to_uppercase() == "ON" {
                    let coll = def_parts[i + 2].trim_matches('\'').trim_end_matches(',');
                    grants.push((token.to_lowercase(), coll.to_string()));
                    i += 3;
                } else {
                    return Err(sqlstate_error("42601", "expected <perm> ON <collection>"));
                }
            }
            _ => {
                // Skip commas and unknown tokens.
                i += 1;
            }
        }
    }

    state
        .scope_defs
        .define(scope_name, grants, includes, &identity.username)
        .map_err(|e| sqlstate_error("42601", &e.to_string()))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("defined scope '{scope_name}'"),
    );

    Ok(vec![Response::Execution(Tag::new("DEFINE SCOPE"))])
}

/// DROP SCOPE '<name>'
pub fn drop_scope(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: requires superuser",
        ));
    }
    if parts.len() < 3 {
        return Err(sqlstate_error("42601", "syntax: DROP SCOPE '<name>'"));
    }
    let name = parts[2].trim_matches('\'');

    let found = state
        .scope_defs
        .drop_scope(name)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    if !found {
        return Err(sqlstate_error(
            "42704",
            &format!("scope '{name}' not found"),
        ));
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("dropped scope '{name}'"),
    );

    Ok(vec![Response::Execution(Tag::new("DROP SCOPE"))])
}

/// GRANT SCOPE '<scope>' TO <ORG|USER|ROLE> '<id>'
pub fn grant_scope(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: requires superuser",
        ));
    }
    // GRANT SCOPE '<scope>' TO <type> '<id>'
    if parts.len() < 6 {
        return Err(sqlstate_error(
            "42601",
            "syntax: GRANT SCOPE '<scope>' TO <ORG|USER|ROLE> '<id>'",
        ));
    }
    let scope_name = parts[2].trim_matches('\'');
    let grantee_type = parts[4].to_lowercase();
    let grantee_id = parts[5].trim_matches('\'');

    if !matches!(grantee_type.as_str(), "org" | "user" | "role" | "team") {
        return Err(sqlstate_error(
            "42601",
            "grantee type must be ORG, USER, ROLE, or TEAM",
        ));
    }

    state
        .scope_grants
        .grant(scope_name, &grantee_type, grantee_id, &identity.username)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("granted scope '{scope_name}' to {grantee_type} '{grantee_id}'"),
    );

    Ok(vec![Response::Execution(Tag::new("GRANT SCOPE"))])
}

/// REVOKE SCOPE '<scope>' FROM <ORG|USER|ROLE> '<id>'
pub fn revoke_scope(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: requires superuser",
        ));
    }
    if parts.len() < 6 {
        return Err(sqlstate_error(
            "42601",
            "syntax: REVOKE SCOPE '<scope>' FROM <ORG|USER|ROLE> '<id>'",
        ));
    }
    let scope_name = parts[2].trim_matches('\'');
    let grantee_type = parts[4].to_lowercase();
    let grantee_id = parts[5].trim_matches('\'');

    state
        .scope_grants
        .revoke(scope_name, &grantee_type, grantee_id)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("revoked scope '{scope_name}' from {grantee_type} '{grantee_id}'"),
    );

    Ok(vec![Response::Execution(Tag::new("REVOKE SCOPE"))])
}

/// SHOW SCOPES / SHOW SCOPE '<name>' / SHOW SCOPES FOR <type> '<id>'
pub fn show_scopes(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    // SHOW SCOPE '<name>' — resolve a single scope.
    if parts.len() >= 3 && parts[1].to_uppercase() == "SCOPE" && parts[2].to_uppercase() != "GRANTS"
    {
        let name = parts[2].trim_matches('\'');
        let resolved = state.scope_defs.resolve(name);
        let schema = Arc::new(vec![text_field("permission"), text_field("collection")]);
        let rows: Vec<_> = resolved
            .iter()
            .map(|(perm, coll)| {
                let mut enc = DataRowEncoder::new(schema.clone());
                let _ = enc.encode_field(perm);
                let _ = enc.encode_field(coll);
                Ok(enc.take_row())
            })
            .collect();
        return Ok(vec![Response::Query(QueryResponse::new(
            schema,
            stream::iter(rows),
        ))]);
    }

    // SHOW SCOPES — list all scope definitions.
    let scopes = state.scope_defs.list();
    let schema = Arc::new(vec![
        text_field("name"),
        text_field("grants"),
        text_field("includes"),
        text_field("created_by"),
    ]);

    let rows: Vec<_> = scopes
        .iter()
        .map(|s| {
            let grants_str: Vec<String> = s
                .grants
                .iter()
                .map(|(p, c)| format!("{p} ON {c}"))
                .collect();
            let mut enc = DataRowEncoder::new(schema.clone());
            let _ = enc.encode_field(&s.name);
            let _ = enc.encode_field(&grants_str.join(", "));
            let _ = enc.encode_field(&s.includes.join(", "));
            let _ = enc.encode_field(&s.created_by);
            Ok(enc.take_row())
        })
        .collect();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
