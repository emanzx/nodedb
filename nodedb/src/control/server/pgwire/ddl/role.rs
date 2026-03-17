use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::{require_admin, sqlstate_error};

/// CREATE ROLE <name> [INHERIT <parent>]
pub fn create_role(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "create roles")?;

    if parts.len() < 3 {
        return Err(sqlstate_error(
            "42601",
            "syntax: CREATE ROLE <name> [INHERIT <parent>]",
        ));
    }

    let name = parts[2];
    let parent = if parts.len() >= 5 && parts[3].eq_ignore_ascii_case("INHERIT") {
        Some(parts[4])
    } else {
        None
    };

    let catalog = state.credentials.catalog();
    state
        .roles
        .create_role(name, identity.tenant_id, parent, catalog.as_ref())
        .map_err(|e| sqlstate_error("42710", &e.to_string()))?;

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "created role '{name}'{}",
            parent.map_or(String::new(), |p| format!(" inheriting from '{p}'"))
        ),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE ROLE"))])
}

/// DROP ROLE <name>
pub fn drop_role(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "drop roles")?;

    if parts.len() < 3 {
        return Err(sqlstate_error("42601", "syntax: DROP ROLE <name>"));
    }

    let name = parts[2];
    let catalog = state.credentials.catalog();
    let dropped = state
        .roles
        .drop_role(name, catalog.as_ref())
        .map_err(|e| sqlstate_error("42704", &e.to_string()))?;

    if dropped {
        state.audit_record(
            AuditEvent::PrivilegeChange,
            Some(identity.tenant_id),
            &identity.username,
            &format!("dropped role '{name}'"),
        );
        Ok(vec![Response::Execution(Tag::new("DROP ROLE"))])
    } else {
        Err(sqlstate_error(
            "42704",
            &format!("role '{name}' does not exist"),
        ))
    }
}
