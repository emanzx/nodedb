use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::state::SharedState;

use super::super::types::sqlstate_error;

/// GRANT ROLE <role> TO <user>
///
/// Assigns a role to a user. Requires superuser or tenant_admin.
pub fn handle_grant(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    // GRANT ROLE <role> TO <user>
    if parts.len() < 5 {
        return Err(sqlstate_error(
            "42601",
            "syntax: GRANT ROLE <role> TO <user>",
        ));
    }

    if !parts[1].eq_ignore_ascii_case("ROLE") {
        return Err(sqlstate_error(
            "42601",
            "only GRANT ROLE is supported (collection-level grants coming soon)",
        ));
    }

    if !identity.is_superuser && !identity.has_role(&Role::TenantAdmin) {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser or tenant_admin can grant roles",
        ));
    }

    let role_name = parts[2];
    let role: Role = role_name
        .parse()
        .unwrap_or(Role::Custom(role_name.to_string()));

    // Prevent non-superusers from granting superuser.
    if matches!(role, Role::Superuser) && !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "only superuser can grant superuser role",
        ));
    }

    if !parts[3].eq_ignore_ascii_case("TO") {
        return Err(sqlstate_error("42601", "expected TO after role name"));
    }

    let username = parts[4];

    state
        .credentials
        .add_role(username, role.clone())
        .map_err(|e| sqlstate_error("42704", &e.to_string()))?;

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("granted role '{role}' to user '{username}'"),
    );

    Ok(vec![Response::Execution(Tag::new("GRANT"))])
}

/// REVOKE ROLE <role> FROM <user>
///
/// Removes a role from a user. Requires superuser or tenant_admin.
pub fn handle_revoke(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 5 {
        return Err(sqlstate_error(
            "42601",
            "syntax: REVOKE ROLE <role> FROM <user>",
        ));
    }

    if !parts[1].eq_ignore_ascii_case("ROLE") {
        return Err(sqlstate_error(
            "42601",
            "only REVOKE ROLE is supported (collection-level grants coming soon)",
        ));
    }

    if !identity.is_superuser && !identity.has_role(&Role::TenantAdmin) {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser or tenant_admin can revoke roles",
        ));
    }

    let role_name = parts[2];
    let role: Role = role_name
        .parse()
        .unwrap_or(Role::Custom(role_name.to_string()));

    if !parts[3].eq_ignore_ascii_case("FROM") {
        return Err(sqlstate_error("42601", "expected FROM after role name"));
    }

    let username = parts[4];

    // Prevent revoking your own superuser.
    if username == identity.username && matches!(role, Role::Superuser) {
        return Err(sqlstate_error(
            "42501",
            "cannot revoke your own superuser role",
        ));
    }

    state
        .credentials
        .remove_role(username, &role)
        .map_err(|e| sqlstate_error("42704", &e.to_string()))?;

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("revoked role '{role}' from user '{username}'"),
    );

    Ok(vec![Response::Execution(Tag::new("REVOKE"))])
}
