use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::types::sqlstate_error;

/// Parse a single-quoted string from split parts starting at `start`.
/// Handles passwords like 'hello world' spanning multiple whitespace-split parts.
pub(crate) fn extract_quoted_string(parts: &[&str], start: usize) -> Option<String> {
    if start >= parts.len() {
        return None;
    }

    let first = parts[start];
    if !first.starts_with('\'') {
        return None;
    }

    if first.ends_with('\'') && first.len() > 1 {
        return Some(first[1..first.len() - 1].to_string());
    }

    let mut result = first[1..].to_string();
    for &part in &parts[start + 1..] {
        result.push(' ');
        if let Some(stripped) = part.strip_suffix('\'') {
            result.push_str(stripped);
            return Some(result);
        }
        result.push_str(part);
    }

    None
}

/// Find the index of the first part after a quoted string starting at `start`.
pub(crate) fn next_after_quoted(parts: &[&str], start: usize) -> usize {
    if start >= parts.len() {
        return parts.len();
    }

    let first = parts[start];
    if !first.starts_with('\'') {
        return start + 1;
    }

    if first.ends_with('\'') && first.len() > 1 {
        return start + 1;
    }

    for (i, part) in parts[start + 1..].iter().enumerate() {
        if part.ends_with('\'') {
            return start + 1 + i + 1;
        }
    }
    parts.len()
}

/// CREATE USER <name> WITH PASSWORD '<password>' [ROLE <role>] [TENANT <id>]
pub fn create_user(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser && !identity.has_role(&Role::TenantAdmin) {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser or tenant_admin can create users",
        ));
    }

    if parts.len() < 6 {
        return Err(sqlstate_error(
            "42601",
            "syntax: CREATE USER <name> WITH PASSWORD '<password>' [ROLE <role>] [TENANT <id>]",
        ));
    }

    let username = parts[2];
    if !parts[3].eq_ignore_ascii_case("WITH") || !parts[4].eq_ignore_ascii_case("PASSWORD") {
        return Err(sqlstate_error(
            "42601",
            "expected WITH PASSWORD after username",
        ));
    }

    let password = extract_quoted_string(parts, 5)
        .ok_or_else(|| sqlstate_error("42601", "password must be a single-quoted string"))?;

    let mut role = Role::ReadWrite;
    let mut tenant_id = identity.tenant_id;
    let mut i = next_after_quoted(parts, 5);
    while i < parts.len() {
        match parts[i].to_uppercase().as_str() {
            "ROLE" if i + 1 < parts.len() => {
                role = parts[i + 1].parse().unwrap_or(Role::ReadWrite);
                i += 2;
            }
            "TENANT" if i + 1 < parts.len() => {
                if !identity.is_superuser {
                    return Err(sqlstate_error("42501", "only superuser can assign tenants"));
                }
                let tid: u32 = parts[i + 1]
                    .parse()
                    .map_err(|_| sqlstate_error("42601", "TENANT must be a numeric ID"))?;
                tenant_id = TenantId::new(tid);
                i += 2;
            }
            _ => i += 1,
        }
    }

    state
        .credentials
        .create_user(username, &password, tenant_id, vec![role])
        .map_err(|e| sqlstate_error("42710", &e.to_string()))?;

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(tenant_id),
        &identity.username,
        &format!("created user '{username}' in tenant {tenant_id}"),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE USER"))])
}

/// ALTER USER <name> SET PASSWORD '<password>'
/// ALTER USER <name> SET ROLE <role>
pub fn alter_user(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 5 {
        return Err(sqlstate_error(
            "42601",
            "syntax: ALTER USER <name> SET PASSWORD '<password>' | ALTER USER <name> SET ROLE <role>",
        ));
    }

    let username = parts[2];

    // Users can change their own password; admin required for anything else.
    let is_self = username == identity.username;
    if !is_self && !identity.is_superuser && !identity.has_role(&Role::TenantAdmin) {
        return Err(sqlstate_error(
            "42501",
            "permission denied: can only alter your own user, or be superuser/tenant_admin",
        ));
    }

    if !parts[3].eq_ignore_ascii_case("SET") {
        return Err(sqlstate_error("42601", "expected SET after username"));
    }

    match parts[4].to_uppercase().as_str() {
        "PASSWORD" => {
            let password = extract_quoted_string(parts, 5).ok_or_else(|| {
                sqlstate_error("42601", "password must be a single-quoted string")
            })?;

            state
                .credentials
                .update_password(username, &password)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

            state.audit_record(
                AuditEvent::PrivilegeChange,
                Some(identity.tenant_id),
                &identity.username,
                &format!("changed password for user '{username}'"),
            );

            Ok(vec![Response::Execution(Tag::new("ALTER USER"))])
        }
        "ROLE" => {
            if is_self && !identity.is_superuser {
                return Err(sqlstate_error("42501", "cannot change your own role"));
            }
            if !identity.is_superuser && !identity.has_role(&Role::TenantAdmin) {
                return Err(sqlstate_error(
                    "42501",
                    "only superuser or tenant_admin can change roles",
                ));
            }

            if parts.len() < 6 {
                return Err(sqlstate_error("42601", "expected role name after SET ROLE"));
            }

            let role: Role = parts[5].parse().unwrap_or(Role::ReadWrite);

            state
                .credentials
                .update_roles(username, vec![role.clone()])
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

            state.audit_record(
                AuditEvent::PrivilegeChange,
                Some(identity.tenant_id),
                &identity.username,
                &format!("set role '{role}' for user '{username}'"),
            );

            Ok(vec![Response::Execution(Tag::new("ALTER USER"))])
        }
        other => Err(sqlstate_error(
            "42601",
            &format!("unknown ALTER USER property: {other}"),
        )),
    }
}

/// DROP USER <name>
pub fn drop_user(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser && !identity.has_role(&Role::TenantAdmin) {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser or tenant_admin can drop users",
        ));
    }

    if parts.len() < 3 {
        return Err(sqlstate_error("42601", "syntax: DROP USER <name>"));
    }

    let username = parts[2];

    if username == identity.username {
        return Err(sqlstate_error("42501", "cannot drop your own user"));
    }

    let dropped = state
        .credentials
        .deactivate_user(username)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    if dropped {
        state.audit_record(
            AuditEvent::PrivilegeChange,
            Some(identity.tenant_id),
            &identity.username,
            &format!("dropped user '{username}'"),
        );
        Ok(vec![Response::Execution(Tag::new("DROP USER"))])
    } else {
        Err(sqlstate_error(
            "42704",
            &format!("user '{username}' does not exist"),
        ))
    }
}
