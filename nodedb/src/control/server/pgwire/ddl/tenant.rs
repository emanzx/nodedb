use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::tenant::TenantQuota;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::types::sqlstate_error;

/// CREATE TENANT <name> [ID <id>]
///
/// Creates a tenant with default quotas. Only superuser can create tenants.
/// `name` is for display; the numeric ID is what's used internally.
pub fn create_tenant(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can create tenants",
        ));
    }

    if parts.len() < 3 {
        return Err(sqlstate_error(
            "42601",
            "syntax: CREATE TENANT <name> [ID <id>]",
        ));
    }

    let name = parts[2];

    // Parse optional ID.
    let tenant_id = if parts.len() >= 5 && parts[3].eq_ignore_ascii_case("ID") {
        let id: u32 = parts[4]
            .parse()
            .map_err(|_| sqlstate_error("42601", "TENANT ID must be a numeric value"))?;
        TenantId::new(id)
    } else {
        // Auto-assign: use tenant count + 1 as a simple ID.
        let count = match state.tenants.lock() {
            Ok(t) => t.tenant_count() as u32,
            Err(p) => p.into_inner().tenant_count() as u32,
        };
        TenantId::new(count + 1)
    };

    // Register the tenant with default quotas.
    match state.tenants.lock() {
        Ok(mut t) => t.set_quota(tenant_id, TenantQuota::default()),
        Err(p) => p.into_inner().set_quota(tenant_id, TenantQuota::default()),
    }

    state.audit_record(
        AuditEvent::TenantCreated,
        Some(tenant_id),
        &identity.username,
        &format!("created tenant '{name}' with id {tenant_id}"),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE TENANT"))])
}

/// DROP TENANT <id>
///
/// Removes tenant quotas. Only superuser. Does NOT delete tenant data
/// (that requires a separate data purge operation).
pub fn drop_tenant(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can drop tenants",
        ));
    }

    if parts.len() < 3 {
        return Err(sqlstate_error("42601", "syntax: DROP TENANT <id>"));
    }

    let tid: u32 = parts[2]
        .parse()
        .map_err(|_| sqlstate_error("42601", "TENANT ID must be a numeric value"))?;
    let tenant_id = TenantId::new(tid);

    // Prevent dropping tenant 0 (system).
    if tid == 0 {
        return Err(sqlstate_error("42501", "cannot drop system tenant (0)"));
    }

    state.audit_record(
        AuditEvent::TenantDeleted,
        Some(tenant_id),
        &identity.username,
        &format!("dropped tenant {tenant_id}"),
    );

    Ok(vec![Response::Execution(Tag::new("DROP TENANT"))])
}
