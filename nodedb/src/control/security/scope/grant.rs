//! Scope grant management: GRANT/REVOKE SCOPE TO/FROM ORG/USER/TEAM.
//!
//! Effective scopes for a user = user scopes UNION team scopes UNION org scopes.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use tracing::info;

use crate::control::security::catalog::{StoredScopeGrant, SystemCatalog};

/// In-memory scope grant record.
#[derive(Debug, Clone)]
pub struct ScopeGrant {
    pub scope_name: String,
    pub grantee_type: String,
    pub grantee_id: String,
    pub granted_by: String,
    pub granted_at: u64,
}

impl ScopeGrant {
    fn from_stored(s: &StoredScopeGrant) -> Self {
        Self {
            scope_name: s.scope_name.clone(),
            grantee_type: s.grantee_type.clone(),
            grantee_id: s.grantee_id.clone(),
            granted_by: s.granted_by.clone(),
            granted_at: s.granted_at,
        }
    }

    fn to_stored(&self) -> StoredScopeGrant {
        StoredScopeGrant {
            scope_name: self.scope_name.clone(),
            grantee_type: self.grantee_type.clone(),
            grantee_id: self.grantee_id.clone(),
            granted_by: self.granted_by.clone(),
            granted_at: self.granted_at,
        }
    }
}

/// Thread-safe scope grant store.
pub struct ScopeGrantStore {
    /// Key: `"{scope}:{type}:{id}"` → grant.
    grants: RwLock<HashMap<String, ScopeGrant>>,
    catalog: Option<SystemCatalog>,
}

impl ScopeGrantStore {
    pub fn new() -> Self {
        Self {
            grants: RwLock::new(HashMap::new()),
            catalog: None,
        }
    }

    pub fn open(catalog: SystemCatalog) -> crate::Result<Self> {
        let stored = catalog.load_all_scope_grants()?;
        let mut grants = HashMap::with_capacity(stored.len());
        for s in &stored {
            let key = grant_key(&s.scope_name, &s.grantee_type, &s.grantee_id);
            grants.insert(key, ScopeGrant::from_stored(s));
        }
        if !grants.is_empty() {
            info!(count = grants.len(), "scope grants loaded from catalog");
        }
        Ok(Self {
            grants: RwLock::new(grants),
            catalog: Some(catalog),
        })
    }

    /// Grant a scope to a user, role, org, or team.
    pub fn grant(
        &self,
        scope_name: &str,
        grantee_type: &str,
        grantee_id: &str,
        granted_by: &str,
    ) -> crate::Result<()> {
        let record = ScopeGrant {
            scope_name: scope_name.into(),
            grantee_type: grantee_type.into(),
            grantee_id: grantee_id.into(),
            granted_by: granted_by.into(),
            granted_at: now_secs(),
        };

        if let Some(ref catalog) = self.catalog {
            catalog.put_scope_grant(&record.to_stored())?;
        }

        let key = grant_key(scope_name, grantee_type, grantee_id);
        let mut grants = self.grants.write().unwrap_or_else(|p| p.into_inner());
        grants.insert(key, record);
        info!(scope = %scope_name, grantee_type, grantee_id, "scope granted");
        Ok(())
    }

    /// Revoke a scope grant.
    pub fn revoke(
        &self,
        scope_name: &str,
        grantee_type: &str,
        grantee_id: &str,
    ) -> crate::Result<bool> {
        if let Some(ref catalog) = self.catalog {
            catalog.delete_scope_grant(scope_name, grantee_type, grantee_id)?;
        }
        let key = grant_key(scope_name, grantee_type, grantee_id);
        let mut grants = self.grants.write().unwrap_or_else(|p| p.into_inner());
        Ok(grants.remove(&key).is_some())
    }

    /// Get all scope names granted to a specific grantee.
    pub fn scopes_for(&self, grantee_type: &str, grantee_id: &str) -> Vec<String> {
        let grants = self.grants.read().unwrap_or_else(|p| p.into_inner());
        grants
            .values()
            .filter(|g| g.grantee_type == grantee_type && g.grantee_id == grantee_id)
            .map(|g| g.scope_name.clone())
            .collect()
    }

    /// Resolve effective scopes for a user.
    ///
    /// Collects: user's direct scopes + org scopes for each org membership.
    /// `org_ids` = list of org IDs the user belongs to.
    pub fn effective_scopes(&self, user_id: &str, org_ids: &[String]) -> HashSet<String> {
        let grants = self.grants.read().unwrap_or_else(|p| p.into_inner());
        let mut effective = HashSet::new();

        for g in grants.values() {
            // Direct user grant.
            if g.grantee_type == "user" && g.grantee_id == user_id {
                effective.insert(g.scope_name.clone());
            }
            // Org grant (user inherits via membership).
            if g.grantee_type == "org" && org_ids.contains(&g.grantee_id) {
                effective.insert(g.scope_name.clone());
            }
        }

        effective
    }

    /// Check if a user (directly or via orgs) has a specific scope.
    pub fn has_scope(&self, user_id: &str, org_ids: &[String], scope_name: &str) -> bool {
        self.effective_scopes(user_id, org_ids).contains(scope_name)
    }

    /// List all grants, optionally filtered by scope name.
    pub fn list(&self, scope_filter: Option<&str>) -> Vec<ScopeGrant> {
        let grants = self.grants.read().unwrap_or_else(|p| p.into_inner());
        grants
            .values()
            .filter(|g| scope_filter.is_none_or(|s| g.scope_name == s))
            .cloned()
            .collect()
    }

    pub fn count(&self) -> usize {
        self.grants.read().unwrap_or_else(|p| p.into_inner()).len()
    }
}

impl Default for ScopeGrantStore {
    fn default() -> Self {
        Self::new()
    }
}

fn grant_key(scope: &str, grantee_type: &str, grantee_id: &str) -> String {
    format!("{scope}:{grantee_type}:{grantee_id}")
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grant_and_check() {
        let store = ScopeGrantStore::new();
        store.grant("profile:read", "user", "u1", "admin").unwrap();

        assert!(store.has_scope("u1", &[], "profile:read"));
        assert!(!store.has_scope("u1", &[], "orders:write"));
        assert!(!store.has_scope("u2", &[], "profile:read"));
    }

    #[test]
    fn org_scope_inheritance() {
        let store = ScopeGrantStore::new();
        store.grant("pro:all", "org", "acme", "admin").unwrap();

        // User u1 is member of acme → inherits pro:all.
        assert!(store.has_scope("u1", &["acme".into()], "pro:all"));
        // User u2 is NOT member → doesn't inherit.
        assert!(!store.has_scope("u2", &[], "pro:all"));
    }

    #[test]
    fn effective_scopes_union() {
        let store = ScopeGrantStore::new();
        store.grant("scope_a", "user", "u1", "admin").unwrap();
        store.grant("scope_b", "org", "acme", "admin").unwrap();
        store.grant("scope_c", "org", "beta", "admin").unwrap();

        let effective = store.effective_scopes("u1", &["acme".into()]);
        assert!(effective.contains("scope_a")); // Direct user grant.
        assert!(effective.contains("scope_b")); // Via acme org.
        assert!(!effective.contains("scope_c")); // Not member of beta.
    }

    #[test]
    fn revoke_removes_grant() {
        let store = ScopeGrantStore::new();
        store.grant("s1", "user", "u1", "admin").unwrap();
        assert!(store.has_scope("u1", &[], "s1"));

        store.revoke("s1", "user", "u1").unwrap();
        assert!(!store.has_scope("u1", &[], "s1"));
    }
}
