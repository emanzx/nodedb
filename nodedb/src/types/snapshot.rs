/// Serializable snapshot of a tenant's Data Plane state.
///
/// Shared between Control Plane (backup/restore DDL) and Data Plane
/// (snapshot creation/restoration). Lives in `types` to avoid
/// cross-plane module visibility leaks.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct TenantDataSnapshot {
    /// Sparse engine documents: [(key, value_bytes), ...]
    pub documents: Vec<(String, Vec<u8>)>,
    /// Sparse engine index entries: [(key, value_bytes), ...]
    pub indexes: Vec<(String, Vec<u8>)>,
}
