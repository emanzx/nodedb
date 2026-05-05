//! `KvHashTable` struct definition, constructor, and introspection methods.

use std::collections::HashMap;

use nodedb_types::Surrogate;

use super::super::entry::KvEntry;
use super::super::hash_helpers::{extract_value_from, read_value_from};
use super::super::slab::SlabAllocator;

/// Metadata about a KV entry, returned by [`KvHashTable::get_entry_meta`].
///
/// Used by [`super::super::engine::KvEngine`] to retrieve the original `expire_at_ms`
/// when cancelling old expiry entries in the timing wheel.
#[derive(Debug, Clone, Copy)]
pub struct EntryMeta {
    /// Whether this key has a TTL set (`expire_at_ms != NO_EXPIRY`).
    pub has_ttl: bool,
    /// Absolute expiry timestamp in milliseconds, or [`NO_EXPIRY`] if persistent.
    pub expire_at_ms: u64,
}

/// Robin Hood hash table with incremental rehash.
///
/// Uses two internal tables during rehash: `primary` (new, larger) and
/// `rehash_source` (old, being migrated). All lookups check both tables.
/// PUTs go to the primary. Each PUT migrates `rehash_batch_size` entries
/// from old to new.
pub struct KvHashTable {
    /// Primary slot array.
    pub(super) slots: Vec<Option<KvEntry>>,
    /// Number of occupied slots in the primary table.
    pub(super) len: usize,
    /// Capacity (number of slots) — always a power of two.
    pub(super) capacity: usize,
    /// Load factor threshold that triggers rehash (0.0–1.0).
    pub(super) load_factor_threshold: f32,
    /// Entries migrated per PUT during incremental rehash.
    pub(super) rehash_batch_size: usize,

    /// Old table being migrated (during incremental rehash).
    pub(super) rehash_source: Option<Vec<Option<KvEntry>>>,
    /// Next index to scan in the old table during incremental rehash.
    pub(super) rehash_cursor: usize,

    /// Slab allocator for overflow values (fixed-size tiers, O(1) alloc/free).
    pub(super) overflow: SlabAllocator,

    /// Inline value threshold in bytes.
    pub(super) inline_threshold: usize,

    /// Reverse mapping `surrogate.0 → primary key bytes`. Populated on
    /// inserts that carry a non-zero surrogate. Read paths consult this
    /// to translate a surrogate back to its user-facing primary key
    /// (e.g. when emitting bitmap-join results).
    pub(super) surrogate_to_key: HashMap<u32, Vec<u8>>,
}

impl KvHashTable {
    /// Create a new hash table with the given initial capacity.
    ///
    /// `capacity` is rounded up to the next power of two.
    /// `inline_threshold` determines whether values are stored inline or in the overflow pool.
    pub fn new(
        capacity: usize,
        load_factor_threshold: f32,
        rehash_batch_size: usize,
        inline_threshold: usize,
    ) -> Self {
        let capacity = capacity.next_power_of_two().max(4);
        Self {
            slots: vec![None; capacity],
            len: 0,
            capacity,
            load_factor_threshold,
            rehash_batch_size,
            rehash_source: None,
            rehash_cursor: 0,
            overflow: SlabAllocator::new(),
            inline_threshold,
            surrogate_to_key: HashMap::new(),
        }
    }

    /// Look up the primary key bytes for a stable surrogate identity.
    pub fn key_for_surrogate(&self, surrogate: Surrogate) -> Option<&[u8]> {
        self.surrogate_to_key
            .get(&surrogate.0)
            .map(|v| v.as_slice())
    }

    /// Number of bound surrogates in the reverse map. Test/diagnostic only.
    #[cfg(test)]
    pub fn surrogate_count(&self) -> usize {
        self.surrogate_to_key.len()
    }

    /// Number of entries in the table (including entries still in rehash source).
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the table is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Export all entries for snapshot/backup.
    ///
    /// Returns `(key_bytes, value_bytes, expire_at_ms)` for every live entry.
    pub fn export_entries(&self) -> Vec<(Vec<u8>, Vec<u8>, u64)> {
        let mut result = Vec::with_capacity(self.len);
        for entry in self.slots.iter().flatten() {
            let value_bytes = extract_value_from(&entry.value, &self.overflow);
            result.push((entry.key.clone(), value_bytes, entry.expire_at_ms));
        }
        result
    }

    /// Current load factor of the primary table.
    pub fn load_factor(&self) -> f32 {
        self.len as f32 / self.capacity as f32
    }

    /// Whether an incremental rehash is in progress.
    pub fn is_rehashing(&self) -> bool {
        self.rehash_source.is_some()
    }

    /// Capacity (number of slots) of the primary table.
    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    /// Number of slots in the rehash source (0 if not rehashing).
    pub(crate) fn rehash_source_len(&self) -> usize {
        self.rehash_source.as_ref().map(|s| s.len()).unwrap_or(0)
    }

    /// Access a slot in the primary table by index.
    pub(crate) fn primary_slot(&self, idx: usize) -> Option<&KvEntry> {
        self.slots[idx].as_ref()
    }

    /// Access a slot in the rehash source by index.
    pub(crate) fn rehash_slot(&self, idx: usize) -> Option<&KvEntry> {
        self.rehash_source.as_ref().and_then(|s| s[idx].as_ref())
    }

    /// Read value bytes from an entry, resolving overflow from the pool.
    pub(crate) fn read_value<'a>(&'a self, entry: &'a KvEntry) -> &'a [u8] {
        read_value_from(entry, &self.overflow)
    }

    /// Approximate memory usage in bytes.
    pub fn mem_usage(&self) -> usize {
        let slot_size = std::mem::size_of::<Option<KvEntry>>();
        let primary = self.capacity * slot_size;
        let rehash = self
            .rehash_source
            .as_ref()
            .map(|s| s.len() * slot_size)
            .unwrap_or(0);
        let overflow = self.overflow.capacity();
        // Entry heap allocations (keys + inline values).
        let entry_heap: usize = self
            .slots
            .iter()
            .filter_map(|s| s.as_ref())
            .map(|e| e.mem_size())
            .sum();
        primary + rehash + overflow + entry_heap
    }
}
