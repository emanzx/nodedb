// SPDX-License-Identifier: BUSL-1.1

//! Data types for the sparse primary index.

/// Default block size: 1024 rows, aligned with FastLanes block size.
pub const DEFAULT_BLOCK_SIZE: usize = 1024;

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

/// Sparse primary index for a single partition.
///
/// Enables sub-partition block-level skip during query.
#[derive(Debug, Clone)]
pub struct SparseIndex {
    /// Rows per block (default 1024).
    pub block_size: u32,
    /// Column names in order (matches the schema at flush time).
    pub column_names: Vec<String>,
    /// Per-block metadata, sorted by row_offset.
    pub blocks: Vec<BlockEntry>,
}

/// Metadata for a single block within a partition.
#[derive(Debug, Clone)]
pub struct BlockEntry {
    /// Starting row index of this block.
    pub row_offset: u32,
    /// Number of rows in this block (last block may be smaller).
    pub row_count: u32,
    /// Minimum timestamp in this block.
    pub min_ts: i64,
    /// Maximum timestamp in this block.
    pub max_ts: i64,
    /// Per-column min/max statistics. Indexed by column position.
    /// For non-numeric columns (Symbol), values are NaN.
    pub column_stats: Vec<BlockColumnStats>,
}

/// Min/max statistics for a single column within a block.
#[derive(Debug, Clone, Copy)]
pub struct BlockColumnStats {
    pub min: f64,
    pub max: f64,
}

impl BlockColumnStats {
    /// No-data sentinel (for symbol columns or empty blocks).
    pub fn none() -> Self {
        Self {
            min: f64::NAN,
            max: f64::NAN,
        }
    }

    /// Whether this stat has valid numeric data (not NaN).
    pub fn is_valid(&self) -> bool {
        !self.min.is_nan() && !self.max.is_nan()
    }
}

// ---------------------------------------------------------------------------
// Predicate types for block-level pushdown
// ---------------------------------------------------------------------------

/// A simple predicate for block-level pushdown.
#[derive(Debug, Clone)]
pub enum BlockPredicate {
    /// Column value > threshold.
    GreaterThan { column_idx: usize, threshold: f64 },
    /// Column value >= threshold.
    GreaterThanOrEqual { column_idx: usize, threshold: f64 },
    /// Column value < threshold.
    LessThan { column_idx: usize, threshold: f64 },
    /// Column value <= threshold.
    LessThanOrEqual { column_idx: usize, threshold: f64 },
    /// Column value between [low, high] inclusive.
    Between {
        column_idx: usize,
        low: f64,
        high: f64,
    },
}

impl BlockPredicate {
    /// Check if a block could possibly contain rows matching this predicate.
    ///
    /// Returns `true` if the block cannot be skipped (might contain matches).
    /// Returns `false` if the block can definitely be skipped.
    pub fn might_match(&self, stats: &BlockColumnStats) -> bool {
        if !stats.is_valid() {
            return true; // Can't skip blocks with no stats.
        }
        match self {
            Self::GreaterThan { threshold, .. } => stats.max > *threshold,
            Self::GreaterThanOrEqual { threshold, .. } => stats.max >= *threshold,
            Self::LessThan { threshold, .. } => stats.min < *threshold,
            Self::LessThanOrEqual { threshold, .. } => stats.min <= *threshold,
            Self::Between { low, high, .. } => stats.max >= *low && stats.min <= *high,
        }
    }

    /// Column index this predicate applies to.
    pub fn column_idx(&self) -> usize {
        match self {
            Self::GreaterThan { column_idx, .. }
            | Self::GreaterThanOrEqual { column_idx, .. }
            | Self::LessThan { column_idx, .. }
            | Self::LessThanOrEqual { column_idx, .. }
            | Self::Between { column_idx, .. } => *column_idx,
        }
    }
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum SparseIndexError {
    Truncated,
    UnsupportedVersion(u32),
    Corrupt(String),
}

impl std::fmt::Display for SparseIndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => write!(f, "sparse index data truncated"),
            Self::UnsupportedVersion(v) => {
                write!(f, "sparse index unsupported version: {v}")
            }
            Self::Corrupt(msg) => write!(f, "sparse index corrupt: {msg}"),
        }
    }
}

impl std::error::Error for SparseIndexError {}
