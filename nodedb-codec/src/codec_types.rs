// SPDX-License-Identifier: Apache-2.0

//! Codec identifiers, resolved codec type, column statistics, and name parsing.
//!
//! [`ColumnCodec`] is the user-facing codec selector (includes `Auto`).
//! [`ResolvedColumnCodec`] is the on-disk form after auto-detection runs.
//! [`ColumnStatistics`] stores per-column flush-time statistics.
//! [`parse_codec_name`] is the single gate for codec names from user input.

use serde::{Deserialize, Serialize};
use zerompk::{FromMessagePack, ToMessagePack};

use crate::error::CodecError;

/// Codec identifier for per-column compression selection.
///
/// Stored in partition schema metadata so the reader knows which decoder
/// to use for each column file.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToMessagePack, FromMessagePack,
)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum ColumnCodec {
    /// Engine selects codec automatically based on column type and data
    /// distribution (analyzed at flush time).
    Auto = 0,

    // -- Cascading chains: hot/warm (lz4 terminal) --
    /// f64 metrics: ALP (decimal→int) → FastLanes → lz4.
    AlpFastLanesLz4 = 1,
    /// f64 true doubles: ALP-RD (front-bit dict) → lz4.
    AlpRdLz4 = 2,
    /// f64/i64 complex: Pcodec → lz4.
    PcodecLz4 = 3,
    /// i64 timestamps/counters: Delta → FastLanes → lz4.
    DeltaFastLanesLz4 = 4,
    /// i64/u32 raw integers: FastLanes → lz4.
    FastLanesLz4 = 5,
    /// Strings/logs: FSST (substring dict) → lz4.
    FsstLz4 = 6,

    // -- Cascading chains: cold/S3 (rANS terminal) --
    /// f64 metrics cold: ALP → FastLanes → rANS.
    AlpFastLanesRans = 7,
    /// i64 cold: Delta → FastLanes → rANS.
    DeltaFastLanesRans = 8,
    /// Strings cold: FSST → rANS.
    FsstRans = 9,

    // -- Single-step codecs used by `detect.rs` auto-selection and timeseries column writers --
    /// Gorilla XOR encoding — f64 codec selected by detect.rs for float columns.
    Gorilla = 10,
    /// DoubleDelta — timestamp codec selected by detect.rs for monotonic timestamp columns.
    DoubleDelta = 11,
    /// Delta + varint — counter codec selected by detect.rs for integer delta columns.
    Delta = 12,
    /// LZ4 block compression — for string/log columns.
    Lz4 = 13,
    /// Zstd — for cold/archived partitions.
    Zstd = 14,
    /// No compression — for pre-compressed or symbol columns.
    Raw = 15,
}

impl ColumnCodec {
    pub fn is_compressed(&self) -> bool {
        !matches!(self, Self::Raw | Self::Auto)
    }

    /// Whether this is a cascading (multi-stage) codec.
    pub fn is_cascading(&self) -> bool {
        matches!(
            self,
            Self::AlpFastLanesLz4
                | Self::AlpRdLz4
                | Self::PcodecLz4
                | Self::DeltaFastLanesLz4
                | Self::FastLanesLz4
                | Self::FsstLz4
                | Self::AlpFastLanesRans
                | Self::DeltaFastLanesRans
                | Self::FsstRans
        )
    }

    /// Whether this codec uses rANS as terminal (cold tier).
    pub fn is_cold_tier(&self) -> bool {
        matches!(
            self,
            Self::AlpFastLanesRans | Self::DeltaFastLanesRans | Self::FsstRans
        )
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::AlpFastLanesLz4 => "alp_fastlanes_lz4",
            Self::AlpRdLz4 => "alp_rd_lz4",
            Self::PcodecLz4 => "pcodec_lz4",
            Self::DeltaFastLanesLz4 => "delta_fastlanes_lz4",
            Self::FastLanesLz4 => "fastlanes_lz4",
            Self::FsstLz4 => "fsst_lz4",
            Self::AlpFastLanesRans => "alp_fastlanes_rans",
            Self::DeltaFastLanesRans => "delta_fastlanes_rans",
            Self::FsstRans => "fsst_rans",
            Self::Gorilla => "gorilla",
            Self::DoubleDelta => "double_delta",
            Self::Delta => "delta",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
            Self::Raw => "raw",
        }
    }

    /// Resolve `Auto` to a concrete codec using the provided detection result,
    /// or return an error if this is called with `Auto` where a concrete value
    /// is required (i.e. a caller forgot to run detection first).
    ///
    /// For callers that have already run detection and hold a non-`Auto`
    /// codec, this is a zero-cost newtype wrap.
    pub fn try_resolve(self) -> Result<ResolvedColumnCodec, CodecError> {
        match self {
            Self::Auto => Err(CodecError::UnresolvedAuto),
            Self::AlpFastLanesLz4 => Ok(ResolvedColumnCodec::AlpFastLanesLz4),
            Self::AlpRdLz4 => Ok(ResolvedColumnCodec::AlpRdLz4),
            Self::PcodecLz4 => Ok(ResolvedColumnCodec::PcodecLz4),
            Self::DeltaFastLanesLz4 => Ok(ResolvedColumnCodec::DeltaFastLanesLz4),
            Self::FastLanesLz4 => Ok(ResolvedColumnCodec::FastLanesLz4),
            Self::FsstLz4 => Ok(ResolvedColumnCodec::FsstLz4),
            Self::AlpFastLanesRans => Ok(ResolvedColumnCodec::AlpFastLanesRans),
            Self::DeltaFastLanesRans => Ok(ResolvedColumnCodec::DeltaFastLanesRans),
            Self::FsstRans => Ok(ResolvedColumnCodec::FsstRans),
            Self::Gorilla => Ok(ResolvedColumnCodec::Gorilla),
            Self::DoubleDelta => Ok(ResolvedColumnCodec::DoubleDelta),
            Self::Delta => Ok(ResolvedColumnCodec::Delta),
            Self::Lz4 => Ok(ResolvedColumnCodec::Lz4),
            Self::Zstd => Ok(ResolvedColumnCodec::Zstd),
            Self::Raw => Ok(ResolvedColumnCodec::Raw),
        }
    }
}

impl std::fmt::Display for ColumnCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Parse a user-supplied codec name string into a [`ColumnCodec`].
///
/// Accepts **only** the exact canonical lowercase snake_case forms produced by
/// [`ColumnCodec::as_str()`]. No case folding, no hyphen variants, no aliases.
/// This is the single gate that must be used whenever codec names enter the
/// system from user input (DDL `WITH (codec=…)`, REST params, config files).
///
/// # Errors
///
/// Returns [`CodecError::UnknownCodec`] if `s` is not an exact match.
pub fn parse_codec_name(s: &str) -> Result<ColumnCodec, CodecError> {
    match s {
        "auto" => Ok(ColumnCodec::Auto),
        "alp_fastlanes_lz4" => Ok(ColumnCodec::AlpFastLanesLz4),
        "alp_rd_lz4" => Ok(ColumnCodec::AlpRdLz4),
        "pcodec_lz4" => Ok(ColumnCodec::PcodecLz4),
        "delta_fastlanes_lz4" => Ok(ColumnCodec::DeltaFastLanesLz4),
        "fastlanes_lz4" => Ok(ColumnCodec::FastLanesLz4),
        "fsst_lz4" => Ok(ColumnCodec::FsstLz4),
        "alp_fastlanes_rans" => Ok(ColumnCodec::AlpFastLanesRans),
        "delta_fastlanes_rans" => Ok(ColumnCodec::DeltaFastLanesRans),
        "fsst_rans" => Ok(ColumnCodec::FsstRans),
        "gorilla" => Ok(ColumnCodec::Gorilla),
        "double_delta" => Ok(ColumnCodec::DoubleDelta),
        "delta" => Ok(ColumnCodec::Delta),
        "lz4" => Ok(ColumnCodec::Lz4),
        "zstd" => Ok(ColumnCodec::Zstd),
        "raw" => Ok(ColumnCodec::Raw),
        _ => Err(CodecError::UnknownCodec {
            name: s.to_owned(),
            valid: "auto, alp_fastlanes_lz4, alp_rd_lz4, pcodec_lz4, delta_fastlanes_lz4, \
                    fastlanes_lz4, fsst_lz4, alp_fastlanes_rans, delta_fastlanes_rans, \
                    fsst_rans, gorilla, double_delta, delta, lz4, zstd, raw",
        }),
    }
}

/// A `ColumnCodec` that has been resolved away from `Auto`.
///
/// Invariant: this type can never hold the `Auto` variant. All on-disk
/// column headers (`ColumnMeta.codec`) and per-column statistics
/// (`ColumnStatistics.codec`) use `ResolvedColumnCodec`, making it a
/// compile-time guarantee that `Auto` never survives to disk.
///
/// The `#[repr(u8)]` discriminants are **identical** to the corresponding
/// `ColumnCodec` discriminants so that on-disk byte values are unchanged.
/// `Auto` (discriminant 0) is intentionally absent.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToMessagePack, FromMessagePack,
)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum ResolvedColumnCodec {
    AlpFastLanesLz4 = 1,
    AlpRdLz4 = 2,
    PcodecLz4 = 3,
    DeltaFastLanesLz4 = 4,
    FastLanesLz4 = 5,
    FsstLz4 = 6,
    AlpFastLanesRans = 7,
    DeltaFastLanesRans = 8,
    FsstRans = 9,
    Gorilla = 10,
    DoubleDelta = 11,
    Delta = 12,
    Lz4 = 13,
    Zstd = 14,
    Raw = 15,
}

impl ResolvedColumnCodec {
    /// Convert back to `ColumnCodec` for use with codec pipelines that
    /// accept the full enum (e.g. `encode_i64_pipeline`, `decode_f64_pipeline`).
    pub fn into_column_codec(self) -> ColumnCodec {
        match self {
            Self::AlpFastLanesLz4 => ColumnCodec::AlpFastLanesLz4,
            Self::AlpRdLz4 => ColumnCodec::AlpRdLz4,
            Self::PcodecLz4 => ColumnCodec::PcodecLz4,
            Self::DeltaFastLanesLz4 => ColumnCodec::DeltaFastLanesLz4,
            Self::FastLanesLz4 => ColumnCodec::FastLanesLz4,
            Self::FsstLz4 => ColumnCodec::FsstLz4,
            Self::AlpFastLanesRans => ColumnCodec::AlpFastLanesRans,
            Self::DeltaFastLanesRans => ColumnCodec::DeltaFastLanesRans,
            Self::FsstRans => ColumnCodec::FsstRans,
            Self::Gorilla => ColumnCodec::Gorilla,
            Self::DoubleDelta => ColumnCodec::DoubleDelta,
            Self::Delta => ColumnCodec::Delta,
            Self::Lz4 => ColumnCodec::Lz4,
            Self::Zstd => ColumnCodec::Zstd,
            Self::Raw => ColumnCodec::Raw,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::AlpFastLanesLz4 => "alp_fastlanes_lz4",
            Self::AlpRdLz4 => "alp_rd_lz4",
            Self::PcodecLz4 => "pcodec_lz4",
            Self::DeltaFastLanesLz4 => "delta_fastlanes_lz4",
            Self::FastLanesLz4 => "fastlanes_lz4",
            Self::FsstLz4 => "fsst_lz4",
            Self::AlpFastLanesRans => "alp_fastlanes_rans",
            Self::DeltaFastLanesRans => "delta_fastlanes_rans",
            Self::FsstRans => "fsst_rans",
            Self::Gorilla => "gorilla",
            Self::DoubleDelta => "double_delta",
            Self::Delta => "delta",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
            Self::Raw => "raw",
        }
    }
}

impl std::fmt::Display for ResolvedColumnCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Column data type hint for codec auto-detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ColumnTypeHint {
    Timestamp,
    Float64,
    Int64,
    Symbol,
    String,
}

/// Per-column statistics computed at flush time.
///
/// Stored in partition metadata for predicate pushdown and approximate
/// query answers without decompression.
#[derive(Debug, Clone, Serialize, Deserialize, ToMessagePack, FromMessagePack)]
pub struct ColumnStatistics {
    /// Codec used for this column in this partition.
    ///
    /// Always a concrete, resolved codec — never `Auto`.
    pub codec: ResolvedColumnCodec,
    /// Number of non-null values.
    pub count: u64,
    /// Minimum value (as f64 for numeric columns, 0.0 for non-numeric).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    /// Maximum value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
    /// Sum of values (for numeric columns).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sum: Option<f64>,
    /// Number of distinct values (for symbol/tag columns).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cardinality: Option<u32>,
    /// Compressed size in bytes for this column.
    pub compressed_bytes: u64,
    /// Uncompressed size in bytes.
    pub uncompressed_bytes: u64,
}

impl ColumnStatistics {
    /// Create empty statistics with just the codec.
    pub fn new(codec: ResolvedColumnCodec) -> Self {
        Self {
            codec,
            count: 0,
            min: None,
            max: None,
            sum: None,
            cardinality: None,
            compressed_bytes: 0,
            uncompressed_bytes: 0,
        }
    }

    /// Compute statistics for an i64 column.
    pub fn from_i64(values: &[i64], codec: ResolvedColumnCodec, compressed_bytes: u64) -> Self {
        if values.is_empty() {
            return Self::new(codec);
        }

        let mut min = values[0];
        let mut max = values[0];
        let mut sum: i128 = 0;

        for &v in values {
            if v < min {
                min = v;
            }
            if v > max {
                max = v;
            }
            sum += v as i128;
        }

        Self {
            codec,
            count: values.len() as u64,
            min: Some(min as f64),
            max: Some(max as f64),
            sum: Some(sum as f64),
            cardinality: None,
            compressed_bytes,
            uncompressed_bytes: (values.len() * 8) as u64,
        }
    }

    /// Compute statistics for an f64 column.
    pub fn from_f64(values: &[f64], codec: ResolvedColumnCodec, compressed_bytes: u64) -> Self {
        if values.is_empty() {
            return Self::new(codec);
        }

        let mut min = values[0];
        let mut max = values[0];
        let mut sum: f64 = 0.0;

        for &v in values {
            if v < min {
                min = v;
            }
            if v > max {
                max = v;
            }
            sum += v;
        }

        Self {
            codec,
            count: values.len() as u64,
            min: Some(min),
            max: Some(max),
            sum: Some(sum),
            cardinality: None,
            compressed_bytes,
            uncompressed_bytes: (values.len() * 8) as u64,
        }
    }

    /// Compute statistics for a symbol column.
    pub fn from_symbols(
        values: &[u32],
        cardinality: u32,
        codec: ResolvedColumnCodec,
        compressed_bytes: u64,
    ) -> Self {
        Self {
            codec,
            count: values.len() as u64,
            min: None,
            max: None,
            sum: None,
            cardinality: Some(cardinality),
            compressed_bytes,
            uncompressed_bytes: (values.len() * 4) as u64,
        }
    }

    /// Compression ratio (uncompressed / compressed). Returns 1.0 if no data.
    pub fn compression_ratio(&self) -> f64 {
        if self.compressed_bytes == 0 {
            return 1.0;
        }
        self.uncompressed_bytes as f64 / self.compressed_bytes as f64
    }
}
