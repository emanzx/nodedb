//! Collection type enum shared between Origin and Lite.
//!
//! Determines routing, storage format, and query execution strategy.

use serde::{Deserialize, Serialize};

use crate::columnar::{ColumnarProfile, DocumentMode};

/// The type of a collection, determining its storage engine and query behavior.
///
/// Two top-level modes:
/// - `Document`: B-tree storage in redb (schemaless MessagePack or strict Binary Tuples).
/// - `Columnar`: Compressed segment files with profile specialization (plain, timeseries, spatial).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "storage")]
pub enum CollectionType {
    /// Document storage in redb B-tree.
    /// Schemaless (MessagePack) or strict (Binary Tuples).
    Document(DocumentMode),
    /// Columnar storage in compressed segment files.
    /// Profile determines constraints and specialized behavior.
    Columnar(ColumnarProfile),
}

impl Default for CollectionType {
    fn default() -> Self {
        Self::Document(DocumentMode::default())
    }
}

impl CollectionType {
    /// Schemaless document (default, backward compatible).
    pub fn document() -> Self {
        Self::Document(DocumentMode::Schemaless)
    }

    /// Strict document with schema.
    pub fn strict(schema: crate::columnar::StrictSchema) -> Self {
        Self::Document(DocumentMode::Strict(schema))
    }

    /// Plain columnar (general analytics).
    pub fn columnar() -> Self {
        Self::Columnar(ColumnarProfile::Plain)
    }

    /// Columnar with timeseries profile.
    pub fn timeseries(time_key: impl Into<String>, interval: impl Into<String>) -> Self {
        Self::Columnar(ColumnarProfile::Timeseries {
            time_key: time_key.into(),
            interval: interval.into(),
        })
    }

    /// Columnar with spatial profile.
    pub fn spatial(geometry_column: impl Into<String>) -> Self {
        Self::Columnar(ColumnarProfile::Spatial {
            geometry_column: geometry_column.into(),
            auto_rtree: true,
            auto_geohash: true,
        })
    }

    pub fn is_document(&self) -> bool {
        matches!(self, Self::Document(_))
    }

    pub fn is_columnar(&self) -> bool {
        matches!(self, Self::Columnar(_))
    }

    pub fn is_timeseries(&self) -> bool {
        matches!(self, Self::Columnar(ColumnarProfile::Timeseries { .. }))
    }

    pub fn is_strict(&self) -> bool {
        matches!(self, Self::Document(DocumentMode::Strict(_)))
    }

    pub fn is_schemaless(&self) -> bool {
        matches!(self, Self::Document(DocumentMode::Schemaless))
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Document(DocumentMode::Schemaless) => "document",
            Self::Document(DocumentMode::Strict(_)) => "strict",
            Self::Columnar(ColumnarProfile::Plain) => "columnar",
            Self::Columnar(ColumnarProfile::Timeseries { .. }) => "timeseries",
            Self::Columnar(ColumnarProfile::Spatial { .. }) => "columnar:spatial",
        }
    }

    /// Get the document mode, if this is a document collection.
    pub fn document_mode(&self) -> Option<&DocumentMode> {
        match self {
            Self::Document(mode) => Some(mode),
            Self::Columnar(_) => None,
        }
    }

    /// Get the columnar profile, if this is a columnar collection.
    pub fn columnar_profile(&self) -> Option<&ColumnarProfile> {
        match self {
            Self::Columnar(profile) => Some(profile),
            Self::Document(_) => None,
        }
    }
}

impl std::fmt::Display for CollectionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for CollectionType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "document" | "doc" => Ok(Self::document()),
            "strict" => Ok(Self::Document(DocumentMode::Strict(
                // Placeholder — real schema comes from DDL parsing, not FromStr.
                // FromStr only resolves the storage mode; schema is attached separately.
                crate::columnar::StrictSchema {
                    columns: vec![],
                    version: 1,
                },
            ))),
            "columnar" => Ok(Self::columnar()),
            "timeseries" | "ts" => Ok(Self::timeseries("time", "1h")),
            other => Err(format!("unknown collection type: '{other}'")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_schemaless_document() {
        let ct = CollectionType::default();
        assert!(ct.is_document());
        assert!(ct.is_schemaless());
        assert!(!ct.is_columnar());
        assert!(!ct.is_timeseries());
    }

    #[test]
    fn factory_methods() {
        assert!(CollectionType::document().is_schemaless());
        assert!(CollectionType::columnar().is_columnar());
        assert!(CollectionType::timeseries("time", "1h").is_timeseries());
        assert!(CollectionType::spatial("geom").is_columnar());
    }

    #[test]
    fn serde_roundtrip_document() {
        let ct = CollectionType::document();
        let json = serde_json::to_string(&ct).unwrap();
        let back: CollectionType = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ct);
    }

    #[test]
    fn serde_roundtrip_columnar() {
        let ct = CollectionType::columnar();
        let json = serde_json::to_string(&ct).unwrap();
        let back: CollectionType = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ct);
    }

    #[test]
    fn serde_roundtrip_timeseries() {
        let ct = CollectionType::timeseries("ts", "1h");
        let json = serde_json::to_string(&ct).unwrap();
        let back: CollectionType = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ct);
    }

    #[test]
    fn display() {
        assert_eq!(CollectionType::document().to_string(), "document");
        assert_eq!(CollectionType::columnar().to_string(), "columnar");
        assert_eq!(
            CollectionType::timeseries("time", "1h").to_string(),
            "timeseries"
        );
    }

    #[test]
    fn from_str() {
        assert!("document".parse::<CollectionType>().unwrap().is_document());
        assert!("columnar".parse::<CollectionType>().unwrap().is_columnar());
        assert!(
            "timeseries"
                .parse::<CollectionType>()
                .unwrap()
                .is_timeseries()
        );
        assert!("ts".parse::<CollectionType>().unwrap().is_timeseries());
        assert!("unknown".parse::<CollectionType>().is_err());
    }

    #[test]
    fn accessors() {
        let ct = CollectionType::timeseries("time", "1h");
        assert!(ct.columnar_profile().is_some());
        assert!(ct.document_mode().is_none());

        let doc = CollectionType::document();
        assert!(doc.document_mode().is_some());
        assert!(doc.columnar_profile().is_none());
    }
}
