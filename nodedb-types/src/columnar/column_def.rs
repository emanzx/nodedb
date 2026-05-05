//! [`ColumnDef`] and [`ColumnModifier`] — typed column definitions for strict
//! document and columnar collections.

use std::fmt;

use serde::{Deserialize, Serialize};

use super::column_type::ColumnType;

/// Column-level modifiers that designate special engine roles.
///
/// These tell the engine which column serves a specialized purpose.
/// Extensible for future column roles (e.g., `PartitionKey`, `SortKey`).
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(c_enum)]
#[repr(u8)]
pub enum ColumnModifier {
    /// This column is the time-partitioning key (timeseries profile).
    /// Exactly one required for timeseries collections.
    TimeKey = 0,
    /// This column has an automatic R-tree spatial index (spatial profile).
    /// Exactly one required for spatial collections.
    SpatialIndex = 1,
}

/// A single column definition in a strict document or columnar schema.
///
/// `#[non_exhaustive]` — new fields may be added (e.g. column-level
/// compression hints, foreign-key metadata).
#[non_exhaustive]
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ColumnDef {
    pub name: String,
    pub column_type: ColumnType,
    pub nullable: bool,
    pub default: Option<String>,
    pub primary_key: bool,
    /// Column-level modifiers (TIME_KEY, SPATIAL_INDEX, etc.).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub modifiers: Vec<ColumnModifier>,
    /// GENERATED ALWAYS AS expression (serialized SqlExpr JSON).
    /// When set, this column is computed at write time, not supplied by the user.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generated_expr: Option<String>,
    /// Column names this generated column depends on.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub generated_deps: Vec<String>,
    /// Schema version at which this column was added. Original columns have
    /// version 1 (the default). Columns added via `ALTER ADD COLUMN` record
    /// the schema version after the bump so the reader can build a physical
    /// sub-schema for tuples written under older versions.
    #[serde(default = "default_added_at_version")]
    pub added_at_version: u32,
}

fn default_added_at_version() -> u32 {
    1
}

impl ColumnDef {
    pub fn required(name: impl Into<String>, column_type: ColumnType) -> Self {
        Self {
            name: name.into(),
            column_type,
            nullable: false,
            default: None,
            primary_key: false,
            modifiers: Vec::new(),
            generated_expr: None,
            generated_deps: Vec::new(),
            added_at_version: 1,
        }
    }

    pub fn nullable(name: impl Into<String>, column_type: ColumnType) -> Self {
        Self {
            name: name.into(),
            column_type,
            nullable: true,
            default: None,
            primary_key: false,
            modifiers: Vec::new(),
            generated_expr: None,
            generated_deps: Vec::new(),
            added_at_version: 1,
        }
    }

    pub fn with_primary_key(mut self) -> Self {
        self.primary_key = true;
        self.nullable = false;
        self
    }

    /// Check if this column has the TIME_KEY modifier.
    pub fn is_time_key(&self) -> bool {
        self.modifiers.contains(&ColumnModifier::TimeKey)
    }

    /// Check if this column has the SPATIAL_INDEX modifier.
    pub fn is_spatial_index(&self) -> bool {
        self.modifiers.contains(&ColumnModifier::SpatialIndex)
    }

    pub fn with_default(mut self, expr: impl Into<String>) -> Self {
        self.default = Some(expr.into());
        self
    }
}

impl fmt::Display for ColumnDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.name, self.column_type)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        if self.primary_key {
            write!(f, " PRIMARY KEY")?;
        }
        if let Some(ref d) = self.default {
            write!(f, " DEFAULT {d}")?;
        }
        Ok(())
    }
}
