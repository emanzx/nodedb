//! Arrow schema construction for strict collections.

use std::sync::Arc;

use nodedb_types::columnar::{ColumnType, StrictSchema};

/// Convert an Arrow `DataType` from a `ColumnType` (for schema construction).
pub fn column_type_to_arrow(ct: &ColumnType) -> datafusion::arrow::datatypes::DataType {
    match ct {
        ColumnType::Int64 => datafusion::arrow::datatypes::DataType::Int64,
        ColumnType::Float64 => datafusion::arrow::datatypes::DataType::Float64,
        ColumnType::String => datafusion::arrow::datatypes::DataType::Utf8,
        ColumnType::Bool => datafusion::arrow::datatypes::DataType::Boolean,
        ColumnType::Bytes | ColumnType::Geometry => datafusion::arrow::datatypes::DataType::Binary,
        ColumnType::Timestamp => datafusion::arrow::datatypes::DataType::Timestamp(
            datafusion::arrow::datatypes::TimeUnit::Microsecond,
            None,
        ),
        ColumnType::Decimal => datafusion::arrow::datatypes::DataType::Utf8, // Lossless string representation
        ColumnType::Uuid => datafusion::arrow::datatypes::DataType::Utf8,
        ColumnType::Vector(_) => datafusion::arrow::datatypes::DataType::Binary, // Packed f32 bytes
    }
}

/// Build an Arrow schema from a StrictSchema (for DataFusion table registration).
pub fn strict_schema_to_arrow(schema: &StrictSchema) -> datafusion::arrow::datatypes::SchemaRef {
    use datafusion::arrow::datatypes::{Field, Schema};
    let fields: Vec<Field> = schema
        .columns
        .iter()
        .map(|col| {
            Field::new(
                &col.name,
                column_type_to_arrow(&col.column_type),
                col.nullable,
            )
        })
        .collect();
    Arc::new(Schema::new(fields))
}
