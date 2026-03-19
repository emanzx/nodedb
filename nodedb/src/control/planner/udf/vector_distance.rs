//! `vector_distance(field, query_vector)` — stub UDF for DataFusion type checking.
//!
//! This function is never actually executed on the Control Plane. It exists
//! solely so DataFusion can parse `ORDER BY vector_distance(embedding, ARRAY[...])`.
//! The PlanConverter recognizes this function name in Sort expressions and
//! rewrites the plan to `PhysicalPlan::VectorSearch`, which executes on the
//! Data Plane via HNSW index traversal.

use std::any::Any;

use datafusion::arrow::array::Float64Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

#[derive(Debug)]
pub struct VectorDistance {
    signature: Signature,
}

impl VectorDistance {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    // vector_distance(field_name, ARRAY[f32, ...])
                    TypeSignature::Any(2),
                ],
                // Volatile because result depends on index state.
                Volatility::Volatile,
            ),
        }
    }
}

impl Default for VectorDistance {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for VectorDistance {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "vector_distance"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_batch(&self, _args: &[ColumnarValue], num_rows: usize) -> DfResult<ColumnarValue> {
        // This function should never execute on the Control Plane.
        // If it does, it means the PlanConverter didn't rewrite the plan.
        // Return zeros as a safe fallback.
        let array = Float64Array::from(vec![0.0f64; num_rows]);
        Ok(ColumnarValue::Array(std::sync::Arc::new(array)))
    }
}
