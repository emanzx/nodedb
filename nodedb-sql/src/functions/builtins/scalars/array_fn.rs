// SPDX-License-Identifier: Apache-2.0

//! Array engine scalar function registrations.

use nodedb_types::columnar::ColumnType;

use crate::functions::arg_types;
use crate::functions::registry::{FunctionCategory::Scalar, FunctionMeta, SearchTrigger};

use super::super::helpers::m;

pub(super) fn array_fn_functions() -> Vec<FunctionMeta> {
    vec![
        m(
            "array_slice",
            Scalar,
            2,
            4,
            SearchTrigger::ArraySlice,
            None,
            arg_types::ARRAY_SLICE_ARGS,
        ),
        m(
            "array_project",
            Scalar,
            2,
            2,
            SearchTrigger::ArrayProject,
            None,
            arg_types::ARRAY_PROJECT_ARGS,
        ),
        m(
            "array_agg",
            Scalar,
            3,
            4,
            SearchTrigger::ArrayAgg,
            None,
            arg_types::ARRAY_AGG_ARGS,
        ),
        m(
            "array_elementwise",
            Scalar,
            4,
            4,
            SearchTrigger::ArrayElementwise,
            None,
            arg_types::ARRAY_ELEMENTWISE_ARGS,
        ),
        m(
            "array_flush",
            Scalar,
            1,
            1,
            SearchTrigger::ArrayFlush,
            Some(ColumnType::Bool),
            arg_types::ARRAY_MAINT_ARGS,
        ),
        m(
            "array_compact",
            Scalar,
            1,
            1,
            SearchTrigger::ArrayCompact,
            Some(ColumnType::Bool),
            arg_types::ARRAY_MAINT_ARGS,
        ),
    ]
}
