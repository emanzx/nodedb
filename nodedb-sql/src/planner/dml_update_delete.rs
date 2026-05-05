//! UPDATE, DELETE, and TRUNCATE planning — extracted from `dml.rs`.

use sqlparser::ast;

use super::super::dml_helpers::{extract_point_keys, extract_table_name_from_table_with_joins};
use crate::engine_rules::{self, DeleteParams, UpdateParams};
use crate::error::{Result, SqlError};
use crate::parser::normalize::{SCHEMA_QUALIFIED_MSG, normalize_object_name_checked};
use crate::resolver::expr::convert_expr;
use crate::types::*;

/// Plan an UPDATE statement.
pub fn plan_update(stmt: &ast::Statement, catalog: &dyn SqlCatalog) -> Result<Vec<SqlPlan>> {
    let ast::Statement::Update(update) = stmt else {
        return Err(SqlError::Parse {
            detail: "expected UPDATE statement".into(),
        });
    };

    let table_name = extract_table_name_from_table_with_joins(&update.table)?;
    let info = catalog
        .get_collection(&table_name)?
        .ok_or_else(|| SqlError::UnknownTable {
            name: table_name.clone(),
        })?;

    let assigns: Vec<(String, SqlExpr)> = update
        .assignments
        .iter()
        .map(|a| {
            let col = match &a.target {
                ast::AssignmentTarget::ColumnName(name) => {
                    if name.0.len() > 1 {
                        return Err(SqlError::Unsupported {
                            detail: format!(
                                "qualified column name in SET target: {SCHEMA_QUALIFIED_MSG}"
                            ),
                        });
                    }
                    normalize_object_name_checked(name)?
                }
                ast::AssignmentTarget::Tuple(names) => names
                    .iter()
                    .map(normalize_object_name_checked)
                    .collect::<Result<Vec<_>>>()?
                    .join(","),
            };
            let val = convert_expr(&a.value)?;
            Ok((col, val))
        })
        .collect::<Result<_>>()?;

    let filters = match &update.selection {
        Some(expr) => super::super::select::convert_where_to_filters(expr)?,
        None => Vec::new(),
    };

    let target_keys = extract_point_keys(update.selection.as_ref(), &info);

    let rules = engine_rules::resolve_engine_rules(info.engine);
    rules.plan_update(UpdateParams {
        collection: table_name,
        assignments: assigns,
        filters,
        target_keys,
        returning: update.returning.is_some(),
    })
}

/// Plan a DELETE statement.
pub fn plan_delete(stmt: &ast::Statement, catalog: &dyn SqlCatalog) -> Result<Vec<SqlPlan>> {
    let ast::Statement::Delete(delete) = stmt else {
        return Err(SqlError::Parse {
            detail: "expected DELETE statement".into(),
        });
    };

    let from_tables = match &delete.from {
        ast::FromTable::WithFromKeyword(tables) | ast::FromTable::WithoutKeyword(tables) => tables,
    };
    let table_name =
        extract_table_name_from_table_with_joins(from_tables.first().ok_or_else(|| {
            SqlError::Parse {
                detail: "DELETE requires a FROM table".into(),
            }
        })?)?;
    let info = catalog
        .get_collection(&table_name)?
        .ok_or_else(|| SqlError::UnknownTable {
            name: table_name.clone(),
        })?;

    let filters = match &delete.selection {
        Some(expr) => super::super::select::convert_where_to_filters(expr)?,
        None => Vec::new(),
    };

    let target_keys = extract_point_keys(delete.selection.as_ref(), &info);

    let rules = engine_rules::resolve_engine_rules(info.engine);
    rules.plan_delete(DeleteParams {
        collection: table_name,
        filters,
        target_keys,
    })
}

/// Plan a TRUNCATE statement.
pub fn plan_truncate_stmt(stmt: &ast::Statement) -> Result<Vec<SqlPlan>> {
    let ast::Statement::Truncate(truncate) = stmt else {
        return Err(SqlError::Parse {
            detail: "expected TRUNCATE statement".into(),
        });
    };
    let restart_identity = matches!(
        truncate.identity,
        Some(sqlparser::ast::TruncateIdentityOption::Restart)
    );
    truncate
        .table_names
        .iter()
        .map(|t| {
            Ok(SqlPlan::Truncate {
                collection: normalize_object_name_checked(&t.name)?,
                restart_identity,
            })
        })
        .collect()
}
