use nodedb_sql::types::{EngineType, Filter, SqlExpr, SqlValue};
use nodedb_types::Surrogate;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::*;
use crate::types::{TenantId, VShardId};

use super::super::super::physical::{PhysicalTask, PostSetOp};
use super::super::convert::ConvertContext;
use super::super::filter::serialize_filters;
use super::super::value::{
    assignments_to_update_values, sql_value_to_bytes, sql_value_to_msgpack, sql_value_to_string,
};

#[allow(clippy::too_many_arguments)]
pub(in super::super) fn convert_update(
    collection: &str,
    engine: &EngineType,
    assignments: &[(String, SqlExpr)],
    filters: &[Filter],
    target_keys: &[SqlValue],
    _returning: bool,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let vshard = VShardId::from_collection(collection);
    let filter_bytes = serialize_filters(filters)?;
    let updates = assignments_to_update_values(assignments)?;

    if matches!(engine, EngineType::KeyValue) && !target_keys.is_empty() {
        if let Some((field, _)) = assignments
            .iter()
            .find(|(_, expr)| !matches!(expr, SqlExpr::Literal(_)))
        {
            return Err(crate::Error::BadRequest {
                detail: format!(
                    "UPDATE with non-literal RHS on KV engine (field '{field}') \
                     is not yet supported; use a literal value"
                ),
            });
        }
        let mut tasks = Vec::new();
        for key in target_keys {
            let field_updates: Vec<(String, Vec<u8>)> = assignments
                .iter()
                .filter_map(|(field, expr)| {
                    if let SqlExpr::Literal(val) = expr {
                        Some((field.clone(), sql_value_to_msgpack(val)))
                    } else {
                        None
                    }
                })
                .collect();
            tasks.push(PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Kv(KvOp::FieldSet {
                    collection: collection.into(),
                    key: sql_value_to_bytes(key),
                    updates: field_updates,
                }),
                post_set_op: PostSetOp::None,
            });
        }
        return Ok(tasks);
    }

    if !target_keys.is_empty() {
        let mut tasks = Vec::new();
        for key in target_keys {
            let pk_string = sql_value_to_string(key);
            let pk_bytes = pk_string.clone().into_bytes();
            let surrogate = match ctx.surrogate_assigner.as_ref() {
                Some(a) => match a.lookup(collection, &pk_bytes)? {
                    Some(s) => s,
                    None => continue,
                },
                None => Surrogate::ZERO,
            };
            tasks.push(PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Document(DocumentOp::PointUpdate {
                    collection: collection.into(),
                    document_id: pk_string,
                    surrogate,
                    pk_bytes,
                    updates: updates.clone(),
                    returning: None,
                }),
                post_set_op: PostSetOp::None,
            });
        }
        Ok(tasks)
    } else {
        Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            plan: PhysicalPlan::Document(DocumentOp::BulkUpdate {
                collection: collection.into(),
                filters: filter_bytes,
                updates,
                returning: None,
                ollp_predicted_surrogates: None,
            }),
            post_set_op: PostSetOp::None,
        }])
    }
}

pub(in super::super) fn convert_delete(
    collection: &str,
    engine: &EngineType,
    filters: &[Filter],
    target_keys: &[SqlValue],
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let vshard = VShardId::from_collection(collection);

    if matches!(engine, EngineType::KeyValue) && !target_keys.is_empty() {
        let keys: Vec<Vec<u8>> = target_keys.iter().map(sql_value_to_bytes).collect();
        return Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            plan: PhysicalPlan::Kv(KvOp::Delete {
                collection: collection.into(),
                keys,
            }),
            post_set_op: PostSetOp::None,
        }]);
    }

    if !target_keys.is_empty() {
        let mut tasks = Vec::new();
        for key in target_keys {
            let pk_string = sql_value_to_string(key);
            let pk_bytes = pk_string.clone().into_bytes();
            let surrogate = match ctx.surrogate_assigner.as_ref() {
                Some(a) => match a.lookup(collection, &pk_bytes)? {
                    Some(s) => s,
                    None => continue,
                },
                None => Surrogate::ZERO,
            };
            tasks.push(PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Document(DocumentOp::PointDelete {
                    collection: collection.into(),
                    document_id: pk_string,
                    surrogate,
                    pk_bytes,
                    returning: None,
                }),
                post_set_op: PostSetOp::None,
            });
        }
        Ok(tasks)
    } else {
        let filter_bytes = serialize_filters(filters)?;
        Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            plan: PhysicalPlan::Document(DocumentOp::BulkDelete {
                collection: collection.into(),
                filters: filter_bytes,
                returning: None,
                ollp_predicted_surrogates: None,
            }),
            post_set_op: PostSetOp::None,
        }])
    }
}
