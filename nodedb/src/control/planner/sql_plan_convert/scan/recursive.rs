//! Recursive (CTE-style) scan converter.

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::*;
use crate::types::VShardId;

use super::super::super::physical::{PhysicalTask, PostSetOp};
use super::super::filter::serialize_filters;
use super::super::scan_params::RecursiveScanParams;

pub(in crate::control::planner::sql_plan_convert) fn convert_recursive_scan(
    p: RecursiveScanParams<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let vshard = VShardId::from_collection(p.collection);
    Ok(vec![PhysicalTask {
        tenant_id: p.tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Query(QueryOp::RecursiveScan {
            collection: p.collection.into(),
            base_filters: serialize_filters(p.base_filters)?,
            recursive_filters: serialize_filters(p.recursive_filters)?,
            join_link: p.join_link.clone(),
            max_iterations: *p.max_iterations,
            distinct: *p.distinct,
            limit: *p.limit,
        }),
        post_set_op: PostSetOp::None,
    }])
}
