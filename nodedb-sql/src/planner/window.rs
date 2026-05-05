//! Window function extraction from SELECT projection.

use sqlparser::ast;

use crate::error::{Result, SqlError};
use crate::functions::registry::{FunctionCategory, FunctionRegistry};
use crate::parser::normalize::{SCHEMA_QUALIFIED_MSG, normalize_ident};
use crate::resolver::expr::convert_expr;
use crate::types::{SortKey, WindowSpec};
use nodedb_query::{FrameBound, WindowFrame};

/// Extract window function specifications from SELECT items.
///
/// Validates each `<func>() OVER (...)` against the function registry.
/// Names that are neither registered window functions nor aggregates
/// (PostgreSQL allows aggregates as windows) are rejected here so the
/// Data-Plane evaluator never receives an unrecognised verb.
pub fn extract_window_functions(
    items: &[ast::SelectItem],
    functions: &FunctionRegistry,
) -> Result<Vec<WindowSpec>> {
    let mut specs = Vec::new();
    for item in items {
        let (expr, alias) = match item {
            ast::SelectItem::UnnamedExpr(e) => (e, format!("{e}")),
            ast::SelectItem::ExprWithAlias { expr, alias } => (expr, normalize_ident(alias)),
            _ => continue,
        };
        if let ast::Expr::Function(func) = expr
            && func.over.is_some()
        {
            specs.push(convert_window_spec(func, &alias, functions)?);
        }
    }
    Ok(specs)
}

fn convert_window_spec(
    func: &ast::Function,
    alias: &str,
    functions: &FunctionRegistry,
) -> Result<WindowSpec> {
    if func.name.0.len() > 1 {
        let qualified: String = func
            .name
            .0
            .iter()
            .map(|p| match p {
                ast::ObjectNamePart::Identifier(ident) => ident.value.clone(),
                _ => String::new(),
            })
            .collect::<Vec<_>>()
            .join(".");
        return Err(SqlError::Unsupported {
            detail: format!(
                "schema-qualified window function name '{qualified}': {SCHEMA_QUALIFIED_MSG}"
            ),
        });
    }
    let name = func
        .name
        .0
        .iter()
        .map(|p| match p {
            ast::ObjectNamePart::Identifier(ident) => normalize_ident(ident),
            _ => String::new(),
        })
        .collect::<Vec<_>>()
        .join(".");

    // Reject unknown names at plan time. PostgreSQL permits aggregates as
    // windows, so accept either Window or Aggregate categories.
    match functions.lookup(&name).map(|m| m.category) {
        Some(FunctionCategory::Window) | Some(FunctionCategory::Aggregate) => {}
        Some(FunctionCategory::Scalar) => {
            return Err(SqlError::InvalidFunction {
                detail: format!(
                    "function '{name}() OVER ()' does not exist as a window function \
                     (it is a scalar function)"
                ),
            });
        }
        None => {
            return Err(SqlError::InvalidFunction {
                detail: format!("function '{name}() OVER ()' does not exist"),
            });
        }
    }

    let args = match &func.args {
        ast::FunctionArguments::List(args) => args
            .args
            .iter()
            .filter_map(|a| match a {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => convert_expr(e).ok(),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };

    let (partition_by, order_by, frame) = match &func.over {
        Some(ast::WindowType::WindowSpec(spec)) => {
            let pb = spec
                .partition_by
                .iter()
                .map(convert_expr)
                .collect::<Result<Vec<_>>>()?;
            let ob = spec
                .order_by
                .iter()
                .map(|o| {
                    Ok(SortKey {
                        expr: convert_expr(&o.expr)?,
                        ascending: o.options.asc.unwrap_or(true),
                        nulls_first: o.options.nulls_first.unwrap_or(false),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let frame = match &spec.window_frame {
                Some(f) => convert_window_frame(f, &ob)?,
                // PostgreSQL default: when ORDER BY is present, RANGE UNBOUNDED
                // PRECEDING TO CURRENT ROW; when no ORDER BY, the window covers
                // the whole partition (RANGE UNBOUNDED PRECEDING TO UNBOUNDED
                // FOLLOWING).
                None => {
                    if ob.is_empty() {
                        WindowFrame {
                            mode: "range".into(),
                            start: FrameBound::UnboundedPreceding,
                            end: FrameBound::UnboundedFollowing,
                        }
                    } else {
                        WindowFrame::default()
                    }
                }
            };
            (pb, ob, frame)
        }
        _ => (
            Vec::new(),
            Vec::new(),
            WindowFrame {
                mode: "range".into(),
                start: FrameBound::UnboundedPreceding,
                end: FrameBound::UnboundedFollowing,
            },
        ),
    };

    Ok(WindowSpec {
        function: name,
        args,
        partition_by,
        order_by,
        alias: alias.into(),
        frame,
    })
}

/// Convert a sqlparser `WindowFrame` to the executor's `WindowFrame`.
///
/// `order_by` is needed for semantic validation:
/// - GROUPS without ORDER BY is invalid (PostgreSQL parity).
/// - RANGE with numeric offsets (Preceding(N)/Following(N)) requires a single
///   numeric ORDER BY column; without one the semantics are undefined and we
///   reject at plan time.
fn convert_window_frame(
    frame: &ast::WindowFrame,
    order_by: &[crate::types::SortKey],
) -> Result<WindowFrame> {
    let mode = match frame.units {
        ast::WindowFrameUnits::Rows => "rows",
        ast::WindowFrameUnits::Range => "range",
        ast::WindowFrameUnits::Groups => {
            if order_by.is_empty() {
                return Err(SqlError::InvalidWindowFrame {
                    detail: "GROUPS mode requires an ORDER BY clause in the window specification"
                        .into(),
                });
            }
            "groups"
        }
    };

    let start = convert_window_frame_bound(&frame.start_bound)?;
    let end = match &frame.end_bound {
        Some(b) => convert_window_frame_bound(b)?,
        None => FrameBound::CurrentRow,
    };

    // RANGE with numeric offsets requires a single-column ORDER BY so we can
    // compare values. Reject if ORDER BY is absent or has more than one key
    // (multi-key RANGE offsets are undefined in SQL standards).
    if mode == "range" {
        let needs_order = matches!(start, FrameBound::Preceding(n) if n > 0)
            || matches!(start, FrameBound::Following(n) if n > 0)
            || matches!(end, FrameBound::Preceding(n) if n > 0)
            || matches!(end, FrameBound::Following(n) if n > 0);
        if needs_order && order_by.len() != 1 {
            return Err(SqlError::InvalidWindowFrame {
                detail: "RANGE with numeric PRECEDING/FOLLOWING offset requires exactly one ORDER BY column".into(),
            });
        }
    }

    Ok(WindowFrame {
        mode: mode.into(),
        start,
        end,
    })
}

fn convert_window_frame_bound(bound: &ast::WindowFrameBound) -> Result<FrameBound> {
    match bound {
        ast::WindowFrameBound::CurrentRow => Ok(FrameBound::CurrentRow),
        ast::WindowFrameBound::Preceding(None) => Ok(FrameBound::UnboundedPreceding),
        ast::WindowFrameBound::Following(None) => Ok(FrameBound::UnboundedFollowing),
        ast::WindowFrameBound::Preceding(Some(expr)) => {
            Ok(FrameBound::Preceding(extract_frame_offset(expr)?))
        }
        ast::WindowFrameBound::Following(Some(expr)) => {
            Ok(FrameBound::Following(extract_frame_offset(expr)?))
        }
    }
}

fn extract_frame_offset(expr: &ast::Expr) -> Result<u64> {
    if let ast::Expr::Value(v) = expr
        && let ast::Value::Number(n, _) = &v.value
        && let Ok(parsed) = n.parse::<u64>()
    {
        return Ok(parsed);
    }
    Err(SqlError::Unsupported {
        detail: format!("window frame offset must be a non-negative integer literal, got {expr}"),
    })
}
