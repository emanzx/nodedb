//! Function call evaluation.

use super::super::ast::Expr;
use super::super::functions;
use super::super::types::*;
use super::{EvalContext, eval};

pub fn eval_call(ctx: &EvalContext, func: &str, args: &[Expr]) -> Result<Value, String> {
    // Range-vector functions.
    if functions::is_range_func(func) {
        if args.is_empty() {
            return Err(format!("{func}() requires at least one argument"));
        }
        let matrix = eval(ctx, &args[0])?;
        let scalar_arg = if args.len() > 1
            && let Value::Scalar(v, _) = eval(ctx, &args[1])?
        {
            Some(v)
        } else {
            None
        };

        let Value::Matrix(range_series) = matrix else {
            return Err(format!("{func}() requires a range vector argument"));
        };

        let mut result = Vec::new();
        for rs in &range_series {
            if let Some(val) = functions::call_range_func(func, &rs.samples, scalar_arg) {
                result.push(InstantSample {
                    labels: rs.labels.clone(),
                    value: val,
                    timestamp_ms: ctx.timestamp_ms,
                });
            }
        }
        return Ok(Value::Vector(result));
    }

    // Scalar math functions.
    match func {
        "abs" => unary_scalar_fn(ctx, args, f64::abs),
        "ceil" => unary_scalar_fn(ctx, args, f64::ceil),
        "floor" => unary_scalar_fn(ctx, args, f64::floor),
        "round" => unary_scalar_fn(ctx, args, f64::round),
        "sqrt" => unary_scalar_fn(ctx, args, f64::sqrt),
        "ln" => unary_scalar_fn(ctx, args, f64::ln),
        "log2" => unary_scalar_fn(ctx, args, f64::log2),
        "log10" => unary_scalar_fn(ctx, args, f64::log10),
        "exp" => unary_scalar_fn(ctx, args, f64::exp),
        "scalar" => {
            let val = eval(ctx, args.first().ok_or("scalar() requires 1 arg")?)?;
            match val {
                Value::Vector(v) if v.len() == 1 => Ok(Value::Scalar(v[0].value, ctx.timestamp_ms)),
                _ => Ok(Value::Scalar(f64::NAN, ctx.timestamp_ms)),
            }
        }
        "vector" => {
            let val = eval(ctx, args.first().ok_or("vector() requires 1 arg")?)?;
            match val {
                Value::Scalar(v, _) => Ok(Value::Vector(vec![InstantSample {
                    labels: Labels::new(),
                    value: v,
                    timestamp_ms: ctx.timestamp_ms,
                }])),
                other => Ok(other),
            }
        }
        "time" => Ok(Value::Scalar(
            ctx.timestamp_ms as f64 / 1000.0,
            ctx.timestamp_ms,
        )),
        _ => Err(format!("unknown function '{func}'")),
    }
}

fn unary_scalar_fn(ctx: &EvalContext, args: &[Expr], f: fn(f64) -> f64) -> Result<Value, String> {
    let val = eval(ctx, args.first().ok_or("function requires 1 arg")?)?;
    match val {
        Value::Scalar(v, ts) => Ok(Value::Scalar(f(v), ts)),
        Value::Vector(samples) => {
            let mapped: Vec<InstantSample> = samples
                .into_iter()
                .map(|s| InstantSample {
                    labels: s.labels,
                    value: f(s.value),
                    timestamp_ms: s.timestamp_ms,
                })
                .collect();
            Ok(Value::Vector(mapped))
        }
        _ => Err("function argument must be scalar or instant vector".into()),
    }
}
