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
        let scalar_arg2 = if args.len() > 2
            && let Value::Scalar(v, _) = eval(ctx, &args[2])?
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
            let val = if func == "holt_winters" {
                functions::call_holt_winters(&rs.samples, scalar_arg, scalar_arg2)
            } else {
                functions::call_range_func(func, &rs.samples, scalar_arg)
            };
            if let Some(v) = val {
                result.push(InstantSample {
                    labels: rs.labels.clone(),
                    value: v,
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

        // Trig functions.
        "acos" => unary_scalar_fn(ctx, args, f64::acos),
        "asin" => unary_scalar_fn(ctx, args, f64::asin),
        "atan" => unary_scalar_fn(ctx, args, f64::atan),
        "cos" => unary_scalar_fn(ctx, args, f64::cos),
        "sin" => unary_scalar_fn(ctx, args, f64::sin),
        "tan" => unary_scalar_fn(ctx, args, f64::tan),
        "deg" => unary_scalar_fn(ctx, args, f64::to_degrees),
        "rad" => unary_scalar_fn(ctx, args, f64::to_radians),
        "sgn" => unary_scalar_fn(ctx, args, f64::signum),

        // Clamp functions.
        "clamp" => eval_clamp(ctx, args),
        "clamp_min" => eval_clamp_min(ctx, args),
        "clamp_max" => eval_clamp_max(ctx, args),

        // absent: returns 1-element vector if input is empty, else empty vector.
        "absent" => eval_absent(ctx, args),

        // Label manipulation.
        "label_replace" => eval_label_replace(ctx, args),
        "label_join" => eval_label_join(ctx, args),

        // histogram_quantile(φ, buckets).
        "histogram_quantile" => eval_histogram_quantile(ctx, args),

        // atan2 is a binary function.
        "atan2" => {
            if args.len() < 2 {
                return Err("atan2() requires 2 args".into());
            }
            let a = eval(ctx, &args[0])?;
            let b = eval(ctx, &args[1])?;
            match (a, b) {
                (Value::Scalar(y, _), Value::Scalar(x, _)) => {
                    Ok(Value::Scalar(y.atan2(x), ctx.timestamp_ms))
                }
                _ => Err("atan2 requires scalar arguments".into()),
            }
        }

        _ => Err(format!("unknown function '{func}'")),
    }
}

fn eval_clamp(ctx: &EvalContext, args: &[Expr]) -> Result<Value, String> {
    if args.len() < 3 {
        return Err("clamp() requires 3 args: vector, min, max".into());
    }
    let val = eval(ctx, &args[0])?;
    let Value::Scalar(min_val, _) = eval(ctx, &args[1])? else {
        return Err("clamp() min must be scalar".into());
    };
    let Value::Scalar(max_val, _) = eval(ctx, &args[2])? else {
        return Err("clamp() max must be scalar".into());
    };
    apply_to_vector(val, ctx.timestamp_ms, |v| v.clamp(min_val, max_val))
}

fn eval_clamp_min(ctx: &EvalContext, args: &[Expr]) -> Result<Value, String> {
    if args.len() < 2 {
        return Err("clamp_min() requires 2 args".into());
    }
    let val = eval(ctx, &args[0])?;
    let Value::Scalar(min_val, _) = eval(ctx, &args[1])? else {
        return Err("clamp_min() min must be scalar".into());
    };
    apply_to_vector(val, ctx.timestamp_ms, |v| v.max(min_val))
}

fn eval_clamp_max(ctx: &EvalContext, args: &[Expr]) -> Result<Value, String> {
    if args.len() < 2 {
        return Err("clamp_max() requires 2 args".into());
    }
    let val = eval(ctx, &args[0])?;
    let Value::Scalar(max_val, _) = eval(ctx, &args[1])? else {
        return Err("clamp_max() max must be scalar".into());
    };
    apply_to_vector(val, ctx.timestamp_ms, |v| v.min(max_val))
}

fn eval_absent(ctx: &EvalContext, args: &[Expr]) -> Result<Value, String> {
    let val = eval(ctx, args.first().ok_or("absent() requires 1 arg")?)?;
    match val {
        Value::Vector(v) if v.is_empty() => Ok(Value::Vector(vec![InstantSample {
            labels: Labels::new(),
            value: 1.0,
            timestamp_ms: ctx.timestamp_ms,
        }])),
        Value::Vector(_) => Ok(Value::Vector(vec![])),
        _ => Ok(Value::Vector(vec![])),
    }
}

fn eval_label_replace(ctx: &EvalContext, args: &[Expr]) -> Result<Value, String> {
    if args.len() < 5 {
        return Err("label_replace() requires 5 args: v, dst, replacement, src, regex".into());
    }
    let val = eval(ctx, &args[0])?;
    let Value::Vector(samples) = val else {
        return Err("label_replace() requires instant vector".into());
    };
    let dst = eval_string_arg(ctx, &args[1])?;
    let replacement = eval_string_arg(ctx, &args[2])?;
    let src = eval_string_arg(ctx, &args[3])?;
    let regex_str = eval_string_arg(ctx, &args[4])?;
    let re = regex::Regex::new(&format!("^(?:{regex_str})$"))
        .map_err(|e| format!("label_replace: invalid regex: {e}"))?;

    let result: Vec<InstantSample> = samples
        .into_iter()
        .map(|mut s| {
            let src_val = s.labels.get(&src).cloned().unwrap_or_default();
            if let Some(caps) = re.captures(&src_val) {
                let mut replaced = replacement.clone();
                // Replace $1, $2, etc. with capture groups.
                for i in 0..caps.len() {
                    if let Some(m) = caps.get(i) {
                        replaced = replaced.replace(&format!("${i}"), m.as_str());
                    }
                }
                if replaced.is_empty() {
                    s.labels.remove(&dst);
                } else {
                    s.labels.insert(dst.clone(), replaced);
                }
            }
            s
        })
        .collect();
    Ok(Value::Vector(result))
}

fn eval_label_join(ctx: &EvalContext, args: &[Expr]) -> Result<Value, String> {
    if args.len() < 3 {
        return Err(
            "label_join() requires at least 3 args: v, dst, separator, ...src_labels".into(),
        );
    }
    let val = eval(ctx, &args[0])?;
    let Value::Vector(samples) = val else {
        return Err("label_join() requires instant vector".into());
    };
    let dst = eval_string_arg(ctx, &args[1])?;
    let separator = eval_string_arg(ctx, &args[2])?;
    let src_labels: Vec<String> = args[3..]
        .iter()
        .filter_map(|a| eval_string_arg(ctx, a).ok())
        .collect();

    let result: Vec<InstantSample> = samples
        .into_iter()
        .map(|mut s| {
            let joined: Vec<&str> = src_labels
                .iter()
                .filter_map(|l| s.labels.get(l).map(|v| v.as_str()))
                .collect();
            let val = joined.join(&separator);
            if val.is_empty() {
                s.labels.remove(&dst);
            } else {
                s.labels.insert(dst.clone(), val);
            }
            s
        })
        .collect();
    Ok(Value::Vector(result))
}

fn eval_histogram_quantile(ctx: &EvalContext, args: &[Expr]) -> Result<Value, String> {
    if args.len() < 2 {
        return Err("histogram_quantile() requires 2 args: φ, buckets".into());
    }
    let Value::Scalar(phi, _) = eval(ctx, &args[0])? else {
        return Err("histogram_quantile: first arg must be scalar".into());
    };
    let val = eval(ctx, &args[1])?;
    let Value::Vector(samples) = val else {
        return Err("histogram_quantile: second arg must be instant vector".into());
    };

    // Group by labels excluding "le".
    let mut groups: std::collections::BTreeMap<String, Vec<(f64, f64)>> =
        std::collections::BTreeMap::new();
    let mut group_labels_map: std::collections::BTreeMap<String, Labels> =
        std::collections::BTreeMap::new();

    for s in &samples {
        let le_str = s.labels.get("le").cloned().unwrap_or_default();
        let le: f64 = le_str.parse().unwrap_or(f64::INFINITY);
        let mut key_labels = s.labels.clone();
        key_labels.remove("le");
        key_labels.remove("__name__");
        let key = super::helpers::labels_key(&key_labels);
        group_labels_map.entry(key.clone()).or_insert(key_labels);
        groups.entry(key).or_default().push((le, s.value));
    }

    let mut result = Vec::new();
    for (key, mut buckets) in groups {
        buckets.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        let quantile_val = histogram_quantile_from_buckets(phi, &buckets);
        if let Some(labels) = group_labels_map.get(&key) {
            result.push(InstantSample {
                labels: labels.clone(),
                value: quantile_val,
                timestamp_ms: ctx.timestamp_ms,
            });
        }
    }
    Ok(Value::Vector(result))
}

/// Compute quantile from sorted histogram buckets (le, count) pairs.
fn histogram_quantile_from_buckets(phi: f64, buckets: &[(f64, f64)]) -> f64 {
    if buckets.is_empty() || phi.is_nan() {
        return f64::NAN;
    }
    let phi = phi.clamp(0.0, 1.0);
    let total = buckets.last().map_or(0.0, |b| b.1);
    if total == 0.0 {
        return f64::NAN;
    }
    let rank = phi * total;

    let mut prev_count = 0.0;
    let mut prev_le = 0.0;
    for &(le, count) in buckets {
        if count >= rank {
            // Linear interpolation within this bucket.
            let bucket_count = count - prev_count;
            if bucket_count <= 0.0 {
                return le;
            }
            let fraction = (rank - prev_count) / bucket_count;
            return prev_le + fraction * (le - prev_le);
        }
        prev_count = count;
        prev_le = le;
    }
    buckets.last().map_or(f64::NAN, |b| b.0)
}

fn eval_string_arg(ctx: &EvalContext, arg: &Expr) -> Result<String, String> {
    match arg {
        Expr::StringLiteral(s) => Ok(s.clone()),
        other => {
            let val = eval(ctx, other)?;
            match val {
                Value::Scalar(v, _) => Ok(format!("{v}")),
                _ => Err("expected string argument".into()),
            }
        }
    }
}

fn apply_to_vector(val: Value, ts: i64, f: impl Fn(f64) -> f64) -> Result<Value, String> {
    match val {
        Value::Scalar(v, _) => Ok(Value::Scalar(f(v), ts)),
        Value::Vector(samples) => Ok(Value::Vector(
            samples
                .into_iter()
                .map(|s| InstantSample {
                    value: f(s.value),
                    ..s
                })
                .collect(),
        )),
        _ => Err("expected scalar or instant vector".into()),
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
