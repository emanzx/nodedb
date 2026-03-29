//! Prometheus-compatible PromQL HTTP API at `/obsv/api/v1/*`.
//!
//! Endpoints:
//! - GET/POST `/obsv/api/v1/query`         — instant query
//! - GET/POST `/obsv/api/v1/query_range`   — range query
//! - GET      `/obsv/api/v1/series`        — find series by label matchers
//! - GET      `/obsv/api/v1/labels`        — list all label names
//! - GET      `/obsv/api/v1/label/:name/values` — list values for a label
//!
//! Grafana data source URL: `http://nodedb:6480/obsv/api`

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;

use super::super::auth::AppState;
use crate::control::promql;

/// Query parameters for `/query`.
#[derive(Debug, serde::Deserialize)]
pub struct InstantQueryParams {
    pub query: String,
    /// Evaluation timestamp (Unix seconds, optional — defaults to now).
    pub time: Option<f64>,
}

/// Query parameters for `/query_range`.
#[derive(Debug, serde::Deserialize)]
pub struct RangeQueryParams {
    pub query: String,
    /// Start timestamp (Unix seconds).
    pub start: f64,
    /// End timestamp (Unix seconds).
    pub end: f64,
    /// Query step (duration string like "15s" or seconds as float).
    pub step: String,
}

/// Query parameters for `/series`.
#[derive(Debug, serde::Deserialize)]
pub struct SeriesParams {
    /// Label matchers (multiple `match[]` params).
    #[serde(rename = "match[]", default)]
    pub matchers: Vec<String>,
    pub start: Option<f64>,
    pub end: Option<f64>,
}

/// Query parameters for `/labels`.
#[derive(Debug, serde::Deserialize)]
pub struct LabelsParams {
    pub start: Option<f64>,
    pub end: Option<f64>,
}

/// GET/POST `/obsv/api/v1/query` — instant query.
pub async fn instant_query(
    State(state): State<AppState>,
    Query(params): Query<InstantQueryParams>,
) -> impl IntoResponse {
    let ts_ms = params.time.map(|t| (t * 1000.0) as i64).unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    });

    let tokens = match promql::lexer::tokenize(&params.query) {
        Ok(t) => t,
        Err(e) => return prom_error("bad_data", &e),
    };
    let expr = match promql::parse(&tokens) {
        Ok(e) => e,
        Err(e) => return prom_error("bad_data", &e),
    };

    // Fetch series from timeseries engine.
    let series =
        fetch_series_for_query(&state, ts_ms - promql::types::DEFAULT_LOOKBACK_MS, ts_ms).await;

    let ctx = promql::EvalContext {
        series,
        timestamp_ms: ts_ms,
        lookback_ms: promql::types::DEFAULT_LOOKBACK_MS,
    };

    match promql::evaluate_instant(&ctx, &expr) {
        Ok(value) => prom_success(value),
        Err(e) => prom_error("execution", &e),
    }
}

/// GET/POST `/obsv/api/v1/query_range` — range query.
pub async fn range_query(
    State(state): State<AppState>,
    Query(params): Query<RangeQueryParams>,
) -> impl IntoResponse {
    let start_ms = (params.start * 1000.0) as i64;
    let end_ms = (params.end * 1000.0) as i64;
    let step_ms = parse_step(&params.step).unwrap_or(15_000);

    if step_ms <= 0 {
        return prom_error("bad_data", "step must be positive");
    }
    if end_ms < start_ms {
        return prom_error("bad_data", "end must be >= start");
    }

    let tokens = match promql::lexer::tokenize(&params.query) {
        Ok(t) => t,
        Err(e) => return prom_error("bad_data", &e),
    };
    let expr = match promql::parse(&tokens) {
        Ok(e) => e,
        Err(e) => return prom_error("bad_data", &e),
    };

    // Fetch series covering the full range + lookback.
    let series = fetch_series_for_query(
        &state,
        start_ms - promql::types::DEFAULT_LOOKBACK_MS,
        end_ms,
    )
    .await;

    let ctx = promql::EvalContext {
        series,
        timestamp_ms: start_ms,
        lookback_ms: promql::types::DEFAULT_LOOKBACK_MS,
    };

    match promql::evaluate_range(&ctx, &expr, start_ms, end_ms, step_ms) {
        Ok(value) => prom_success(value),
        Err(e) => prom_error("execution", &e),
    }
}

/// GET `/obsv/api/v1/series` — find series by label matchers.
pub async fn series_query(
    State(state): State<AppState>,
    Query(params): Query<SeriesParams>,
) -> impl IntoResponse {
    let end_ms = params.end.map(|t| (t * 1000.0) as i64).unwrap_or(now_ms());
    let start_ms = params
        .start
        .map(|t| (t * 1000.0) as i64)
        .unwrap_or(end_ms - promql::types::DEFAULT_LOOKBACK_MS);

    let all_series = fetch_series_for_query(&state, start_ms, end_ms).await;

    // Filter by matchers if provided.
    let filtered: Vec<&promql::Series> = if params.matchers.is_empty() {
        all_series.iter().collect()
    } else {
        all_series
            .iter()
            .filter(|s| {
                params
                    .matchers
                    .iter()
                    .any(|m| match parse_series_matcher(m) {
                        Some(matchers) => promql::label::matches_all(&matchers, &s.labels),
                        None => false,
                    })
            })
            .collect()
    };

    let mut out = String::from(r#"{"status":"success","data":["#);
    for (i, s) in filtered.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        promql::types::write_labels_json(&mut out, &s.labels);
    }
    out.push_str("]}");

    (StatusCode::OK, [("content-type", "application/json")], out)
}

/// GET `/obsv/api/v1/labels` — list all label names.
pub async fn label_names(
    State(state): State<AppState>,
    Query(params): Query<LabelsParams>,
) -> impl IntoResponse {
    let end_ms = params.end.map(|t| (t * 1000.0) as i64).unwrap_or(now_ms());
    let start_ms = params
        .start
        .map(|t| (t * 1000.0) as i64)
        .unwrap_or(end_ms - promql::types::DEFAULT_LOOKBACK_MS);

    let all_series = fetch_series_for_query(&state, start_ms, end_ms).await;

    let mut names: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for s in &all_series {
        for k in s.labels.keys() {
            names.insert(k.clone());
        }
    }

    let mut out = String::from(r#"{"status":"success","data":["#);
    for (i, n) in names.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('"');
        promql::types::json_escape(&mut out, n);
        out.push('"');
    }
    out.push_str("]}");

    (StatusCode::OK, [("content-type", "application/json")], out)
}

/// GET `/obsv/api/v1/label/:name/values` — list values for a label.
pub async fn label_values(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<LabelsParams>,
) -> impl IntoResponse {
    let end_ms = params.end.map(|t| (t * 1000.0) as i64).unwrap_or(now_ms());
    let start_ms = params
        .start
        .map(|t| (t * 1000.0) as i64)
        .unwrap_or(end_ms - promql::types::DEFAULT_LOOKBACK_MS);

    let all_series = fetch_series_for_query(&state, start_ms, end_ms).await;

    let mut values: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for s in &all_series {
        if let Some(v) = s.labels.get(&name) {
            values.insert(v.clone());
        }
    }

    let mut out = String::from(r#"{"status":"success","data":["#);
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('"');
        promql::types::json_escape(&mut out, v);
        out.push('"');
    }
    out.push_str("]}");

    (StatusCode::OK, [("content-type", "application/json")], out)
}

// ── Helpers ──────────────────────────────────────────────────────────────

/// Fetch all timeseries data within [start_ms, end_ms] from the shared state.
///
/// This reads from the SystemMetrics (NodeDB's own metrics) as a built-in
/// metrics source. External timeseries collections (via ILP ingest) would
/// additionally go through the Data Plane bridge.
async fn fetch_series_for_query(
    state: &AppState,
    _start_ms: i64,
    _end_ms: i64,
) -> Vec<promql::Series> {
    // Built-in: expose NodeDB's own system metrics as PromQL-queryable series.
    // Each SystemMetrics counter/gauge becomes a series with __name__ label.
    let mut series = Vec::new();

    if let Some(ref sys) = state.shared.system_metrics {
        use std::sync::atomic::Ordering;

        let ts = now_ms();
        let metrics: Vec<(&str, f64)> = vec![
            (
                "nodedb_queries_total",
                sys.queries_total.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_query_errors_total",
                sys.query_errors.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_active_connections",
                sys.active_connections.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_wal_fsync_latency_us",
                sys.wal_fsync_latency_us.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_raft_apply_lag",
                sys.raft_apply_lag.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_bridge_utilization",
                sys.bridge_utilization.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_compaction_debt",
                sys.compaction_debt.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_vector_searches_total",
                sys.vector_searches.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_graph_traversals_total",
                sys.graph_traversals.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_text_searches_total",
                sys.text_searches.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_kv_gets_total",
                sys.kv_gets_total.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_kv_memory_bytes",
                sys.kv_memory_bytes.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_pgwire_connections",
                sys.pgwire_connections.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_http_connections",
                sys.http_connections.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_websocket_connections",
                sys.websocket_connections.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_slow_queries_total",
                sys.slow_queries_total.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_storage_l0_bytes",
                sys.storage_l0_bytes.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_storage_l1_bytes",
                sys.storage_l1_bytes.load(Ordering::Relaxed) as f64,
            ),
        ];

        for (name, value) in metrics {
            let mut labels = std::collections::BTreeMap::new();
            labels.insert("__name__".into(), name.into());
            series.push(promql::Series {
                labels,
                samples: vec![promql::Sample {
                    timestamp_ms: ts,
                    value,
                }],
            });
        }
    }

    series
}

fn prom_success(value: promql::Value) -> (StatusCode, [(&'static str, &'static str); 1], String) {
    let result = promql::PromResult::success(value);
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        result.to_json(),
    )
}

fn prom_error(
    err_type: &str,
    message: &str,
) -> (StatusCode, [(&'static str, &'static str); 1], String) {
    let result = promql::PromResult::error(err_type, message.to_string());
    let status = match err_type {
        "bad_data" => StatusCode::BAD_REQUEST,
        "execution" => StatusCode::UNPROCESSABLE_ENTITY,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (
        status,
        [("content-type", "application/json")],
        result.to_json(),
    )
}

fn parse_step(s: &str) -> Option<i64> {
    // Try as duration string first.
    if let Some(d) = promql::ast::Duration::parse(s) {
        return Some(d.ms());
    }
    // Try as float seconds.
    if let Ok(secs) = s.parse::<f64>() {
        return Some((secs * 1000.0) as i64);
    }
    None
}

fn parse_series_matcher(input: &str) -> Option<Vec<promql::LabelMatcher>> {
    let tokens = promql::lexer::tokenize(input).ok()?;
    let expr = promql::parse(&tokens).ok()?;
    match expr {
        promql::ast::Expr::VectorSelector { matchers, .. } => Some(matchers),
        _ => None,
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
