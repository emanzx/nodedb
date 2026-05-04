//! Integration tests for SQL window function dispatch.
//!
//! Covers PostgreSQL-compatible window verbs that must either be implemented
//! with their documented semantics or rejected at plan time. The dispatcher
//! must never silently drop a windowed projection — that produces queries
//! which "succeed" with the windowed column as NULL for every row, with no
//! error, log line, or other diagnostic.

mod common;

use common::pgwire_harness::TestServer;

async fn setup_numbered_rows(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION s TYPE DOCUMENT STRICT (\
                id STRING PRIMARY KEY,\
                n INT\
             )",
        )
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO s (id, n) VALUES \
             ('a', 1), ('b', 2), ('c', 3), ('d', 4), ('e', 5)",
        )
        .await
        .unwrap();
}

fn parse_f64s(rows: &[Vec<String>], col: usize) -> Vec<f64> {
    rows.iter()
        .map(|r| {
            r.get(col)
                .unwrap_or(&String::new())
                .parse::<f64>()
                .unwrap_or(f64::NAN)
        })
        .collect()
}

// ── percent_rank: (rank - 1) / (partition_rows - 1) ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn percent_rank_returns_postgres_values() {
    let server = TestServer::start().await;
    setup_numbered_rows(&server).await;

    let rows = server
        .query_rows(
            "SELECT id, percent_rank() OVER (ORDER BY n) AS pr \
             FROM s ORDER BY n",
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 5, "expected 5 rows: {rows:?}");

    // Regression guard for the silent-catch-all bug: pr column must not be
    // NULL/empty for any row. simple_query renders SQL NULL as the empty
    // string, so any empty cell here means dispatch silently dropped the
    // projection.
    for (i, row) in rows.iter().enumerate() {
        let pr = row.get(1).cloned().unwrap_or_default();
        assert!(
            !pr.is_empty() && pr.to_lowercase() != "null",
            "percent_rank produced NULL/empty at row {i}: {row:?}"
        );
    }

    let got = parse_f64s(&rows, 1);
    let want = [0.0, 0.25, 0.5, 0.75, 1.0];
    for (i, (g, w)) in got.iter().zip(want.iter()).enumerate() {
        assert!(
            (g - w).abs() < 1e-9,
            "percent_rank[{i}] = {g}, want {w}; rows = {rows:?}"
        );
    }
}

// ── cume_dist: rows_at_or_before / partition_rows ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cume_dist_returns_postgres_values() {
    let server = TestServer::start().await;
    setup_numbered_rows(&server).await;

    let rows = server
        .query_rows(
            "SELECT id, cume_dist() OVER (ORDER BY n) AS cd \
             FROM s ORDER BY n",
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 5, "expected 5 rows: {rows:?}");

    for (i, row) in rows.iter().enumerate() {
        let cd = row.get(1).cloned().unwrap_or_default();
        assert!(
            !cd.is_empty() && cd.to_lowercase() != "null",
            "cume_dist produced NULL/empty at row {i}: {row:?}"
        );
    }

    let got = parse_f64s(&rows, 1);
    let want = [0.2, 0.4, 0.6, 0.8, 1.0];
    for (i, (g, w)) in got.iter().zip(want.iter()).enumerate() {
        assert!(
            (g - w).abs() < 1e-9,
            "cume_dist[{i}] = {g}, want {w}; rows = {rows:?}"
        );
    }
}

// ── nth_value(col, n): nth row's value of `col` within the window ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nth_value_returns_nth_row_value() {
    let server = TestServer::start().await;
    setup_numbered_rows(&server).await;

    let rows = server
        .query_rows(
            "SELECT id, nth_value(n, 2) OVER (ORDER BY n) AS nv \
             FROM s ORDER BY n",
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 5, "expected 5 rows: {rows:?}");

    // Default frame is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW; PG
    // returns NULL for rows before the 2nd, and the 2nd value (=2) for rows
    // at or after the 2nd. The first row's NULL is correct PG semantics, but
    // every later row must be a non-NULL "2" — silent dispatch drop would
    // make all five NULL, which is the bug we are guarding against.
    for (i, row) in rows.iter().enumerate().skip(1) {
        let nv = row.get(1).cloned().unwrap_or_default();
        assert!(
            !nv.is_empty() && nv.to_lowercase() != "null",
            "nth_value produced NULL/empty at row {i} (should be 2): {row:?}"
        );
        let parsed: i64 = nv.parse().unwrap_or(-1);
        assert_eq!(parsed, 2, "nth_value[{i}] = {nv}, want 2");
    }
}

// ── Unknown window function name must be rejected at plan time ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_window_function_is_rejected() {
    let server = TestServer::start().await;
    setup_numbered_rows(&server).await;

    let result = server
        .query_rows(
            "SELECT id, frobnicate() OVER (ORDER BY n) AS junk \
             FROM s ORDER BY n",
        )
        .await;

    assert!(
        result.is_err(),
        "unknown window function 'frobnicate' must error, got rows: {result:?}"
    );
    let msg = result.unwrap_err().to_lowercase();
    assert!(
        msg.contains("frobnicate")
            || msg.contains("does not exist")
            || msg.contains("unknown")
            || msg.contains("function"),
        "error should identify the unknown function: {msg}"
    );
}

// ── Window-function aliases must surface as response columns ──
//
// The window pass writes its result into the row under the alias, but the
// projection step (JSON path) silently skips keys that aren't present in the
// projection list — no NULL emitted, the alias just disappears from the row.
// The msgpack projection path emits NULL for missing keys; the JSON path must
// match. Otherwise window-aliased columns vanish from the response with no
// error or diagnostic.

async fn setup_scored_rows(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION s TYPE DOCUMENT STRICT (\
                id STRING PRIMARY KEY,\
                score FLOAT\
             )",
        )
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO s (id, score) VALUES \
             ('a', 1.0), ('b', 2.0), ('c', 3.0), ('d', 4.0), ('e', 5.0)",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn window_alias_appears_as_response_column() {
    let server = TestServer::start().await;
    setup_scored_rows(&server).await;

    let rows = server
        .query_rows(
            "SELECT id, score, percent_rank() OVER (ORDER BY score) AS pr_score \
             FROM s ORDER BY score",
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 5, "expected 5 rows: {rows:?}");
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(
            row.len(),
            3,
            "row {i} must have 3 columns (id, score, pr_score) — \
             window alias was silently dropped from response: {row:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_window_aliases_do_not_drop_adjacent_columns() {
    let server = TestServer::start().await;
    setup_scored_rows(&server).await;

    let rows = server
        .query_rows(
            "SELECT id, score, \
                    percent_rank() OVER (ORDER BY score DESC) AS pr_desc, \
                    percent_rank() OVER (ORDER BY score ASC)  AS pr_asc  \
             FROM s ORDER BY score",
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 5, "expected 5 rows: {rows:?}");
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(
            row.len(),
            4,
            "row {i} must have 4 columns (id, score, pr_desc, pr_asc) — \
             window aliases plus adjacent score were silently dropped: {row:?}"
        );
        let id = row.first().cloned().unwrap_or_default();
        let score = row.get(1).cloned().unwrap_or_default();
        assert!(
            !id.is_empty(),
            "id column must be present, got empty at row {i}: {row:?}"
        );
        assert!(
            !score.is_empty() && score.to_lowercase() != "null",
            "non-window column `score` must not be silently dropped/NULL at row {i}: {row:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn window_alias_carries_value_not_null() {
    let server = TestServer::start().await;
    setup_scored_rows(&server).await;

    let rows = server
        .query_rows(
            "SELECT id, percent_rank() OVER (ORDER BY score) AS pr_score \
             FROM s ORDER BY score",
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 5, "expected 5 rows: {rows:?}");
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(
            row.len(),
            2,
            "row {i} must have 2 columns (id, pr_score) — \
             window alias dropped from response: {row:?}"
        );
        let pr = row.get(1).cloned().unwrap_or_default();
        assert!(
            !pr.is_empty() && pr.to_lowercase() != "null",
            "row {i}: pr_score must carry the window value, got NULL/empty: {row:?}"
        );
    }
}

// ── Regression guard: silent-catch-all must not produce NULL projections ──
//
// This is the original symptom from the bug report. It overlaps with the
// percent_rank test above, but is kept as an independent test so a partial
// fix (e.g. implementing the verb but leaving the catch-all in place for
// other names) still shows progress without hiding the systemic flaw.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn window_dispatch_never_silently_drops_projection() {
    let server = TestServer::start().await;
    setup_numbered_rows(&server).await;

    // If the verb is implemented, the column is non-NULL.
    // If the verb is not implemented, the planner must reject — not return
    // five rows with a silently-NULL column.
    let outcome = server
        .query_rows(
            "SELECT id, percent_rank() OVER (ORDER BY n) AS pr \
             FROM s ORDER BY n",
        )
        .await;

    match outcome {
        Ok(rows) => {
            assert_eq!(rows.len(), 5);
            let all_null_or_empty = rows.iter().all(|r| {
                let v = r.get(1).cloned().unwrap_or_default();
                v.is_empty() || v.to_lowercase() == "null"
            });
            assert!(
                !all_null_or_empty,
                "every row's window column was NULL/empty — \
                 dispatch silently dropped the projection: {rows:?}"
            );
        }
        Err(_) => {
            // Acceptable alternative: planner rejects the unimplemented verb.
        }
    }
}
