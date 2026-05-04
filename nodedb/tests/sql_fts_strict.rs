//! Full-text search coverage for strict-doc collections.
//!
//! A search index attached to a strict-doc collection must observe writes
//! the same way it observes writes to a schemaless collection: every
//! INSERT must populate the inverted index for the indexed text fields,
//! so `bm25_score(field, term)` returns a non-NULL score for rows whose
//! `field` contains `term`, regardless of underlying storage mode.
//!
//! The class of bug captured here is "the strict-doc INSERT path and the
//! shared FTS / stats / aggregate-cache side-effect block silently
//! diverge" — a format-detection helper that does not auto-detect Binary
//! Tuple bytes, called inside an `if let Some(..)` guard, drops every
//! side-effect for strict rows without surfacing an error.
//!
//! Tests assert the spec via the `bm25_score` projection so the
//! silent-skip failure mode appears as a literal NULL cell on rows whose
//! field contains the term — directly visible at the wire, no count
//! aggregation in the path.
//!
//! `CREATE FULLTEXT INDEX` is the documented keyword alias of
//! `CREATE SEARCH INDEX`; both DDL handlers share the same data-plane
//! wire-up gap and both keywords must populate the same indexer.

mod common;

use common::pgwire_harness::TestServer;

const SCHEMALESS_DDL: &str = "CREATE COLLECTION docs_schemaless";

const STRICT_DDL: &str = "CREATE COLLECTION docs_strict TYPE DOCUMENT STRICT (\
     id STRING PRIMARY KEY,\
     content STRING\
   )";

async fn seed_three(server: &TestServer, coll: &str) {
    server
        .exec(&format!(
            "INSERT INTO {coll} (id, content) VALUES \
             ('r0', 'consensus algorithm distributed'), \
             ('r1', 'consensus memory replication'), \
             ('r2', 'cats and dogs')"
        ))
        .await
        .unwrap();
}

/// Pull `(id, bm25_score)` from each row. NULL/empty score → `None`.
fn id_score_pairs(rows: &[Vec<String>]) -> Vec<(String, Option<f64>)> {
    rows.iter()
        .map(|r| {
            let id = r[0].trim().to_string();
            let cell = r[1].trim();
            let score = if cell.is_empty() {
                None
            } else {
                cell.parse::<f64>().ok()
            };
            (id, score)
        })
        .collect()
}

fn pair<'a>(pairs: &'a [(String, Option<f64>)], id: &str) -> &'a (String, Option<f64>) {
    pairs
        .iter()
        .find(|(i, _)| i == id)
        .unwrap_or_else(|| panic!("row {id} missing from result {pairs:?}"))
}

fn assert_term_rows_scored(pairs: &[(String, Option<f64>)], context: &str) {
    // Spec: rows whose `content` contains 'consensus' (r0, r1) must have
    // a positive numeric bm25_score; the row that does not (r2) is allowed
    // to be NULL or zero — only the term-bearing rows are load-bearing.
    for id in ["r0", "r1"] {
        let (_, score) = pair(pairs, id);
        assert!(
            score.is_some(),
            "[{context}] bm25_score for row {id} was NULL — \
             the inverted index was never populated for this row. \
             This is the silent-skip class: the FTS write site's \
             format-detection guard failed and dropped the row."
        );
        let s = score.unwrap();
        assert!(
            s > 0.0,
            "[{context}] bm25_score for row {id} must be positive when \
             the term occurs in `content`; got {s}"
        );
    }
}

// ── 1. bm25_score on strict-doc must return non-NULL for indexed rows ──────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bm25_score_strict_returns_non_null_for_indexed_rows() {
    let server = TestServer::start().await;
    server.exec(STRICT_DDL).await.unwrap();
    server
        .exec("CREATE SEARCH INDEX idx_strict_bm25 ON docs_strict FIELDS content ANALYZER 'simple'")
        .await
        .unwrap();
    seed_three(&server, "docs_strict").await;

    let rows = server
        .query_rows("SELECT id, bm25_score(content, 'consensus') FROM docs_strict ORDER BY id")
        .await
        .expect("bm25_score projection must succeed");
    assert_eq!(rows.len(), 3, "expected 3 rows, got {rows:?}");

    let pairs = id_score_pairs(&rows);
    assert_term_rows_scored(&pairs, "strict / simple analyzer");
}

// ── 2. Schemaless control: same INSERTs / index produce non-NULL scores ────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bm25_score_schemaless_control_returns_non_null_for_indexed_rows() {
    // Control case: confirms the bug class is strict-specific. If this
    // test fails alongside the strict tests, the fix has regressed the
    // schemaless path — the side-effect block must run for both modes.
    let server = TestServer::start().await;
    server.exec(SCHEMALESS_DDL).await.unwrap();
    server
        .exec("CREATE SEARCH INDEX idx_schemaless_bm25 ON docs_schemaless FIELDS content ANALYZER 'simple'")
        .await
        .unwrap();
    seed_three(&server, "docs_schemaless").await;

    let rows = server
        .query_rows("SELECT id, bm25_score(content, 'consensus') FROM docs_schemaless ORDER BY id")
        .await
        .expect("bm25_score projection on schemaless must succeed");
    assert_eq!(rows.len(), 3, "expected 3 rows, got {rows:?}");

    let pairs = id_score_pairs(&rows);
    assert_term_rows_scored(&pairs, "schemaless control");
}

// ── 3. Strict-doc must work under a non-'simple' analyzer ─────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bm25_score_strict_works_under_standard_analyzer() {
    // The bug is at the write site, not the analyzer — every analyzer
    // must see the same indexed content. If only `simple` worked, the
    // fix would be analyzer-specific and the systemic flaw would remain.
    let server = TestServer::start().await;
    server.exec(STRICT_DDL).await.unwrap();
    server
        .exec("CREATE SEARCH INDEX idx_strict_standard ON docs_strict FIELDS content ANALYZER 'standard'")
        .await
        .unwrap();
    seed_three(&server, "docs_strict").await;

    let rows = server
        .query_rows("SELECT id, bm25_score(content, 'consensus') FROM docs_strict ORDER BY id")
        .await
        .expect("bm25_score under standard analyzer must succeed");
    assert_eq!(rows.len(), 3, "expected 3 rows, got {rows:?}");

    let pairs = id_score_pairs(&rows);
    assert_term_rows_scored(&pairs, "strict / standard analyzer");
}

// ── 4. CREATE FULLTEXT INDEX (alias) must wire the same indexer ────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fulltext_index_keyword_populates_strict_inverted_index() {
    // `CREATE FULLTEXT INDEX` and `CREATE SEARCH INDEX` are documented
    // as equivalents. Both DDL handlers currently only record ownership
    // and audit — neither dispatches to the data plane to populate the
    // index. The same observable failure mode (NULL bm25 score on rows
    // that contain the term) must be captured for both keywords so a
    // fix has to wire both, not one.
    let server = TestServer::start().await;
    server.exec(STRICT_DDL).await.unwrap();
    server
        .exec("CREATE FULLTEXT INDEX idx_strict_fulltext ON docs_strict FIELDS content ANALYZER 'simple'")
        .await
        .unwrap();
    seed_three(&server, "docs_strict").await;

    let rows = server
        .query_rows("SELECT id, bm25_score(content, 'consensus') FROM docs_strict ORDER BY id")
        .await
        .expect("bm25_score after CREATE FULLTEXT INDEX must succeed");
    assert_eq!(rows.len(), 3, "expected 3 rows, got {rows:?}");

    let pairs = id_score_pairs(&rows);
    assert_term_rows_scored(&pairs, "strict / FULLTEXT keyword");
}

// ── 5. Index-creation order must not matter (insert-then-index) ─────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bm25_score_strict_indexes_existing_rows_when_index_created_after_insert() {
    // If a fix only patches the INSERT side-effect block, rows inserted
    // *before* the index existed will still be invisible to the index.
    // The spec: creating a search index after the data is already there
    // must backfill (or otherwise make discoverable) those rows. Same
    // systemic gap (DDL not reaching the data plane) exposed from the
    // other direction.
    let server = TestServer::start().await;
    server.exec(STRICT_DDL).await.unwrap();
    seed_three(&server, "docs_strict").await;
    server
        .exec(
            "CREATE SEARCH INDEX idx_strict_after ON docs_strict FIELDS content ANALYZER 'simple'",
        )
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT id, bm25_score(content, 'consensus') FROM docs_strict ORDER BY id")
        .await
        .expect("bm25_score against post-hoc index must succeed");
    assert_eq!(rows.len(), 3, "expected 3 rows, got {rows:?}");

    let pairs = id_score_pairs(&rows);
    assert_term_rows_scored(&pairs, "strict / DDL after INSERT");
}
