// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for PostgreSQL-style JSON path operators.
//!
//! Covers all 9 operators in SELECT projection and WHERE filter positions:
//! `->`, `->>`', `#>`, `#>>`, `@>`, `<@`, `?`, `?|`, `?&`.
//!
//! Each test inserts a row with a JSON-valued field into a document_strict
//! collection and exercises the operator end-to-end through the pgwire wire.

mod common;
use common::pgwire_harness::TestServer;

// ── helpers ──────────────────────────────────────────────────────────────────

/// Bootstrap a document_strict collection with a single TEXT column `data`
/// used to hold JSON strings, plus an `id` primary key.
async fn setup_json_table(srv: &TestServer, name: &str) {
    srv.exec(&format!(
        "CREATE COLLECTION {name} (id TEXT PRIMARY KEY, data TEXT) \
         WITH (engine='document_strict')"
    ))
    .await
    .unwrap_or_else(|e| panic!("CREATE {name}: {e}"));
}

// ── `->` get-field-as-json ────────────────────────────────────────────────

#[tokio::test]
async fn arrow_projection_returns_field_value() {
    let srv = TestServer::start().await;
    setup_json_table(&srv, "t_arrow").await;

    srv.exec("INSERT INTO t_arrow (id, data) VALUES ('r1', '{\"name\":\"Alice\",\"age\":30}')")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT data->'name' FROM t_arrow WHERE id = 'r1'")
        .await
        .expect("query should succeed");
    assert_eq!(rows.len(), 1, "expected one row");
    assert_eq!(rows[0][0], "Alice", "->  should return the field value");
}

#[tokio::test]
async fn arrow_missing_field_returns_null() {
    let srv = TestServer::start().await;
    setup_json_table(&srv, "t_arrow_null").await;

    srv.exec("INSERT INTO t_arrow_null (id, data) VALUES ('r1', '{\"x\":1}')")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT data->'missing' FROM t_arrow_null WHERE id = 'r1'")
        .await
        .expect("query should succeed");
    assert_eq!(rows.len(), 1);
    // Missing key → NULL, rendered as empty string by pgwire harness.
    assert_eq!(rows[0][0], "", "missing key should be NULL");
}

#[tokio::test]
async fn arrow_array_index() {
    let srv = TestServer::start().await;
    setup_json_table(&srv, "t_arrow_arr").await;

    srv.exec("INSERT INTO t_arrow_arr (id, data) VALUES ('r1', '[10,20,30]')")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT data->1 FROM t_arrow_arr WHERE id = 'r1'")
        .await
        .expect("query should succeed");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][0], "20",
        "integer index should select array element"
    );
}

// ── `->>`  get-field-as-text ───────────────────────────────────────────────

#[tokio::test]
async fn long_arrow_returns_text() {
    let srv = TestServer::start().await;
    setup_json_table(&srv, "t_long").await;

    srv.exec("INSERT INTO t_long (id, data) VALUES ('r1', '{\"score\":42}')")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT data->>'score' FROM t_long WHERE id = 'r1'")
        .await
        .expect("query should succeed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "42", "->>'  should return value as text");
}

#[tokio::test]
async fn long_arrow_in_where_filter() {
    let srv = TestServer::start().await;
    setup_json_table(&srv, "t_long_where").await;

    srv.exec("INSERT INTO t_long_where (id, data) VALUES ('r1', '{\"status\":\"active\"}')")
        .await
        .unwrap();
    srv.exec("INSERT INTO t_long_where (id, data) VALUES ('r2', '{\"status\":\"inactive\"}')")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT id FROM t_long_where WHERE data->>'status' = 'active' ORDER BY id")
        .await
        .expect("query should succeed");
    assert_eq!(rows.len(), 1, "only the active row should match");
    assert_eq!(rows[0][0], "r1");
}

// ── `#>` path-get-as-json ─────────────────────────────────────────────────

#[tokio::test]
async fn hash_arrow_nested_path() {
    let srv = TestServer::start().await;
    setup_json_table(&srv, "t_hash").await;

    srv.exec("INSERT INTO t_hash (id, data) VALUES ('r1', '{\"a\":{\"b\":{\"c\":\"deep\"}}}')")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT data#>'{a,b,c}' FROM t_hash WHERE id = 'r1'")
        .await
        .expect("query should succeed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "deep", "#> should traverse nested path");
}

// ── `#>>` path-get-as-text ────────────────────────────────────────────────

#[tokio::test]
async fn hash_long_arrow_returns_text() {
    let srv = TestServer::start().await;
    setup_json_table(&srv, "t_hashlong").await;

    srv.exec("INSERT INTO t_hashlong (id, data) VALUES ('r1', '{\"x\":{\"v\":99}}')")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT data#>>'{x,v}' FROM t_hashlong WHERE id = 'r1'")
        .await
        .expect("query should succeed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "99", "#>> should return nested value as text");
}

// ── `@>` contains ─────────────────────────────────────────────────────────

#[tokio::test]
async fn at_arrow_contains_filter() {
    let srv = TestServer::start().await;
    setup_json_table(&srv, "t_contains").await;

    srv.exec("INSERT INTO t_contains (id, data) VALUES ('r1', '{\"a\":1,\"b\":2}')")
        .await
        .unwrap();
    srv.exec("INSERT INTO t_contains (id, data) VALUES ('r2', '{\"a\":1}')")
        .await
        .unwrap();

    // Only the row that contains {"a":1,"b":2} should pass the @> check.
    let rows = srv
        .query_rows("SELECT id FROM t_contains WHERE data @> '{\"a\":1,\"b\":2}' ORDER BY id")
        .await
        .expect("query should succeed");
    assert_eq!(rows.len(), 1, "@> should only match the superset");
    assert_eq!(rows[0][0], "r1");
}

#[tokio::test]
async fn at_arrow_contains_projection() {
    let srv = TestServer::start().await;
    setup_json_table(&srv, "t_contains_proj").await;

    srv.exec("INSERT INTO t_contains_proj (id, data) VALUES ('r1', '{\"k\":\"v\"}')")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT data @> '{\"k\":\"v\"}' FROM t_contains_proj WHERE id = 'r1'")
        .await
        .expect("query should succeed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "true", "@> should return true for exact match");
}

// ── `<@` contained-by ─────────────────────────────────────────────────────

#[tokio::test]
async fn arrow_at_contained_by_projection() {
    let srv = TestServer::start().await;
    setup_json_table(&srv, "t_contained").await;

    srv.exec("INSERT INTO t_contained (id, data) VALUES ('r1', '{\"a\":1,\"b\":2}')")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT '{\"a\":1}' <@ data FROM t_contained WHERE id = 'r1'")
        .await
        .expect("query should succeed");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][0], "true",
        "<@ should return true when left is contained by right"
    );
}

// ── `?` key-exists ────────────────────────────────────────────────────────

#[tokio::test]
async fn question_key_exists_projection() {
    let srv = TestServer::start().await;
    setup_json_table(&srv, "t_qmark").await;

    srv.exec("INSERT INTO t_qmark (id, data) VALUES ('r1', '{\"present\":1}')")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT data ? 'present', data ? 'absent' FROM t_qmark WHERE id = 'r1'")
        .await
        .expect("query should succeed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "true", "? should return true for present key");
    assert_eq!(rows[0][1], "false", "? should return false for absent key");
}

#[tokio::test]
async fn question_key_exists_where_filter() {
    let srv = TestServer::start().await;
    setup_json_table(&srv, "t_qmark_where").await;

    srv.exec("INSERT INTO t_qmark_where (id, data) VALUES ('r1', '{\"email\":\"a@b.com\"}')")
        .await
        .unwrap();
    srv.exec("INSERT INTO t_qmark_where (id, data) VALUES ('r2', '{\"phone\":\"555\"}')")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT id FROM t_qmark_where WHERE data ? 'email' ORDER BY id")
        .await
        .expect("query should succeed");
    assert_eq!(rows.len(), 1, "only the row with 'email' should match");
    assert_eq!(rows[0][0], "r1");
}

// ── `?|` any-of-keys-exist ────────────────────────────────────────────────

#[tokio::test]
async fn question_pipe_any_key_exists() {
    let srv = TestServer::start().await;
    setup_json_table(&srv, "t_qpipe").await;

    srv.exec("INSERT INTO t_qpipe (id, data) VALUES ('r1', '{\"b\":2}')")
        .await
        .unwrap();
    srv.exec("INSERT INTO t_qpipe (id, data) VALUES ('r2', '{\"c\":3}')")
        .await
        .unwrap();

    // r1 has key 'b' which is in the list; r2 has neither 'a' nor 'b'.
    let rows = srv
        .query_rows("SELECT id FROM t_qpipe WHERE data ?| ARRAY['a','b'] ORDER BY id")
        .await
        .expect("query should succeed");
    assert_eq!(rows.len(), 1, "?| should match row with any listed key");
    assert_eq!(rows[0][0], "r1");
}

// ── `?&` all-keys-exist ───────────────────────────────────────────────────

#[tokio::test]
async fn question_amp_all_keys_exist() {
    let srv = TestServer::start().await;
    setup_json_table(&srv, "t_qamp").await;

    srv.exec("INSERT INTO t_qamp (id, data) VALUES ('r1', '{\"a\":1,\"b\":2,\"c\":3}')")
        .await
        .unwrap();
    srv.exec("INSERT INTO t_qamp (id, data) VALUES ('r2', '{\"a\":1}')")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT id FROM t_qamp WHERE data ?& ARRAY['a','b'] ORDER BY id")
        .await
        .expect("query should succeed");
    assert_eq!(
        rows.len(),
        1,
        "?& should only match row with all required keys"
    );
    assert_eq!(rows[0][0], "r1");
}

// ── error cases ───────────────────────────────────────────────────────────

#[tokio::test]
async fn hash_arrow_out_of_bounds_array_path_returns_null() {
    let srv = TestServer::start().await;
    setup_json_table(&srv, "t_oob").await;

    srv.exec("INSERT INTO t_oob (id, data) VALUES ('r1', '[1,2,3]')")
        .await
        .unwrap();

    // Path '{99}' is an out-of-bounds integer index → PG returns NULL.
    let rows = srv
        .query_rows("SELECT data#>'{99}' FROM t_oob WHERE id = 'r1'")
        .await
        .expect("out-of-bounds path should return NULL, not an error");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][0], "",
        "out-of-bounds index should yield NULL (empty string)"
    );
}
