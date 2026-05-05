//! Parse CREATE/DROP/ALTER/DESCRIBE/SHOW for COLLECTION (and TABLE alias).
//!
//! `DROP COLLECTION` extensions (sqlparser 0.61 does not tokenize
//! these, hence custom-handled upper-case keyword scan):
//! - `PURGE` — hard-delete, skipping the retention window
//! - `CASCADE` — recursively drop dependents
//! - `CASCADE FORCE` — cascade through dynamic-SQL schedules
//!
//! `UNDROP COLLECTION <name>` restores a soft-deleted record.

mod alter_ops;
mod column_list;
mod with_clause;

use super::helpers::{extract_name_after_if_exists, extract_name_after_keyword};
use crate::ddl_ast::statement::NodedbStatement;
use crate::error::SqlError;

use alter_ops::parse_alter_operation;
use column_list::extract_column_pairs;
use with_clause::{extract_balanced_raw, extract_with_options};

pub(super) fn try_parse(
    upper: &str,
    parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    (|| -> Result<Option<NodedbStatement>, SqlError> {
        if upper.starts_with("CREATE COLLECTION ") {
            let if_not_exists = upper.contains("IF NOT EXISTS");
            let name = match extract_name_after_keyword(parts, "COLLECTION") {
                None => return Ok(None),
                Some(r) => r?,
            };
            let (engine, columns, options, flags, balanced_raw) =
                parse_collection_body(trimmed, &name)?;
            return Ok(Some(NodedbStatement::CreateCollection {
                name,
                if_not_exists,
                engine,
                columns,
                options,
                flags,
                balanced_raw,
            }));
        }
        if upper.starts_with("CREATE TABLE ") {
            let if_not_exists = upper.contains("IF NOT EXISTS");
            let name = match extract_name_after_keyword(parts, "TABLE") {
                None => return Ok(None),
                Some(r) => r?,
            };
            let (engine, columns, options, flags, balanced_raw) =
                parse_collection_body(trimmed, &name)?;
            return Ok(Some(NodedbStatement::CreateTable {
                name,
                if_not_exists,
                engine,
                columns,
                options,
                flags,
                balanced_raw,
            }));
        }
        if upper.starts_with("UNDROP COLLECTION ") || upper.starts_with("UNDROP TABLE ") {
            let name = match extract_name_after_keyword(parts, "COLLECTION")
                .or_else(|| extract_name_after_keyword(parts, "TABLE"))
            {
                None => return Ok(None),
                Some(r) => r?,
            };
            return Ok(Some(NodedbStatement::UndropCollection { name }));
        }
        if upper.starts_with("DROP COLLECTION ") || upper.starts_with("DROP TABLE ") {
            let if_exists = upper.contains("IF EXISTS");
            let name = match extract_name_after_if_exists(parts, "COLLECTION")
                .or_else(|| extract_name_after_if_exists(parts, "TABLE"))
            {
                None => return Ok(None),
                Some(r) => r?,
            };
            let purge = upper.contains(" PURGE");
            let cascade = upper.contains(" CASCADE");
            let cascade_force =
                upper.contains(" CASCADE FORCE") || upper.contains(" FORCE CASCADE");
            return Ok(Some(NodedbStatement::DropCollection {
                name,
                if_exists,
                purge,
                cascade: cascade || cascade_force,
                cascade_force,
            }));
        }
        if upper.starts_with("ALTER COLLECTION ") || upper.starts_with("ALTER TABLE ") {
            let name = match extract_name_after_keyword(parts, "COLLECTION")
                .or_else(|| extract_name_after_keyword(parts, "TABLE"))
            {
                None => return Ok(None),
                Some(r) => r?,
            };
            let operation = match parse_alter_operation(upper, parts, trimmed, &name) {
                None => return Ok(None),
                Some(op) => op,
            };
            return Ok(Some(NodedbStatement::AlterCollection { name, operation }));
        }
        if upper.starts_with("DESCRIBE ") && !upper.starts_with("DESCRIBE SEQUENCE") {
            let name = match parts.get(1) {
                None => return Ok(None),
                Some(s) => s.to_string(),
            };
            return Ok(Some(NodedbStatement::DescribeCollection { name }));
        }
        if upper == "\\D" || upper == "SHOW COLLECTIONS" || upper.starts_with("SHOW COLLECTIONS") {
            return Ok(Some(NodedbStatement::ShowCollections));
        }
        Ok(None)
    })()
    .transpose()
}

// ── Body parsing ─────────────────────────────────────────────────────────────

/// Parse everything after the collection/table name into typed fields.
///
/// Returns `(engine, columns, options, flags, balanced_raw)`:
/// - `engine`: value of `engine=` from the WITH clause (lowercased), if present.
/// - `columns`: `(name, type)` pairs from the parenthesised column list.
/// - `options`: remaining WITH clause `key=value` pairs (excluding `engine`).
/// - `flags`: free-standing modifier keywords: `APPEND_ONLY`, `HASH_CHAIN`, `BITEMPORAL`.
/// - `balanced_raw`: raw interior of the `BALANCED ON (...)` clause, or `None`.
type CollectionBody = (
    Option<String>,
    Vec<(String, String)>,
    Vec<(String, String)>,
    Vec<String>,
    Option<String>,
);

fn parse_collection_body(trimmed: &str, name: &str) -> Result<CollectionBody, SqlError> {
    // Skip past the name to find the body.
    let lower = trimmed.to_lowercase();
    let name_lower = name.to_lowercase();
    let body = if let Some(pos) = lower.find(&name_lower) {
        trimmed[pos + name.len()..].trim()
    } else {
        return Ok((None, Vec::new(), Vec::new(), Vec::new(), None));
    };

    let upper_body = body.to_uppercase();

    // ── Column list ───────────────────────────────────────────────
    let columns = extract_column_pairs(body)?;

    // ── WITH clause ───────────────────────────────────────────────
    let (engine, options) = extract_with_options(body);

    // ── Free-standing flags ───────────────────────────────────────
    let mut flags: Vec<String> = Vec::new();
    if upper_body.contains("APPEND_ONLY") {
        flags.push("APPEND_ONLY".to_string());
    }
    if upper_body.contains("HASH_CHAIN") {
        flags.push("HASH_CHAIN".to_string());
    }
    if upper_body.contains("BITEMPORAL") {
        flags.push("BITEMPORAL".to_string());
    }

    // ── BALANCED ON (group_key = col, ...) ───────────────────────
    let balanced_raw = extract_balanced_raw(&upper_body, body);

    Ok((engine, columns, options, flags, balanced_raw))
}
