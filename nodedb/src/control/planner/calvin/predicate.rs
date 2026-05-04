//! Predicate class hashing for OLLP circuit-breaker keying.
//!
//! The `predicate_class` function maps a (collection, filter-text) pair to a
//! stable u64 hash used by the `OllpOrchestrator` to track per-predicate
//! retry and circuit-breaker state. Two queries with the same predicate shape
//! but different bound values produce the same class hash.

use crate::util::fnv1a_hash;

/// Compute a stable hash for a predicate class.
///
/// **Degraded path note**: `Filter` is not zerompk-encodable. This function
/// accepts the canonical SQL text representation of the filter and normalizes
/// numeric and string literals to their type tags before hashing. Two queries
/// with the same predicate shape but different bound values will produce the
/// same `predicate_class`. Example: `WHERE balance > 1000` and
/// `WHERE balance > 9999` both normalize to `WHERE balance > i64`.
///
/// The collection name is mixed in so predicates on different collections
/// don't collide.
pub fn predicate_class(canonical_filter_sql: &str, collection: &str) -> u64 {
    let normalized = normalize_predicate_text(canonical_filter_sql);
    let mut buf = Vec::with_capacity(collection.len() + normalized.len() + 1);
    buf.extend_from_slice(collection.as_bytes());
    buf.push(b'\x00');
    buf.extend_from_slice(normalized.as_bytes());
    fnv1a_hash(&buf)
}

/// Normalize a SQL text predicate by replacing literal values with type tags.
///
/// - Integer/float literals → `i64` or `f64`
/// - Quoted string literals → `str`
/// - Preserves operators, field names, and keywords
fn normalize_predicate_text(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];

        // Quoted string literal
        if c == '\'' {
            out.push_str("str");
            i += 1;
            while i < chars.len() {
                if chars[i] == '\'' {
                    i += 1;
                    // Handle escaped quote ''
                    if i < chars.len() && chars[i] == '\'' {
                        i += 1;
                    } else {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            continue;
        }

        // Numeric literal (integer or float)
        if c.is_ascii_digit() || (c == '-' && i + 1 < chars.len() && chars[i + 1].is_ascii_digit())
        {
            let mut is_float = false;
            i += 1; // skip leading digit or minus
            while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                if chars[i] == '.' {
                    is_float = true;
                }
                i += 1;
            }
            if is_float {
                out.push_str("f64");
            } else {
                out.push_str("i64");
            }
            continue;
        }

        out.push(c);
        i += 1;
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_shape_different_literal_same_hash() {
        let h1 = predicate_class("WHERE balance > 1000", "accounts");
        let h2 = predicate_class("WHERE balance > 9999", "accounts");
        assert_eq!(h1, h2);
    }

    #[test]
    fn different_field_different_hash() {
        let h1 = predicate_class("WHERE balance > 1000", "accounts");
        let h2 = predicate_class("WHERE age > 1000", "accounts");
        assert_ne!(h1, h2);
    }

    #[test]
    fn different_collection_different_hash() {
        let h1 = predicate_class("WHERE x > 1", "col_a");
        let h2 = predicate_class("WHERE x > 1", "col_b");
        assert_ne!(h1, h2);
    }

    #[test]
    fn string_literals_normalized() {
        let h1 = predicate_class("WHERE name = 'alice'", "users");
        let h2 = predicate_class("WHERE name = 'bob'", "users");
        assert_eq!(h1, h2);
    }

    #[test]
    fn float_literals_normalized() {
        let h1 = predicate_class("WHERE score > 1.5", "items");
        let h2 = predicate_class("WHERE score > 9.9", "items");
        assert_eq!(h1, h2);
    }
}
