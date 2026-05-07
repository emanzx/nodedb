// SPDX-License-Identifier: Apache-2.0

//! Constant folding: evaluate constant expressions at plan time.
//!
//! Examples: `1 + 2` → `3`, `WHERE 1 = 1` → remove filter.
//! Currently a placeholder — the Data Plane handles expression evaluation.
