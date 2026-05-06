// SPDX-License-Identifier: BUSL-1.1

use thiserror::Error;

#[derive(Debug, Error)]
pub enum PromqlError {
    #[error("unexpected token: expected {expected}, found {found}")]
    UnexpectedToken { expected: String, found: String },

    #[error("unexpected end of input")]
    UnexpectedEof,

    #[error("unknown PromQL function '{name}'")]
    UnknownFunction { name: String },

    #[error("wrong argument count for '{func}': expected {expected}, got {got}")]
    WrongArgCount {
        func: String,
        expected: usize,
        got: usize,
    },

    #[error("type error in '{context}': {detail}")]
    TypeError { context: String, detail: String },

    #[error("invalid duration literal '{literal}'")]
    InvalidDuration { literal: String },

    #[error("invalid string literal: {detail}")]
    InvalidString { detail: String },

    #[error("label matcher error: {detail}")]
    LabelMatcher { detail: String },

    #[error("selector error: {detail}")]
    Selector { detail: String },
}
