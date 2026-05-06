// SPDX-License-Identifier: BUSL-1.1

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExprParseError {
    #[error("unexpected token '{found}' at position {pos}")]
    UnexpectedToken { found: String, pos: usize },

    #[error("depth limit exceeded in expression (max {max})")]
    DepthLimitExceeded { max: usize },

    #[error("unknown function '{name}'")]
    UnknownFunction { name: String },

    #[error("unexpected end of expression")]
    UnexpectedEof,

    #[error("invalid literal: {detail}")]
    InvalidLiteral { detail: String },
}
