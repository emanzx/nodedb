//! TUI application module.

mod editor;
mod execute;
mod input_handler;
mod search;
mod state;

pub use state::App;

/// Actions that require terminal access (can't be done inside handle_key).
pub(super) enum UiAction {
    None,
    OpenEditor,
}
