//! App struct definition and lifecycle (new, run).

use std::io::Stdout;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, MouseEventKind};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

use nodedb_client::NativeClient;

use crate::args::OutputFormat;
use crate::completion::{CompletionState, Completor};
use crate::history::History;

use super::super::input::InputState;
use super::super::render;
use super::UiAction;

/// Main TUI application.
pub struct App {
    pub client: NativeClient,
    pub input: InputState,
    pub result_output: Option<String>,
    pub error_message: Option<String>,
    pub output_format: OutputFormat,
    pub last_query_time: Option<Duration>,
    pub host: String,
    pub port: u16,
    pub scroll_offset: u16,
    pub show_timing: bool,
    pub expanded_mode: bool,
    pub last_query: Option<String>,
    pub output_file: Option<PathBuf>,
    pub history: History,
    pub running: bool,
    // Completion
    pub completor: Completor,
    pub completion: CompletionState,
    // History search (Ctrl+R)
    pub search_mode: bool,
    pub search_query: String,
    pub search_match_index: usize,
    // Watch mode
    pub watch_interval: Option<Duration>,
    pub watch_last_exec: Option<Instant>,
}

impl App {
    pub fn new(
        client: NativeClient,
        format: OutputFormat,
        host: String,
        port: u16,
        timing: bool,
        output_file: Option<PathBuf>,
    ) -> Self {
        Self {
            client,
            input: InputState::new(),
            result_output: None,
            error_message: None,
            output_format: format,
            last_query_time: None,
            host,
            port,
            scroll_offset: 0,
            show_timing: timing,
            expanded_mode: false,
            last_query: None,
            output_file,
            history: History::load(),
            running: true,
            completor: Completor::new(),
            completion: CompletionState::empty(),
            search_mode: false,
            search_query: String::new(),
            search_match_index: 0,
            watch_interval: None,
            watch_last_exec: None,
        }
    }

    /// Run the TUI event loop.
    pub async fn run(
        &mut self,
        terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    ) -> crate::error::CliResult<()> {
        self.completor.refresh_collections(&self.client).await;

        while self.running {
            terminal.draw(|frame| render::render(frame, self))?;

            // Watch mode: re-execute query on interval.
            if let (Some(interval), Some(last)) = (self.watch_interval, self.watch_last_exec)
                && last.elapsed() >= interval
                && let Some(query) = self.last_query.clone()
            {
                self.execute_sql(&query).await;
                self.watch_last_exec = Some(Instant::now());
            }

            if event::poll(Duration::from_millis(100))? {
                match event::read()? {
                    Event::Key(key) => {
                        let action = self.handle_key(key).await;
                        if let UiAction::OpenEditor = action {
                            self.open_external_editor(terminal)?;
                        }
                    }
                    Event::Mouse(mouse) => match mouse.kind {
                        MouseEventKind::ScrollUp => {
                            self.scroll_offset = self.scroll_offset.saturating_sub(3);
                        }
                        MouseEventKind::ScrollDown => {
                            self.scroll_offset = self.scroll_offset.saturating_add(3);
                        }
                        _ => {}
                    },
                    _ => {}
                }
            }
        }

        self.history.save();
        Ok(())
    }
}
