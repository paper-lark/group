use std::collections::HashMap;

use tui::backend;
use tui::layout;
use tui::Frame;

use crate::io::dataframe;
use crate::ui::card;
use crate::ui::footer;
use crate::ui::table;
use crate::ui::timeline;
use std::collections::VecDeque;

struct AppState<'a> {
    table_view_model: table::ViewModel<'a>,
    mode: AppMode,
}

enum AppMode {
    Grouped,
    Filtered(bool),
}

impl AppMode {
    fn get_name(&self) -> &'static str {
        match self {
            AppMode::Grouped => "GROUPED",
            AppMode::Filtered(_) => "FILTERED",
        }
    }
}

pub struct ViewModel<'a> {
    source_df: &'a dataframe::MaterializedDataFrame,
    state: VecDeque<AppState<'a>>,
}

impl<'a> ViewModel<'a> {
    pub fn new(
        source_df: &'a dataframe::MaterializedDataFrame,
        group_columns: &'a [String],
        show_in_grouped_mode: &'a [String],
        timeline_column: &'a Option<String>,
    ) -> ViewModel<'a> {
        let df = source_df.group_by(group_columns, show_in_grouped_mode);

        let timeline_column = timeline_column
            .as_ref()
            .map(|c| timeline::create_timeline_column(source_df, &df, c, table::TIMELINE_WIDTH));
        ViewModel {
            source_df,
            state: VecDeque::from([AppState {
                table_view_model: table::ViewModel::new(Box::from(df), timeline_column),
                mode: AppMode::Grouped,
            }]),
        }
    }

    pub fn move_selected(&mut self, up: bool) {
        self.get_current_state_mut().table_view_model.move_selected(up);
    }

    pub fn focus(&mut self) {
        let state = self.get_current_state_mut();
        match &mut state.mode {
            AppMode::Grouped => {
                let selected = state.table_view_model.selected_row();
                let filter: HashMap<String, dataframe::ColumnValue> = state
                    .table_view_model
                    .df
                    .column_names()
                    .into_iter()
                    .enumerate()
                    .map(|(i, c)| (c.clone(), selected[i].clone()))
                    .collect();

                let df = self.source_df.filter(&filter);
                self.state.push_back(AppState {
                    table_view_model: table::ViewModel::new(Box::from(df), None),
                    mode: AppMode::Filtered(false),
                });
            }
            AppMode::Filtered(focused) => {
                *focused = !*focused;
            }
        };
    }

    pub fn back(&mut self) -> bool {
        let state = self.get_current_state_mut();
        match &mut state.mode {
            AppMode::Grouped => false,
            AppMode::Filtered(focused) => {
                if *focused {
                    *focused = false;
                } else {
                    self.state.pop_back();
                }
                true
            }
        }
    }

    fn get_current_state(&self) -> &AppState<'a> {
        self.state.back().expect("app state cannot be empty")
    }
    fn get_current_state_mut(&mut self) -> &mut AppState<'a> {
        self.state.back_mut().expect("app state cannot be empty")
    }
}

pub struct View<'a: 'b, 'b> {
    view_model: &'b mut ViewModel<'a>,
}

impl<'a: 'b, 'b> View<'a, 'b> {
    pub fn new(view_model: &'b mut ViewModel<'a>) -> View<'a, 'b> {
        View { view_model }
    }

    pub fn render<B: backend::Backend>(&mut self, frame: &'b mut Frame<B>) {
        // create views
        let current_state = self.view_model.get_current_state();
        let row_count = current_state.table_view_model.df.len();
        let selected = current_state.table_view_model.selected;
        let footer_view = footer::Footer::new(current_state.mode.get_name(), selected + 1, row_count);
        let card_view = if let AppMode::Filtered(focused) = &current_state.mode {
            if *focused {
                Some(card::View::new(current_state.table_view_model.df.raw(selected)))
            } else {
                None
            }
        } else {
            None
        };
        let current_state = &mut self.view_model.get_current_state_mut().table_view_model;
        let table_view = table::View::new(current_state);

        // render views
        let size = frame.size();
        let chunks = layout::Layout::default()
            .direction(layout::Direction::Vertical)
            .constraints(
                [
                    layout::Constraint::Min(0),
                    layout::Constraint::Length(usize_to_u16(footer_view.get_height())),
                ]
                .as_ref(),
            )
            .split(size);
        let table_size = if let Some(card_view) = card_view {
            let chunks = layout::Layout::default()
                .direction(layout::Direction::Vertical)
                .constraints(
                    [
                        layout::Constraint::Min(0),
                        layout::Constraint::Length(usize_to_u16(card_view.get_height())),
                    ]
                    .as_ref(),
                )
                .split(chunks[0]);
            card_view.render(frame, chunks[1]);
            chunks[0]
        } else {
            chunks[0]
        };
        footer_view.render(frame, chunks[1]);
        table_view.render(frame, table_size);
    }
}

#[allow(clippy::cast_possible_truncation)]
fn usize_to_u16(v: usize) -> u16 {
    if v < std::u16::MAX as usize {
        v as u16
    } else {
        std::u16::MAX
    }
}
