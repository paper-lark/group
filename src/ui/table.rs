use std::collections::HashMap;

use tui::backend;
use tui::layout;
use tui::style;
use tui::widgets;
use tui::Frame;

use crate::io::dataframe;
use crate::io::dataframe::DataFrame;
use crate::ui::card;
use crate::ui::colorizer;
use crate::ui::footer;
use crate::ui::timeline;
use std::collections::VecDeque;

pub struct Table<'a> {
    source_df: &'a dataframe::MaterializedDataFrame,
    state: VecDeque<TableState<'a>>,
    group_columns: &'a [String],
    timeline_column: &'a Option<String>,
}

struct TableState<'a> {
    mode_state: TableModeState<'a>,
    table_state: widgets::TableState,
    selected: usize,
}

enum TableModeState<'a> {
    Grouped(dataframe::DataFrameGroupView<'a>),
    Filtered(dataframe::DataFrameFilterView<'a>, bool),
}

impl<'a> TableModeState<'a> {
    fn get_name(&self) -> &'static str {
        match self {
            TableModeState::Grouped(_) => "GROUPED",
            TableModeState::Filtered(_, _) => "FILTERED",
        }
    }
}

const TIMELINE_WIDTH: u16 = 32;
const MAX_STRING_WIDTH: u16 = 32;

impl<'a> Table<'a> {
    pub fn new(
        source_df: &'a dataframe::MaterializedDataFrame,
        group_columns: &'a [String],
        show_in_grouped_mode: &'a [String],
        timeline_column: &'a Option<String>,
    ) -> Table<'a> {
        let mut table = Table {
            source_df,
            state: VecDeque::from([TableState {
                mode_state: TableModeState::Grouped(source_df.group_by(group_columns, show_in_grouped_mode)),
                table_state: widgets::TableState::default(),
                selected: 0,
            }]),
            group_columns,
            timeline_column,
        };
        table.set_selected(0);
        table
    }

    pub fn focus(&mut self) {
        let state = self.get_current_state();
        if let TableModeState::Grouped(df) = &state.mode_state {
            let mut filter: HashMap<String, dataframe::ColumnValue> = HashMap::new();
            for name in self.group_columns {
                let v = &df[(name, state.selected)];
                filter.insert(name.clone(), v.clone());
            }

            self.state.push_back(TableState {
                mode_state: TableModeState::Filtered(self.source_df.filter(&filter), false),
                table_state: widgets::TableState::default(),
                selected: 0,
            });
            self.set_selected(0);
            return;
        }

        let mut_state = self.get_current_state_mut();
        if let TableModeState::Filtered(_, focused) = &mut mut_state.mode_state {
            *focused = !*focused;
        }
    }

    pub fn back(&mut self) -> bool {
        match &mut self.get_current_state_mut().mode_state {
            TableModeState::Filtered(_, focused) => {
                if *focused {
                    *focused = false;
                } else {
                    self.state.pop_back();
                }
                true
            }
            TableModeState::Grouped(_) => false,
        }
    }

    pub fn move_selected(&mut self, up: bool) {
        let new_index = {
            let state = self.get_current_state();
            let len = match &state.mode_state {
                TableModeState::Filtered(df, _) => df.len(),
                TableModeState::Grouped(df) => df.len(),
            };
            (state.selected + (if up { len - 1 } else { 1 })) % len
        };
        self.set_selected(new_index);
    }

    pub fn render<B: backend::Backend>(&mut self, f: &mut Frame<B>) {
        // create table widget
        let column_widths = self.get_column_widths();
        let table_contents = self.get_table_contents();
        let row_count = table_contents.len();
        let current_state = self.get_current_state();
        let table_widget = widgets::Table::new(table_contents)
            .header(self.get_table_header())
            .highlight_symbol("> ")
            .highlight_style(style::Style::default().add_modifier(style::Modifier::HIDDEN | style::Modifier::BOLD))
            .widths(&column_widths)
            .column_spacing(2);

        // create footer
        let footer_widget = footer::Footer::new(current_state.mode_state.get_name(), current_state.selected + 1, row_count);

        // create card
        let card_widget = if let TableModeState::Filtered(df, focused) = &current_state.mode_state {
            if *focused {
                Some(card::Card::new(df.raw(current_state.selected)))
            } else {
                None
            }
        } else {
            None
        };

        // place widgets
        let size = f.size();
        let chunks = layout::Layout::default()
            .direction(layout::Direction::Vertical)
            .constraints(
                [
                    layout::Constraint::Min(0),
                    layout::Constraint::Length(usize_to_u16(footer_widget.height)),
                ]
                .as_ref(),
            )
            .split(size);
        let footer_size = chunks[1];
        let table_size = if let Some(card_widget) = card_widget {
            let chunks = layout::Layout::default()
                .direction(layout::Direction::Vertical)
                .constraints(
                    [
                        layout::Constraint::Min(0),
                        layout::Constraint::Length(usize_to_u16(card_widget.height)),
                    ]
                    .as_ref(),
                )
                .split(chunks[0]);
            f.render_widget(card_widget.widget, chunks[1]);
            chunks[0]
        } else {
            chunks[0]
        };

        f.render_widget(footer_widget.widget, footer_size);

        let state = self.get_current_state_mut();
        f.render_stateful_widget(table_widget, table_size, &mut state.table_state);
    }

    fn set_selected(&mut self, value: usize) {
        let state = self.get_current_state_mut();
        state.selected = value;
        state.table_state.select(Some(value));
    }

    fn get_current_state(&self) -> &TableState<'a> {
        self.state.back().expect("table state cannot be empty")
    }
    fn get_current_state_mut(&mut self) -> &mut TableState<'a> {
        self.state.back_mut().expect("table state cannot be empty")
    }

    fn get_table_contents<'b>(&mut self) -> Vec<widgets::Row<'b>> {
        let mut table_contents: Vec<widgets::Row> = Vec::new();

        macro_rules! put_entries {
            ($df:ident, $timeline_column:expr => $contents:ident) => {
                for i in 0..$df.len() {
                    let mut row_cells = Vec::new();
                    for name in self.get_column_names() {
                        let column = &self.source_df[name];
                        let colorize = colorizer::select(column);
                        let v = &$df[(name, i)];
                        row_cells.push(widgets::Cell::from(v.to_string()).style(style::Style::default().fg(colorize(v))));
                    }
                    if let Some(t) = &$timeline_column {
                        row_cells.push(widgets::Cell::from(t[i].clone()));
                    }
                    $contents.push(widgets::Row::new(row_cells));
                }
            };
        }

        match &self.get_current_state().mode_state {
            TableModeState::Filtered(df, _) => put_entries!(df, None::<Vec<String>> => table_contents),
            TableModeState::Grouped(df) => {
                let timeline_column = self
                    .timeline_column
                    .as_ref()
                    .map(|c| timeline::create_timeline_column(self.source_df, df, c, TIMELINE_WIDTH));
                put_entries!(df, timeline_column => table_contents)
            }
        };

        table_contents
    }

    fn get_table_header<'b>(&self) -> widgets::Row<'b> {
        let cells: Vec<widgets::Cell> = self
            .get_column_names()
            .into_iter()
            .map(|c| widgets::Cell::from(c.clone()))
            .collect();
        widgets::Row::new(cells)
            .style(style::Style::default().fg(style::Color::Yellow).add_modifier(style::Modifier::BOLD))
            .bottom_margin(1)
    }

    fn get_column_widths(&self) -> Vec<layout::Constraint> {
        let mut contraints: Vec<_> = self
            .get_column_names()
            .into_iter()
            .map(|name| {
                let column = &self.source_df[name];
                let lens: Vec<usize> = column.values.iter().map(get_column_value_width).collect();
                let max_len = lens.iter().fold(name.len(), |a, b| a.max(*b));
                if max_len < MAX_STRING_WIDTH as usize {
                    #[allow(clippy::cast_possible_truncation)]
                    layout::Constraint::Length(max_len as u16)
                } else {
                    layout::Constraint::Min(MAX_STRING_WIDTH)
                }
            })
            .collect();
        if self.timeline_column.is_some() {
            if let TableModeState::Grouped(_) = self.get_current_state().mode_state {
                contraints.push(layout::Constraint::Length(TIMELINE_WIDTH));
            }
        }
        contraints
    }

    fn get_column_names(&self) -> Vec<&String> {
        match &self.get_current_state().mode_state {
            TableModeState::Filtered(df, _) => df.column_names(),
            TableModeState::Grouped(df) => df.column_names(),
        }
    }
}

fn get_column_value_width(value: &dataframe::ColumnValue) -> usize {
    match value {
        dataframe::ColumnValue::Boolean(_) | dataframe::ColumnValue::None => 1,
        dataframe::ColumnValue::String(s) => s.len(),
        dataframe::ColumnValue::Integer(_) => 16,
        dataframe::ColumnValue::DateTime(_) => 12,
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
