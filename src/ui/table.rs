use std::collections::HashMap;

use tui::backend;
use tui::layout;
use tui::style;
use tui::widgets;
use tui::Frame;

use crate::io::dataframe;
use crate::io::dataframe::DataFrame;
use crate::ui::colorizer;
use std::collections::VecDeque;

pub struct Table<'a> {
    source_df: &'a dataframe::MaterializedDataFrame,
    state: VecDeque<TableState<'a>>,
    group_columns: &'a [String],
}

struct TableState<'a> {
    mode_state: TableModeState<'a>,
    table_state: widgets::TableState,
    selected: usize,
}

enum TableModeState<'a> {
    Grouped(dataframe::DataFrameGroupView<'a>),
    Filtered(dataframe::DataFrameFilterView<'a>, HashMap<String, dataframe::ColumnValue>),
}

impl<'a> Table<'a> {
    pub fn new(source_df: &'a dataframe::MaterializedDataFrame, group_columns: &'a [String]) -> Table<'a> {
        let mut table = Table {
            source_df,
            state: VecDeque::from([TableState {
                mode_state: TableModeState::Grouped(source_df.group_by(group_columns)),
                table_state: widgets::TableState::default(),
                selected: 0,
            }]),
            group_columns,
        };
        table.set_selected(0);
        table
    }

    pub fn set_filter(&mut self) {
        let state = self.get_current_state();
        if let TableModeState::Grouped(df) = &state.mode_state {
            let mut filter: HashMap<String, dataframe::ColumnValue> = HashMap::new();
            for name in self.group_columns {
                let v = &df[(name.clone(), state.selected)];
                filter.insert(name.clone(), v.clone());
            }

            self.state.push_back(TableState {
                mode_state: TableModeState::Filtered(self.source_df.filter(&filter), filter),
                table_state: widgets::TableState::default(),
                selected: 0,
            });
            self.set_selected(0);
        }
    }

    pub fn has_filter(&self) -> bool {
        match self.get_current_state().mode_state {
            TableModeState::Filtered(_, _) => true,
            TableModeState::Grouped(_) => false,
        }
    }

    pub fn reset_filter(&mut self) {
        if let TableModeState::Filtered(_, _) = self.get_current_state().mode_state {
            self.state.pop_back();
        }
    }

    fn set_selected(&mut self, value: usize) {
        let state = self.get_current_state_mut();
        state.selected = value;
        state.table_state.select(Some(value));
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
        let column_widths = self.get_column_widths();
        let table = widgets::Table::new(self.get_table_contents())
            .header(self.get_table_header())
            .block(widgets::Block::default().borders(widgets::Borders::ALL))
            .highlight_style(
                style::Style::default()
                    .bg(style::Color::DarkGray)
                    .add_modifier(style::Modifier::BOLD),
            )
            .widths(&column_widths)
            .column_spacing(1);

        let size = f.size();
        let table_state = &mut self.get_current_state_mut().table_state;
        f.render_stateful_widget(table, size, table_state);
    }

    fn get_current_state(&self) -> &TableState<'a> {
        self.state.back().expect("table state cannot be empty")
    }
    fn get_current_state_mut(&mut self) -> &mut TableState<'a> {
        self.state.back_mut().expect("table state cannot be empty")
    }

    fn get_table_contents<'b>(&mut self) -> Vec<widgets::Row<'b>> {
        let mut table_contents: Vec<widgets::Row> = Vec::new();

        match &self.get_current_state().mode_state {
            TableModeState::Filtered(df, _) => {
                for i in 0..df.len() {
                    let mut row_cells = Vec::new();
                    for name in self.get_column_names() {
                        let column = &self.source_df[name.clone()];
                        let colorize = colorizer::select(column);
                        let v = &df[(name.clone(), i)];
                        row_cells.push(widgets::Cell::from(v.to_string()).style(style::Style::default().fg(colorize(v))));
                    }
                    table_contents.push(widgets::Row::new(row_cells));
                }
            }
            TableModeState::Grouped(df) => {
                for i in 0..df.len() {
                    let mut row_cells = Vec::new();
                    for name in self.get_column_names() {
                        let column = &self.source_df[name.clone()];
                        let colorize = colorizer::select(column);
                        let v = &df[(name.clone(), i)];
                        row_cells.push(widgets::Cell::from(v.to_string()).style(style::Style::default().fg(colorize(v))));
                    }
                    table_contents.push(widgets::Row::new(row_cells));
                }
            }
        }

        table_contents
    }

    fn get_table_header<'b>(&self) -> widgets::Row<'b> {
        let cells: Vec<widgets::Cell> = self
            .get_column_names()
            .into_iter()
            .map(|c| widgets::Cell::from(c.clone()))
            .collect();
        widgets::Row::new(cells)
            .style(style::Style::default().fg(style::Color::Yellow))
            .bottom_margin(1)
    }

    fn get_column_widths(&self) -> Vec<layout::Constraint> {
        self.get_column_names()
            .into_iter()
            .map(|name| match self.source_df[name.clone()].attr_type {
                dataframe::InputAttributeType::String => layout::Constraint::Percentage(30),
                dataframe::InputAttributeType::Integer => layout::Constraint::Length(16),
                dataframe::InputAttributeType::DateTime => layout::Constraint::Length(12),
            })
            .collect()
    }

    fn get_column_names(&self) -> Vec<&String> {
        match &self.get_current_state().mode_state {
            TableModeState::Filtered(df, _) => df.column_names(),
            TableModeState::Grouped(df) => df.column_names(),
        }
    }
}
