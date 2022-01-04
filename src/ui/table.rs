use std::collections::HashMap;

use tui::backend;
use tui::layout;
use tui::style;
use tui::widgets;
use tui::Frame;

use crate::io::dataframe;
use crate::ui::colorizer;

pub struct Table<'a> {
    source_df: &'a dataframe::MaterializedDataFrame,
    displayed_df: Box<dyn dataframe::DataFrame + 'a>,
    group_columns: &'a [String],
    grouped_content: Vec<Vec<usize>>, // indices of all occurences in order of occurence
    filter: HashMap<String, dataframe::ColumnValue>,
    selected: usize,
    state: widgets::TableState,
}

impl<'a> Table<'a> {
    pub fn new(source_df: &'a dataframe::MaterializedDataFrame, group_columns: &'a [String]) -> Table<'a> {
        let mut table = Table {
            source_df,
            displayed_df: Box::new(source_df.filter(&HashMap::new())),
            group_columns,
            grouped_content: Vec::new(),
            filter: HashMap::new(),
            selected: 0,
            state: widgets::TableState::default(),
        };
        table.reset_filter();
        table
    }

    pub fn set_filter(&mut self) {
        let index_in_grouped = self.selected;
        let index_in_df = self.grouped_content[index_in_grouped][0];
        if !self.has_filter() {
            for name in self.group_columns {
                let column = &self.displayed_df[name.clone()];
                self.filter.insert(name.clone(), column[index_in_df].clone());
            }
            self.set_selected(0);
            self.group_content();
        }
    }

    pub fn has_filter(&self) -> bool {
        !self.filter.is_empty()
    }

    pub fn reset_filter(&mut self) {
        self.filter.clear();
        self.set_selected(0);
        self.group_content();
    }

    pub fn set_selected(&mut self, value: usize) {
        self.selected = value;
        self.state.select(Some(self.selected));
    }

    pub fn move_selected(&mut self, up: bool) {
        let len = self.grouped_content.len();
        self.set_selected((self.selected + (if up { len - 1 } else { 1 })) % len);
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
        f.render_stateful_widget(table, size, &mut self.state);
    }

    fn get_table_contents<'b>(&mut self) -> Vec<widgets::Row<'b>> {
        let mut table_contents: Vec<widgets::Row> = Vec::new();
        for idx in &self.grouped_content {
            let i = idx[0];
            let mut row_cells = Vec::new();
            for name in self.get_column_names() {
                let column = &self.displayed_df[name.clone()];
                let colorize = colorizer::select(column);
                let v = &self.displayed_df[(name.clone(), i)];
                row_cells.push(widgets::Cell::from(v.to_string()).style(style::Style::default().fg(colorize(v))));
            }
            table_contents.push(widgets::Row::new(row_cells));
        }

        table_contents
    }

    fn get_table_header<'b>(&self) -> widgets::Row<'b> {
        let cells: Vec<widgets::Cell> = self.get_column_names().map(|c| widgets::Cell::from(c.clone())).collect();
        widgets::Row::new(cells)
            .style(style::Style::default().fg(style::Color::Yellow))
            .bottom_margin(1)
    }

    fn get_column_widths(&self) -> Vec<layout::Constraint> {
        self.get_column_names()
            .map(|name| match self.displayed_df[name.clone()].attr_type {
                dataframe::InputAttributeType::String => layout::Constraint::Min(16),
                dataframe::InputAttributeType::Integer => layout::Constraint::Length(16),
            })
            .collect()
    }

    fn get_column_names(&self) -> Box<dyn Iterator<Item = &'a String> + 'a> {
        if self.has_filter() {
            Box::new(self.source_df.columns.keys())
        } else {
            Box::new(self.group_columns.iter())
        }
    }

    fn group_content(&mut self) {
        self.displayed_df = Box::new(self.source_df.filter(&self.filter));

        if self.has_filter() {
            // filter set, no grouping
            self.grouped_content = (0..self.displayed_df.len()).map(|c| vec![c]).collect();
        } else {
            // filter not set, group data
            self.grouped_content = self.displayed_df.group_by(self.group_columns);
        }
    }
}
