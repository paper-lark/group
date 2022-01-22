use tui::backend;
use tui::layout;
use tui::style;
use tui::widgets;
use tui::Frame;

use crate::io::dataframe;
use crate::io::dataframe::DataFrame;
use crate::ui::colorizer;

pub struct ViewModel<'a> {
    pub df: Box<dyn DataFrame + 'a>,
    pub selected: usize,
    timeline_column: Option<Vec<String>>,
    table_state: widgets::TableState,
}

impl<'a> ViewModel<'a> {
    pub fn new(df: Box<dyn DataFrame + 'a>, timeline_column: Option<Vec<String>>) -> ViewModel<'a> {
        let mut model = ViewModel {
            df,
            timeline_column,
            table_state: widgets::TableState::default(),
            selected: 0,
        };
        model.set_selected(0);
        model
    }

    pub fn set_selected(&mut self, value: usize) {
        self.selected = value;
        self.table_state.select(Some(value));
    }

    pub fn move_selected(&mut self, up: bool) {
        let new_index = {
            let len = self.df.len();
            (self.selected + (if up { len - 1 } else { 1 })) % len
        };
        self.set_selected(new_index);
    }

    pub fn selected_row(&self) -> Vec<&dataframe::ColumnValue> {
        self.df.column_names().iter().map(|c| self.df.get((c, self.selected))).collect()
    }
}

pub struct View<'a: 'b, 'b> {
    view_model: &'b mut ViewModel<'a>,
}

pub const MAX_STRING_WIDTH: u16 = 32;
pub const TIMELINE_WIDTH: u16 = 32;

impl<'a: 'c, 'c> View<'a, 'c> {
    pub fn new(view_model: &'c mut ViewModel<'a>) -> View<'a, 'c> {
        View { view_model }
    }

    pub fn render<B: backend::Backend>(self, f: &mut Frame<B>, size: layout::Rect) {
        // create table widget
        let column_widths = self.get_column_widths();
        let table_contents = self.get_table_contents();
        let table_widget = widgets::Table::new(table_contents)
            .header(self.get_table_header())
            .highlight_symbol("> ")
            .highlight_style(style::Style::default().add_modifier(style::Modifier::HIDDEN | style::Modifier::BOLD))
            .widths(&column_widths)
            .column_spacing(2);

        f.render_stateful_widget(table_widget, size, &mut self.view_model.table_state);
    }

    fn get_table_contents<'b>(&self) -> Vec<widgets::Row<'b>> {
        let mut table_contents: Vec<widgets::Row> = Vec::new();
        let df = &self.view_model.df;

        for i in 0..df.len() {
            let mut row_cells = Vec::new();
            for name in self.get_column_names() {
                let column = df.column(name);
                let colorize = colorizer::select(column);
                let v = &df.get((name, i));
                row_cells.push(widgets::Cell::from(v.to_string()).style(style::Style::default().fg(colorize(v))));
            }
            if let Some(t) = &self.view_model.timeline_column {
                row_cells.push(widgets::Cell::from(t[i].clone()));
            }
            table_contents.push(widgets::Row::new(row_cells));
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
            .style(style::Style::default().fg(style::Color::Yellow).add_modifier(style::Modifier::BOLD))
            .bottom_margin(1)
    }

    fn get_column_widths(&self) -> Vec<layout::Constraint> {
        let mut contraints: Vec<_> = self
            .get_column_names()
            .into_iter()
            .map(|name| {
                let column = &self.view_model.df.column(name);
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
        if self.view_model.timeline_column.is_some() {
            contraints.push(layout::Constraint::Length(TIMELINE_WIDTH));
        }
        contraints
    }

    fn get_column_names(&self) -> Vec<&String> {
        self.view_model.df.column_names()
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
