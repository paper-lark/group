use std::collections::HashMap;
use std::io;

use crossterm::{event, execute, terminal};
use tui::backend;
use tui::backend::CrosstermBackend;
use tui::layout;
use tui::style;
use tui::widgets;
use tui::Frame;
use tui::Terminal;

use crate::colorizer;
use crate::dataframe;

struct TableComponent<'a> {
    df: &'a dataframe::DataFrame,
    row_count: usize,
    filter: HashMap<String, dataframe::ColumnValue>,
    selected: usize,
    state: widgets::TableState,
}

impl<'a> TableComponent<'a> {
    fn new(df: &dataframe::DataFrame) -> TableComponent {
        assert!(!df.columns.is_empty(), "data should have at least one column");
        let row_counts: Vec<usize> = df.columns.values().map(|c| c.values.len()).collect();
        assert!(!row_counts.is_empty(), "data should have at least one row");
        assert!(row_counts.iter().min() == row_counts.iter().max(), "columns have different number of rows");

        let mut state = widgets::TableState::default();
        let selected = 0;
        state.select(Some(selected));
        TableComponent {
            df,
            row_count: row_counts[0],
            filter: HashMap::new(),
            selected,
            state,
        }
    }

    fn set_filter(&mut self) {
        let index = self.selected;
        if self.filter.is_empty() {
            for name in &self.df.group_columns {
                if let Some(column) = self.df.columns.get(name) {
                    self.filter.insert(name.clone(), column.values[index].clone());
                }
            }
        }
    }

    fn reset_filter(&mut self) {
        self.filter.clear();
    }

    fn move_selected(&mut self, up: bool) {
        self.selected = (self.selected + (if up { self.row_count - 1 } else { self.row_count + 1 })) % self.row_count;
        self.state.select(Some(self.selected));
    }

    fn render<B: backend::Backend>(&mut self, f: &mut Frame<B>) {
        let table = widgets::Table::new(self.get_table_contents())
            .header(self.get_table_header())
            .block(widgets::Block::default().borders(widgets::Borders::ALL))
            .highlight_style(style::Style::default().bg(style::Color::Blue))
            .widths(&[
                layout::Constraint::Percentage(50),
                layout::Constraint::Length(30),
                layout::Constraint::Min(10),
            ])
            .column_spacing(1);

        let size = f.size();
        f.render_stateful_widget(table, size, &mut self.state);
    }

    fn get_table_contents<'b>(&self) -> Vec<widgets::Row<'b>> {
        let mut table_contents: Vec<widgets::Row> = Vec::new();
        let filter_index = self.get_content_filter();
        for i in filter_index {
            let mut row_values = Vec::new();
            for name in self.get_column_names() {
                if let Some(c) = self.df.columns.get(name) {
                    let colorize = colorizer::select(c);
                    let v = &c.values[i];
                    row_values.push(widgets::Cell::from(v.to_string()).style(style::Style::default().fg(colorize(v))));
                }
            }
            table_contents.push(widgets::Row::new(row_values));
        }
        table_contents
    }

    fn get_content_filter(&self) -> Vec<usize> {
        (0..self.row_count)
            .filter(|i| {
                for c in self.df.columns.values() {
                    if let Some(expected_value) = self.filter.get(&c.name) {
                        if expected_value != &c.values[*i] {
                            return false;
                        }
                    }
                }
                true
            })
            .collect()
    }

    fn get_table_header<'b>(&self) -> widgets::Row<'b> {
        let cells: Vec<widgets::Cell> = self.get_column_names().map(|c| widgets::Cell::from(c.clone())).collect();
        widgets::Row::new(cells)
            .style(style::Style::default().fg(style::Color::Yellow))
            .bottom_margin(1)
    }

    fn get_column_names(&self) -> Box<dyn Iterator<Item = &'a String> + 'a> {
        if self.filter.is_empty() {
            Box::new(self.df.group_columns.iter())
        } else {
            Box::new(self.df.columns.keys())
        }
    }
}

pub fn show_dataframe(df: &dataframe::DataFrame) -> Result<(), io::Error> {
    // prepare tui
    let stdout = io::stdout();
    let backend = CrosstermBackend::new(stdout);
    let mut term = Terminal::new(backend)?;
    terminal::enable_raw_mode()?;
    term.clear()?;

    // draw table
    let mut table = TableComponent::new(df);
    loop {
        term.draw(|f| table.render(f))?;

        if let event::Event::Key(key) = event::read()? {
            let should_exit = key.code == event::KeyCode::Char('q')
                || key.code == event::KeyCode::Char('c') && key.modifiers == event::KeyModifiers::CONTROL;
            if should_exit {
                break;
            }
            match key.code {
                event::KeyCode::Char('w') | event::KeyCode::Up => table.move_selected(true),
                event::KeyCode::Char('s') | event::KeyCode::Down => table.move_selected(false),
                event::KeyCode::Enter => table.set_filter(),
                event::KeyCode::Esc => table.reset_filter(),
                _ => {}
            }
        }
    }

    // clean up tui
    term.clear()?;
    terminal::disable_raw_mode()?;
    execute!(term.backend_mut(), terminal::LeaveAlternateScreen, event::DisableMouseCapture)?;
    term.show_cursor()?;

    Ok(())
}
