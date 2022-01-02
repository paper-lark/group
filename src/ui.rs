use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io;

use crossterm::{event, execute, terminal};
use tui::backend;
use tui::backend::CrosstermBackend;
use tui::layout;
use tui::style;
use tui::widgets;
use tui::Frame;
use tui::Terminal;

use crate::dataframe;
use crate::max;

struct TableComponent<'a> {
    df: &'a dataframe::DataFrame,
    row_count: usize,
    state: widgets::TableState,
}

impl<'a> TableComponent<'a> {
    fn new(df: &dataframe::DataFrame) -> TableComponent {
        assert!(!df.columns.is_empty(), "data should have at least one column");
        let row_counts: Vec<usize> = df.columns.iter().map(|c| c.values.len()).collect();
        assert!(!row_counts.is_empty(), "data should have at least one row");
        assert!(row_counts.iter().min() == row_counts.iter().max(), "columns have different number of rows");

        let mut state = widgets::TableState::default();
        state.select(Some(0));
        TableComponent {
            df,
            row_count: row_counts[0],
            state,
        }
    }

    fn move_selected(&mut self, up: bool) {
        let i = match self.state.selected() {
            Some(i) => (i + (if up { self.row_count - 1 } else { self.row_count + 1 })) % self.row_count,
            None => 0,
        };
        self.state.select(Some(i));
    }

    fn render<B: backend::Backend>(&mut self, f: &mut Frame<B>) {
        let table_header: Vec<widgets::Cell> = self
            .df
            .columns
            .iter()
            .map(|c| widgets::Cell::from(c.name.clone()))
            .collect();

        let mut table_cells: Vec<Vec<widgets::Cell>> = Vec::new();
        for c in &self.df.columns {
            for (i, v) in c.values.iter().enumerate() {
                let s = v.to_string();
                let color = get_color(&s);

                let cell = widgets::Cell::from(s).style(style::Style::default().fg(color));
                match table_cells.get(i) {
                    Some(_) => table_cells[i].push(cell),
                    None => table_cells.push(vec![cell]),
                }
            }
        }
        let table_contents: Vec<widgets::Row> = table_cells.into_iter().map(widgets::Row::new).collect();

        let table = widgets::Table::new(table_contents)
            .header(
                widgets::Row::new(table_header)
                    .style(style::Style::default().fg(style::Color::Yellow))
                    .bottom_margin(1),
            )
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
            match key.code {
                event::KeyCode::Esc | event::KeyCode::Char('q') => break,
                event::KeyCode::Char('w') | event::KeyCode::Up => table.move_selected(true),
                event::KeyCode::Char('s') | event::KeyCode::Down => table.move_selected(false),
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

#[allow(clippy::cast_possible_truncation)]
fn get_color<T: Hash>(value: &T) -> style::Color {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    let hash = hasher.finish();

    let r = max!(hash as u8, (hash >> 8) as u8, (hash >> 16) as u8);
    let g = max!((hash >> 24) as u8, (hash >> 32) as u8, (hash >> 40) as u8);
    let b = max!((hash >> 48) as u8, (hash >> 56) as u8, hash as u8);
    style::Color::Rgb(r, g, b)
}
