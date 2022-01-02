use std::io;
use tui::Terminal;
use tui::Frame;
use tui::backend;
use tui::widgets;
use tui::layout;
use tui::style;
use tui::backend::CrosstermBackend;

use crossterm::{
    event,
    execute,
    terminal
};

use crate::dataframe;

struct TableComponent<'a> {
    df: &'a dataframe::DataFrame,
    row_count: usize, 
    state: widgets::TableState,
}

impl<'a> TableComponent<'a> {
    fn new(df: &dataframe::DataFrame) -> TableComponent {
        let mut state = widgets::TableState::default();
        state.select(Some(0));
        TableComponent{
            df: df,
            row_count: df.columns[0].values.len(),
            state: state,
        }
    }
    
    fn move_selected(&mut self, up: bool) {
        let i = match self.state.selected() {
            Some(i) => (i + (if up { self.row_count - 1 } else { self.row_count + 1 })) % self.row_count,
            None => 0
        };
        self.state.select(Some(i))
    }

    fn render<B: backend::Backend>(&mut self, f: &mut Frame<B>) {
        let table_header: Vec<widgets::Cell> = self.df.columns.iter().map(|c| {
            widgets::Cell::from(c.name.clone())
        }).collect();

        let mut table_cells: Vec<Vec<widgets::Cell>> = Vec::new();
        for c in &self.df.columns {
            for (i, v) in c.values.iter().enumerate() {
                let cell = widgets::Cell::from(v.to_string());
                match table_cells.get(i) {
                    Some(_) => table_cells[i].push(cell),
                    None => table_cells.push(vec!(cell))
                }
            }
        }
        let table_contents: Vec<widgets::Row> = table_cells.into_iter().map(|v| widgets::Row::new(v)).collect();

        let table = widgets::Table::new(table_contents)
            .header(
                widgets::Row::new(table_header)
                    .style(style::Style::default().fg(style::Color::Yellow))
                    .bottom_margin(1)
            )
            .block(widgets::Block::default().borders(widgets::Borders::ALL))
            .highlight_style(style::Style::default().bg(style::Color::Blue).add_modifier(style::Modifier::BOLD))
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


pub fn show_dataframe(df: dataframe::DataFrame) -> Result<(), io::Error> {
    let stdout = io::stdout();
    let backend = CrosstermBackend::new(stdout);
    let mut term = Terminal::new(backend)?;

    terminal::enable_raw_mode()?;
    term.clear()?;

    let mut table = TableComponent::new(&df);
    loop {
        term.draw(|f| table.render(f) )?;

        if let event::Event::Key(key) = event::read()? {
            match key.code {
                event::KeyCode::Char('q') => break,
                event::KeyCode::Esc => break,

                event::KeyCode::Up => table.move_selected(true),
                event::KeyCode::Char('w') => table.move_selected(true),

                event::KeyCode::Down => table.move_selected(false),
                event::KeyCode::Char('s') => table.move_selected(false),
                _ => {}
            }
        }
    }

    // FIXME: call before exit
    term.clear()?;
    terminal::disable_raw_mode()?;
    execute!(
        term.backend_mut(),
        terminal::LeaveAlternateScreen,
        event::DisableMouseCapture
    )?;
    term.show_cursor()?;


    Ok(())
}