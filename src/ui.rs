use std::io;
use tui::Terminal;
use tui::widgets;
use tui::layout;
use tui::style;
use tui::text;
use tui::backend::CrosstermBackend;

use crossterm::{
    event,
    execute,
    terminal
};

use crate::dataframe;

pub fn show_dataframe(_df: dataframe::DataFrame) -> Result<(), io::Error> {
    let stdout = io::stdout();
    let backend = CrosstermBackend::new(stdout);
    let mut term = Terminal::new(backend)?;

    terminal::enable_raw_mode()?;
    term.clear()?;
    loop {
        term.draw(|f| {
            let size = f.size();
            let table = widgets::Table::new(vec![
                    // widgets::Row can be created from simple strings.
                    widgets::Row::new(vec!["widgets::Row11", "widgets::Row12", "widgets::Row13"]),
                    // You can style the entire row.
                    widgets::Row::new(vec!["widgets::Row21", "widgets::Row22", "widgets::Row23"]).style(style::Style::default().fg(style::Color::Blue)),
                    // If you need more control over the styling you may need to create Cells directly
                    widgets::Row::new(vec![
                        widgets::Cell::from("widgets::Row31"),
                        widgets::Cell::from("widgets::Row32").style(style::Style::default().fg(style::Color::Yellow)),
                        widgets::Cell::from(text::Spans::from(vec![
                            text::Span::raw("widgets::Row"),
                            text::Span::styled("33", style::Style::default().fg(style::Color::Green))
                        ])),
                    ]),
                    widgets::Row::new(vec![
                        widgets::Cell::from("widgets::Row\n41"),
                        widgets::Cell::from("widgets::Row\n42"),
                        widgets::Cell::from("widgets::Row\n43"),
                    ]).height(2),
                ])
                .header(
                    widgets::Row::new(vec!["Col1", "Col2", "Col3"])
                        .style(style::Style::default().fg(style::Color::Yellow))
                        .bottom_margin(1)
                )
                .block(widgets::Block::default().borders(widgets::Borders::ALL))
                .highlight_symbol(">> ")
                .widths(&[
                    layout::Constraint::Percentage(50),
                    layout::Constraint::Length(30),
                    layout::Constraint::Min(10),
                ])
                .column_spacing(1);
            f.render_widget(table, size);
        })?;

        if let event::Event::Key(key) = event::read()? {
            match key.code {
                event::KeyCode::Char('q') => break,
                event::KeyCode::Esc => break,
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