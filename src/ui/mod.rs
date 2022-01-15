mod colorizer;
mod table;

use std::io;

use crossterm::{event, execute, terminal};
use tui::backend::CrosstermBackend;
use tui::Terminal;

use crate::io::dataframe;

pub fn show_dataframe(
    df: &dataframe::MaterializedDataFrame,
    group_columns: &[String],
    show_in_grouped_mode: &[String],
    timeline_column: &Option<String>,
) -> Result<(), io::Error> {
    // prepare tui
    let stdout = io::stdout();
    let backend = CrosstermBackend::new(stdout);
    let mut term = Terminal::new(backend)?;
    terminal::enable_raw_mode()?;
    term.clear()?;

    // draw table
    let mut table = table::Table::new(df, group_columns, show_in_grouped_mode, timeline_column);
    loop {
        term.draw(|f| table.render(f))?;

        if let event::Event::Key(key) = event::read()? {
            if key.code == event::KeyCode::Char('c') && key.modifiers == event::KeyModifiers::CONTROL {
                break;
            }
            match key.code {
                event::KeyCode::Char('w') | event::KeyCode::Up => table.move_selected(true),
                event::KeyCode::Char('s') | event::KeyCode::Down => table.move_selected(false),
                event::KeyCode::Enter => table.focus(),
                event::KeyCode::Char('q') | event::KeyCode::Esc => {
                    if !table.back() {
                        break;
                    }
                }
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
