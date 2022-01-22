mod app;
mod card;
mod colorizer;
mod footer;
mod table;
mod timeline;

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
    let mut stdout = io::stdout();
    execute!(stdout, event::EnableMouseCapture, terminal::EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut term = Terminal::new(backend)?;
    terminal::enable_raw_mode()?;
    term.clear()?;

    // draw table
    let mut app_view_model = app::ViewModel::new(df, group_columns, show_in_grouped_mode, timeline_column);
    loop {
        term.draw(|f| app::View::new(&mut app_view_model).render(f))?;
        match event::read()? {
            event::Event::Key(key) => {
                if key.code == event::KeyCode::Char('c') && key.modifiers == event::KeyModifiers::CONTROL {
                    break;
                }
                match key.code {
                    event::KeyCode::Char('w') | event::KeyCode::Up => app_view_model.move_selected(true),
                    event::KeyCode::Char('s') | event::KeyCode::Down => app_view_model.move_selected(false),
                    event::KeyCode::Enter => app_view_model.focus(),
                    event::KeyCode::Char('q') | event::KeyCode::Esc => {
                        if !app_view_model.back() {
                            break;
                        }
                    }
                    _ => {}
                }
            }

            event::Event::Mouse(me) => match me.kind {
                event::MouseEventKind::ScrollDown => app_view_model.move_selected(false),
                event::MouseEventKind::ScrollUp => app_view_model.move_selected(true),
                _ => {}
            },
            event::Event::Resize(_, _) => {}
        }
    }

    // clean up tui
    term.clear()?;
    terminal::disable_raw_mode()?;
    execute!(term.backend_mut(), terminal::LeaveAlternateScreen, event::DisableMouseCapture)?;
    term.show_cursor()?;

    Ok(())
}
