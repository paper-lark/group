use std::io;
use tui::Terminal;
use tui::widgets;
use tui::backend::CrosstermBackend;

use crossterm::{
    cursor::position,
    event::{read, Event, KeyCode},
    terminal::{disable_raw_mode, enable_raw_mode},
};

use crate::dataframe;

pub fn show_dataframe(df: dataframe::DataFrame) -> Result<(), io::Error> {
    let stdout = io::stdout();
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    enable_raw_mode()?;
    terminal.clear()?;
    terminal.draw(|f| {
        let size = f.size();
        let block = widgets::Block::default()
            .title("Block")
            .borders(widgets::Borders::ALL);
        f.render_widget(block, size);
    })?;
    terminal.flush()?;

    loop {
        let event = read()?;
        //println!("Event::{:?}\r", event);

        //if event == Event::Key(KeyCode::Char('c').into()) {
        //    println!("Cursor position: {:?}\r", position());
        //}

        if event == Event::Key(KeyCode::Esc.into()) {
            break;
        }
    }
    disable_raw_mode()?;

    Ok(())
}