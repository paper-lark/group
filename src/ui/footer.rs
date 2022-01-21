use tui::style;
use tui::text;
use tui::widgets;

pub struct Footer<'a> {
    pub widget: widgets::Paragraph<'a>,
    pub height: usize,
}

impl<'a> Footer<'a> {
    pub fn new<'b>(mode: &'b str, line_number: usize, line_count: usize) -> Footer<'a> {
        let contents = text::Spans::from(vec![
            text::Span::from("  "),
            text::Span::styled(format!("[{}]", mode), style::Style::default().add_modifier(style::Modifier::BOLD)),
            text::Span::from("  "),
            text::Span::from(format!("{}/{}", line_number, line_count)),
        ]);
        let para = widgets::Paragraph::new(contents).style(style::Style::default().add_modifier(style::Modifier::REVERSED));

        Footer { widget: para, height: 1 }
    }
}
