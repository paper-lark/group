use tui::backend;
use tui::layout;
use tui::style;
use tui::text;
use tui::widgets;
use tui::Frame;

pub struct Footer<'a> {
    widget: widgets::Paragraph<'a>,
    height: usize,
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

    pub fn get_height(&self) -> usize {
        self.height
    }

    pub fn render<B: backend::Backend>(self, f: &mut Frame<B>, size: layout::Rect) {
        f.render_widget(self.widget, size);
    }
}
