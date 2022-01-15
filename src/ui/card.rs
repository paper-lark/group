use tui::text;
use tui::widgets;

pub struct Card<'a> {
    pub widget: widgets::Paragraph<'a>,
    pub text_height: usize,
}

impl<'a> Card<'a> {
    pub fn new<'b>(txt: &'b str) -> Card<'a> {
        let text_element = text::Text::from(String::from(txt));
        let text_height = text_element.height();
        let para = widgets::Paragraph::new(text_element).block(widgets::Block::default().borders(widgets::Borders::TOP));

        Card { widget: para, text_height }
    }
}
