use tui::backend;
use tui::layout;
use tui::style;
use tui::text;
use tui::widgets;
use tui::Frame;

pub struct View<'a> {
    widget: widgets::Paragraph<'a>,
    height: usize,
}

impl<'a> View<'a> {
    pub fn new<'b>(txt: &'b str) -> View<'a> {
        let obj: serde_json::Value = serde_json::from_str(txt).expect("failed to parse");
        let colored = to_colored_yaml(&obj);
        let text_element = text::Text::from(colored);
        let height = text_element.height() + 1;
        let para = widgets::Paragraph::new(text_element).block(widgets::Block::default().borders(widgets::Borders::TOP));

        View { widget: para, height }
    }

    pub fn get_height(&self) -> usize {
        self.height
    }

    pub fn render<B: backend::Backend>(self, f: &mut Frame<B>, size: layout::Rect) {
        f.render_widget(self.widget, size);
    }
}

fn to_colored_yaml<'a>(obj: &serde_json::Value) -> Vec<text::Spans<'a>> {
    const PADDING_INCR: usize = 2;
    const KEY_COLOR: style::Color = style::Color::Red;
    const STRING_COLOR: style::Color = style::Color::LightGreen;
    const LITERAL_COLOR: style::Color = style::Color::LightCyan;
    const SYNTAX_COLOR: style::Color = style::Color::Yellow;

    fn serialize_obj<'a>(obj: &serde_json::Value, padding: usize) -> (Vec<text::Spans<'a>>, bool) {
        macro_rules! new_line {
            ($padding:ident, $( $span:expr),*) => {
                text::Spans::from(vec![
                    text::Span::from(" ".repeat(padding)),
                    $($span,)*
                ])
            };
        }
        match obj {
            serde_json::Value::Bool(v) => (
                vec![new_line!(
                    padding_str,
                    colored_text_ref(if *v { "true" } else { "false" }, LITERAL_COLOR,)
                )],
                false,
            ),
            serde_json::Value::Number(v) => (vec![new_line!(padding, colored_text(format!("{}", v), LITERAL_COLOR))], false),
            serde_json::Value::String(v) => {
                if v.is_empty() {
                    (vec![new_line!(padding, colored_text_ref("\"\"", STRING_COLOR))], false)
                } else {
                    let mut lines: Vec<_> = v
                        .split('\n')
                        .map(|l| new_line!(padding, colored_text(String::from(l), STRING_COLOR)))
                        .collect();
                    if lines.len() > 1 {
                        lines.insert(0, new_line!(padding, colored_text_ref("|", SYNTAX_COLOR)));
                    }
                    (lines, false)
                }
            }
            serde_json::Value::Null => (vec![new_line!(padding, colored_text_ref("null", LITERAL_COLOR))], false),
            serde_json::Value::Array(_) => {
                (vec![new_line!(padding, colored_text_ref("[]", SYNTAX_COLOR))], false)
                // TODO: support arrays
            }
            serde_json::Value::Object(v) => {
                if v.is_empty() {
                    (vec![new_line!(padding, colored_text_ref("{}", SYNTAX_COLOR))], false)
                } else {
                    let result: Vec<_> = v
                        .iter()
                        .flat_map(|(k, v)| {
                            let mut result = vec![new_line!(
                                padding,
                                colored_text(k.clone(), KEY_COLOR),
                                colored_text_ref(": ", SYNTAX_COLOR)
                            )];
                            let (mut value, is_multiline) = serialize_obj(v, PADDING_INCR + padding);
                            if is_multiline {
                                result.append(&mut value);
                            } else {
                                let mut first = value.remove(0).0;
                                first.remove(0);
                                result[0].0.append(&mut first);
                                result.append(&mut value);
                            }
                            result
                        })
                        .collect();
                    (result, true)
                }
            }
        }
    }

    serialize_obj(obj, 0).0
}

fn colored_text_ref<'a>(txt: &'static str, color: style::Color) -> text::Span<'a> {
    text::Span::styled(txt, style::Style::default().fg(color))
}

fn colored_text<'a>(txt: String, color: style::Color) -> text::Span<'a> {
    text::Span::styled(txt, style::Style::default().fg(color))
}
