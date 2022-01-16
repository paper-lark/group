use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::io::dataframe::{Column, ColumnValue};
use crate::max;
use tui::style;

type Colorizer = fn(value: &ColumnValue) -> style::Color;

pub fn select(col: &Column) -> Colorizer {
    const MAX_COLORS: usize = 16;

    let mut unique_values = col.unique();
    unique_values.remove(&ColumnValue::None);
    if (2..=MAX_COLORS).contains(&unique_values.len()) {
        colorize_rgb
    } else {
        colorize_static
    }
}

#[allow(clippy::cast_possible_truncation)]
fn colorize_rgb(value: &ColumnValue) -> style::Color {
    const MIN_INTENSITY: u8 = 128;
    const MAX_INTENSITY: u8 = 250;
    macro_rules! intensify {
        ($x: expr) => {
            MIN_INTENSITY + ($x) % (MAX_INTENSITY - MIN_INTENSITY)
        };
    }

    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    let hash = hasher.finish();

    let r = intensify!(max!(hash as u8, (hash >> 8) as u8, (hash >> 16) as u8));
    let g = intensify!(max!((hash >> 24) as u8, (hash >> 32) as u8, (hash >> 40) as u8));
    let b = intensify!(max!((hash >> 48) as u8, (hash >> 56) as u8, hash as u8));

    style::Color::Rgb(r, g, b)
}

fn colorize_static(_: &ColumnValue) -> style::Color {
    style::Color::White
}
