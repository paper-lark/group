use crate::io::dataframe::{Column, ColumnValue, DataFrame, DataFrameGroupView, MaterializedDataFrame};
use chrono::{DateTime, Utc};

pub fn create_timeline_column<'a>(
    source_df: &MaterializedDataFrame,
    df: &DataFrameGroupView<'a>,
    column_name: &str,
    resolution: u16,
) -> Vec<String> {
    // create time grid
    let time_column = match source_df.columns.get(column_name) {
        None => return vec![String::from(""); df.len()],
        Some(column) => column,
    };
    let grid = create_timeline_grid(time_column, resolution);

    // create timelines
    (0..df.len())
        .map(|i| {
            let timestamps: Vec<_> = df
                .group_indices(i)
                .iter()
                .map(|j| time_column[*j].clone())
                .filter_map(|c| if let ColumnValue::DateTime(ts) = c { Some(ts) } else { None })
                .collect();
            let mut slots: Vec<usize> = vec![0; resolution.into()];
            for ts in timestamps {
                let slot_index = grid
                    .iter()
                    .enumerate()
                    .filter(|(_, t)| **t <= ts)
                    .map(|(j, _)| j)
                    .last()
                    .unwrap_or(0);
                slots[slot_index] += 1;
            }
            (0..resolution as usize).map(|j| if slots[j] > 0 { '█' } else { ' ' }).collect()
        })
        .collect()
}

fn create_timeline_grid(time_column: &Column, resolution: u16) -> Vec<DateTime<Utc>> {
    let ts: Vec<_> = time_column
        .values
        .iter()
        .filter_map(|c| if let ColumnValue::DateTime(ts) = c { Some(ts) } else { None })
        .collect();

    if let Some(min_ts) = ts.iter().min() {
        if let Some(max_ts) = ts.iter().max() {
            let delta = (**max_ts - **min_ts) / ((resolution - 1).into());
            let mut intervals: Vec<DateTime<Utc>> = Vec::new();
            let mut ts = **min_ts;
            for _ in 0..resolution {
                intervals.push(ts);
                ts = ts + delta;
            }

            return intervals;
        }
    }
    Vec::new()
}
