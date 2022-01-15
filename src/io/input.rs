use crate::io::dataframe::{Column, ColumnValue, MaterializedDataFrame};
use crate::io::serialize::to_pretty_json;
use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use std::error::Error;
use string_error::into_err;

pub fn read_dataframe(
    reader: impl std::io::BufRead,
    attributes: &[String],
    as_single_object: bool,
) -> Result<MaterializedDataFrame, Box<dyn Error>> {
    let input: Vec<serde_json::Value> = if as_single_object {
        serde_json::from_reader(reader)?
    } else {
        let mut result: Vec<serde_json::Value> = Vec::new();
        let deserializer = serde_json::Deserializer::from_reader(reader);
        for v in deserializer.into_iter::<serde_json::Value>() {
            result.push(v?);
        }
        result
    };

    let mut columns: IndexMap<String, Column> = IndexMap::new();
    for attr in attributes {
        let result = extract_column(attr, &input);
        match result {
            Ok(column) => columns.insert(column.name.clone(), column),
            Err(err) => return Err(err),
        };
    }
    let mut raw: Vec<String> = Vec::new();
    for v in &input {
        raw.push(to_pretty_json(v)?);
    }

    Ok(MaterializedDataFrame::new(columns, raw))
}

fn extract_column(name: &str, input: &[serde_json::Value]) -> Result<Column, Box<dyn Error>> {
    let mut values: Vec<ColumnValue> = Vec::new();
    let attr_path: Vec<&str> = name.split('.').collect();
    if attr_path.is_empty() {
        return Err(into_err(format!("invalid attribute name={}", name)));
    }

    for input_element in input {
        let mut element = input_element;
        let mut not_found = false;
        for path_element in &attr_path {
            if let serde_json::Value::Object(obj) = element {
                if let Some(value) = obj.get(*path_element) {
                    element = value;
                } else {
                    not_found = true;
                    break;
                }
            } else {
                not_found = true;
                break;
            }
        }

        if not_found {
            values.push(ColumnValue::None);
        } else {
            match extract_column_value(element) {
                Ok(v) => values.push(v),
                Err(e) => return Err(into_err(format!("failed to parse value={}: {}", element, e))),
            }
        }
    }
    Ok(Column {
        name: String::from(name),
        values,
    })
}

fn extract_column_value(value: &serde_json::Value) -> Result<ColumnValue, Box<dyn Error>> {
    match value {
        serde_json::Value::Bool(b) => Ok(ColumnValue::Boolean(*b)),
        serde_json::Value::Number(v) => {
            if let Some(n) = v.as_i64() {
                Ok(ColumnValue::Integer(n))
            } else {
                Err(into_err(format!("number={} is not i64", v)))
            }
        }
        serde_json::Value::String(s) => match DateTime::parse_from_rfc3339(s) {
            Ok(d) => Ok(ColumnValue::DateTime(d.with_timezone(&Utc {}))),
            Err(_) => Ok(ColumnValue::String(s.clone())),
        },
        serde_json::Value::Null => Ok(ColumnValue::None),
        _ => Err(into_err(format!("unsupported value={}", value))),
    }
}

#[cfg(test)]
mod test {
    use crate::configuration::InputSpec;
    use crate::io::dataframe::{Column, ColumnValue, MaterializedDataFrame};
    use crate::io::input::read_dataframe;
    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
    use indexmap::IndexMap;

    macro_rules! columns {
        ($( $x:expr ),*) => {{
            let mut temp_map = IndexMap::new();
            $(
                let element = $x;
                temp_map.insert(element.name.clone(), element);
            )*
            temp_map
        }};
    }

    macro_rules! simple_dataframe {
        ($column_name:expr => $( $value:expr, $serialized:expr );*) => {
            MaterializedDataFrame::new(
                columns![Column {
                    name: String::from($column_name),
                    values: vec![$($value),*]
                }],
                vec![$(String::from($serialized)),*],
            )
        };
    }

    macro_rules! simple_spec {
        ($column_name:expr) => {
            InputSpec {
                attrs: vec![String::from($column_name)],
                group_by: vec![String::from($column_name)],
                show_in_grouped: vec![],
                timeline_column: None,
            }
        };
    }

    macro_rules! datetime_value {
        ($y:expr, $m:expr, $d:expr, $h:expr, $min:expr, $s:expr, $ms:expr) => {
            ColumnValue::DateTime(DateTime::<Utc>::from_utc(
                NaiveDateTime::new(NaiveDate::from_ymd($y, $m, $d), NaiveTime::from_hms_milli($h, $min, $s, $ms)),
                Utc,
            ))
        };
    }

    macro_rules! integer_value {
        ($i:expr) => {
            ColumnValue::Integer($i)
        };
    }

    macro_rules! string_value {
        ($s:expr) => {
            ColumnValue::String(String::from($s))
        };
    }

    #[test]
    fn read_dataframe_parses_integer_column() {
        let input = "{\"int\": 10}\n{\"int\": 20}\n";
        let spec = simple_spec!("int");
        let expected = simple_dataframe!("int" => integer_value!(10), "{\n  \"int\": 10\n}"; integer_value!(20), "{\n  \"int\": 20\n}");
        let actual = read_dataframe(input.as_bytes(), &spec.attrs, false);
        assert_eq!(Some(expected), actual.ok());
    }

    #[test]
    fn read_dataframe_parses_datetime_column() {
        let input = "{\"d\": \"2022-01-05T00:50:03.432Z\"}\n{\"d\": \"2022-01-05T00:50:05Z\"}";
        let spec = simple_spec!("d");
        let expected = simple_dataframe!("d" => datetime_value!(2022, 1, 5, 0, 50, 3, 432), "{\n  \"d\": \"2022-01-05T00:50:03.432Z\"\n}"; datetime_value!(2022, 1, 5, 0, 50, 5, 0), "{\n  \"d\": \"2022-01-05T00:50:05Z\"\n}");
        let actual = read_dataframe(input.as_bytes(), &spec.attrs, false);
        assert_eq!(Some(expected), actual.ok());
    }

    #[test]
    fn read_dataframe_converts_datetime_to_utc() {
        let input = "{\"d\": \"2022-01-05T03:50:03.432+03:00\"}";
        let spec = simple_spec!("d");
        let expected =
            simple_dataframe!("d" => datetime_value!(2022, 1, 5, 0, 50, 3, 432), "{\n  \"d\": \"2022-01-05T03:50:03.432+03:00\"\n}");
        let actual = read_dataframe(input.as_bytes(), &spec.attrs, false);
        assert_eq!(Some(expected), actual.ok());
    }

    #[test]
    fn read_dataframe_parses_string_column() {
        let input = "{\"s\": \"hello\"}\n{\"s\": \"world\"}\n";
        let spec = simple_spec!("s");
        let expected = simple_dataframe!("s" => string_value!("hello"), "{\n  \"s\": \"hello\"\n}"; string_value!("world"), "{\n  \"s\": \"world\"\n}");
        let actual = read_dataframe(input.as_bytes(), &spec.attrs, false);
        assert_eq!(Some(expected), actual.ok());
    }

    #[test]
    fn read_dataframe_parses_column_with_missing_values() {
        let input = "{\"s\": \"hello\"}\n{}\n";
        let spec = simple_spec!("s");
        let expected = simple_dataframe!("s" => string_value!("hello"), "{\n  \"s\": \"hello\"\n}"; ColumnValue::None, "{}");
        let actual = read_dataframe(input.as_bytes(), &spec.attrs, false);
        assert_eq!(Some(expected), actual.ok());
    }

    #[test]
    fn read_dataframe_parses_integer_column_when_reading_single_object() {
        let input = "[{\"int\": 10}, {\"int\": 20}]";
        let spec = simple_spec!("int");
        let expected = simple_dataframe!("int" => integer_value!(10), "{\n  \"int\": 10\n}"; integer_value!(20), "{\n  \"int\": 20\n}");
        let actual = read_dataframe(input.as_bytes(), &spec.attrs, true);
        assert_eq!(Some(expected), actual.ok());
    }

    #[test]
    fn read_dataframe_parses_string_column_when_reading_single_object() {
        let input = "[{\"s\": \"hello\"}, {\"s\": \"world\"}]";
        let spec = simple_spec!("s");
        let expected = simple_dataframe!("s" => string_value!("hello"), "{\n  \"s\": \"hello\"\n}"; string_value!("world"), "{\n  \"s\": \"world\"\n}");
        let actual = read_dataframe(input.as_bytes(), &spec.attrs, true);
        assert_eq!(Some(expected), actual.ok());
    }

    #[test]
    fn read_dataframe_parses_datetime_column_when_readin_single_object() {
        let input = "[{\"d\": \"2022-01-05T00:50:03.432Z\"}, {\"d\": \"2022-01-05T00:50:05Z\"}]";
        let spec = simple_spec!("d");
        let expected = simple_dataframe!("d" => datetime_value!(2022, 1, 5, 0, 50, 3, 432), "{\n  \"d\": \"2022-01-05T00:50:03.432Z\"\n}"; datetime_value!(2022, 1, 5, 0, 50, 5, 0), "{\n  \"d\": \"2022-01-05T00:50:05Z\"\n}");
        let actual = read_dataframe(input.as_bytes(), &spec.attrs, true);
        assert_eq!(Some(expected), actual.ok());
    }

    #[test]
    fn read_dataframe_parses_column_with_missing_values_when_reading_single_object() {
        let input = "[{\"s\": \"hello\"}, {}]";
        let spec = simple_spec!("s");
        let expected = simple_dataframe!("s" => string_value!("hello"), "{\n  \"s\": \"hello\"\n}"; ColumnValue::None, "{}");
        let actual = read_dataframe(input.as_bytes(), &spec.attrs, true);
        assert_eq!(Some(expected), actual.ok());
    }
}
