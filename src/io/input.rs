use crate::configuration::InputSpec;
use crate::io::dataframe::{Column, ColumnValue, InputAttributeType, MaterializedDataFrame};
use crate::io::serialize::to_pretty_json;
use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use std::error::Error;
use string_error::into_err;

type JSONInput = serde_json::Map<String, serde_json::Value>;
type ColumnValueExtractor = fn(value: &serde_json::Value) -> Result<ColumnValue, &'static str>;

pub fn read_dataframe(
    reader: impl std::io::BufRead,
    spec: &InputSpec,
    as_single_object: bool,
) -> Result<MaterializedDataFrame, Box<dyn Error>> {
    let input: Vec<JSONInput> = if as_single_object {
        serde_json::from_reader(reader)?
    } else {
        let mut result: Vec<JSONInput> = Vec::new();
        let deserializer = serde_json::Deserializer::from_reader(reader);
        for v in deserializer.into_iter::<JSONInput>() {
            result.push(v?);
        }
        result
    };

    let mut columns: IndexMap<String, Column> = IndexMap::new();
    for attr in &spec.attrs {
        let result = extract_column(&attr.name, attr.attr_type, &input, get_extractor_by_attribute_type(attr.attr_type));
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

fn extract_column(
    name: &str,
    attr_type: InputAttributeType,
    input: &[JSONInput],
    extractor: ColumnValueExtractor,
) -> Result<Column, Box<dyn Error>> {
    let mut values: Vec<ColumnValue> = Vec::new();
    let attr_path: Vec<&str> = name.split('.').collect();
    if attr_path.is_empty() {
        return Err(into_err(format!("invalid attribute name={}", name)));
    }

    for input_element in input {
        let mut element_ref = input_element;
        for (i, path_element) in attr_path.iter().enumerate() {
            if let Some(input_value) = element_ref.get(*path_element) {
                if i == attr_path.len() - 1 {
                    match extractor(input_value) {
                        Ok(v) => values.push(v),
                        Err(e) => return Err(into_err(format!("failed to parse value={}: {}", input_value, e))),
                    }
                } else if let Some(serde_json::Value::Object(o)) = element_ref.get(*path_element) {
                    element_ref = o;
                } else {
                    values.push(ColumnValue::None);
                    break;
                }
            } else {
                values.push(ColumnValue::None);
                break;
            }
        }
    }
    Ok(Column {
        name: String::from(name),
        attr_type,
        values,
    })
}

fn get_extractor_by_attribute_type(attr_type: InputAttributeType) -> ColumnValueExtractor {
    match attr_type {
        InputAttributeType::Integer => extract_integer_from_json,
        InputAttributeType::String => extract_string_from_json,
        InputAttributeType::DateTime => extract_datetime_from_json,
    }
}

fn extract_integer_from_json(value: &serde_json::Value) -> Result<ColumnValue, &'static str> {
    if let serde_json::Value::Number(n) = value {
        if let Some(v) = n.as_i64() {
            Ok(ColumnValue::Integer(v))
        } else {
            Err("value is not integer")
        }
    } else {
        Err("value is not a number")
    }
}

fn extract_string_from_json(value: &serde_json::Value) -> Result<ColumnValue, &'static str> {
    if let serde_json::Value::String(v) = value {
        Ok(ColumnValue::String(v.clone()))
    } else {
        Err("value is not a number")
    }
}

fn extract_datetime_from_json(value: &serde_json::Value) -> Result<ColumnValue, &'static str> {
    if let serde_json::Value::String(v) = value {
        match DateTime::parse_from_rfc3339(v) {
            Ok(d) => Ok(ColumnValue::DateTime(d.with_timezone(&Utc {}))),
            Err(_) => Err("value is not an RFC 3339 date"),
        }
    } else {
        Err("value is not a number")
    }
}

#[cfg(test)]
mod test {
    use crate::configuration::{InputAttributeSpec, InputSpec};
    use crate::io::dataframe::{Column, ColumnValue, InputAttributeType, MaterializedDataFrame};
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
        ($column_name:expr, $attr_type:expr => $( $value:expr, $serialized:expr );*) => {
            MaterializedDataFrame::new(
                columns![Column {
                    name: String::from($column_name),
                    attr_type: $attr_type,
                    values: vec![$($value),*]
                }],
                vec![$(String::from($serialized)),*],
            )
        };
    }

    macro_rules! simple_spec {
        ($column_name:expr, $attr_type:expr) => {
            InputSpec {
                attrs: vec![InputAttributeSpec {
                    name: String::from($column_name),
                    attr_type: $attr_type,
                }],
                group_by: vec![String::from($column_name)],
                show_in_grouped: vec![],
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
        let spec = simple_spec!("int", InputAttributeType::Integer);
        let expected = simple_dataframe!("int", InputAttributeType::Integer => integer_value!(10), "{\n  \"int\": 10\n}"; integer_value!(20), "{\n  \"int\": 20\n}");
        let actual = read_dataframe(input.as_bytes(), &spec, false);
        assert_eq!(Some(expected), actual.ok());
    }

    #[test]
    fn read_dataframe_parses_datetime_column() {
        let input = "{\"d\": \"2022-01-05T00:50:03.432Z\"}\n{\"d\": \"2022-01-05T00:50:05Z\"}";
        let spec = simple_spec!("d", InputAttributeType::DateTime);
        let expected = simple_dataframe!("d", InputAttributeType::DateTime => datetime_value!(2022, 1, 5, 0, 50, 3, 432), "{\n  \"d\": \"2022-01-05T00:50:03.432Z\"\n}"; datetime_value!(2022, 1, 5, 0, 50, 5, 0), "{\n  \"d\": \"2022-01-05T00:50:05Z\"\n}");
        let actual = read_dataframe(input.as_bytes(), &spec, false);
        assert_eq!(Some(expected), actual.ok());
    }

    #[test]
    fn read_dataframe_converts_datetime_to_utc() {
        let input = "{\"d\": \"2022-01-05T03:50:03.432+03:00\"}";
        let spec = simple_spec!("d", InputAttributeType::DateTime);
        let expected = simple_dataframe!("d", InputAttributeType::DateTime => datetime_value!(2022, 1, 5, 0, 50, 3, 432), "{\n  \"d\": \"2022-01-05T03:50:03.432+03:00\"\n}");
        let actual = read_dataframe(input.as_bytes(), &spec, false);
        assert_eq!(Some(expected), actual.ok());
    }

    #[test]
    fn read_dataframe_parses_string_column() {
        let input = "{\"s\": \"hello\"}\n{\"s\": \"world\"}\n";
        let spec = simple_spec!("s", InputAttributeType::String);
        let expected = simple_dataframe!("s", InputAttributeType::String => string_value!("hello"), "{\n  \"s\": \"hello\"\n}"; string_value!("world"), "{\n  \"s\": \"world\"\n}");
        let actual = read_dataframe(input.as_bytes(), &spec, false);
        assert_eq!(Some(expected), actual.ok());
    }

    #[test]
    fn read_dataframe_parses_column_with_missing_values() {
        let input = "{\"s\": \"hello\"}\n{}\n";
        let spec = simple_spec!("s", InputAttributeType::String);
        let expected = simple_dataframe!("s", InputAttributeType::String => string_value!("hello"), "{\n  \"s\": \"hello\"\n}"; ColumnValue::None, "{}");
        let actual = read_dataframe(input.as_bytes(), &spec, false);
        assert_eq!(Some(expected), actual.ok());
    }

    #[test]
    fn read_dataframe_parses_integer_column_when_reading_single_object() {
        let input = "[{\"int\": 10}, {\"int\": 20}]";
        let spec = simple_spec!("int", InputAttributeType::Integer);
        let expected = simple_dataframe!("int", InputAttributeType::Integer => integer_value!(10), "{\n  \"int\": 10\n}"; integer_value!(20), "{\n  \"int\": 20\n}");
        let actual = read_dataframe(input.as_bytes(), &spec, true);
        assert_eq!(Some(expected), actual.ok());
    }

    #[test]
    fn read_dataframe_parses_string_column_when_reading_single_object() {
        let input = "[{\"s\": \"hello\"}, {\"s\": \"world\"}]";
        let spec = simple_spec!("s", InputAttributeType::String);
        let expected = simple_dataframe!("s", InputAttributeType::String => string_value!("hello"), "{\n  \"s\": \"hello\"\n}"; string_value!("world"), "{\n  \"s\": \"world\"\n}");
        let actual = read_dataframe(input.as_bytes(), &spec, true);
        assert_eq!(Some(expected), actual.ok());
    }

    #[test]
    fn read_dataframe_parses_datetime_column_when_readin_single_object() {
        let input = "[{\"d\": \"2022-01-05T00:50:03.432Z\"}, {\"d\": \"2022-01-05T00:50:05Z\"}]";
        let spec = simple_spec!("d", InputAttributeType::DateTime);
        let expected = simple_dataframe!("d", InputAttributeType::DateTime => datetime_value!(2022, 1, 5, 0, 50, 3, 432), "{\n  \"d\": \"2022-01-05T00:50:03.432Z\"\n}"; datetime_value!(2022, 1, 5, 0, 50, 5, 0), "{\n  \"d\": \"2022-01-05T00:50:05Z\"\n}");
        let actual = read_dataframe(input.as_bytes(), &spec, true);
        assert_eq!(Some(expected), actual.ok());
    }

    #[test]
    fn read_dataframe_parses_column_with_missing_values_when_reading_single_object() {
        let input = "[{\"s\": \"hello\"}, {}]";
        let spec = simple_spec!("s", InputAttributeType::String);
        let expected = simple_dataframe!("s", InputAttributeType::String => string_value!("hello"), "{\n  \"s\": \"hello\"\n}"; ColumnValue::None, "{}");
        let actual = read_dataframe(input.as_bytes(), &spec, true);
        assert_eq!(Some(expected), actual.ok());
    }
}
