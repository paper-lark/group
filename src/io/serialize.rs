use serde;
use serde::Serialize;
use serde_json;

fn sort_alphabetically<T: Serialize, S: serde::Serializer>(value: &T, serializer: S) -> Result<S::Ok, S::Error> {
    let value = serde_json::to_value(value).map_err(serde::ser::Error::custom)?;
    value.serialize(serializer)
}

#[derive(Serialize)]
struct SortAlphabetically<T: Serialize>(#[serde(serialize_with = "sort_alphabetically")] T);

pub fn to_pretty_json<T: Serialize>(value: &T) -> serde_json::Result<String> {
    serde_json::to_string_pretty(&SortAlphabetically(value))
}
