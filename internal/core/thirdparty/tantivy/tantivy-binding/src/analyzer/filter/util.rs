use crate::error::{Result, TantivyBindingError};
use serde_json as json;

pub fn get_string_list(value: &json::Value, label: &str) -> Result<Vec<String>> {
    if !value.is_array() {
        return Err(TantivyBindingError::InternalError(
            format!("{} should be array", label).to_string(),
        ));
    }

    let stop_words = value.as_array().unwrap();
    let mut str_list = Vec::<String>::new();
    for element in stop_words {
        match element.as_str() {
            Some(word) => str_list.push(word.to_string()),
            _ => {
                return Err(TantivyBindingError::InternalError(
                    format!("{} list item should be string", label).to_string(),
                ))
            }
        }
    }
    Ok(str_list)
}
