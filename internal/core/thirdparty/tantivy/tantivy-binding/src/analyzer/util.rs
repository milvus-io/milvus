use serde_json as json;

use crate::error::{Result,TantivyBindingError};
use crate::analyzer::stop_words;

pub(crate) fn get_string_list(value: &json::Value, label: &str) -> Result<Vec<String>> {
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

pub(crate) fn get_stop_words_list(str_list: Vec<String>) -> Vec<String> {
    let mut stop_words = Vec::new();
    for str in str_list {
        if str.len() > 0 && str.chars().nth(0).unwrap() == '_' {
            match str.as_str() {
                "_english_" => {
                    for word in stop_words::ENGLISH {
                        stop_words.push(word.to_string());
                    }
                    continue;
                }
                _other => {}
            }
        }
        stop_words.push(str);
    }
    stop_words
}
