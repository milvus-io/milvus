use crate::analyzer::options::get_resource_path;
use crate::error::{Result, TantivyBindingError};
use serde_json as json;
use std::io::BufRead;

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

pub(crate) fn read_line_file(
    dict: &mut Vec<String>,
    params: &json::Value,
    key: &str,
) -> Result<()> {
    let path = get_resource_path(params, key)?;
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);
    for line in reader.lines() {
        if let Ok(row_data) = line {
            dict.push(row_data);
        } else {
            return Err(TantivyBindingError::InternalError(format!(
                "read {} file failed, error: {}",
                key,
                line.unwrap_err().to_string()
            )));
        }
    }
    Ok(())
}
