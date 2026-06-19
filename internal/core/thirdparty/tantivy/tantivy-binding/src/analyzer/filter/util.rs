use crate::analyzer::options::get_resource_path;
use crate::analyzer::options::FileResourcePathHelper;
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
    helper: &mut FileResourcePathHelper,
    dict: &mut Vec<String>,
    params: &json::Value,
    key: &str,
) -> Result<()> {
    let path = get_resource_path(helper, params, key)?;
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);
    for (line_index, line) in reader.lines().enumerate() {
        let mut row_data = line.map_err(|err| {
            TantivyBindingError::InternalError(format!(
                "read {} file failed, error: {}",
                key,
                err.to_string()
            ))
        })?;
        if line_index == 0 {
            row_data = row_data
                .strip_prefix('\u{feff}')
                .unwrap_or(&row_data)
                .to_string();
        }
        dict.push(row_data);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::read_line_file;
    use crate::analyzer::options::{FileResourcePathHelper, ResourceInfo};
    use serde_json as json;
    use std::sync::Arc;

    #[test]
    fn test_read_line_file_strips_utf8_bom_from_first_line() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("stop_words.txt");
        std::fs::write(&file_path, b"\xEF\xBB\xBFthe\r\nand\r\n").unwrap();

        let params = json::json!({
            "type": "local",
            "path": file_path.to_string_lossy()
        });
        let mut helper = FileResourcePathHelper::new(Arc::new(ResourceInfo::new()));
        let mut dict = Vec::new();

        read_line_file(&mut helper, &mut dict, &params, "test dict file").unwrap();

        assert_eq!(dict, vec!["the".to_string(), "and".to_string()]);
    }
}
