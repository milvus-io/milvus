use super::filter::FilterBuilder;
use super::util::read_line_file;
use crate::error::{Result, TantivyBindingError};
use serde_json as json;
use tantivy::tokenizer::SplitCompoundWords;

const WORD_LIST_KEY: &str = "word_list";
const WORD_LIST_FILE_KEY: &str = "word_list_file";

impl FilterBuilder for SplitCompoundWords {
    fn from_json(params: &json::Map<String, json::Value>) -> Result<Self> {
        let mut dict = Vec::<String>::new();
        if let Some(value) = params.get(WORD_LIST_KEY) {
            if !value.is_array() {
                return Err(TantivyBindingError::InternalError(
                    "decompounder word list should be array".to_string(),
                ));
            }
            let words = value.as_array().unwrap();
            for element in words {
                if let Some(word) = element.as_str() {
                    dict.push(word.to_string());
                } else {
                    return Err(TantivyBindingError::InternalError(
                        "decompounder word list item should be string".to_string(),
                    ));
                }
            }
        }

        if let Some(file_params) = params.get(WORD_LIST_FILE_KEY) {
            read_line_file(&mut dict, file_params, "decompounder word list file")?;
        }

        if dict.is_empty() {
            return Err(TantivyBindingError::InternalError(
                "decompounder word list is empty".to_string(),
            ));
        }

        SplitCompoundWords::from_dictionary(dict).map_err(|e| {
            TantivyBindingError::InternalError(format!(
                "create decompounder failed: {}",
                e.to_string()
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::SplitCompoundWords;
    use crate::analyzer::filter::FilterBuilder;
    use crate::analyzer::tokenizers::standard_builder;
    use crate::log::init_log;
    use serde_json as json;
    use std::collections::HashSet;
    use std::path::Path;

    #[test]
    fn test_decompounder_filter_with_file() {
        init_log();
        let file_dir = Path::new(file!()).parent().unwrap();
        let decompounder_path = file_dir.join("../data/test/decompounder_dict.txt");
        let decompounder_path_str = decompounder_path.to_string_lossy().to_string();
        let params = format!(
            r#"{{
                "type": "decompounder",
                "word_list_file": {{
                    "type": "local",
                    "path": "{decompounder_path_str}"
                }}
            }}"#
        );
        let json_params = json::from_str::<json::Value>(&params).unwrap();
        // let filter = SplitCompoundWords::from_dictionary(vec!["bank", "note"]);
        let filter = SplitCompoundWords::from_json(json_params.as_object().unwrap());
        assert!(filter.is_ok(), "error: {}", filter.err().unwrap());
        let builder = standard_builder().filter(filter.unwrap());
        let mut analyzer = builder.build();
        let mut stream = analyzer.token_stream("banknote");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            let token = stream.token();
            results.push(token.text.clone());
        }

        assert_eq!(
            results
                .iter()
                .map(|s| s.as_str())
                .collect::<HashSet<&str>>(),
            HashSet::from(["bank", "note"])
        );
    }
}
