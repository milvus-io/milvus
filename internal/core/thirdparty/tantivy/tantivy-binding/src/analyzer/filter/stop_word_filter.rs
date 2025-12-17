use super::filter::FilterBuilder;
use super::stop_words::fetch_language_stop_words;
use super::util::*;
use crate::error::{Result, TantivyBindingError};
use serde_json as json;
use tantivy::tokenizer::StopWordFilter;

const STOP_WORDS_LIST_KEY: &str = "stop_words";
const STOP_WORDS_FILE_KEY: &str = "stop_words_file";

pub(crate) fn get_stop_words_list(str_list: Vec<String>) -> Vec<String> {
    let mut stop_words = Vec::new();
    for str in str_list {
        if str.len() > 0 && str.chars().nth(0).unwrap() == '_' {
            match fetch_language_stop_words(str.as_str()) {
                Some(words) => {
                    for word in words {
                        stop_words.push(word.to_string());
                    }
                    continue;
                }
                None => {}
            }
        }
        stop_words.push(str);
    }
    stop_words
}

impl FilterBuilder for StopWordFilter {
    fn from_json(params: &json::Map<String, json::Value>) -> Result<Self> {
        let mut dict = Vec::<String>::new();
        if let Some(value) = params.get(STOP_WORDS_LIST_KEY) {
            dict = get_stop_words_list(get_string_list(value, "stop_words")?);
        }

        if let Some(file_params) = params.get(STOP_WORDS_FILE_KEY) {
            read_line_file(&mut dict, file_params, "stop words dict file")?;
        }

        Ok(StopWordFilter::remove(dict))
    }
}

#[cfg(test)]
mod tests {
    use super::StopWordFilter;
    use crate::analyzer::filter::FilterBuilder;
    use crate::analyzer::tokenizers::standard_builder;
    use crate::log::init_log;
    use serde_json as json;
    use std::collections::HashSet;
    use std::path::Path;

    #[test]
    fn test_stop_words_filter_with_file() {
        init_log();
        let file_dir = Path::new(file!()).parent().unwrap();
        let stop_words_path = file_dir.join("../data/test/stop_words_dict.txt");
        let stop_words_path_str = stop_words_path.to_string_lossy().to_string();
        let params = format!(
            r#"{{
                "type": "stop_words",
                "stop_words_file": {{
                    "type": "local",
                    "path": "{stop_words_path_str}"
                }}
            }}"#
        );

        let json_params = json::from_str::<json::Value>(&params).unwrap();
        let filter = StopWordFilter::from_json(json_params.as_object().unwrap());
        assert!(filter.is_ok(), "error: {}", filter.err().unwrap());

        let builder = standard_builder().filter(filter.unwrap());
        let mut analyzer = builder.build();
        let mut stream = analyzer
            .token_stream("this is a simple test of the stop words filter in an indexing system");

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
            HashSet::from(["simple", "test", "stop", "words", "filter", "indexing", "system"])
        );
    }
}
