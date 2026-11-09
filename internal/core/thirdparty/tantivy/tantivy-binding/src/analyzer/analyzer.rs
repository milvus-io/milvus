use serde_json as json;
use std::collections::HashMap;
use tantivy::tokenizer::*;

use super::options::{get_global_file_resource_helper, FileResourcePathHelper};
use super::{
    build_in_analyzer::*,
    char_filter::{build_char_filters, OffsetMappingMode},
    filter::*,
    tokenizers::{get_builder_with_tokenizer, CharFilterTokenizer},
};
use crate::analyzer::filter::{create_filter, get_stop_words_list, get_string_list};
use crate::error::Result;
use crate::error::TantivyBindingError;

struct AnalyzerBuilder<'a> {
    filters: HashMap<String, SystemFilter>,
    helper: &'a mut FileResourcePathHelper,
    params: &'a json::Map<String, json::Value>,
}

impl<'a> AnalyzerBuilder<'a> {
    fn new(
        params: &'a json::Map<String, json::Value>,
        helper: &'a mut FileResourcePathHelper,
    ) -> Result<AnalyzerBuilder<'a>> {
        Ok(AnalyzerBuilder {
            filters: HashMap::new(),
            params: params,
            helper: helper,
        })
    }

    fn build_filter(
        &mut self,
        mut builder: TextAnalyzerBuilder,
        params: &json::Value,
    ) -> Result<TextAnalyzerBuilder> {
        if !params.is_array() {
            return Err(TantivyBindingError::InvalidArgument(
                "filter params should be array".to_string(),
            ));
        }

        let filters = params.as_array().unwrap();

        for filter in filters {
            if filter.is_string() {
                let filter_name = filter.as_str().unwrap();
                let customize = self.filters.remove(filter_name);
                if !customize.is_none() {
                    builder = customize.unwrap().transform(builder);
                    continue;
                }

                // check if filter was system filter
                let system = SystemFilter::from(filter_name);
                match system {
                    SystemFilter::Invalid => {
                        return Err(TantivyBindingError::InvalidArgument(format!(
                            "build analyzer failed, filter not found :{}",
                            filter_name
                        )))
                    }
                    other => {
                        builder = other.transform(builder);
                    }
                }
            } else if filter.is_object() {
                let filter = create_filter(filter.as_object().unwrap(), &mut self.helper)?;
                builder = filter.transform(builder);
            }
        }
        Ok(builder)
    }

    fn build_option(&mut self, mut builder: TextAnalyzerBuilder) -> Result<TextAnalyzerBuilder> {
        for (key, value) in self.params {
            match key.as_str() {
                "tokenizer" => {}
                "char_filter" => {}
                "char_filter_offset_mode" => {}
                "filter" => {
                    // build with filter if filter param exist
                    builder = self.build_filter(builder, value)?;
                }
                other => {
                    return Err(TantivyBindingError::InvalidArgument(format!(
                        "unknown analyzer option key: {}",
                        other
                    )))
                }
            }
        }
        Ok(builder)
    }

    fn get_stop_words_option(&self) -> Result<Vec<String>> {
        let value = self.params.get("stop_words");
        match value {
            Some(value) => {
                let str_list = get_string_list(value, "filter stop_words")?;
                Ok(get_stop_words_list(str_list))
            }
            _ => Ok(vec![]),
        }
    }

    fn build_template(mut self, type_: &str) -> Result<TextAnalyzer> {
        match type_ {
            "standard" => Ok(standard_analyzer(self.get_stop_words_option()?)),
            "chinese" => Ok(chinese_analyzer(
                self.get_stop_words_option()?,
                &mut self.helper,
            )),
            "english" => Ok(english_analyzer(self.get_stop_words_option()?)),
            "arabic" => Ok(arabic_analyzer(self.get_stop_words_option()?)),
            "thai" => Ok(thai_analyzer(self.get_stop_words_option()?)),
            other_ => Err(TantivyBindingError::InvalidArgument(format!(
                "unknown build-in analyzer type: {}",
                other_
            ))),
        }
    }

    fn build(mut self) -> Result<TextAnalyzer> {
        // build base build-in analyzer
        match self.params.get("type") {
            Some(type_) => {
                if self.params.contains_key("char_filter")
                    || self.params.contains_key("char_filter_offset_mode")
                {
                    return Err(TantivyBindingError::InvalidArgument(
                        "char_filter options cannot be used with build-in analyzer type"
                            .to_string(),
                    ));
                }
                if !type_.is_string() {
                    return Err(TantivyBindingError::InvalidArgument(format!(
                        "analyzer type should be string"
                    )));
                }
                return self.build_template(type_.as_str().unwrap());
            }
            _ => {}
        };

        //build custom analyzer
        let tokenizer_params = self.params.get("tokenizer");
        if tokenizer_params.is_none() {
            return Err(TantivyBindingError::InvalidArgument(format!(
                "tokenizer name or type must be set"
            )));
        }

        let value = tokenizer_params.unwrap();
        if !value.is_object() && !value.is_string() {
            return Err(TantivyBindingError::InvalidArgument(format!(
                "tokenizer name should be string or dict"
            )));
        }

        let mut builder = get_builder_with_tokenizer(
            tokenizer_params.unwrap(),
            &mut self.helper,
            create_analyzer_by_json,
        )?;

        if self.params.contains_key("char_filter_offset_mode")
            && !self.params.contains_key("char_filter")
        {
            return Err(TantivyBindingError::InvalidArgument(
                "char_filter_offset_mode requires char_filter".to_string(),
            ));
        }
        let offset_mode = OffsetMappingMode::from_json(self.params.get("char_filter_offset_mode"))?;
        if let Some(char_filter_params) = self.params.get("char_filter") {
            let char_filters = build_char_filters(char_filter_params)?;
            if !char_filters.is_empty() {
                builder = TextAnalyzer::builder(CharFilterTokenizer::new(
                    char_filters,
                    builder.build(),
                    offset_mode,
                ))
                .dynamic();
            }
        }

        // build and check other options
        builder = self.build_option(builder)?;
        Ok(builder.build())
    }
}

pub fn create_analyzer_by_json(
    analyzer_params: &json::Map<String, json::Value>,
    helper: &mut FileResourcePathHelper,
) -> Result<TextAnalyzer> {
    if analyzer_params.is_empty() {
        return Ok(standard_analyzer(vec![]));
    }

    let builder = AnalyzerBuilder::new(analyzer_params, helper)?;
    builder.build()
}

pub fn create_helper(extra_info: &str) -> Result<FileResourcePathHelper> {
    if extra_info.is_empty() {
        Ok(get_global_file_resource_helper())
    } else {
        Ok(FileResourcePathHelper::from_json(
            &json::from_str::<json::Value>(&extra_info)
                .map_err(|e| TantivyBindingError::JsonError(e))?,
        )?)
    }
}

pub fn create_analyzer(params: &str, extra_info: &str) -> Result<TextAnalyzer> {
    if params.len() == 0 {
        return Ok(standard_analyzer(vec![]));
    }

    let json_params = &json::from_str::<json::Map<String, json::Value>>(&params)
        .map_err(|e| TantivyBindingError::JsonError(e))?;

    let mut helper = create_helper(extra_info)?;
    create_analyzer_by_json(json_params, &mut helper)
}

pub fn validate_analyzer(params: &str, extra_info: &str) -> Result<Vec<i64>> {
    if params.len() == 0 {
        return Ok(vec![]);
    }

    let json_params = &json::from_str::<json::Map<String, json::Value>>(&params).map_err(|e| {
        TantivyBindingError::InvalidArgument(format!("invalid analyzer parameters: {}", e))
    })?;

    let mut helper = create_helper(extra_info)?;
    create_analyzer_by_json(json_params, &mut helper)?;
    Ok(helper.get_resource_ids())
}

#[cfg(test)]
mod tests {
    use crate::analyzer::analyzer::create_analyzer;
    use tantivy::tokenizer::TokenStream;

    #[test]
    fn test_analyzer_json_error_origin() {
        use crate::error::TantivyBindingError;

        for params in [r#"{"tokenizer":"#, "[]", "null", r#"{"limit":1e999}"#] {
            assert!(matches!(
                create_analyzer(params, ""),
                Err(TantivyBindingError::JsonError(_))
            ));
            assert!(matches!(
                super::validate_analyzer(params, ""),
                Err(TantivyBindingError::InvalidArgument(_))
            ));
        }
        for params in ["{}", r#"{"tokenizer":"standard"}"#] {
            assert!(matches!(
                create_analyzer(params, "invalid internal snapshot"),
                Err(TantivyBindingError::JsonError(_))
            ));
            assert!(matches!(
                super::validate_analyzer(params, "invalid internal snapshot"),
                Err(TantivyBindingError::JsonError(_))
            ));
        }
    }

    #[test]
    fn test_missing_resource_name_classification_uses_validation_snapshot() {
        let params = r#"{"tokenizer":"standard","filter":[{"type":"stop","stop_words_file":{"type":"remote","resource_name":"missing_dictionary","file_name":"stop.txt"}}]}"#;
        let runtime_info = r#"{"resource_map":{}}"#;
        let validation_info = r#"{"resource_map":{},"validate_resource_names":true}"#;
        assert!(matches!(
            super::validate_analyzer(params, validation_info),
            Err(crate::error::TantivyBindingError::InvalidArgument(_))
        ));
        assert!(matches!(
            super::validate_analyzer(params, runtime_info),
            Err(crate::error::TantivyBindingError::InternalError(_))
        ));
        assert!(matches!(
            create_analyzer(params, runtime_info),
            Err(crate::error::TantivyBindingError::InternalError(_))
        ));
    }

    #[test]
    fn test_remote_filename_validation_uses_metadata_not_file_existence() {
        use crate::analyzer::options::set_options;
        use crate::error::TantivyBindingError;
        use serde_json::json;

        let directory = tempfile::tempdir().unwrap();
        // An absolute storage_name keeps this fixture independent of other
        // tests changing the global resource_path.
        set_options(&json!({"resource_path": "/tmp"}).to_string()).unwrap();
        let resource_dir = directory.path().join("10");
        std::fs::create_dir(&resource_dir).unwrap();
        let file = resource_dir.join("stop.txt");
        std::fs::write(&file, "the\n").unwrap();
        let mut info = json!({
            "resource_map": {"dictionary": 10},
            "resource_file_names": {"dictionary": "stop.txt"},
            "storage_name": directory.path().to_str().unwrap(),
            "validate_resource_names": true
        });
        let params = |file_name: &str| {
            json!({
                "tokenizer": "standard",
                "filter": [{"type": "stop", "stop_words_file": {
                    "type": "remote", "resource_name": "dictionary", "file_name": file_name
                }}]
            })
            .to_string()
        };

        assert_eq!(
            super::validate_analyzer(&params("stop.txt"), &info.to_string()).unwrap(),
            vec![10]
        );
        // Even an existing file must not override the registered filename.
        std::fs::write(resource_dir.join("typo.txt"), "a\n").unwrap();
        for file_name in ["typo.txt", "missing.txt", "../stop.txt", ""] {
            let err = super::validate_analyzer(&params(file_name), &info.to_string()).unwrap_err();
            assert!(
                matches!(&err, TantivyBindingError::InvalidArgument(_)),
                "{err}"
            );
            assert!(err
                .to_string()
                .contains("does not match registered filename"));
        }
        // A correct reference followed by disappearance is still an I/O error.
        std::fs::remove_file(&file).unwrap();
        assert!(matches!(
            super::validate_analyzer(&params("stop.txt"), &info.to_string()),
            Err(TantivyBindingError::IOError(err)) if err.kind() == std::io::ErrorKind::NotFound
        ));
        // Runtime extra-info callers retain their previous lookup semantics.
        info["validate_resource_names"] = json!(false);
        assert!(super::validate_analyzer(&params("typo.txt"), &info.to_string()).is_ok());
        info.as_object_mut().unwrap().remove("resource_file_names");
        assert!(super::validate_analyzer(&params("typo.txt"), &info.to_string()).is_ok());
    }

    #[test]
    fn test_jieba_custom_word_is_not_a_system_dictionary_name() {
        assert!(create_analyzer(
            r#"{"tokenizer":{"type":"jieba","dict":["custom_word"]}}"#,
            "",
        )
        .is_ok());
    }

    #[test]
    fn test_invalid_analyzer_configuration_is_input_error() {
        for params in [
            r#"{"type":"klingon"}"#,
            r#"{"tokenizer":"standard","filter":[{}]}"#,
            r#"{"tokenizer":"standard","filter":[{"type":1}]}"#,
            r#"{"tokenizer":"standard","filter":[{"type":"unknown_filter"}]}"#,
            r#"{"tokenizer":"standard","filter":[{"type":"length"}]}"#,
            r#"{"tokenizer":"standard","filter":[{"type":"length","max":"10"}]}"#,
            r#"{"tokenizer":"standard","filter":[{"type":"pinyin","keep_original":"yes"}]}"#,
            r#"{"tokenizer":"standard","filter":[{"type":"pinyin","keep_full_pinyin":"yes"}]}"#,
            r#"{"tokenizer":"standard","filter":[{"type":"pinyin","keep_joined_full_pinyin":"yes"}]}"#,
            r#"{"tokenizer":"standard","filter":[{"type":"pinyin","keep_separate_first_letter":"yes"}]}"#,
            r#"{"tokenizer":"standard","filter":[{"type":"regex"}]}"#,
            r#"{"tokenizer":"standard","filter":[{"type":"regex","expr":1}]}"#,
            r#"{"tokenizer":"standard","filter":[{"type":"decompounder","word_list":"word"}]}"#,
            r#"{"tokenizer":"standard","filter":[{"type":"decompounder","word_list":[1]}]}"#,
            r#"{"tokenizer":"standard","filter":[{"type":"decompounder","word_list":[]}]}"#,
            r#"{"tokenizer":"standard","filter":[{"type":"stop","stop_words":"word"}]}"#,
            r#"{"tokenizer":"standard","filter":[{"type":"stop","stop_words":[1]}]}"#,
            r#"{"tokenizer":"standard","filter":[{"type":"synonym","synonyms":[1]}]}"#,
            r#"{"tokenizer":{"type":"lang_ident","identifier":{"type":"whatlang","confidence":"high"}}}"#,
            r#"{"type":1}"#,
            r#"{"tokenizer":"standard","filter":"lowercase"}"#,
            r#"{"tokenizer":"standard","filter":["unknown_filter"]}"#,
            r#"{"tokenizer":"standard","unknown_option":true}"#,
            r#"{"tokenizer":"standard","filter":[{"type":"stemmer","language":"xx"}]}"#,
            r#"{"tokenizer":"standard","filter":[{"type":"stemmer","language":1}]}"#,
        ] {
            assert!(
                matches!(
                    create_analyzer(params, ""),
                    Err(crate::error::TantivyBindingError::InvalidArgument(_))
                ),
                "{}",
                params
            );
        }
    }

    #[test]
    fn test_standard_analyzer() {
        let params = r#"{
            "type": "standard",
            "stop_words": ["_english_"]
        }"#;

        let tokenizer = create_analyzer(&params.to_string(), "");
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());
    }

    #[test]
    fn test_analyzer_with_empty_char_filter() {
        let params = r#"{
            "char_filter": [],
            "tokenizer": "standard",
            "filter": ["lowercase"]
        }"#;

        let analyzer = create_analyzer(params, "");
        assert!(analyzer.is_ok(), "error: {}", analyzer.err().unwrap());

        let mut analyzer = analyzer.unwrap();
        let mut stream = analyzer.token_stream("MILVUS");
        assert!(stream.advance());

        let token = stream.token();
        assert_eq!(token.text, "milvus");
        assert_eq!(token.offset_from, 0);
        assert_eq!(token.offset_to, 6);
        assert!(!stream.advance());
    }

    #[test]
    fn test_analyzer_with_mapping_char_filter() {
        let params = r#"{
            "char_filter": [
                {
                    "type": "mapping",
                    "mappings": ["&=>and"]
                }
            ],
            "tokenizer": "standard",
            "filter": ["lowercase"]
        }"#;

        let analyzer = create_analyzer(params, "");
        assert!(analyzer.is_ok(), "error: {}", analyzer.err().unwrap());

        let mut analyzer = analyzer.unwrap();
        let mut stream = analyzer.token_stream("A&B");
        assert!(stream.advance());

        let token = stream.token();
        assert_eq!(token.text, "aandb");
        assert_eq!(token.offset_from, 0);
        assert_eq!(token.offset_to, 3);
        assert!(!stream.advance());
    }

    #[test]
    fn test_analyzer_rejects_unsupported_char_filter() {
        let params = r#"{
            "char_filter": [
                {
                    "type": "unsupported"
                }
            ],
            "tokenizer": "standard"
        }"#;

        let analyzer = create_analyzer(params, "");
        assert!(analyzer.is_err());
    }

    #[test]
    fn test_analyzer_rejects_unsupported_char_filter_offset_mode() {
        let params = r#"{
            "char_filter": [],
            "char_filter_offset_mode": "unsupported",
            "tokenizer": "standard"
        }"#;

        assert!(create_analyzer(params, "").is_err());
    }

    #[test]
    fn test_char_filter_rejects_build_in_analyzer() {
        let params = r#"{
            "type": "standard",
            "char_filter": []
        }"#;

        let analyzer = create_analyzer(params, "");
        assert!(analyzer.is_err());
    }

    #[test]
    fn test_chinese_analyzer() {
        let params = r#"{
            "type": "chinese"
        }"#;

        let tokenizer = create_analyzer(&params.to_string(), "");
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());
        let mut bining = tokenizer.unwrap();
        let mut stream = bining.token_stream("系统安全;,'';lxyz密码");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            let token = stream.token();
            results.push(token.text.clone());
        }

        print!("test tokens :{:?}\n", results)
    }

    #[test]
    fn test_arabic_analyzer() {
        let params = r#"{
            "type": "arabic"
        }"#;

        let tokenizer = create_analyzer(&params.to_string(), "");
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());

        let mut binding = tokenizer.unwrap();
        // "الكتاب" (the book) with diacritics + Arabic-Indic digits
        let mut stream = binding.token_stream("كِتَابٌ عـــربي ١٢٣");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            let token = stream.token();
            results.push(token.text.clone());
        }

        // Verify diacritics removed, tatweel removed, digits converted
        assert!(!results.is_empty(), "Should produce tokens");
        // "١٢٣" should become "123"
        assert!(
            results.contains(&"123".to_string()),
            "Arabic-Indic digits should be converted: {:?}",
            results
        );
        // Tatweel should be removed from "عـــربي"
        assert!(
            results.iter().all(|t| !t.contains('\u{0640}')),
            "Tatweel should be removed: {:?}",
            results
        );

        print!("test arabic analyzer tokens: {:?}\n", results);
    }

    #[test]
    fn test_arabic_analyzer_with_custom_stop_words() {
        let params = r#"{
            "type": "arabic",
            "stop_words": ["كتاب"]
        }"#;

        let tokenizer = create_analyzer(&params.to_string(), "");
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());
    }

    #[test]
    fn test_thai_analyzer() {
        let params = r#"{
            "type": "thai"
        }"#;

        let tokenizer = create_analyzer(&params.to_string(), "");
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());

        let mut binding = tokenizer.unwrap();
        let mut stream = binding.token_stream("การทดสอบภาษาไทย ๑๒๓");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            let token = stream.token();
            results.push(token.text.clone());
        }

        assert!(!results.is_empty(), "Should produce tokens");
        // Thai digits should be converted
        assert!(
            results.contains(&"123".to_string()),
            "Thai digits should be converted: {:?}",
            results
        );

        print!("test thai analyzer tokens: {:?}\n", results);
    }

    #[test]
    fn test_thai_analyzer_with_custom_stop_words() {
        let params = r#"{
            "type": "thai",
            "stop_words": ["ทดสอบ"]
        }"#;

        let tokenizer = create_analyzer(&params.to_string(), "");
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());
    }

    #[test]
    #[cfg(feature = "lindera-ipadic")]
    fn test_lindera_analyzer() {
        let params = r#"{
            "tokenizer": {
                "type": "lindera",
                "dict_kind": "ipadic"
            }
        }"#;

        let tokenizer = create_analyzer(&params.to_string(), "");
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());

        let mut bining = tokenizer.unwrap();
        let mut stream =
            bining.token_stream("東京スカイツリーの最寄り駅はとうきょうスカイツリー駅です");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            let token = stream.token();
            results.push(token.text.clone());
        }

        print!("test tokens :{:?}\n", results)
    }
}
