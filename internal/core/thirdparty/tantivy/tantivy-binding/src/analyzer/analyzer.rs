use log::warn;
use serde_json as json;
use std::collections::HashMap;
use tantivy::tokenizer::*;

use super::options::{get_global_file_resource_helper, FileResourcePathHelper};
use super::{build_in_analyzer::*, filter::*, tokenizers::get_builder_with_tokenizer};
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
            return Err(TantivyBindingError::InternalError(
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
                        return Err(TantivyBindingError::InternalError(format!(
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
                "filter" => {
                    // build with filter if filter param exist
                    builder = self.build_filter(builder, value)?;
                }
                other => {
                    return Err(TantivyBindingError::InternalError(format!(
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
            other_ => Err(TantivyBindingError::InternalError(format!(
                "unknown build-in analyzer type: {}",
                other_
            ))),
        }
    }

    fn build(mut self) -> Result<TextAnalyzer> {
        // build base build-in analyzer
        match self.params.get("type") {
            Some(type_) => {
                if !type_.is_string() {
                    return Err(TantivyBindingError::InternalError(format!(
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
            return Err(TantivyBindingError::InternalError(format!(
                "tokenizer name or type must be set"
            )));
        }

        let value = tokenizer_params.unwrap();
        if !value.is_object() && !value.is_string() {
            return Err(TantivyBindingError::InternalError(format!(
                "tokenizer name should be string or dict"
            )));
        }

        let mut builder = get_builder_with_tokenizer(
            tokenizer_params.unwrap(),
            &mut self.helper,
            create_analyzer_by_json,
        )?;

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

    let json_params = &json::from_str::<json::Map<String, json::Value>>(&params)
        .map_err(|e| TantivyBindingError::JsonError(e))?;

    let mut helper = create_helper(extra_info)?;
    create_analyzer_by_json(json_params, &mut helper)?;
    Ok(helper.get_resource_ids())
}

#[cfg(test)]
mod tests {
    use crate::analyzer::analyzer::create_analyzer;

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
