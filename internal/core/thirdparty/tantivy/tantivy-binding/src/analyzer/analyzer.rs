use serde_json as json;
use std::collections::HashMap;
use tantivy::tokenizer::*;

use super::{build_in_analyzer::*, filter::*, tokenizers::get_builder_with_tokenizer};
use crate::error::Result;
use crate::error::TantivyBindingError;

struct AnalyzerBuilder<'a> {
    filters: HashMap<String, SystemFilter>,
    params: &'a json::Map<String, json::Value>,
}

impl AnalyzerBuilder<'_> {
    fn new(params: &json::Map<String, json::Value>) -> AnalyzerBuilder {
        AnalyzerBuilder {
            filters: HashMap::new(),
            params: params,
        }
    }

    fn get_tokenizer_params(&self) -> Result<&json::Value> {
        let tokenizer = self.params.get("tokenizer");
        if tokenizer.is_none() {
            return Err(TantivyBindingError::InternalError(format!(
                "tokenizer name or type must be set"
            )));
        }
        let value = tokenizer.unwrap();
        if value.is_object() || value.is_string() {
            return Ok(tokenizer.unwrap());
        }

        Err(TantivyBindingError::InternalError(format!(
            "tokenizer name should be string or dict"
        )))
    }

    fn add_custom_filter(
        &mut self,
        name: &String,
        params: &json::Map<String, json::Value>,
    ) -> Result<()> {
        match SystemFilter::try_from(params) {
            Ok(filter) => {
                self.filters.insert(name.to_string(), filter);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    // not used now
    // support add custom filter with filter name
    fn add_custom_filters(&mut self, params: &json::Map<String, json::Value>) -> Result<()> {
        for (name, value) in params {
            if !value.is_object() {
                continue;
            }
            self.add_custom_filter(name, value.as_object().unwrap())?;
        }
        Ok(())
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
                let costum = self.filters.remove(filter_name);
                if !costum.is_none() {
                    builder = costum.unwrap().transform(builder);
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
                let filter = SystemFilter::try_from(filter.as_object().unwrap())?;
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

    fn build_template(self, type_: &str) -> Result<TextAnalyzer> {
        match type_ {
            "standard" => Ok(standard_analyzer(self.get_stop_words_option()?)),
            "chinese" => Ok(chinese_analyzer(self.get_stop_words_option()?)),
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
                        "analyzer type shoud be string"
                    )));
                }
                return self.build_template(type_.as_str().unwrap());
            }
            _ => {}
        };

        //build custom analyzer
        let tokenizer_params = self.get_tokenizer_params()?;
        let mut builder = get_builder_with_tokenizer(&tokenizer_params, create_analyzer_by_json)?;

        // build and check other options
        builder = self.build_option(builder)?;
        Ok(builder.build())
    }
}

pub fn create_analyzer_by_json(
    analyzer_params: &json::Map<String, json::Value>,
) -> Result<TextAnalyzer> {
    if analyzer_params.is_empty() {
        return Ok(standard_analyzer(vec![]));
    }

    let builder = AnalyzerBuilder::new(analyzer_params);
    builder.build()
}

pub fn create_analyzer(params: &str) -> Result<TextAnalyzer> {
    if params.len() == 0 {
        return Ok(standard_analyzer(vec![]));
    }

    let json_params =
        json::from_str::<json::Value>(&params).map_err(|e| TantivyBindingError::JsonError(e))?;

    create_analyzer_by_json(
        json_params
            .as_object()
            .ok_or(TantivyBindingError::InternalError(
                "params should be a json map".to_string(),
            ))?,
    )
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

        let tokenizer = create_analyzer(&params.to_string());
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());
    }

    #[test]
    fn test_chinese_analyzer() {
        let params = r#"{
            "type": "chinese"
        }"#;

        let tokenizer = create_analyzer(&params.to_string());
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

        let tokenizer = create_analyzer(&params.to_string());
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
