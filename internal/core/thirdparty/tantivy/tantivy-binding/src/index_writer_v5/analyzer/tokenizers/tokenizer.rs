use log::warn;
use serde_json as json;
use tantivy_5::tokenizer::*;
use tantivy_5::tokenizer::{TextAnalyzer, TextAnalyzerBuilder};

use crate::error::{Result, TantivyBindingError};

use super::jieba_tokenizer::JiebaTokenizer;
use super::lindera_tokenizer::LinderaTokenizer;

pub fn standard_builder() -> TextAnalyzerBuilder {
    TextAnalyzer::builder(SimpleTokenizer::default()).dynamic()
}

pub fn whitespace_builder() -> TextAnalyzerBuilder {
    TextAnalyzer::builder(WhitespaceTokenizer::default()).dynamic()
}

pub fn jieba_builder() -> TextAnalyzerBuilder {
    TextAnalyzer::builder(JiebaTokenizer::new()).dynamic()
}

pub fn lindera_builder(
    params: Option<&json::Map<String, json::Value>>,
) -> Result<TextAnalyzerBuilder> {
    if params.is_none() {
        return Err(TantivyBindingError::InvalidArgument(
            "lindera tokenizer must be costum".to_string(),
        ));
    }
    let tokenizer = LinderaTokenizer::from_json(params.unwrap())?;
    Ok(TextAnalyzer::builder(tokenizer).dynamic())
}

pub fn get_builder_with_tokenizer(params: &json::Value) -> Result<TextAnalyzerBuilder> {
    let name;
    let params_map;
    if params.is_string() {
        name = params.as_str().unwrap();
        params_map = None;
    } else {
        let m = params.as_object().unwrap();
        match m.get("type") {
            Some(val) => {
                if !val.is_string() {
                    return Err(TantivyBindingError::InvalidArgument(
                        "tokenizer type should be string".to_string(),
                    ));
                }
                name = val.as_str().unwrap();
            }
            _ => {
                return Err(TantivyBindingError::InvalidArgument(
                    "costum tokenizer must set type".to_string(),
                ))
            }
        }
        params_map = Some(m);
    }

    match name {
        "standard" => Ok(standard_builder()),
        "whitespace" => Ok(whitespace_builder()),
        "jieba" => Ok(jieba_builder()),
        "lindera" => lindera_builder(params_map),
        other => {
            warn!("unsupported tokenizer: {}", other);
            Err(TantivyBindingError::InvalidArgument(format!(
                "unsupported tokenizer: {}",
                other
            )))
        }
    }
}
