use lindera::token;
use tantivy::tokenizer::{TextAnalyzer, TextAnalyzerBuilder};
use lindera::segmenter::Segmenter;
use tantivy::tokenizer::*;
use lindera::mode::Mode;
use serde_json as json;
use log::warn;

use crate::analyzer::tokenizers::{JiebaTokenizer, LinderaTokenizer};
use crate::error::{Result,TantivyBindingError};

use super::custom_delimiter_tokenizer::CustomDelimiterTokenizer;


pub fn standard_builder() -> TextAnalyzerBuilder {
    TextAnalyzer::builder(SimpleTokenizer::default()).dynamic()
}

pub fn whitespace_builder() -> TextAnalyzerBuilder {
    TextAnalyzer::builder(WhitespaceTokenizer::default()).dynamic()
}

pub fn jieba_builder() -> TextAnalyzerBuilder {
    TextAnalyzer::builder(JiebaTokenizer::new()).dynamic()
}

pub fn lindera_builder(params: Option<&json::Map<String, json::Value>>) -> Result<TextAnalyzerBuilder>{
    if params.is_none(){
        return Err(TantivyBindingError::InvalidArgument(format!(
            "lindera tokenizer must be costum"
        )))
    }
    let tokenizer = LinderaTokenizer::from_json(params.unwrap())?;
    Ok(TextAnalyzer::builder(tokenizer).dynamic())
}


pub fn custom_delimiter_builder(params: Option<&json::Map<String, json::Value>>) -> Result<TextAnalyzerBuilder> {
    let delimiter = if let Some(map) = params {
        match map.get("delimiter") {
            Some(val) => val.as_str().ok_or_else(|| {
                TantivyBindingError::InvalidArgument("delimiter must be a string".into())
            })?,
            None => " ", // Default to whitespace if no delimiter is provided
        }
    } else {
        " " // Default to whitespace if no params are provided
    };

    let chars: Vec<char> = delimiter.chars().collect();
    if chars.len() != 1 {
        return Err(TantivyBindingError::InvalidArgument(format!(
            "delimiter must be a single character, got: {}",
            delimiter
        )));
    }

    let tokenizer = CustomDelimiterTokenizer::new(chars[0]);
    Ok(TextAnalyzer::builder(tokenizer).dynamic())
}

pub fn get_builder_with_tokenizer(params: &json::Value) -> Result<TextAnalyzerBuilder> {
    let name;
    let params_map;
    if params.is_string(){
        name = params.as_str().unwrap();
        params_map = None;
    }else{
        let m = params.as_object().unwrap();
        match m.get("type"){
            Some(val) => {
                if !val.is_string(){
                    return Err(TantivyBindingError::InvalidArgument(format!(
                        "tokenizer type should be string"
                    )))
                }
                name = val.as_str().unwrap();
            },
            _ => {
                return Err(TantivyBindingError::InvalidArgument(format!(
                    "costum tokenizer must set type"
                )))
            },
        }
        params_map = Some(m);
    }

    match name {
        "standard" => Ok(standard_builder()),
        "whitespace" => Ok(whitespace_builder()),
        "jieba" => Ok(jieba_builder()),
        "lindera" => lindera_builder(params_map),
        "custom" => custom_delimiter_builder(params_map),
        other => {
            warn!("unsupported tokenizer: {}", other);
            Err(TantivyBindingError::InvalidArgument(format!(
                "unsupported tokenizer: {}",
                other
            )))
        }
    }
}
