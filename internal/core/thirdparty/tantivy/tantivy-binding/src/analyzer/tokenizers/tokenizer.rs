use tantivy::tokenizer::{TextAnalyzer, TextAnalyzerBuilder};
use lindera::segmenter::Segmenter;
use tantivy::tokenizer::*;
use lindera::mode::Mode;
use serde_json as json;
use log::warn;

use crate::analyzer::tokenizers::{JiebaTokenizer, LinderaTokenizer};
use crate::error::{Result,TantivyBindingError};


pub fn standard_builder() -> TextAnalyzerBuilder {
    TextAnalyzer::builder(SimpleTokenizer::default()).dynamic()
}

pub fn whitespace_builder() -> TextAnalyzerBuilder {
    TextAnalyzer::builder(WhitespaceTokenizer::default()).dynamic()
}

pub fn jieba_builder(params: Option<&json::Map<String, json::Value>>) -> Result<TextAnalyzerBuilder> {
    if params.is_none(){
        return Ok(TextAnalyzer::builder(JiebaTokenizer::new()).dynamic());
    }
    let tokenizer = JiebaTokenizer::from_json(params.unwrap())?;
    Ok(TextAnalyzer::builder(tokenizer).dynamic())
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
        "jieba" => jieba_builder(params_map),
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
