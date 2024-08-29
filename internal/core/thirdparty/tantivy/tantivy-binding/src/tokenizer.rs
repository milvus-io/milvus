use std::collections::HashMap;

use log::info;
use tantivy::tokenizer::{TextAnalyzer, TokenizerManager};

pub(crate) fn default_tokenizer() -> TextAnalyzer {
    TokenizerManager::default().get("default").unwrap()
}

fn jieba_tokenizer() -> TextAnalyzer {
    tantivy_jieba::JiebaTokenizer {}.into()
}

pub(crate) fn create_tokenizer(params: &HashMap<String, String>) -> Option<TextAnalyzer> {
    match params.get("tokenizer") {
        Some(tokenizer_name) => match tokenizer_name.as_str() {
            "default" => {
                return Some(default_tokenizer());
            }
            "jieba" => return Some(jieba_tokenizer()),
            _ => {
                return None;
            }
        },
        None => {
            info!("no tokenizer is specific, use default tokenizer");
            return Some(default_tokenizer());
        }
    }
}
