use lazy_static::lazy_static;
use log::info;
use std::collections::HashMap;
use tantivy::tokenizer::{TextAnalyzer, TokenizerManager};

lazy_static! {
    static ref DEFAULT_TOKENIZER_MANAGER: TokenizerManager = TokenizerManager::default();
}

pub(crate) fn default_tokenizer() -> TextAnalyzer {
    DEFAULT_TOKENIZER_MANAGER.get("default").unwrap()
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
