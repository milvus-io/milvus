use core::{option::Option::Some, result::Result::Ok};
use jieba_rs;
use lazy_static::lazy_static;
use serde_json as json;
use std::borrow::Cow;
use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

use crate::error::{Result, TantivyBindingError};

lazy_static! {
    static ref JIEBA: jieba_rs::Jieba = jieba_rs::Jieba::new();
}

#[allow(dead_code)]
#[derive(Clone)]
pub enum JiebaMode {
    Exact,
    Search,
}

#[derive(Clone)]
pub struct JiebaTokenizer<'a> {
    mode: JiebaMode,
    hmm: bool,
    tokenizer: Cow<'a, jieba_rs::Jieba>,
}

pub struct JiebaTokenStream {
    tokens: Vec<Token>,
    index: usize,
}

impl TokenStream for JiebaTokenStream {
    fn advance(&mut self) -> bool {
        if self.index < self.tokens.len() {
            self.index += 1;
            true
        } else {
            false
        }
    }

    fn token(&self) -> &Token {
        &self.tokens[self.index - 1]
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.tokens[self.index - 1]
    }
}

fn get_jieba_dict(params: &json::Map<String, json::Value>) -> Result<(Vec<String>, bool)> {
    match params.get("dict") {
        Some(value) => {
            if !value.is_array() {
                return Err(TantivyBindingError::InvalidArgument(format!(
                    "jieba tokenizer dict must be array"
                )));
            }
            let mut dict = Vec::<String>::new();
            let mut use_default = false;
            // value
            for word in value.as_array().unwrap() {
                if !word.is_string() {
                    return Err(TantivyBindingError::InvalidArgument(format!(
                        "jieba tokenizer dict item must be string"
                    )));
                }
                let text = word.as_str().unwrap().to_string();
                if text == "_default_" {
                    use_default = true;
                } else {
                    dict.push(text);
                }
            }
            Ok((dict, use_default))
        }
        _ => {
            // tokenizer.load_dict(dict)
            Ok((vec![], true))
        }
    }
}

fn get_jieba_mode(params: &json::Map<String, json::Value>) -> Result<JiebaMode> {
    match params.get("mode") {
        Some(value) => {
            if !value.is_string() {
                return Err(TantivyBindingError::InvalidArgument(format!(
                    "jieba tokenizer mode must be string"
                )));
            }

            let mode = value.as_str().unwrap();
            match mode {
                "exact" => Ok(JiebaMode::Exact),
                "search" => Ok(JiebaMode::Search),
                _ => Err(TantivyBindingError::InvalidArgument(format!(
                    "jieba tokenizer mode must be \"exact\" or \"search\""
                ))),
            }
        }
        _ => Ok(JiebaMode::Search),
    }
}

fn get_jieba_hmm(params: &json::Map<String, json::Value>) -> Result<bool> {
    match params.get("hmm") {
        Some(value) => {
            if !value.is_boolean() {
                return Err(TantivyBindingError::InvalidArgument(format!(
                    "jieba tokenizer mode must be boolean"
                )));
            }

            return Ok(value.as_bool().unwrap());
        }
        _ => Ok(true),
    }
}

impl<'a> JiebaTokenizer<'a> {
    pub fn new() -> JiebaTokenizer<'a> {
        JiebaTokenizer {
            mode: JiebaMode::Search,
            hmm: true,
            tokenizer: Cow::Borrowed(&JIEBA),
        }
    }

    pub fn from_json(params: &json::Map<String, json::Value>) -> Result<JiebaTokenizer<'a>> {
        let mut tokenizer: jieba_rs::Jieba;
        let (dict, use_default) = get_jieba_dict(params)?;
        if use_default {
            tokenizer = jieba_rs::Jieba::new()
        } else {
            tokenizer = jieba_rs::Jieba::empty()
        }

        for word in dict {
            tokenizer.add_word(word.as_str(), None, None);
        }

        let mode = get_jieba_mode(params)?;
        let hmm = get_jieba_hmm(params)?;

        Ok(JiebaTokenizer {
            mode: mode,
            hmm: hmm,
            tokenizer: Cow::Owned(tokenizer),
        })
    }

    fn tokenize(&self, text: &str) -> Vec<Token> {
        let mut indices = text.char_indices().collect::<Vec<_>>();
        indices.push((text.len(), '\0'));
        let ori_tokens = match self.mode {
            JiebaMode::Exact => {
                self.tokenizer
                    .tokenize(text, jieba_rs::TokenizeMode::Default, self.hmm)
            }
            JiebaMode::Search => {
                self.tokenizer
                    .tokenize(text, jieba_rs::TokenizeMode::Search, self.hmm)
            }
        };

        let mut tokens = Vec::with_capacity(ori_tokens.len());
        for token in ori_tokens {
            tokens.push(Token {
                offset_from: indices[token.start].0,
                offset_to: indices[token.end].0,
                position: token.start,
                text: String::from(&text[(indices[token.start].0)..(indices[token.end].0)]),
                position_length: token.end - token.start,
            });
        }
        tokens
    }
}

impl Tokenizer for JiebaTokenizer<'static> {
    type TokenStream<'a> = JiebaTokenStream;

    fn token_stream(&mut self, text: &str) -> JiebaTokenStream {
        let tokens = self.tokenize(text);
        JiebaTokenStream { tokens, index: 0 }
    }
}

#[cfg(test)]
mod tests {
    use serde_json as json;

    use crate::analyzer::tokenizers::jieba_tokenizer::JiebaTokenizer;
    use tantivy::tokenizer::TokenStream;
    use tantivy::tokenizer::Tokenizer;

    #[test]
    fn test_jieba_tokenizer() {
        let params = r#"{
            "type": "jieba"
        }"#;
        let json_param = json::from_str::<json::Map<String, json::Value>>(&params);
        assert!(json_param.is_ok());

        let tokenizer = JiebaTokenizer::from_json(&json_param.unwrap());
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());
        let mut bining = tokenizer.unwrap();
        let mut stream = bining.token_stream("结巴分词器");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            let token = stream.token();
            results.push(token.text.clone());
        }

        print!("test tokens :{:?}\n", results)
    }

    #[test]
    fn test_jieba_tokenizer_with_dict() {
        let params = r#"{
            "type": "jieba",
            "dict": ["结巴分词器"],
            "mode": "exact",
            "hmm": false
        }"#;
        let json_param = json::from_str::<json::Map<String, json::Value>>(&params);
        assert!(json_param.is_ok());

        let tokenizer = JiebaTokenizer::from_json(&json_param.unwrap());
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());
        let mut bining = tokenizer.unwrap();
        let mut stream = bining.token_stream("milvus结巴分词器中文测试");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            let token = stream.token();
            results.push(token.text.clone());
        }

        print!("test tokens :{:?}\n", results)
    }
}
