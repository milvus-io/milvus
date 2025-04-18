use core::result::Result::Err;

use lindera::dictionary::{load_dictionary_from_kind, DictionaryKind};
use lindera::mode::Mode;
use lindera::segmenter::Segmenter;
use lindera::token::Token as LToken;
use lindera::tokenizer::Tokenizer as LTokenizer;
use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

use crate::error::{Result, TantivyBindingError};
use serde_json as json;

pub struct LinderaTokenStream<'a> {
    pub tokens: Vec<LToken<'a>>,
    pub token: &'a mut Token,
}

impl<'a> TokenStream for LinderaTokenStream<'a> {
    fn advance(&mut self) -> bool {
        if self.tokens.is_empty() {
            return false;
        }
        let token = self.tokens.remove(0);
        self.token.text = token.text.to_string();
        self.token.offset_from = token.byte_start;
        self.token.offset_to = token.byte_end;
        self.token.position = token.position;
        self.token.position_length = token.position_length;

        true
    }

    fn token(&self) -> &Token {
        self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        self.token
    }
}

#[derive(Clone)]
pub struct LinderaTokenizer {
    tokenizer: LTokenizer,
    token: Token,
}

impl LinderaTokenizer {
    /// Create a new `LinderaTokenizer`.
    /// This function will create a new `LinderaTokenizer` with settings from the YAML file specified in the `LINDERA_CONFIG_PATH` environment variable.
    pub fn from_json(params: &json::Map<String, json::Value>) -> Result<LinderaTokenizer> {
        let kind = fetch_lindera_kind(params)?;
        let dictionary = load_dictionary_from_kind(kind);
        if dictionary.is_err() {
            return Err(TantivyBindingError::InvalidArgument(format!(
                "lindera tokenizer with invalid dict_kind"
            )));
        }
        let segmenter = Segmenter::new(Mode::Normal, dictionary.unwrap(), None);
        Ok(LinderaTokenizer::from_segmenter(segmenter))
    }

    /// Create a new `LinderaTokenizer`.
    /// This function will create a new `LinderaTokenizer` with the specified `lindera::segmenter::Segmenter`.
    pub fn from_segmenter(segmenter: lindera::segmenter::Segmenter) -> LinderaTokenizer {
        LinderaTokenizer {
            tokenizer: LTokenizer::new(segmenter),
            token: Default::default(),
        }
    }
}

impl Tokenizer for LinderaTokenizer {
    type TokenStream<'a> = LinderaTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> LinderaTokenStream<'a> {
        self.token.reset();
        LinderaTokenStream {
            tokens: self.tokenizer.tokenize(text).unwrap(),
            token: &mut self.token,
        }
    }
}

trait DictionaryKindParser {
    fn into_dict_kind(self) -> Result<DictionaryKind>;
}

impl DictionaryKindParser for &str {
    fn into_dict_kind(self) -> Result<DictionaryKind> {
        match self {
            "ipadic" => Ok(DictionaryKind::IPADIC),
            "ipadic-neologd" => Ok(DictionaryKind::IPADICNEologd),
            "unidic" => Ok(DictionaryKind::UniDic),
            "ko-dic" => Ok(DictionaryKind::KoDic),
            "cc-cedict" => Ok(DictionaryKind::CcCedict),
            other => Err(TantivyBindingError::InvalidArgument(format!(
                "unsupported lindera dict type: {}",
                other
            ))),
        }
    }
}

fn fetch_lindera_kind(params: &json::Map<String, json::Value>) -> Result<DictionaryKind> {
    match params.get("dict_kind") {
        Some(val) => {
            if !val.is_string() {
                return Err(TantivyBindingError::InvalidArgument(format!(
                    "lindera tokenizer dict kind should be string"
                )));
            }
            val.as_str().unwrap().into_dict_kind()
        }
        _ => {
            return Err(TantivyBindingError::InvalidArgument(format!(
                "lindera tokenizer dict_kind must be set"
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::LinderaTokenizer;
    use serde_json as json;

    #[test]
    #[cfg(feature = "lindera-ipadic")]
    fn test_lindera_tokenizer() {
        let params = r#"{
            "type": "lindera",
            "dict_kind": "ipadic"
        }"#;
        let json_param = json::from_str::<json::Map<String, json::Value>>(&params);
        assert!(json_param.is_ok());

        let tokenizer = LinderaTokenizer::from_json(&json_param.unwrap());
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());
    }

    #[test]
    #[cfg(feature = "lindera-cc-cedict")]
    fn test_lindera_tokenizer_cc() {
        let params = r#"{
            "type": "lindera",
            "dict_kind": "cc-cedict"
        }"#;
        let json_param = json::from_str::<json::Map<String, json::Value>>(&params);
        assert!(json_param.is_ok());

        let tokenizer = LinderaTokenizer::from_json(&json_param.unwrap());
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());
    }
}
