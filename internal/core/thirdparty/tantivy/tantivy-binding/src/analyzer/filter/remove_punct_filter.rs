use super::fixed_regex::FixedRegex;
use crate::error::{Result, TantivyBindingError};
use once_cell::sync::Lazy;
use regex_automata::meta::Regex;
use serde_json as json;
use tantivy::tokenizer::{Token, TokenFilter, TokenStream, Tokenizer};

static PUNCT_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"[\p{Punct}\s]+").unwrap());
static STANDALONE_PUNCT_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\A[\p{Punct}\s]+\z").unwrap());

#[derive(Default)]
pub struct RemovePunctFilter {
    standalone: bool,
}

impl RemovePunctFilter {
    pub fn from_json(params: &json::Map<String, json::Value>) -> Result<Self> {
        let standalone = match params.get("mode") {
            None => false,
            Some(value) => match value.as_str() {
                Some("contains") => false,
                Some("standalone") => true,
                _ => {
                    return Err(TantivyBindingError::InvalidArgument(
                        "removepunct mode must be 'contains' or 'standalone'".to_string(),
                    ));
                }
            },
        };
        Ok(Self { standalone })
    }
}

pub struct RemovePunctFilterStream<'a, T> {
    regex: &'a mut FixedRegex,
    tail: T,
}

impl TokenFilter for RemovePunctFilter {
    type Tokenizer<T: Tokenizer> = RemovePunctFilterWrapper<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> RemovePunctFilterWrapper<T> {
        RemovePunctFilterWrapper {
            inner: tokenizer,
            regex: FixedRegex::new(if self.standalone {
                &STANDALONE_PUNCT_REGEX
            } else {
                &PUNCT_REGEX
            }),
        }
    }
}
#[derive(Clone)]
pub struct RemovePunctFilterWrapper<T> {
    inner: T,
    regex: FixedRegex,
}

impl<T: Tokenizer> Tokenizer for RemovePunctFilterWrapper<T> {
    type TokenStream<'a> = RemovePunctFilterStream<'a, T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        RemovePunctFilterStream {
            regex: &mut self.regex,
            tail: self.inner.token_stream(text),
        }
    }
}

impl<T: TokenStream> TokenStream for RemovePunctFilterStream<'_, T> {
    fn advance(&mut self) -> bool {
        while self.tail.advance() {
            if !self.regex.is_match(&self.tail.token().text) {
                return true;
            }
        }

        false
    }

    fn token(&self) -> &Token {
        self.tail.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }
}

#[cfg(test)]
mod tests {
    use crate::analyzer::analyzer::create_analyzer;
    use serde_json::json;
    use tantivy::tokenizer::{RawTokenizer, TextAnalyzer, Token};

    #[test]
    fn test_standalone_punctuation_token_boundaries() {
        let params = json!({"mode": "standalone"});
        let filter = super::RemovePunctFilter::from_json(params.as_object().unwrap()).unwrap();
        let mut analyzer = TextAnalyzer::builder(RawTokenizer::default())
            .filter(filter)
            .build();
        for (text, keep) in [
            ("", true),
            ("!，。\n\t\u{a0}\u{2003}", false),
            (" ", false),
            ("don't", true),
            ("3.5kg", true),
            ("u.s.a", true),
            ("!abc", true),
            ("abc!", true),
            ("a b", true),
            ("a\n", true),
            ("\n!a", true),
            ("😀", true),
            ("$+", true),
        ] {
            let mut stream = analyzer.token_stream(text);
            let mut tokens: Vec<Token> = Vec::new();
            while stream.advance() {
                tokens.push(stream.token().clone());
            }
            assert_eq!(tokens.len(), usize::from(keep), "{text:?}");
            if keep {
                assert_eq!(tokens[0].text, text);
                assert_eq!(tokens[0].offset_from, 0);
                assert_eq!(tokens[0].offset_to, text.len());
            }
        }
    }

    #[test]
    fn test_remove_punct_filter() {
        let params = r#"{
            "tokenizer": "jieba",
            "filter": ["removepunct"]
        }"#;

        let tokenizer = create_analyzer(&params.to_string(), "");
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());

        let mut bining = tokenizer.unwrap();
        let mut stream = bining.token_stream("中文标点，测试。");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            let token = stream.token();
            results.push(token.text.clone());
        }

        print!("test tokens :{:?}\n", results)
    }
}
