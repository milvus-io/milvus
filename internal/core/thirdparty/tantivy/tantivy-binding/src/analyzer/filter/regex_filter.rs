use crate::error::{Result, TantivyBindingError};
use serde_json as json;
use fancy_regex as regex;
use tantivy::tokenizer::{Token, TokenFilter, TokenStream, Tokenizer};

#[derive(Clone)]
pub struct RegexFilter {
    regex: regex::Regex,
}

impl RegexFilter {
    /// Creates a `RegexFilter` given regex expression
    pub fn new(expr: &str) -> Result<RegexFilter> {
        regex::Regex::new(expr).map_or_else(
            |e| {
                Err(TantivyBindingError::InvalidArgument(format!(
                    "regex expression invalid, expr:{}, err: {}",
                    expr, e
                )))
            },
            |regex| Ok(RegexFilter { regex }),
        )
    }

    pub fn from_json(params: &json::Map<String, json::Value>) -> Result<RegexFilter> {
        params.get("expr").map_or(
            Err(TantivyBindingError::InternalError(format!(
                "must set expr for regex filter"
            ))),
            |value| {
                value.as_str().map_or(
                    Err(TantivyBindingError::InternalError(format!(
                        "expr must be string"
                    ))),
                    |expr| RegexFilter::new(expr),
                )
            },
        )
    }
}

impl TokenFilter for RegexFilter {
    type Tokenizer<T: Tokenizer> = RegexFilterWrapper<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> RegexFilterWrapper<T> {
        RegexFilterWrapper {
            regex: self.regex,
            inner: tokenizer,
        }
    }
}

#[derive(Clone)]
pub struct RegexFilterWrapper<T> {
    regex: regex::Regex,
    inner: T,
}

impl<T: Tokenizer> Tokenizer for RegexFilterWrapper<T> {
    type TokenStream<'a> = RegexFilterStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        RegexFilterStream {
            regex: self.regex.clone(),
            tail: self.inner.token_stream(text),
        }
    }
}

pub struct RegexFilterStream<T> {
    regex: regex::Regex,
    tail: T,
}

impl<T> RegexFilterStream<T> {
    fn predicate(&self, token: &Token) -> bool {
        self.regex.is_match(&token.text).map_or(true, |b|b)
    }
}

impl<T: TokenStream> TokenStream for RegexFilterStream<T> {
    fn advance(&mut self) -> bool {
        while self.tail.advance() {
            if self.predicate(self.tail.token()) {
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

    #[test]
    fn test_regex_filter() {
        let params = r#"{
            "tokenizer": "standard",
            "filter": [{
                "type": "regex",
                "expr": "^(?!test)"
            }]
        }"#;

        let tokenizer = create_analyzer(&params.to_string());
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());

        let mut bining = tokenizer.unwrap();
        let mut stream = bining.token_stream("test milvus");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            let token = stream.token();
            results.push(token.text.clone());
        }

        print!("test tokens :{:?}\n", results)
    }
}