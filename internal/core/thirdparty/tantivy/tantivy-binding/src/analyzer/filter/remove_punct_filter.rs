use super::fixed_regex::FixedRegex;
use once_cell::sync::Lazy;
use regex_automata::meta::Regex;
use tantivy::tokenizer::{Token, TokenFilter, TokenStream, Tokenizer};

static PUNCT_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"[\p{Punct}\s]+").unwrap());

pub struct RemovePunctFilter;

pub struct RemovePunctFilterStream<'a, T> {
    regex: &'a mut FixedRegex,
    tail: T,
}

impl TokenFilter for RemovePunctFilter {
    type Tokenizer<T: Tokenizer> = RemovePunctFilterWrapper<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> RemovePunctFilterWrapper<T> {
        RemovePunctFilterWrapper {
            inner: tokenizer,
            regex: FixedRegex::new(&PUNCT_REGEX),
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
