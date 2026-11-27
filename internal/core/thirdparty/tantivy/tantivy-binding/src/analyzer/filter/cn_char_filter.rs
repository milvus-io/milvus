use super::fixed_regex::FixedRegex;
use once_cell::sync::Lazy;
use regex_automata::meta::Regex;
use tantivy::tokenizer::{Token, TokenFilter, TokenStream, Tokenizer};

static HAN_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"\p{Han}+").unwrap());
static HAN_ALPHANUM_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"[\p{Han}a-zA-Z0-9]+").unwrap());

pub struct CnCharOnlyFilter;

pub struct CnCharOnlyFilterStream<'a, T> {
    regex: &'a mut FixedRegex,
    tail: T,
}

impl TokenFilter for CnCharOnlyFilter {
    type Tokenizer<T: Tokenizer> = CnCharOnlyFilterWrapper<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> CnCharOnlyFilterWrapper<T> {
        CnCharOnlyFilterWrapper {
            inner: tokenizer,
            regex: FixedRegex::new(&HAN_REGEX),
        }
    }
}

#[derive(Clone)]
pub struct CnCharOnlyFilterWrapper<T> {
    inner: T,
    regex: FixedRegex,
}

impl<T: Tokenizer> Tokenizer for CnCharOnlyFilterWrapper<T> {
    type TokenStream<'a> = CnCharOnlyFilterStream<'a, T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        CnCharOnlyFilterStream {
            regex: &mut self.regex,
            tail: self.inner.token_stream(text),
        }
    }
}

impl<T: TokenStream> TokenStream for CnCharOnlyFilterStream<'_, T> {
    fn advance(&mut self) -> bool {
        while self.tail.advance() {
            if self.regex.is_match(&self.tail.token().text) {
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

pub struct CnAlphaNumOnlyFilter;

pub struct CnAlphaNumOnlyFilterStream<'a, T> {
    regex: &'a mut FixedRegex,
    tail: T,
}

impl TokenFilter for CnAlphaNumOnlyFilter {
    type Tokenizer<T: Tokenizer> = CnAlphaNumOnlyFilterWrapper<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> CnAlphaNumOnlyFilterWrapper<T> {
        CnAlphaNumOnlyFilterWrapper {
            inner: tokenizer,
            regex: FixedRegex::new(&HAN_ALPHANUM_REGEX),
        }
    }
}
#[derive(Clone)]
pub struct CnAlphaNumOnlyFilterWrapper<T> {
    inner: T,
    regex: FixedRegex,
}

impl<T: Tokenizer> Tokenizer for CnAlphaNumOnlyFilterWrapper<T> {
    type TokenStream<'a> = CnAlphaNumOnlyFilterStream<'a, T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        CnAlphaNumOnlyFilterStream {
            regex: &mut self.regex,
            tail: self.inner.token_stream(text),
        }
    }
}

impl<T: TokenStream> TokenStream for CnAlphaNumOnlyFilterStream<'_, T> {
    fn advance(&mut self) -> bool {
        while self.tail.advance() {
            if self.regex.is_match(&self.tail.token().text) {
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
