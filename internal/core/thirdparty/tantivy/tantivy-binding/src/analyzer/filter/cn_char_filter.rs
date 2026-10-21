use tantivy::tokenizer::{Token, TokenFilter, TokenStream, Tokenizer};

pub struct CnCharOnlyFilter;

pub struct CnCharOnlyFilterStream<'a, T> {
    regex: &'a regex::Regex,
    tail: T,
}

impl TokenFilter for CnCharOnlyFilter {
    type Tokenizer<T: Tokenizer> = CnCharOnlyFilterWrapper<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> CnCharOnlyFilterWrapper<T> {
        CnCharOnlyFilterWrapper {
            inner: tokenizer,
            regex: regex::Regex::new("\\p{Han}+").unwrap(),
        }
    }
}

#[derive(Clone)]
pub struct CnCharOnlyFilterWrapper<T> {
    inner: T,
    // Clone with the analyzer: share compiled state, but keep worker-local search caches.
    regex: regex::Regex,
}

impl<T: Tokenizer> Tokenizer for CnCharOnlyFilterWrapper<T> {
    type TokenStream<'a> = CnCharOnlyFilterStream<'a, T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        CnCharOnlyFilterStream {
            regex: &self.regex,
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
    regex: &'a regex::Regex,
    tail: T,
}

impl TokenFilter for CnAlphaNumOnlyFilter {
    type Tokenizer<T: Tokenizer> = CnAlphaNumOnlyFilterWrapper<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> CnAlphaNumOnlyFilterWrapper<T> {
        CnAlphaNumOnlyFilterWrapper {
            inner: tokenizer,
            regex: regex::Regex::new(r"[\p{Han}a-zA-Z0-9]+").unwrap(),
        }
    }
}
#[derive(Clone)]
pub struct CnAlphaNumOnlyFilterWrapper<T> {
    inner: T,
    // Clone with the analyzer: share compiled state, but keep worker-local search caches.
    regex: regex::Regex,
}

impl<T: Tokenizer> Tokenizer for CnAlphaNumOnlyFilterWrapper<T> {
    type TokenStream<'a> = CnAlphaNumOnlyFilterStream<'a, T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        CnAlphaNumOnlyFilterStream {
            regex: &self.regex,
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
