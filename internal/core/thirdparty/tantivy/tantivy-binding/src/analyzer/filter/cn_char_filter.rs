use tantivy::tokenizer::{Token, TokenFilter, TokenStream, Tokenizer};

pub struct CnCharOnlyFilter;

pub struct CnCharOnlyFilterStream<T> {
    regex: regex::Regex,
    tail: T,
}

impl TokenFilter for CnCharOnlyFilter {
    type Tokenizer<T: Tokenizer> = CnCharOnlyFilterWrapper<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> CnCharOnlyFilterWrapper<T> {
        CnCharOnlyFilterWrapper(tokenizer)
    }
}

#[derive(Clone)]
pub struct CnCharOnlyFilterWrapper<T>(T);

impl<T: Tokenizer> Tokenizer for CnCharOnlyFilterWrapper<T> {
    type TokenStream<'a> = CnCharOnlyFilterStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        CnCharOnlyFilterStream {
            regex: regex::Regex::new("\\p{Han}+").unwrap(),
            tail: self.0.token_stream(text),
        }
    }
}

impl<T: TokenStream> TokenStream for CnCharOnlyFilterStream<T> {
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

pub struct CnAlphaNumOnlyFilterStream<T> {
    regex: regex::Regex,
    tail: T,
}

impl TokenFilter for CnAlphaNumOnlyFilter {
    type Tokenizer<T: Tokenizer> = CnAlphaNumOnlyFilterWrapper<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> CnAlphaNumOnlyFilterWrapper<T> {
        CnAlphaNumOnlyFilterWrapper(tokenizer)
    }
}
#[derive(Clone)]
pub struct CnAlphaNumOnlyFilterWrapper<T>(T);

impl<T: Tokenizer> Tokenizer for CnAlphaNumOnlyFilterWrapper<T> {
    type TokenStream<'a> = CnAlphaNumOnlyFilterStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        CnAlphaNumOnlyFilterStream {
            regex: regex::Regex::new(r"[\p{Han}a-zA-Z0-9]+").unwrap(),
            tail: self.0.token_stream(text),
        }
    }
}

impl<T: TokenStream> TokenStream for CnAlphaNumOnlyFilterStream<T> {
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
