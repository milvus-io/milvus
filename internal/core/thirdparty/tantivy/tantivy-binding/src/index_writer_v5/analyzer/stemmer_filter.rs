use std::{borrow::Cow, mem};

use rust_stemmers::Algorithm;
use tantivy_5::tokenizer::{Token, TokenFilter, TokenStream, Tokenizer};

#[derive(Clone)]
pub(crate) struct Stemmer(Algorithm);

impl Stemmer {
    pub(crate) fn new(algorithm: Algorithm) -> Self {
        Self(algorithm)
    }
}

impl TokenFilter for Stemmer {
    type Tokenizer<T: Tokenizer> = StemmerTokenizer<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> Self::Tokenizer<T> {
        StemmerTokenizer {
            algorithm: self.0,
            inner: tokenizer,
        }
    }
}

#[derive(Clone)]
pub(crate) struct StemmerTokenizer<T> {
    algorithm: Algorithm,
    inner: T,
}

impl<T: Tokenizer> Tokenizer for StemmerTokenizer<T> {
    type TokenStream<'a> = StemmerTokenStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        StemmerTokenStream {
            tail: self.inner.token_stream(text),
            stemmer: rust_stemmers::Stemmer::create(self.algorithm),
            buffer: String::new(),
        }
    }
}

pub(crate) struct StemmerTokenStream<T> {
    tail: T,
    stemmer: rust_stemmers::Stemmer,
    buffer: String,
}

impl<T: TokenStream> TokenStream for StemmerTokenStream<T> {
    fn advance(&mut self) -> bool {
        if !self.tail.advance() {
            return false;
        }
        let token = self.tail.token_mut();
        match self.stemmer.stem(&token.text) {
            Cow::Owned(stemmed) => token.text = stemmed,
            Cow::Borrowed(stemmed) => {
                self.buffer.clear();
                self.buffer.push_str(stemmed);
                mem::swap(&mut token.text, &mut self.buffer);
            }
        }
        true
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
    use crate::index_writer_v5::analyzer::create_analyzer;

    #[test]
    fn test_legacy_english_stems() {
        for params in [
            r#"{"type":"english"}"#,
            r#"{"tokenizer":"standard","filter":["lowercase",{"type":"stemmer","language":"english"}]}"#,
        ] {
            let analyzer = create_analyzer(params).unwrap();
            for mut analyzer in [analyzer.clone(), analyzer] {
                for _ in 0..2 {
                    let mut stream =
                        analyzer.token_stream("Internal international interval running");
                    for (position, (text, start, end)) in [
                        ("intern", 0, 8),
                        ("intern", 9, 22),
                        ("interv", 23, 31),
                        ("run", 32, 39),
                    ]
                    .into_iter()
                    .enumerate()
                    {
                        assert!(stream.advance());
                        let token = stream.token();
                        assert_eq!(token.text, text);
                        assert_eq!(token.position, position);
                        assert_eq!((token.offset_from, token.offset_to), (start, end));
                        assert_eq!(token.position_length, 1);
                    }
                    assert!(!stream.advance());
                }
            }
        }
    }
}
