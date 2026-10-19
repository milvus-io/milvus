use std::{borrow::Cow, mem};

use rust_stemmers::Algorithm;
use tantivy::tokenizer::{Token, TokenFilter, TokenStream, Tokenizer};

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
    use std::{collections::HashSet, ffi::c_void};

    use crate::analyzer::create_analyzer;
    use crate::index_reader::IndexReaderWrapper;
    use crate::index_writer::IndexWriterWrapper;
    use crate::{util::set_bitset, TantivyIndexVersion};

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

    #[test]
    fn test_existing_english_index() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let mut writer = IndexWriterWrapper::create_text_writer(
            "text",
            path,
            "milvus_tokenizer",
            r#"{"tokenizer":"whitespace"}"#,
            1,
            50_000_000,
            false,
            false,
            TantivyIndexVersion::default_version(),
        )
        .unwrap();
        // Persist pre-upgrade English tokens, then load them with the current analyzer.
        writer.add("intern intern interv", Some(0)).unwrap();
        writer.finish().unwrap();

        let reader = IndexReaderWrapper::load(path, true, set_bitset).unwrap();
        reader.register_tokenizer(
            "milvus_tokenizer".to_string(),
            create_analyzer(r#"{"type":"english"}"#).unwrap(),
        );
        let mut result = HashSet::<u32>::new();
        reader
            .match_query("internal", &mut result as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(result, HashSet::from([0]));
        result.clear();
        reader
            .phrase_match_query(
                "internal international interval",
                0,
                &mut result as *mut _ as *mut c_void,
            )
            .unwrap();
        assert_eq!(result, HashSet::from([0]));
    }
}
