use tantivy::tokenizer::{BoxTokenStream, TextAnalyzer, Token, TokenStream, Tokenizer};

use crate::analyzer::char_filter::{BoxCharFilter, FilteredText};

pub struct CharFilterTokenizer {
    char_filters: Vec<BoxCharFilter>,
    inner: TextAnalyzer,
    filtered: FilteredText,
}

impl Clone for CharFilterTokenizer {
    fn clone(&self) -> Self {
        CharFilterTokenizer {
            char_filters: self.char_filters.clone(),
            inner: self.inner.clone(),
            filtered: FilteredText::default(),
        }
    }
}

impl CharFilterTokenizer {
    pub(crate) fn new(char_filters: Vec<BoxCharFilter>, inner: TextAnalyzer) -> Self {
        CharFilterTokenizer {
            char_filters,
            inner,
            filtered: FilteredText::default(),
        }
    }

    fn apply_char_filters(&self, text: &str) -> FilteredText {
        self.char_filters
            .iter()
            .fold(FilteredText::new(text), |input, filter| filter.apply(input))
    }
}

impl Tokenizer for CharFilterTokenizer {
    type TokenStream<'a> = CharFilterTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        let filtered = self.apply_char_filters(text);
        self.filtered = filtered;

        let filtered = &self.filtered;
        let tail = self.inner.token_stream(&filtered.text);
        CharFilterTokenStream { tail, filtered }
    }
}

pub struct CharFilterTokenStream<'a> {
    tail: BoxTokenStream<'a>,
    filtered: &'a FilteredText,
}

impl TokenStream for CharFilterTokenStream<'_> {
    fn advance(&mut self) -> bool {
        if !self.tail.advance() {
            return false;
        }

        let token = self.tail.token_mut();
        token.offset_from = self.filtered.correct_offset(token.offset_from);
        token.offset_to = self.filtered.correct_offset(token.offset_to);
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
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use serde_json as json;
    use tantivy::tokenizer::{TextAnalyzer, Token, TokenStream, Tokenizer};

    use super::CharFilterTokenizer;
    use crate::analyzer::char_filter::{build_char_filters, BoxCharFilter};

    #[derive(Clone)]
    struct CountingTokenizer {
        advance_count: Arc<AtomicUsize>,
    }

    struct CountingTokenStream<'a> {
        text: &'a str,
        advance_count: Arc<AtomicUsize>,
        emitted: bool,
        token: Token,
    }

    impl Tokenizer for CountingTokenizer {
        type TokenStream<'a> = CountingTokenStream<'a>;

        fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
            CountingTokenStream {
                text,
                advance_count: self.advance_count.clone(),
                emitted: false,
                token: Token::default(),
            }
        }
    }

    impl TokenStream for CountingTokenStream<'_> {
        fn advance(&mut self) -> bool {
            if self.emitted {
                return false;
            }
            self.emitted = true;
            self.advance_count.fetch_add(1, Ordering::SeqCst);
            self.token.offset_from = 0;
            self.token.offset_to = self.text.len();
            self.token.position = 0;
            self.token.text = self.text.to_string();
            true
        }

        fn token(&self) -> &Token {
            &self.token
        }

        fn token_mut(&mut self) -> &mut Token {
            &mut self.token
        }
    }

    #[test]
    fn test_char_filter_tokenizer_corrects_offsets() {
        let params = json::json!([
            {
                "type": "mapping",
                "mappings": ["&=>and"]
            }
        ]);
        let filters: Vec<BoxCharFilter> = build_char_filters(&params).unwrap();
        let mut tokenizer = CharFilterTokenizer::new(
            filters,
            TextAnalyzer::builder(tantivy::tokenizer::SimpleTokenizer::default())
                .dynamic()
                .build(),
        );
        let mut stream = tokenizer.token_stream("a&b");

        assert!(stream.advance());
        let token = stream.token();
        assert_eq!(token.text, "aandb");
        assert_eq!(token.offset_from, 0);
        assert_eq!(token.offset_to, 3);
    }

    #[test]
    fn test_char_filter_tokenizer_keeps_inner_stream_lazy() {
        let params = json::json!([
            {
                "type": "mapping",
                "mappings": ["&=>and"]
            }
        ]);
        let filters: Vec<BoxCharFilter> = build_char_filters(&params).unwrap();
        let advance_count = Arc::new(AtomicUsize::new(0));
        let inner = CountingTokenizer {
            advance_count: advance_count.clone(),
        };
        let mut tokenizer =
            CharFilterTokenizer::new(filters, TextAnalyzer::builder(inner).dynamic().build());

        let mut stream = tokenizer.token_stream("a&b");
        assert_eq!(advance_count.load(Ordering::SeqCst), 0);
        assert!(stream.advance());
        assert_eq!(advance_count.load(Ordering::SeqCst), 1);
        assert_eq!(stream.token().text, "aandb");
    }
}
