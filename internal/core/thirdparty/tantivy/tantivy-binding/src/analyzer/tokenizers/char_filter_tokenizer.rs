use tantivy::tokenizer::{TextAnalyzer, Token, TokenStream, Tokenizer};

use crate::analyzer::char_filter::{BoxCharFilter, FilteredText};

#[derive(Clone)]
pub struct CharFilterTokenizer {
    char_filters: Vec<BoxCharFilter>,
    inner: TextAnalyzer,
}

impl CharFilterTokenizer {
    pub(crate) fn new(char_filters: Vec<BoxCharFilter>, inner: TextAnalyzer) -> Self {
        CharFilterTokenizer {
            char_filters,
            inner,
        }
    }

    fn apply_char_filters(&self, text: &str) -> FilteredText {
        self.char_filters
            .iter()
            .fold(FilteredText::new(text), |input, filter| filter.apply(input))
    }

    fn tokenize(&mut self, text: &str) -> Vec<Token> {
        let filtered = self.apply_char_filters(text);
        let mut stream = self.inner.token_stream(&filtered.text);
        let mut tokens = Vec::new();

        while stream.advance() {
            let mut token = stream.token().clone();
            token.offset_from = filtered.correct_offset(token.offset_from);
            token.offset_to = filtered.correct_offset(token.offset_to);
            tokens.push(token);
        }

        tokens
    }
}

impl Tokenizer for CharFilterTokenizer {
    type TokenStream<'a> = CharFilterTokenStream;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        CharFilterTokenStream {
            tokens: self.tokenize(text),
            index: 0,
        }
    }
}

pub struct CharFilterTokenStream {
    tokens: Vec<Token>,
    index: usize,
}

impl TokenStream for CharFilterTokenStream {
    fn advance(&mut self) -> bool {
        if self.index < self.tokens.len() {
            self.index += 1;
            true
        } else {
            false
        }
    }

    fn token(&self) -> &Token {
        &self.tokens[self.index - 1]
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.tokens[self.index - 1]
    }
}

#[cfg(test)]
mod tests {
    use serde_json as json;
    use tantivy::tokenizer::{TextAnalyzer, TokenStream, Tokenizer};

    use super::CharFilterTokenizer;
    use crate::analyzer::char_filter::{build_char_filters, BoxCharFilter};

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
}
