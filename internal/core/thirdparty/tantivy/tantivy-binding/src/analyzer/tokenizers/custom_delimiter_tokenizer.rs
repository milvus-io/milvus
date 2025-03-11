use std::str::CharIndices;
use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

/// Tokenize the text by splitting on whitespaces.
#[derive(Clone)]
pub struct CustomDelimiterTokenizer {
    token: Token,
    delimiter: char,
}

impl CustomDelimiterTokenizer {
    /// Create a new `CustomDelimiterTokenizer`.
    pub fn new(delimiter: char) -> Self {
        Self {
            token: Token::default(),
            delimiter,
        }
    }
}

impl Default for CustomDelimiterTokenizer {
    fn default() -> Self {
        // default is whitespace delimiter
        Self {
            token: Token::default(),
            delimiter: ' ',
        }
    }
}

pub struct CustomDelimiterTokenStream<'a> {
    text: &'a str,
    chars: CharIndices<'a>,
    token: &'a mut Token,
    delimiter: char,
}

impl CustomDelimiterTokenStream<'_> {
    fn search_token_end(&mut self) -> usize {
        (&mut self.chars)
            .filter(|(_, c)| *c == self.delimiter)
            .map(|(offset, _)| offset)
            .next()
            .unwrap_or(self.text.len())
    }
}

impl TokenStream for CustomDelimiterTokenStream<'_> {
    fn advance(&mut self) -> bool {
        self.token.text.clear();
        self.token.position = self.token.position.wrapping_add(1);
        while let Some((offset_from, c)) = self.chars.next() {
            if c != self.delimiter {
                let offset_to = self.search_token_end();
                self.token.offset_from = offset_from;
                self.token.offset_to = offset_to;
                self.token.text.push_str(&self.text[offset_from..offset_to]);
                return true;
            }
        }
        false
    }

    fn token(&self) -> &Token {
        self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        self.token
    }
}

impl Tokenizer for CustomDelimiterTokenizer {
    type TokenStream<'a> = CustomDelimiterTokenStream<'a>;
    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        self.token.reset();
        CustomDelimiterTokenStream {
            text,
            chars: text.char_indices(),
            token: &mut self.token,
            delimiter: self.delimiter,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::analyzer::tokenizers::custom_delimiter_tokenizer::CustomDelimiterTokenizer;
    use tantivy::tokenizer::TextAnalyzer;
    use tantivy::tokenizer::Token;

    use crate::analyzer::tokenizers::tests::assert_token;

    #[test]
    fn test_custom_delimiter_tokenizer() {
        // let tokens = token_stream
        let tokens = token_stream_helper(
            "Hello,World,Split,Me",
            Some(','),
        );
        assert_eq!(tokens.len(), 4);
        assert_token(&tokens[0], 0, "Hello", 0, 5);
        assert_token(&tokens[1], 1, "World", 6, 11);
        assert_token(&tokens[2], 2, "Split", 12, 17);
        assert_token(&tokens[3], 3, "Me", 18, 20);
    }

    #[test]
    fn test_default_custom_delimiter_tokenizer(){
        let tokens = token_stream_helper("This is default", None);
        assert_eq!(tokens.len(), 3);
        assert_token(&tokens[0], 0, "This", 0, 4);
        assert_token(&tokens[1], 1, "is", 5, 7);
        assert_token(&tokens[2], 2, "default", 8, 15);
    }

    fn token_stream_helper(text: &str, delimiter: Option<char>) -> Vec<Token> {
        let tokenizer = match delimiter {
            Some(c) => CustomDelimiterTokenizer::new(c),
            None => CustomDelimiterTokenizer::default(),
        };
        let mut analyzer = TextAnalyzer::from(tokenizer);
        let mut token_stream = analyzer.token_stream(text);
        let mut tokens: Vec<Token> = vec![];
        let mut add_token = |token: &Token| {
            tokens.push(token.clone());
        };
        token_stream.process(&mut add_token);
        tokens
    }

}
