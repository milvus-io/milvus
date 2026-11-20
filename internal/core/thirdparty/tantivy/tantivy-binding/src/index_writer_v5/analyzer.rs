use crate::analyzer::create_analyzer as create_new_analyzer;
use crate::error::Result;
use tantivy::tokenizer::{BoxTokenStream as NewBoxTokenStream, TextAnalyzer as NewAnalyzer};
use tantivy_5::tokenizer::{TextAnalyzer, Token, TokenStream, Tokenizer};

struct AdapterTokenStream<'a> {
    token_stream: NewBoxTokenStream<'a>,
    token: Token,
}

impl<'a> AdapterTokenStream<'a> {
    fn new(token_stream: NewBoxTokenStream<'a>) -> Self {
        Self {
            token_stream,
            token: Token::default(),
        }
    }
}

impl<'a> TokenStream for AdapterTokenStream<'a> {
    fn advance(&mut self) -> bool {
        if self.token_stream.advance() {
            let new_token = self.token_stream.token();
            self.token = Token {
                offset_from: new_token.offset_from,
                offset_to: new_token.offset_to,
                position: new_token.position,
                text: new_token.text.clone(),
                position_length: new_token.position_length,
            };
            true
        } else {
            false
        }
    }
    fn token(&self) -> &Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.token
    }
}

#[derive(Clone)]
struct AdapterAnalyzer {
    tokenizer: NewAnalyzer,
}

impl AdapterAnalyzer {
    fn new(tokenizer: NewAnalyzer) -> Self {
        Self { tokenizer }
    }
}

impl Tokenizer for AdapterAnalyzer {
    type TokenStream<'a> = AdapterTokenStream<'a>;
    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        AdapterTokenStream::new(self.tokenizer.token_stream(text))
    }
}

pub fn create_analyzer(params: &str) -> Result<TextAnalyzer> {
    let tokenizer = create_new_analyzer(params, "")?;
    Ok(TextAnalyzer::builder(AdapterAnalyzer::new(tokenizer)).build())
}

#[cfg(test)]
mod tests {
    use super::create_analyzer;

    #[test]
    fn test_removepunct_standalone_adapter() {
        let params = r#"{"tokenizer":"icu","filter":[{"type":"removepunct","mode":"standalone"}]}"#;
        let mut legacy = create_analyzer(params).unwrap();
        let mut current = crate::analyzer::create_analyzer(params, "").unwrap();
        let input = "don't buy 3.5kg from u.s.a, see modash.io or a@b.com";
        for _ in 0..2 {
            let mut old_stream = legacy.token_stream(input);
            let mut new_stream = current.token_stream(input);
            while new_stream.advance() {
                assert!(old_stream.advance());
                let old = old_stream.token();
                let new = new_stream.token();
                assert_eq!(old.text, new.text);
                assert_eq!(old.offset_from, new.offset_from);
                assert_eq!(old.offset_to, new.offset_to);
                assert_eq!(old.position, new.position);
                assert_eq!(old.position_length, new.position_length);
            }
            assert!(!old_stream.advance());
        }
    }

    #[test]
    fn test_standard_analyzer() {
        let params = r#"{
            "type": "standard",
            "stop_words": ["_english_"]
        }"#;

        let tokenizer = create_analyzer(&params.to_string());
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());
    }

    #[test]
    fn test_chinese_analyzer() {
        let params = r#"{
            "type": "chinese"
        }"#;

        let tokenizer = create_analyzer(&params.to_string());
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());
        let mut bining = tokenizer.unwrap();
        let mut stream = bining.token_stream("系统安全;,'';lxyz密码");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            let token = stream.token();
            results.push(token.text.clone());
        }

        print!("test tokens :{:?}\n", results)
    }
}
