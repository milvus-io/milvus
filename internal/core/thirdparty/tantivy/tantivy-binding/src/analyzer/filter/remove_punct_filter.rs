use tantivy::tokenizer::{Token, TokenFilter, TokenStream, Tokenizer};

pub struct RemovePunctFilter;

pub struct RemovePunctFilterStream<T> {
    regex: regex::Regex,
    tail: T,
}

impl TokenFilter for RemovePunctFilter {
    type Tokenizer<T: Tokenizer> = RemovePunctFilterWrapper<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> RemovePunctFilterWrapper<T> {
        RemovePunctFilterWrapper(tokenizer)
    }
}
#[derive(Clone)]
pub struct RemovePunctFilterWrapper<T>(T);

impl<T: Tokenizer> Tokenizer for RemovePunctFilterWrapper<T> {
    type TokenStream<'a> = RemovePunctFilterStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        RemovePunctFilterStream {
            regex: regex::Regex::new(r"[\p{Punct}\s]+").unwrap(),
            tail: self.0.token_stream(text),
        }
    }
}

impl<T: TokenStream> TokenStream for RemovePunctFilterStream<T> {
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
    #[cfg(feature = "lindera-ipadic")]
    fn test_remove_punct_filter() {
        let params = r#"{
            "tokenizer": {
                "type": "lindera",
                "dict_kind": "ipadic"
            },
            "filter": ["removepunct"]
        }"#;

        let tokenizer = create_analyzer(&params.to_string());
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());

        let mut bining = tokenizer.unwrap();
        let mut stream = bining.token_stream("ミルヴァスの日本語テスト、句読点テスト");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            let token = stream.token();
            results.push(token.text.clone());
        }

        print!("test tokens :{:?}\n", results)
    }
}
