use icu_segmenter::WordSegmenter;
use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

pub struct IcuTokenizer {
    segmenter: WordSegmenter,
}

impl Clone for IcuTokenizer {
    fn clone(&self) -> Self {
        IcuTokenizer {
            segmenter: WordSegmenter::new_auto(Default::default()),
        }
    }
}

#[derive(Clone)]
pub struct IcuTokenStream {
    tokens: Vec<Token>,
    index: usize,
}

impl TokenStream for IcuTokenStream {
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

impl IcuTokenizer {
    pub fn new() -> IcuTokenizer {
        IcuTokenizer {
            segmenter: WordSegmenter::new_auto(Default::default()),
        }
    }

    fn tokenize(&self, text: &str) -> Vec<Token> {
        let breakpoints: Vec<usize> = self.segmenter.segment_str(text).collect();
        let mut tokens = vec![];
        let mut offset = 0;
        let mut position = 0;
        for breakpoint in breakpoints.iter() {
            if *breakpoint == offset {
                continue;
            }
            let token_str: &str = &text[offset..*breakpoint];
            let token = Token {
                text: token_str.to_string(),
                offset_from: offset,
                offset_to: *breakpoint,
                position: position,
                position_length: token_str.chars().count(),
            };

            tokens.push(token);
            offset = *breakpoint;
            position += token_str.chars().count();
        }

        tokens
    }
}

impl Tokenizer for IcuTokenizer {
    type TokenStream<'a> = IcuTokenStream;

    fn token_stream(&mut self, text: &str) -> IcuTokenStream {
        let tokens = self.tokenize(text);
        IcuTokenStream {
            tokens: tokens,
            index: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use tantivy::tokenizer::{TokenStream, Tokenizer};

    #[test]
    fn test_icu_tokenizer() {
        let mut tokenizer = super::IcuTokenizer::new();
        let text =
            "tokenizer for global doc, 中文分词测试, 東京スカイツリーの最寄り駅はとうきょうスカイツリー駅です";
        let mut stream = tokenizer.token_stream(text);

        let mut results = Vec::<String>::new();
        while stream.advance() {
            let token = stream.token();
            results.push(token.text.clone());
        }

        print!("test tokens :{:?}\n", results);
        assert_eq!(results.len(), 24);
    }
}
