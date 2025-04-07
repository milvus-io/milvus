use jieba_rs;
use tantivy::tokenizer::{Token, TokenStream, Tokenizer};
use lazy_static::lazy_static;

lazy_static! {
    static ref JIEBA: jieba_rs::Jieba = jieba_rs::Jieba::new();
}

#[allow(dead_code)]
#[derive(Clone)]
pub enum JiebaMode {
    Exact,
    Search,
}

#[derive(Clone)]
pub struct JiebaTokenizer{
    mode: JiebaMode,
    hmm: bool,
}

pub struct JiebaTokenStream {
    tokens: Vec<Token>,
    index: usize,
}

impl TokenStream for JiebaTokenStream {
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

impl JiebaTokenizer {
    pub fn new() -> JiebaTokenizer{
        JiebaTokenizer{mode: JiebaMode::Search, hmm: true}
    }

    fn tokenize(&self, text: &str) -> Vec<Token>{
        let mut indices = text.char_indices().collect::<Vec<_>>();
        indices.push((text.len(), '\0'));
        let ori_tokens = match self.mode{
            JiebaMode::Exact => {
                JIEBA.tokenize(text, jieba_rs::TokenizeMode::Default, self.hmm)
            },
            JiebaMode::Search => {
                JIEBA.tokenize(text, jieba_rs::TokenizeMode::Search, self.hmm)
            },
        };

        let mut tokens = Vec::with_capacity(ori_tokens.len());
        for token in ori_tokens {
            tokens.push(Token {
                offset_from: indices[token.start].0,
                offset_to: indices[token.end].0,
                position: token.start,
                text: String::from(&text[(indices[token.start].0)..(indices[token.end].0)]),
                position_length: token.end - token.start,
            });
        }
        tokens
    }
}

impl Tokenizer for JiebaTokenizer {
    type TokenStream<'a> = JiebaTokenStream;

    fn token_stream(&mut self, text: &str) -> JiebaTokenStream {
        let tokens = self.tokenize(text);
        JiebaTokenStream { tokens, index: 0 }
    }
}