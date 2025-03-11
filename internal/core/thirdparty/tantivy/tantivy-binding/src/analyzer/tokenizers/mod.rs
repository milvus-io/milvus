mod custom_delimiter_tokenizer;
mod jieba_tokenizer;
mod lindera_tokenizer;
mod tokenizer;

use self::custom_delimiter_tokenizer::CustomDelimiterTokenizer;
use self::jieba_tokenizer::JiebaTokenizer;
use self::lindera_tokenizer::LinderaTokenizer;
pub(crate) use self::tokenizer::*;

pub(crate) mod tests {

    use tantivy::tokenizer::Token;

    pub fn assert_token(token: &Token, position: usize, text: &str, from: usize, to: usize) {
        assert_eq!(
            token.position, position,
            "expected position: {position}, but got: {token:?}",
        );
        assert_eq!(
            token.text, text,
            "expected text: {text}, but got: {token:?}",
        );
        assert_eq!(
            token.offset_from, from,
            "expected offset_from: {from}, but got: {token:?}",
        );
        assert_eq!(
            token.offset_to, to,
            "expected offset_to: {to}, but got: {token:?}",
        );
    }
}
