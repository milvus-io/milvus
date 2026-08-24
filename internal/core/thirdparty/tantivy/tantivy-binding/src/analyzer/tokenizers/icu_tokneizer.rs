use icu_segmenter::options::WordBreakOptions;
use icu_segmenter::WordSegmenter;
use serde_json as json;
use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

use crate::error::{Result, TantivyBindingError};

use super::standard_tokenizer::is_word_or_emoji;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IcuPositionMode {
    Char,
    Token,
}

pub struct IcuTokenizer {
    segmenter: WordSegmenter,
    position_mode: IcuPositionMode,
    remove_punctuation: bool,
}

impl Clone for IcuTokenizer {
    fn clone(&self) -> Self {
        IcuTokenizer {
            segmenter: WordSegmenter::try_new_auto(WordBreakOptions::default()).unwrap(),
            position_mode: self.position_mode,
            remove_punctuation: self.remove_punctuation,
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
            segmenter: WordSegmenter::try_new_auto(WordBreakOptions::default()).unwrap(),
            position_mode: IcuPositionMode::Char,
            remove_punctuation: false,
        }
    }

    pub(crate) fn from_json(params: &json::Map<String, json::Value>) -> Result<IcuTokenizer> {
        let position_mode = match params.get("position_mode") {
            None => IcuPositionMode::Char,
            Some(value) => match value.as_str() {
                Some("char") => IcuPositionMode::Char,
                Some("token") => IcuPositionMode::Token,
                Some(other) => {
                    return Err(TantivyBindingError::InvalidArgument(format!(
                        "unsupported ICU tokenizer position_mode: {}",
                        other
                    )))
                }
                None => {
                    return Err(TantivyBindingError::InvalidArgument(
                        "ICU tokenizer position_mode must be a string".to_string(),
                    ))
                }
            },
        };

        let remove_punctuation = match params.get("remove_punctuation") {
            None => false,
            Some(value) => value.as_bool().ok_or_else(|| {
                TantivyBindingError::InvalidArgument(
                    "ICU tokenizer remove_punctuation must be a boolean".to_string(),
                )
            })?,
        };

        Ok(IcuTokenizer {
            segmenter: WordSegmenter::try_new_auto(WordBreakOptions::default()).unwrap(),
            position_mode,
            remove_punctuation,
        })
    }

    fn tokenize(&self, text: &str) -> Vec<Token> {
        let borrowed_segmenter = self.segmenter.as_borrowed();
        let boundaries = borrowed_segmenter.segment_str(text).iter_with_word_type();

        let mut tokens = Vec::new();
        let mut offset = 0;
        let mut position = 0;
        for (breakpoint, word_type) in boundaries {
            if breakpoint == offset {
                continue;
            }

            let token_str = &text[offset..breakpoint];
            let char_length = token_str.chars().count();
            let token_length = match self.position_mode {
                IcuPositionMode::Char => char_length,
                IcuPositionMode::Token => 1,
            };
            // Dictionary-segmented CJK spans can retain WordType::None in ICU4X 2.0.
            // Fall back to character properties so they are not mistaken for separators.
            let keep_token =
                !self.remove_punctuation || word_type.is_word_like() || is_word_or_emoji(token_str);

            if keep_token {
                tokens.push(Token {
                    text: token_str.to_string(),
                    offset_from: offset,
                    offset_to: breakpoint,
                    position,
                    position_length: token_length,
                });
            }

            offset = breakpoint;
            position += match self.position_mode {
                IcuPositionMode::Char => char_length,
                IcuPositionMode::Token if keep_token => 1,
                IcuPositionMode::Token => 0,
            };
        }

        tokens
    }
}

impl Tokenizer for IcuTokenizer {
    type TokenStream<'a> = IcuTokenStream;

    fn token_stream(&mut self, text: &str) -> IcuTokenStream {
        let tokens = self.tokenize(text);
        IcuTokenStream { tokens, index: 0 }
    }
}

#[cfg(test)]
mod tests {
    use serde_json as json;
    use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

    use super::IcuTokenizer;

    fn collect(tokenizer: &mut IcuTokenizer, text: &str) -> Vec<Token> {
        let mut stream = tokenizer.token_stream(text);
        let mut tokens = Vec::new();
        while stream.advance() {
            tokens.push(stream.token().clone());
        }
        tokens
    }

    #[test]
    fn test_icu_tokenizer() {
        let mut tokenizer = IcuTokenizer::new();
        let text =
            "tokenizer for global doc, 中文分词测试, 東京スカイツリーの最寄り駅はとうきょうスカイツリー駅です";
        let mut stream = tokenizer.token_stream(text);

        let mut results = Vec::<String>::new();
        while stream.advance() {
            let token = stream.token();
            results.push(token.text.clone());
        }

        println!("test tokens: {:?}", results);
        assert_eq!(results.len(), 24);
    }

    #[test]
    fn test_icu_tokenizer_defaults_to_character_positions() {
        let tokens = collect(&mut IcuTokenizer::new(), "hello world");

        assert_eq!(
            tokens
                .iter()
                .map(|token| token.text.as_str())
                .collect::<Vec<_>>(),
            vec!["hello", " ", "world"]
        );
        assert_eq!(
            tokens
                .iter()
                .map(|token| token.position)
                .collect::<Vec<_>>(),
            vec![0, 5, 6]
        );
        assert_eq!(
            tokens
                .iter()
                .map(|token| token.position_length)
                .collect::<Vec<_>>(),
            vec![5, 1, 5]
        );
    }

    #[test]
    fn test_icu_tokenizer_supports_token_positions() {
        let params = json::json!({"position_mode": "token"});
        let mut tokenizer = IcuTokenizer::from_json(params.as_object().unwrap()).unwrap();
        let tokens = collect(&mut tokenizer, "hello world");

        assert_eq!(
            tokens
                .iter()
                .map(|token| token.position)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert!(tokens.iter().all(|token| token.position_length == 1));
    }

    #[test]
    fn test_icu_tokenizer_removes_punctuation_with_character_positions() {
        let params = json::json!({"remove_punctuation": true});
        let mut tokenizer = IcuTokenizer::from_json(params.as_object().unwrap()).unwrap();
        let tokens = collect(&mut tokenizer, "hello, world! 👋");

        assert_eq!(
            tokens
                .iter()
                .map(|token| token.text.as_str())
                .collect::<Vec<_>>(),
            vec!["hello", "world", "👋"]
        );
        assert_eq!(
            tokens
                .iter()
                .map(|token| token.position)
                .collect::<Vec<_>>(),
            vec![0, 7, 14]
        );
    }

    #[test]
    fn test_icu_tokenizer_removes_punctuation_with_token_positions() {
        let params = json::json!({
            "position_mode": "token",
            "remove_punctuation": true
        });
        let mut tokenizer = IcuTokenizer::from_json(params.as_object().unwrap()).unwrap();
        let tokens = collect(&mut tokenizer, "龟山岛，龟山岛");

        assert_eq!(
            tokens
                .iter()
                .map(|token| token.text.as_str())
                .collect::<Vec<_>>(),
            vec!["龟山岛", "龟山岛"]
        );
        assert_eq!(
            tokens
                .iter()
                .map(|token| token.position)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert!(tokens.iter().all(|token| token.position_length == 1));
    }

    #[test]
    fn test_icu_tokenizer_rejects_invalid_position_mode() {
        for params in [
            json::json!({"position_mode": "word"}),
            json::json!({"position_mode": 1}),
        ] {
            assert!(IcuTokenizer::from_json(params.as_object().unwrap()).is_err());
        }
    }

    #[test]
    fn test_icu_tokenizer_rejects_invalid_remove_punctuation() {
        let params = json::json!({"remove_punctuation": "true"});
        assert!(IcuTokenizer::from_json(params.as_object().unwrap()).is_err());
    }
}
