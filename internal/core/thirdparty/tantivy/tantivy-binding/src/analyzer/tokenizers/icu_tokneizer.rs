use icu_segmenter::options::{WordBreakOptions, WordType};
use icu_segmenter::WordSegmenter;
use serde_json as json;
use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

use crate::error::{Result, TantivyBindingError};

use super::standard_tokenizer::is_emoji;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IcuPositionMode {
    Char,
    Token,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IcuTokenType {
    None,
    Number,
    Letter,
    Emoji,
}

impl IcuTokenType {
    const ALL: [Self; 4] = [Self::None, Self::Number, Self::Letter, Self::Emoji];

    fn from_str(value: &str) -> Option<Self> {
        match value {
            "none" => Some(Self::None),
            "number" => Some(Self::Number),
            "letter" => Some(Self::Letter),
            "emoji" => Some(Self::Emoji),
            _ => None,
        }
    }
}

pub struct IcuTokenizer {
    segmenter: WordSegmenter,
    position_mode: IcuPositionMode,
    token_types: Vec<IcuTokenType>,
}

impl Clone for IcuTokenizer {
    fn clone(&self) -> Self {
        IcuTokenizer {
            segmenter: WordSegmenter::try_new_auto(WordBreakOptions::default()).unwrap(),
            position_mode: self.position_mode,
            token_types: self.token_types.clone(),
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
            token_types: IcuTokenType::ALL.to_vec(),
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

        let token_types = match params.get("token_types") {
            None => IcuTokenType::ALL.to_vec(),
            Some(value) => parse_token_types(value)?,
        };

        Ok(IcuTokenizer {
            segmenter: WordSegmenter::try_new_auto(WordBreakOptions::default()).unwrap(),
            position_mode,
            token_types,
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
            let token_type = classify_token(word_type, token_str);
            let keep_token = self.token_types.contains(&token_type);

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

fn parse_token_types(value: &json::Value) -> Result<Vec<IcuTokenType>> {
    let values = value.as_array().ok_or_else(|| {
        TantivyBindingError::InvalidArgument(
            "ICU tokenizer token_types must be a non-empty array of strings".to_string(),
        )
    })?;
    if values.is_empty() {
        return Err(TantivyBindingError::InvalidArgument(
            "ICU tokenizer token_types must be a non-empty array of strings".to_string(),
        ));
    }

    let mut token_types = Vec::with_capacity(values.len());
    for value in values {
        let value = value.as_str().ok_or_else(|| {
            TantivyBindingError::InvalidArgument(
                "ICU tokenizer token_types must be a non-empty array of strings".to_string(),
            )
        })?;
        let token_type = IcuTokenType::from_str(value).ok_or_else(|| {
            TantivyBindingError::InvalidArgument(format!(
                "unsupported ICU tokenizer token type: {}",
                value
            ))
        })?;
        if !token_types.contains(&token_type) {
            token_types.push(token_type);
        }
    }
    Ok(token_types)
}

fn classify_token(word_type: WordType, text: &str) -> IcuTokenType {
    if is_emoji(text) {
        return IcuTokenType::Emoji;
    }

    match word_type {
        WordType::Number => IcuTokenType::Number,
        WordType::Letter => IcuTokenType::Letter,
        // Dictionary-segmented CJK spans can retain WordType::None in ICU4X 2.0.
        // Fall back to character properties so they are not mistaken for separators.
        WordType::None if text.chars().any(char::is_alphabetic) => IcuTokenType::Letter,
        WordType::None if text.chars().any(char::is_numeric) => IcuTokenType::Number,
        WordType::None => IcuTokenType::None,
        _ => IcuTokenType::None,
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
                .map(|token| token.text.as_str())
                .collect::<Vec<_>>(),
            vec!["hello", " ", "world"]
        );
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
    fn test_icu_tokenizer_token_types_preserve_character_positions() {
        let params = json::json!({"token_types": ["letter", "number", "emoji"]});
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
    fn test_icu_tokenizer_token_types_with_token_positions_support_cjk_and_emoji() {
        let params = json::json!({
            "position_mode": "token",
            "token_types": ["letter", "number", "emoji"]
        });
        let mut tokenizer = IcuTokenizer::from_json(params.as_object().unwrap()).unwrap();
        let tokens = collect(&mut tokenizer, "龟山岛，龟山岛 👋");

        assert_eq!(
            tokens
                .iter()
                .map(|token| token.text.as_str())
                .collect::<Vec<_>>(),
            vec!["龟山岛", "龟山岛", "👋"]
        );
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
    fn test_icu_tokenizer_selects_individual_token_types() {
        let cases = [
            ("letter", vec!["hello", "中文"]),
            ("number", vec!["42"]),
            ("emoji", vec!["👋"]),
            ("none", vec![",", " ", " ", " "]),
        ];

        for (token_type, expected) in cases {
            let params = json::json!({"token_types": [token_type]});
            let mut tokenizer = IcuTokenizer::from_json(params.as_object().unwrap()).unwrap();
            let tokens = collect(&mut tokenizer, "hello, 42 中文 👋");
            assert_eq!(
                tokens
                    .iter()
                    .map(|token| token.text.as_str())
                    .collect::<Vec<_>>(),
                expected
            );
        }
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
    fn test_icu_tokenizer_rejects_invalid_token_types() {
        for params in [
            json::json!({"token_types": []}),
            json::json!({"token_types": "letter"}),
            json::json!({"token_types": ["punctuation"]}),
            json::json!({"token_types": [true]}),
        ] {
            assert!(IcuTokenizer::from_json(params.as_object().unwrap()).is_err());
        }
    }
}
