use icu_properties::props::{Emoji, ExtendedPictographic};
use icu_properties::{CodePointSetData, CodePointSetDataBorrowed};
use icu_segmenter::options::{WordBreakOptions, WordType};
use icu_segmenter::WordSegmenter;
use serde_json as json;
use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

use crate::error::{Result, TantivyBindingError};

use super::position_mode::PositionMode;

const EMOJI: CodePointSetDataBorrowed<'static> = CodePointSetData::new::<Emoji>();
const EXTENDED_PICTOGRAPHIC: CodePointSetDataBorrowed<'static> =
    CodePointSetData::new::<ExtendedPictographic>();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IcuTokenType {
    None,
    Number,
    Letter,
    Emoji,
}

pub struct IcuTokenizer {
    segmenter: WordSegmenter,
    position_mode: PositionMode,
    skip_none: bool,
    keep_types: Option<Vec<IcuTokenType>>,
}

impl Clone for IcuTokenizer {
    fn clone(&self) -> Self {
        IcuTokenizer {
            segmenter: WordSegmenter::try_new_auto(WordBreakOptions::default()).unwrap(),
            position_mode: self.position_mode,
            skip_none: self.skip_none,
            keep_types: self.keep_types.clone(),
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
            position_mode: PositionMode::Char,
            skip_none: false,
            keep_types: None,
        }
    }

    pub(crate) fn from_json(params: &json::Map<String, json::Value>) -> Result<IcuTokenizer> {
        let position_mode = PositionMode::from_json(params.get("position_mode"))?;
        let skip_none = match params.get("skip_none") {
            None => false,
            Some(value) => value.as_bool().ok_or_else(|| {
                TantivyBindingError::InvalidArgument(
                    "ICU tokenizer skip_none must be a boolean".to_string(),
                )
            })?,
        };

        let keep_types = match params.get("keep_types") {
            None => None,
            Some(value) => Some(parse_keep_types(value)?),
        };

        Ok(IcuTokenizer {
            segmenter: WordSegmenter::try_new_auto(WordBreakOptions::default()).unwrap(),
            position_mode,
            skip_none,
            keep_types,
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
            let keep = if self.skip_none || self.keep_types.is_some() {
                let token_type = classify_token(word_type, token_str);
                // Emoji must be classified before skipping the remaining non-word segments.
                if self.skip_none && token_type == IcuTokenType::None {
                    offset = breakpoint;
                    continue;
                }
                self.keep_types
                    .as_ref()
                    .map_or(true, |types| types.contains(&token_type))
            } else {
                true
            };
            let position_length = match self.position_mode {
                PositionMode::Char => token_str.chars().count(),
                PositionMode::Token => 1,
            };
            let token_position = position;
            // Type filtering preserves the positions occupied by excluded tokens.
            position += position_length;

            if keep {
                tokens.push(Token {
                    text: token_str.to_string(),
                    offset_from: offset,
                    offset_to: breakpoint,
                    position: token_position,
                    position_length,
                });
            }

            offset = breakpoint;
        }

        tokens
    }
}

fn parse_keep_types(value: &json::Value) -> Result<Vec<IcuTokenType>> {
    let values = value.as_array().ok_or_else(|| {
        TantivyBindingError::InvalidArgument(
            "ICU tokenizer keep_types must be a non-empty array of strings".to_string(),
        )
    })?;
    if values.is_empty() {
        return Err(TantivyBindingError::InvalidArgument(
            "ICU tokenizer keep_types must be a non-empty array of strings".to_string(),
        ));
    }

    let mut keep_types = Vec::with_capacity(values.len());
    for value in values {
        let value = value.as_str().ok_or_else(|| {
            TantivyBindingError::InvalidArgument(
                "ICU tokenizer keep_types must be a non-empty array of strings".to_string(),
            )
        })?;
        let token_type = match value {
            "none" => IcuTokenType::None,
            "number" => IcuTokenType::Number,
            "letter" => IcuTokenType::Letter,
            "emoji" => IcuTokenType::Emoji,
            _ => {
                return Err(TantivyBindingError::InvalidArgument(format!(
                    "unsupported ICU tokenizer token type: {}",
                    value
                )))
            }
        };
        if !keep_types.contains(&token_type) {
            keep_types.push(token_type);
        }
    }
    Ok(keep_types)
}

fn classify_token(word_type: WordType, text: &str) -> IcuTokenType {
    // Emoji selectors/keycaps can follow characters that ICU tags as numbers or letters.
    if is_emoji_segment(text) {
        return IcuTokenType::Emoji;
    }
    match word_type {
        WordType::Number => IcuTokenType::Number,
        WordType::Letter => IcuTokenType::Letter,
        _ => IcuTokenType::None,
    }
}

fn is_emoji_segment(text: &str) -> bool {
    let mut chars = text.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !EMOJI.contains(first) && !EXTENDED_PICTOGRAPHIC.contains(first) {
        return false;
    }
    // Like Lucene's BreakIteratorWrapper, ambiguous bases need an emoji selector or keycap.
    match first {
        '0'..='9' | '#' | '*' | '©' | '®' | '™' | '〰' | '〽' => {
            matches!(chars.next(), Some('\u{fe0f}' | '\u{20e3}'))
        }
        _ => true,
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
    fn test_icu_omitted_keep_types_matches_all_types() {
        let text = "é, 中文 35KG don't 👩‍🔬 1️⃣!";
        for mode in ["char", "token"] {
            for skip_none in [false, true] {
                let mut params = json::json!({
                    "position_mode": mode,
                    "skip_none": skip_none
                });
                let mut unfiltered = IcuTokenizer::from_json(params.as_object().unwrap()).unwrap();
                params["keep_types"] = json::json!(["none", "letter", "number", "emoji"]);
                let mut all_types = IcuTokenizer::from_json(params.as_object().unwrap()).unwrap();
                let expected = collect(&mut all_types, text);
                assert_eq!(collect(&mut unfiltered, text), expected);
                assert_eq!(collect(&mut unfiltered.clone(), text), expected);
                if mode == "char" && !skip_none {
                    assert_eq!(collect(&mut IcuTokenizer::new(), text), expected);
                }
            }
        }
    }

    #[test]
    fn test_icu_tokenizer_keep_types_preserve_character_positions() {
        let params = json::json!({"keep_types": ["letter", "number"]});
        let mut tokenizer = IcuTokenizer::from_json(params.as_object().unwrap()).unwrap();
        let tokens = collect(&mut tokenizer, "hello, world! 👋");

        assert_eq!(
            tokens
                .iter()
                .map(|token| token.text.as_str())
                .collect::<Vec<_>>(),
            vec!["hello", "world"]
        );
        assert_eq!(
            tokens
                .iter()
                .map(|token| token.position)
                .collect::<Vec<_>>(),
            vec![0, 7]
        );
    }

    #[test]
    fn test_icu_keep_types_preserves_non_emoji_upstream_tags() {
        use icu_segmenter::options::WordType;

        let text = "龟山岛，中文 35KG don't";
        let tokenizer = IcuTokenizer::new();
        let segmenter = tokenizer.segmenter.as_borrowed();
        let native: Vec<_> = segmenter
            .segment_str(text)
            .iter_with_word_type()
            .scan(0, |start, (end, tag)| {
                let span = *start..end;
                *start = end;
                Some((span, tag))
            })
            .filter(|(span, _)| !span.is_empty())
            .collect();
        for (name, tag) in [
            ("none", WordType::None),
            ("letter", WordType::Letter),
            ("number", WordType::Number),
        ] {
            for skip_none in [false, true] {
                let params = json::json!({
                    "position_mode": "token", "skip_none": skip_none, "keep_types": [name]
                });
                let mut tokenizer = IcuTokenizer::from_json(params.as_object().unwrap()).unwrap();
                let tokens = collect(&mut tokenizer, text);
                // ICU is the oracle: do not infer types from the segment's characters.
                let expected: Vec<_> = native
                    .iter()
                    .filter(|(_, t)| *t == tag && !(skip_none && *t == WordType::None))
                    .map(|(span, _)| span.clone())
                    .collect();
                assert_eq!(
                    tokens
                        .iter()
                        .map(|t| t.offset_from..t.offset_to)
                        .collect::<Vec<_>>(),
                    expected,
                    "{name}, skip_none={skip_none}"
                );
            }
        }
    }

    #[test]
    fn test_icu_tokenizer_selects_individual_keep_types() {
        let cases = [
            ("letter", vec!["hello", "world"]),
            ("number", vec!["42"]),
            ("none", vec![",", " ", " ", " "]),
            ("emoji", vec!["👋"]),
        ];

        for (token_type, expected) in cases {
            let params = json::json!({"keep_types": [token_type]});
            let mut tokenizer = IcuTokenizer::from_json(params.as_object().unwrap()).unwrap();
            let tokens = collect(&mut tokenizer, "hello, 42 world 👋");
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
    fn test_icu_skip_none_and_keep_types_positions() {
        for (mode, skip_none, last_position) in [
            ("char", false, 7),
            ("char", true, 4),
            ("token", false, 5),
            ("token", true, 2),
        ] {
            let params = json::json!({
                "position_mode": mode,
                "skip_none": skip_none,
                "keep_types": ["letter"]
            });
            let tokenizer = IcuTokenizer::from_json(params.as_object().unwrap()).unwrap();
            let mut tokenizer = tokenizer.clone();
            // The filtered number still occupies positions; offsets remain in UTF-8 bytes.
            for _ in 0..2 {
                let tokens = collect(&mut tokenizer, "é, 123 ô!");
                assert_eq!(
                    tokens
                        .iter()
                        .map(|t| (
                            t.text.as_str(),
                            t.position,
                            t.position_length,
                            t.offset_from,
                            t.offset_to
                        ))
                        .collect::<Vec<_>>(),
                    vec![("é", 0, 1, 0, 2), ("ô", last_position, 1, 8, 10)],
                    "{mode}, skip_none={skip_none}"
                );
            }
        }
    }

    #[test]
    fn test_icu_skip_none_preserves_word_segments() {
        for (mode, positions, lengths) in [
            ("char", vec![0, 5, 9], vec![5, 4, 2]),
            ("token", vec![0, 1, 2], vec![1, 1, 1]),
        ] {
            let params = json::json!({"position_mode": mode, "skip_none": true});
            let mut tokenizer = IcuTokenizer::from_json(params.as_object().unwrap()).unwrap();
            let tokens = collect(&mut tokenizer, "don't, 35KG! 42");
            assert_eq!(
                tokens.iter().map(|t| t.text.as_str()).collect::<Vec<_>>(),
                vec!["don't", "35KG", "42"]
            );
            assert_eq!(
                tokens.iter().map(|t| t.position).collect::<Vec<_>>(),
                positions
            );
            assert_eq!(
                tokens.iter().map(|t| t.position_length).collect::<Vec<_>>(),
                lengths
            );
            assert!(collect(&mut tokenizer, " ,!").is_empty());
        }
    }

    #[test]
    fn test_icu_emoji_classification_overrides_upstream_tags() {
        use super::{classify_token, IcuTokenType};
        use icu_segmenter::options::WordType;

        for text in ["🔥", "👩🏽‍💻", "👍🏽", "🇹🇭", "1️⃣", "#️⃣", "*\u{20e3}", "©️"]
        {
            for tag in [WordType::None, WordType::Number, WordType::Letter] {
                assert_eq!(
                    classify_token(tag, text),
                    IcuTokenType::Emoji,
                    "{text}: {tag:?}"
                );
            }
        }
        for text in [
            "",
            " ",
            ",",
            "1",
            "#",
            "*",
            "©",
            "®",
            "™",
            "〰",
            "〽",
            "©\u{fe0e}",
            "中文",
        ] {
            assert_eq!(
                classify_token(WordType::None, text),
                IcuTokenType::None,
                "{text}"
            );
            assert_eq!(classify_token(WordType::Number, text), IcuTokenType::Number);
            assert_eq!(classify_token(WordType::Letter, text), IcuTokenType::Letter);
        }
    }

    #[test]
    fn test_icu_emoji_positions_and_filter_gaps() {
        let emojis = ["🔥", "👩🏽‍💻", "👍🏽", "🇹🇭", "1️⃣", "#️⃣", "©️"];
        for emoji in emojis {
            let text = format!("a, {emoji} b!");
            let defaults = collect(&mut IcuTokenizer::new(), &text);
            assert_eq!(
                defaults.iter().map(|t| t.text.as_str()).collect::<String>(),
                text
            );
            assert!(defaults
                .iter()
                .all(|t| t.position == text[..t.offset_from].chars().count()));
            for mode in ["char", "token"] {
                let params = json::json!({"position_mode": mode, "skip_none": true});
                let mut tokenizer = IcuTokenizer::from_json(params.as_object().unwrap()).unwrap();
                let tokens = collect(&mut tokenizer, &text);
                let emoji_length = if mode == "char" {
                    emoji.chars().count()
                } else {
                    1
                };
                assert_eq!(
                    tokens
                        .iter()
                        .map(|t| (
                            t.text.as_str(),
                            t.position,
                            t.position_length,
                            t.offset_from,
                            t.offset_to
                        ))
                        .collect::<Vec<_>>(),
                    vec![
                        ("a", 0, 1, 0, 1),
                        (emoji, 1, emoji_length, 3, 3 + emoji.len()),
                        ("b", 1 + emoji_length, 1, 4 + emoji.len(), 5 + emoji.len()),
                    ]
                );
                for (keep, expected) in [
                    ("emoji", vec![(emoji, 1)]),
                    ("letter", vec![("a", 0), ("b", 1 + emoji_length)]),
                    ("none", vec![]),
                ] {
                    let mut params = params.clone();
                    params["keep_types"] = json::json!([keep]);
                    let mut tokenizer =
                        IcuTokenizer::from_json(params.as_object().unwrap()).unwrap();
                    let tokens = collect(&mut tokenizer, &text);
                    assert_eq!(
                        tokens
                            .iter()
                            .map(|t| (t.text.as_str(), t.position))
                            .collect::<Vec<_>>(),
                        expected
                    );
                }
            }
        }
    }

    #[test]
    fn test_icu_tokenizer_rejects_invalid_skip_none() {
        for value in [json::Value::Null, json::json!("true"), json::json!(1)] {
            let params = json::json!({"skip_none": value});
            assert!(IcuTokenizer::from_json(params.as_object().unwrap()).is_err());
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
    fn test_icu_tokenizer_rejects_invalid_keep_types() {
        for params in [
            json::json!({"keep_types": null}),
            json::json!({"keep_types": []}),
            json::json!({"keep_types": "letter"}),
            json::json!({"keep_types": ["punctuation"]}),
            json::json!({"keep_types": [true]}),
        ] {
            assert!(IcuTokenizer::from_json(params.as_object().unwrap()).is_err());
        }
    }
}
