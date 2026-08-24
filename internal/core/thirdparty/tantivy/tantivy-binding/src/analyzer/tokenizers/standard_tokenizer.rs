use std::str::CharIndices;

use icu_properties::props::{EmojiPresentation, ExtendedPictographic, RegionalIndicator};
use icu_properties::{CodePointSetData, CodePointSetDataBorrowed};
use serde_json as json;
use tantivy::tokenizer::{Token, TokenStream, Tokenizer};
use unicode_segmentation::{UWordBoundIndices, UnicodeSegmentation};

use crate::error::{Result, TantivyBindingError};

// Match Lucene StandardTokenizer's default and absolute maximum.
const DEFAULT_MAX_TOKEN_LENGTH: usize = 255;
const MAX_TOKEN_LENGTH_LIMIT: usize = 1024 * 1024;
const EXTENDED_PICTOGRAPHIC: CodePointSetDataBorrowed<'static> =
    CodePointSetData::new::<ExtendedPictographic>();
const EMOJI_PRESENTATION: CodePointSetDataBorrowed<'static> =
    CodePointSetData::new::<EmojiPresentation>();
const REGIONAL_INDICATOR: CodePointSetDataBorrowed<'static> =
    CodePointSetData::new::<RegionalIndicator>();

/// Uses the legacy alphanumeric boundaries by default and UAX #29 when requested.
#[derive(Clone)]
pub struct StandardTokenizer {
    uax29: bool,
    max_token_length: usize,
}

impl Default for StandardTokenizer {
    fn default() -> Self {
        Self {
            uax29: false,
            max_token_length: DEFAULT_MAX_TOKEN_LENGTH,
        }
    }
}

impl StandardTokenizer {
    pub(crate) fn from_json(params: &json::Map<String, json::Value>) -> Result<Self> {
        let uax29 = match params.get("uax29") {
            None => false,
            Some(value) => value.as_bool().ok_or_else(|| {
                TantivyBindingError::InvalidArgument(
                    "standard tokenizer uax29 must be a boolean".to_string(),
                )
            })?,
        };

        if !uax29 && params.contains_key("max_token_length") {
            return Err(TantivyBindingError::InvalidArgument(
                "standard tokenizer max_token_length requires uax29 to be true".to_string(),
            ));
        }

        let max_token_length = match params.get("max_token_length") {
            None => DEFAULT_MAX_TOKEN_LENGTH,
            Some(value) => parse_max_token_length(value)?,
        };

        Ok(Self {
            uax29,
            max_token_length,
        })
    }
}

fn parse_max_token_length(value: &json::Value) -> Result<usize> {
    let value = value.as_u64().ok_or_else(|| {
        TantivyBindingError::InvalidArgument(
            "standard tokenizer max_token_length must be a positive integer".to_string(),
        )
    })?;

    if value == 0 || value > MAX_TOKEN_LENGTH_LIMIT as u64 {
        return Err(TantivyBindingError::InvalidArgument(format!(
            "standard tokenizer max_token_length must be between 1 and {}",
            MAX_TOKEN_LENGTH_LIMIT
        )));
    }

    Ok(value as usize)
}

impl Tokenizer for StandardTokenizer {
    type TokenStream<'a> = StandardTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> StandardTokenStream<'a> {
        let scanner = if self.uax29 {
            StandardScanner::Uax29(Uax29Scanner::new(text, self.max_token_length))
        } else {
            StandardScanner::Alphanumeric(AlphanumericScanner::new(text))
        };

        StandardTokenStream {
            scanner,
            token: Token::default(),
        }
    }
}

pub struct StandardTokenStream<'a> {
    scanner: StandardScanner<'a>,
    token: Token,
}

impl TokenStream for StandardTokenStream<'_> {
    fn advance(&mut self) -> bool {
        let Some(token) = self.scanner.next() else {
            return false;
        };

        self.token.text.clear();
        self.token.text.push_str(token.text);
        self.token.offset_from = token.offset_from;
        self.token.offset_to = token.offset_to;
        self.token.position = self.token.position.wrapping_add(1);
        self.token.position_length = 1;
        true
    }

    fn token(&self) -> &Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.token
    }
}

struct TokenSlice<'a> {
    text: &'a str,
    offset_from: usize,
    offset_to: usize,
}

enum StandardScanner<'a> {
    Alphanumeric(AlphanumericScanner<'a>),
    Uax29(Uax29Scanner<'a>),
}

impl<'a> Iterator for StandardScanner<'a> {
    type Item = TokenSlice<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Alphanumeric(scanner) => scanner.next(),
            Self::Uax29(scanner) => scanner.next(),
        }
    }
}

struct AlphanumericScanner<'a> {
    text: &'a str,
    chars: CharIndices<'a>,
}

impl<'a> AlphanumericScanner<'a> {
    fn new(text: &'a str) -> Self {
        Self {
            text,
            chars: text.char_indices(),
        }
    }
}

impl<'a> Iterator for AlphanumericScanner<'a> {
    type Item = TokenSlice<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((offset_from, ch)) = self.chars.next() {
            if !ch.is_alphanumeric() {
                continue;
            }

            let offset_to = self
                .chars
                .by_ref()
                .find_map(|(offset, ch)| (!ch.is_alphanumeric()).then_some(offset))
                .unwrap_or(self.text.len());
            return Some(TokenSlice {
                text: &self.text[offset_from..offset_to],
                offset_from,
                offset_to,
            });
        }

        None
    }
}

struct Uax29Scanner<'a> {
    boundaries: UWordBoundIndices<'a>,
    pending: Option<PendingSegment<'a>>,
    max_token_length: usize,
}

impl<'a> Uax29Scanner<'a> {
    fn new(text: &'a str, max_token_length: usize) -> Self {
        Self {
            boundaries: text.split_word_bound_indices(),
            pending: None,
            max_token_length,
        }
    }
}

impl<'a> Iterator for Uax29Scanner<'a> {
    type Item = TokenSlice<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(pending) = &mut self.pending {
                if let Some(token) = pending.next(self.max_token_length) {
                    return Some(token);
                }
                self.pending = None;
            }

            let (offset, segment) = self
                .boundaries
                .by_ref()
                .find(|(_, segment)| is_word_or_emoji(segment))?;
            self.pending = Some(PendingSegment::new(segment, offset));
        }
    }
}

struct PendingSegment<'a> {
    text: &'a str,
    offset: usize,
    chunk_start: usize,
}

impl<'a> PendingSegment<'a> {
    fn new(text: &'a str, offset: usize) -> Self {
        Self {
            text,
            offset,
            chunk_start: 0,
        }
    }

    fn next(&mut self, max_token_length: usize) -> Option<TokenSlice<'a>> {
        if self.chunk_start == self.text.len() {
            return None;
        }

        let remaining = &self.text[self.chunk_start..];
        let chunk_len = remaining
            .char_indices()
            .nth(max_token_length)
            .map_or(remaining.len(), |(offset, _)| offset);
        let chunk_start = self.chunk_start;
        self.chunk_start += chunk_len;

        Some(TokenSlice {
            text: &self.text[chunk_start..self.chunk_start],
            offset_from: self.offset + chunk_start,
            offset_to: self.offset + self.chunk_start,
        })
    }
}

pub(super) fn is_word_or_emoji(segment: &str) -> bool {
    segment
        .chars()
        .any(|ch| ch.is_alphanumeric() || is_emoji_character(ch))
}

pub(super) fn is_emoji(segment: &str) -> bool {
    segment.chars().any(is_emoji_character)
}

fn is_emoji_character(ch: char) -> bool {
    EXTENDED_PICTOGRAPHIC.contains(ch)
        || EMOJI_PRESENTATION.contains(ch)
        || REGIONAL_INDICATOR.contains(ch)
        || ch == '\u{20e3}'
}

#[cfg(test)]
mod tests {
    use serde_json as json;
    use tantivy::tokenizer::{SimpleTokenizer, Token, TokenStream, Tokenizer};

    use super::{StandardTokenizer, MAX_TOKEN_LENGTH_LIMIT};

    fn collect(tokenizer: &mut StandardTokenizer, text: &str) -> Vec<Token> {
        let mut stream = tokenizer.token_stream(text);
        let mut tokens = Vec::new();
        while stream.advance() {
            tokens.push(stream.token().clone());
        }
        tokens
    }

    fn uax29_tokenizer() -> StandardTokenizer {
        let params = json::json!({"uax29": true});
        StandardTokenizer::from_json(params.as_object().unwrap()).unwrap()
    }

    #[test]
    fn test_standard_tokenizer_default_matches_tantivy_simple_tokenizer() {
        let texts = [
            "",
            "The 2 QUICK Brown-Foxes dog's 32.3 中文 👩‍🔬",
            "---abc123---DEF_456---",
            "éclair Straße Ελληνικά русский 日本語",
        ];

        for text in texts {
            let standard_tokens = collect(&mut StandardTokenizer::default(), text);
            let params = json::json!({"uax29": false});
            let explicitly_disabled_tokens = collect(
                &mut StandardTokenizer::from_json(params.as_object().unwrap()).unwrap(),
                text,
            );
            let mut simple = SimpleTokenizer::default();
            let mut simple_stream = simple.token_stream(text);
            let mut simple_tokens = Vec::new();
            while simple_stream.advance() {
                simple_tokens.push(simple_stream.token().clone());
            }

            assert_eq!(standard_tokens, simple_tokens);
            assert_eq!(explicitly_disabled_tokens, simple_tokens);
        }
    }

    #[test]
    fn test_standard_tokenizer_uses_uax29_word_boundaries() {
        let text = "O'Reilly can't jump 32.3 feet from 216.239.63.104";
        let tokens = collect(&mut uax29_tokenizer(), text);

        assert_eq!(
            tokens
                .iter()
                .map(|token| token.text.as_str())
                .collect::<Vec<_>>(),
            vec![
                "O'Reilly",
                "can't",
                "jump",
                "32.3",
                "feet",
                "from",
                "216.239.63.104"
            ]
        );
        assert_eq!(
            tokens
                .iter()
                .map(|token| token.position)
                .collect::<Vec<_>>(),
            (0..tokens.len()).collect::<Vec<_>>()
        );
        for token in tokens {
            assert_eq!(&text[token.offset_from..token.offset_to], token.text);
            assert_eq!(token.position_length, 1);
        }
    }

    #[test]
    fn test_standard_tokenizer_handles_cjk_and_emoji() {
        let tokens = collect(
            &mut uax29_tokenizer(),
            "中文 仮名遣い カタカナ 한국어 👩‍🔬 🇨🇳 1️⃣",
        );

        assert_eq!(
            tokens
                .iter()
                .map(|token| token.text.as_str())
                .collect::<Vec<_>>(),
            vec![
                "中",
                "文",
                "仮",
                "名",
                "遣",
                "い",
                "カタカナ",
                "한국어",
                "👩‍🔬",
                "🇨🇳",
                "1️⃣"
            ]
        );
    }

    #[test]
    fn test_standard_tokenizer_streams_long_tokens_in_character_chunks() {
        let params = json::json!({"uax29": true, "max_token_length": 5});
        let mut tokenizer = StandardTokenizer::from_json(params.as_object().unwrap()).unwrap();
        let text = "jumped 日本語日本語";
        let tokens = collect(&mut tokenizer, text);

        assert_eq!(
            tokens
                .iter()
                .map(|token| token.text.as_str())
                .collect::<Vec<_>>(),
            vec!["jumpe", "d", "日", "本", "語", "日", "本", "語"]
        );
        for token in tokens {
            assert_eq!(&text[token.offset_from..token.offset_to], token.text);
        }
    }

    #[test]
    fn test_standard_tokenizer_rejects_invalid_max_token_length() {
        for params in [
            json::json!({"uax29": true, "max_token_length": 0}),
            json::json!({"uax29": true, "max_token_length": MAX_TOKEN_LENGTH_LIMIT + 1}),
            json::json!({"uax29": true, "max_token_length": -1}),
            json::json!({"uax29": true, "max_token_length": "255"}),
        ] {
            assert!(StandardTokenizer::from_json(params.as_object().unwrap()).is_err());
        }
    }

    #[test]
    fn test_standard_tokenizer_rejects_invalid_uax29_options() {
        for params in [
            json::json!({"uax29": "true"}),
            json::json!({"uax29": 1}),
            json::json!({"max_token_length": 5}),
        ] {
            assert!(StandardTokenizer::from_json(params.as_object().unwrap()).is_err());
        }
    }
}
