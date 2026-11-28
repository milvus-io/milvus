use icu_properties::props::{ExtendedPictographic, RegionalIndicator};
use icu_properties::CodePointSetData;
use icu_segmenter::options::WordBreakOptions;
use icu_segmenter::WordSegmenter;
use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

pub struct IcuTokenizer {
    segmenter: WordSegmenter,
}

impl Clone for IcuTokenizer {
    fn clone(&self) -> Self {
        IcuTokenizer {
            segmenter: WordSegmenter::try_new_auto(WordBreakOptions::default()).unwrap(),
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
        }
    }

    fn tokenize(&self, text: &str) -> Vec<Token> {
        let segmenter = self.segmenter.as_borrowed();
        let pictographs = CodePointSetData::new::<ExtendedPictographic>();
        let regional_indicators = CodePointSetData::new::<RegionalIndicator>();
        let mut tokens = Vec::new();
        let mut offset_from = 0;
        for offset_to in segmenter.segment_str(text).skip(1) {
            let segment = &text[offset_from..offset_to];
            if segment.chars().any(|ch| {
                ch.is_alphanumeric() || pictographs.contains(ch) || regional_indicators.contains(ch)
            }) {
                tokens.push(Token {
                    text: segment.to_string(),
                    offset_from,
                    offset_to,
                    position: tokens.len(),
                    position_length: 1,
                });
            }
            offset_from = offset_to;
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
    use std::ops::Range;

    use tantivy::tokenizer::{TokenStream, Tokenizer};

    fn assert_tokens(input: &str, expected: &[(&str, usize, usize, Range<usize>)]) {
        let mut tokenizer = super::IcuTokenizer::new();
        let mut stream = tokenizer.token_stream(input);
        for (text, position, position_length, offsets) in expected {
            assert!(stream.advance(), "missing {text:?} in {input:?}");
            let token = stream.token();
            assert_eq!(token.text, *text, "{input:?}");
            assert_eq!(token.position, *position, "{input:?}");
            assert_eq!(token.position_length, *position_length, "{input:?}");
            assert_eq!(&(token.offset_from..token.offset_to), offsets, "{input:?}");
        }
        assert!(!stream.advance(), "extra token in {input:?}");
    }

    #[test]
    fn test_icu_word_positions_and_offsets() {
        for (input, expected) in [
            (
                "home  workout",
                [("home", 0, 1, 0..4), ("workout", 1, 1, 6..13)].as_slice(),
            ),
            (
                "! café,\u{3000}3.5?",
                [("café", 0, 1, 2..7), ("3.5", 1, 1, 11..14)].as_slice(),
            ),
            (" \t!?", [].as_slice()),
            ("", [].as_slice()),
        ] {
            assert_tokens(input, expected);
        }
    }

    #[test]
    fn test_icu_emoji_positions_and_offsets() {
        assert_tokens(
            "love it 😍🔥 so good",
            &[
                ("love", 0, 1, 0..4),
                ("it", 1, 1, 5..7),
                ("😍", 2, 1, 8..12),
                ("🔥", 3, 1, 12..16),
                ("so", 4, 1, 17..19),
                ("good", 5, 1, 20..24),
            ],
        );
        assert_tokens(
            "👩🏽‍💻 👍🏽 🇹🇭",
            &[
                ("👩🏽‍💻", 0, 1, 0..15),
                ("👍🏽", 1, 1, 16..24),
                ("🇹🇭", 2, 1, 25..33),
            ],
        );
    }

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

        assert_eq!(
            results,
            [
                "tokenizer",
                "for",
                "global",
                "doc",
                "中文",
                "分词",
                "测试",
                "東京",
                "スカイツリー",
                "の",
                "最寄り駅",
                "はと",
                "う",
                "きょう",
                "スカイツリー",
                "駅",
                "です",
            ]
        );
    }
}
