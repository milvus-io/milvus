use icu_segmenter::options::WordBreakOptions;
use icu_segmenter::WordSegmenter;
use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

/// Thai tokenizer using ICU4X WordSegmenter with LSTM model.
/// Uses try_new_lstm which provides Thai word segmentation via LSTM while
/// not segmenting CJK text (no dictionary model loaded), keeping it Thai-focused.
/// Unlike the generic IcuTokenizer, this also filters out whitespace and punctuation,
/// returning only meaningful word tokens — matching ES Thai tokenizer behavior.
pub struct ThaiTokenizer {
    segmenter: WordSegmenter,
}

impl Clone for ThaiTokenizer {
    fn clone(&self) -> Self {
        ThaiTokenizer {
            segmenter: WordSegmenter::try_new_lstm(WordBreakOptions::default())
                .expect("failed to create ICU LSTM word segmenter"),
        }
    }
}

#[derive(Clone)]
pub struct ThaiTokenStream {
    tokens: Vec<Token>,
    index: usize,
}

impl TokenStream for ThaiTokenStream {
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

/// Check if a segment contains at least one word character.
/// Thai consonants/vowels (Lo) and digits (Nd) are all alphanumeric;
/// Thai combining marks (Mn) always attach to a consonant in the same segment.
/// So is_alphanumeric() alone is sufficient to identify word segments.
fn is_word_segment(s: &str) -> bool {
    s.chars().any(|c| c.is_alphanumeric())
}

impl ThaiTokenizer {
    pub fn new() -> ThaiTokenizer {
        ThaiTokenizer {
            segmenter: WordSegmenter::try_new_lstm(WordBreakOptions::default())
                .expect("failed to create ICU LSTM word segmenter"),
        }
    }

    fn tokenize(&self, text: &str) -> Vec<Token> {
        let borrowed_segmenter = self.segmenter.as_borrowed();
        let mut breakpoints: Vec<usize> = Vec::new();
        breakpoints.push(0);
        breakpoints.extend(borrowed_segmenter.segment_str(text));

        let mut tokens = Vec::with_capacity(breakpoints.len());
        let mut position = 0;

        for pair in breakpoints.windows(2) {
            let start = pair[0];
            let end = pair[1];
            let segment = &text[start..end];

            // Skip whitespace, punctuation, and empty segments
            if !is_word_segment(segment) {
                continue;
            }

            tokens.push(Token {
                text: segment.to_string(),
                offset_from: start,
                offset_to: end,
                position,
                position_length: 1,
            });
            position += 1;
        }

        tokens
    }
}

impl Tokenizer for ThaiTokenizer {
    type TokenStream<'a> = ThaiTokenStream;

    fn token_stream(&mut self, text: &str) -> ThaiTokenStream {
        let tokens = self.tokenize(text);
        ThaiTokenStream { tokens, index: 0 }
    }
}

#[cfg(test)]
mod tests {
    use tantivy::tokenizer::{TokenStream, Tokenizer};

    #[test]
    fn test_thai_tokenizer_basic() {
        let mut tokenizer = super::ThaiTokenizer::new();
        let mut stream = tokenizer.token_stream("การทดสอบภาษาไทย");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            results.push(stream.token().text.clone());
        }

        println!("Thai tokens: {:?}", results);
        assert!(!results.is_empty(), "Should produce tokens for Thai text");
        // Should not contain whitespace-only tokens
        assert!(
            results.iter().all(|t| !t.trim().is_empty()),
            "Should not contain empty tokens"
        );
    }

    #[test]
    fn test_thai_tokenizer_mixed() {
        let mut tokenizer = super::ThaiTokenizer::new();
        let mut stream = tokenizer.token_stream("สวัสดี Hello ครับ 中文测试");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            results.push(stream.token().text.clone());
        }

        println!("Mixed tokens: {:?}", results);
        assert!(
            results.iter().any(|t| t == "Hello"),
            "Should handle English text: {:?}",
            results
        );
        // Should not contain space tokens
        assert!(
            results.iter().all(|t| t.trim() == t && !t.is_empty()),
            "Should not contain whitespace tokens: {:?}",
            results
        );
    }

    #[test]
    fn test_thai_tokenizer_no_punctuation() {
        let mut tokenizer = super::ThaiTokenizer::new();
        let mut stream = tokenizer.token_stream("สวัสดี! ทดสอบ, ระบบ.");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            results.push(stream.token().text.clone());
        }

        println!("Punct test tokens: {:?}", results);
        // Should not have standalone punctuation tokens
        assert!(
            results
                .iter()
                .all(|t| t.chars().any(|c| c.is_alphanumeric())),
            "Should not contain punctuation-only tokens: {:?}",
            results
        );
    }
}
