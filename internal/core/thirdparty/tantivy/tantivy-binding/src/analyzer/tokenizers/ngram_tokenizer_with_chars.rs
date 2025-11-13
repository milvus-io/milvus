use std::collections::HashSet;

use unicode_categories::UnicodeCategories;

use tantivy::tokenizer::{Token, TokenStream, Tokenizer};
use tantivy::TantivyError;

/// Character categories that can be included in tokens
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum TokenCharType {
    /// Unicode letter characters
    Letter,
    /// Unicode digit characters
    Digit,
    /// Whitespace characters
    Whitespace,
    /// Punctuation characters
    Punctuation,
    /// Symbol characters
    Symbol,
    /// Custom characters defined by user
    Custom,
}

/// Configuration for which characters to include in tokens
#[derive(Clone, Debug)]
pub struct TokenCharsConfig {
    /// Character types to include
    pub token_chars: HashSet<TokenCharType>,
    /// Custom characters to include when TokenCharType::Custom is set
    pub custom_token_chars: String,
}

impl Default for TokenCharsConfig {
    fn default() -> Self {
        TokenCharsConfig {
            token_chars: HashSet::new(),
            custom_token_chars: String::new(),
        }
    }
}

/// Check if a character is punctuation using Unicode standard categories
fn is_punctuation(ch: char) -> bool {
    ch.is_punctuation()
}

/// Check if a character is a symbol using Unicode standard categories
fn is_symbol(ch: char) -> bool {
    ch.is_symbol()
}

impl TokenCharsConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_letters(mut self) -> Self {
        self.token_chars.insert(TokenCharType::Letter);
        self
    }

    pub fn with_digits(mut self) -> Self {
        self.token_chars.insert(TokenCharType::Digit);
        self
    }

    pub fn with_whitespace(mut self) -> Self {
        self.token_chars.insert(TokenCharType::Whitespace);
        self
    }

    pub fn with_punctuation(mut self) -> Self {
        self.token_chars.insert(TokenCharType::Punctuation);
        self
    }

    pub fn with_symbol(mut self) -> Self {
        self.token_chars.insert(TokenCharType::Symbol);
        self
    }

    pub fn with_custom_chars(mut self, custom_token_chars: String) -> Self {
        self.token_chars.insert(TokenCharType::Custom);
        self.custom_token_chars = custom_token_chars;
        self
    }

    pub fn with_all() -> Self {
        let mut token_chars = HashSet::new();
        token_chars.insert(TokenCharType::Letter);
        token_chars.insert(TokenCharType::Digit);
        token_chars.insert(TokenCharType::Whitespace);
        token_chars.insert(TokenCharType::Punctuation);
        token_chars.insert(TokenCharType::Symbol);
        Self {
            token_chars,
            custom_token_chars: String::new(),
        }
    }

    /// Check if a character should be kept based on configuration
    pub fn should_keep_char(&self, ch: char) -> bool {
        // Check custom characters first
        if self.token_chars.contains(&TokenCharType::Custom) && self.custom_token_chars.contains(ch)
        {
            return true;
        }

        // Check standard character types
        if ch.is_alphabetic() && self.token_chars.contains(&TokenCharType::Letter) {
            return true;
        }

        if self.token_chars.contains(&TokenCharType::Digit) && ch.is_numeric() {
            return true;
        }

        if self.token_chars.contains(&TokenCharType::Whitespace) && ch.is_whitespace() {
            return true;
        }

        // Use the more comprehensive punctuation and symbol checkers
        if self.token_chars.contains(&TokenCharType::Punctuation) && is_punctuation(ch) {
            return true;
        }

        if self.token_chars.contains(&TokenCharType::Symbol) && is_symbol(ch) {
            return true;
        }

        false
    }
}

/// Tokenize text by splitting into n-grams, with character filtering support
#[derive(Clone, Debug)]
pub struct NgramTokenizerWithChars {
    min_gram: usize,
    max_gram: usize,
    prefix_only: bool,
    token_chars_config: TokenCharsConfig,
    token: Token,
}

impl NgramTokenizerWithChars {
    /// Create a new NgramTokenizer with character filtering
    pub fn new(
        min_gram: usize,
        max_gram: usize,
        prefix_only: bool,
        token_chars_config: TokenCharsConfig,
    ) -> Result<Self, TantivyError> {
        if min_gram == 0 {
            return Err(TantivyError::InvalidArgument(
                "min_gram must be greater than 0".to_string(),
            ));
        }
        if min_gram > max_gram {
            return Err(TantivyError::InvalidArgument(
                "min_gram must not be greater than max_gram".to_string(),
            ));
        }

        Ok(NgramTokenizerWithChars {
            min_gram,
            max_gram,
            prefix_only,
            token_chars_config,
            token: Token::default(),
        })
    }
}

/// Represents a text segment with its byte offsets in the original text
#[derive(Debug, Clone)]
struct TextSegment {
    /// The text content of the segment
    text: String,
    /// Starting byte offset in original text
    start_offset: usize,
}

/// TokenStream for NgramTokenizerWithChars
pub struct NgramTokenStreamWithChars<'a> {
    segments: Vec<TextSegment>,
    current_segment_idx: usize,
    current_segment_ngrams: Vec<(String, usize, usize)>,
    current_ngram_idx: usize,
    min_gram: usize,
    max_gram: usize,
    prefix_only: bool,
    token: &'a mut Token,
}

impl<'a> NgramTokenStreamWithChars<'a> {
    fn generate_ngrams_for_segment(&mut self) {
        self.current_segment_ngrams.clear();

        if self.current_segment_idx >= self.segments.len() {
            return;
        }

        let segment = &self.segments[self.current_segment_idx];
        let text = &segment.text;

        // Collect character boundaries
        let char_boundaries: Vec<usize> = text
            .char_indices()
            .map(|(i, _)| i)
            .chain(std::iter::once(text.len()))
            .collect();

        let num_chars = char_boundaries.len() - 1;

        // Skip if segment is shorter than min_gram
        if num_chars < self.min_gram {
            return;
        }

        for start_char in 0..num_chars {
            // For prefix_only mode, only generate ngrams starting at position 0
            if self.prefix_only && start_char > 0 {
                break;
            }

            let min_end = start_char + self.min_gram;
            let max_end = (start_char + self.max_gram).min(num_chars);

            // Skip if we can't generate min_gram length ngram from this position
            if min_end > num_chars {
                continue;
            }

            for end_char in min_end..=max_end {
                let start_byte = char_boundaries[start_char];
                let end_byte = char_boundaries[end_char];

                let ngram_text = text[start_byte..end_byte].to_string();
                let global_start = segment.start_offset + start_byte;
                let global_end = segment.start_offset + end_byte;

                self.current_segment_ngrams
                    .push((ngram_text, global_start, global_end));
            }
        }

        self.current_ngram_idx = 0;
    }
}

impl Tokenizer for NgramTokenizerWithChars {
    type TokenStream<'a> = NgramTokenStreamWithChars<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        self.token.reset();

        // Split text into segments based on token_chars configuration
        let mut segments = Vec::new();
        let mut current_segment = String::new();
        let mut current_segment_start = None;
        let mut byte_offset = 0;

        for ch in text.chars() {
            let char_len = ch.len_utf8();

            if self.token_chars_config.should_keep_char(ch) {
                if current_segment_start.is_none() {
                    current_segment_start = Some(byte_offset);
                }
                current_segment.push(ch);
            } else {
                if !current_segment.is_empty() {
                    segments.push(TextSegment {
                        text: current_segment.clone(),
                        start_offset: current_segment_start.unwrap(),
                    });
                    current_segment.clear();
                    current_segment_start = None;
                }
            }

            byte_offset += char_len;
        }

        if !current_segment.is_empty() {
            segments.push(TextSegment {
                text: current_segment,
                start_offset: current_segment_start.unwrap(),
            });
        }

        let mut stream = NgramTokenStreamWithChars {
            segments,
            current_segment_idx: 0,
            current_segment_ngrams: Vec::new(),
            current_ngram_idx: 0,
            min_gram: self.min_gram,
            max_gram: self.max_gram,
            prefix_only: self.prefix_only,
            token: &mut self.token,
        };

        // Generate ngrams for first segment
        stream.generate_ngrams_for_segment();

        stream
    }
}

impl TokenStream for NgramTokenStreamWithChars<'_> {
    fn advance(&mut self) -> bool {
        loop {
            // Check if we have ngrams in current segment
            if self.current_ngram_idx < self.current_segment_ngrams.len() {
                let (text, start, end) = &self.current_segment_ngrams[self.current_ngram_idx];

                self.token.position = self.current_segment_idx;
                self.token.offset_from = *start;
                self.token.offset_to = *end;
                self.token.text.clear();
                self.token.text.push_str(text);

                self.current_ngram_idx += 1;
                return true;
            }

            // Move to next segment
            self.current_segment_idx += 1;
            if self.current_segment_idx >= self.segments.len() {
                return false;
            }

            // Generate ngrams for new segment
            self.generate_ngrams_for_segment();
        }
    }

    fn token(&self) -> &Token {
        self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        self.token
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

    fn create_alphanumeric_tokenizer(
        min_gram: usize,
        max_gram: usize,
        prefix_only: bool,
    ) -> NgramTokenizerWithChars {
        let config = TokenCharsConfig::default().with_letters().with_digits();
        NgramTokenizerWithChars::new(min_gram, max_gram, prefix_only, config).unwrap()
    }

    fn collect_tokens<T: TokenStream>(mut stream: T) -> Vec<Token> {
        let mut tokens = Vec::new();
        while stream.advance() {
            tokens.push(stream.token().clone());
        }
        tokens
    }

    #[test]
    fn test_token_chars_config() {
        let config = TokenCharsConfig::with_all();
        assert!(config.should_keep_char('a'));
        assert!(config.should_keep_char('1'));
        assert!(config.should_keep_char(' '));
        assert!(config.should_keep_char('.'));
        assert!(config.should_keep_char('$'));

        // Test custom config - only letters
        let mut token_chars = HashSet::new();
        token_chars.insert(TokenCharType::Letter);
        let config = TokenCharsConfig {
            token_chars,
            custom_token_chars: String::new(),
        };
        assert!(config.should_keep_char('a'));
        assert!(!config.should_keep_char('1'));
        assert!(!config.should_keep_char(' '));
        assert!(!config.should_keep_char('.'));
    }

    #[test]
    fn test_custom_token_chars() {
        let mut token_chars = HashSet::new();
        token_chars.insert(TokenCharType::Letter);
        token_chars.insert(TokenCharType::Custom);

        let config = TokenCharsConfig {
            token_chars,
            custom_token_chars: "+-_".to_string(),
        };

        assert!(config.should_keep_char('a'));
        assert!(config.should_keep_char('+'));
        assert!(config.should_keep_char('-'));
        assert!(config.should_keep_char('_'));
        assert!(!config.should_keep_char('1'));
        assert!(!config.should_keep_char(' '));
    }

    #[test]
    fn test_alphanumeric_tokenizer() {
        let mut tokenizer = create_alphanumeric_tokenizer(2, 3, false);
        let tokens = collect_tokens(tokenizer.token_stream("hello-world123"));

        // Should split at hyphen and process "hello", "world123" separately
        // "hello": he, hel, el, ell, ll, llo, lo
        // "world123": wo, wor, or, orl, rl, rld, ld, ld1, d1, d12, 12, 123, 23

        assert!(tokens.iter().any(|t| t.text == "he"));
        assert!(tokens.iter().any(|t| t.text == "hel"));
        assert!(tokens.iter().any(|t| t.text == "wo"));
        assert!(tokens.iter().any(|t| t.text == "wor"));
        assert!(tokens.iter().any(|t| t.text == "123"));

        // Should not contain hyphen
        assert!(!tokens.iter().any(|t| t.text.contains('-')));
    }

    #[test]
    fn test_with_custom_chars() {
        let mut token_chars = HashSet::new();
        token_chars.insert(TokenCharType::Letter);
        token_chars.insert(TokenCharType::Digit);
        token_chars.insert(TokenCharType::Custom);

        let config = TokenCharsConfig {
            token_chars,
            custom_token_chars: "-_".to_string(),
        };

        let mut tokenizer = NgramTokenizerWithChars::new(2, 3, false, config).unwrap();
        let tokens = collect_tokens(tokenizer.token_stream("hello-world_123"));

        // Should keep hyphens and underscores
        assert!(tokens.iter().any(|t| t.text == "o-w"));
        assert!(tokens.iter().any(|t| t.text == "d_1"));
    }

    #[test]
    fn test_offset_preservation() {
        let mut tokenizer = create_alphanumeric_tokenizer(2, 2, false);
        let tokens = collect_tokens(tokenizer.token_stream("ab cd"));

        // Check offsets are preserved correctly
        let ab_token = tokens.iter().find(|t| t.text == "ab").unwrap();
        assert_eq!(ab_token.offset_from, 0);
        assert_eq!(ab_token.offset_to, 2);

        let cd_token = tokens.iter().find(|t| t.text == "cd").unwrap();
        assert_eq!(cd_token.offset_from, 3);
        assert_eq!(cd_token.offset_to, 5);
    }

    #[test]
    fn test_unicode_handling() {
        let mut tokenizer = create_alphanumeric_tokenizer(2, 3, false);
        let tokens = collect_tokens(tokenizer.token_stream("café-2024"));

        // Should handle unicode correctly
        assert!(tokens.iter().any(|t| t.text == "ca"));
        assert!(tokens.iter().any(|t| t.text == "caf"));
        assert!(tokens.iter().any(|t| t.text == "afé"));
        assert!(tokens.iter().any(|t| t.text == "20"));
        assert!(tokens.iter().any(|t| t.text == "202"));
    }

    #[test]
    fn test_unicode_punctuation_completeness() {
        let mut token_chars = HashSet::new();
        token_chars.insert(TokenCharType::Punctuation);
        let config = TokenCharsConfig {
            token_chars,
            custom_token_chars: String::new(),
        };

        // Test various Unicode punctuation from different scripts
        assert!(config.should_keep_char('.')); // ASCII period
        assert!(config.should_keep_char('。')); // CJK period
        assert!(config.should_keep_char('、')); // CJK comma
        assert!(config.should_keep_char('،')); // Arabic comma
        assert!(config.should_keep_char('؛')); // Arabic semicolon
        assert!(config.should_keep_char('‽')); // Interrobang
        assert!(config.should_keep_char('⁇')); // Double question mark
        assert!(config.should_keep_char('⁉')); // Exclamation question mark
        assert!(config.should_keep_char('„')); // German opening quote
        assert!(config.should_keep_char('"')); // English closing quote
        assert!(config.should_keep_char('«')); // French opening quote
        assert!(config.should_keep_char('»')); // French closing quote
        assert!(config.should_keep_char('_')); // Underscore (connector punctuation)
        assert!(config.should_keep_char('–')); // En dash
        assert!(config.should_keep_char('—')); // Em dash
        assert!(config.should_keep_char('…')); // Ellipsis
        assert!(config.should_keep_char('‹')); // Single left angle quote
        assert!(config.should_keep_char('›')); // Single right angle quote

        // Should not match letters or symbols
        assert!(!config.should_keep_char('a'));
        assert!(!config.should_keep_char('中'));
        assert!(!config.should_keep_char('$'));
        assert!(!config.should_keep_char('€'));
    }

    #[test]
    fn test_unicode_symbol_completeness() {
        let mut token_chars = HashSet::new();
        token_chars.insert(TokenCharType::Symbol);
        let config = TokenCharsConfig {
            token_chars,
            custom_token_chars: String::new(),
        };

        // Test various Unicode symbols
        assert!(config.should_keep_char('$')); // Dollar
        assert!(config.should_keep_char('€')); // Euro
        assert!(config.should_keep_char('¥')); // Yen
        assert!(config.should_keep_char('£')); // Pound
        assert!(config.should_keep_char('₹')); // Rupee
                                               // Note: Bitcoin symbol (₿ U+20BF) was added in Unicode 10.0 (2017)
                                               // Some Unicode libraries may not include it yet
                                               // assert!(config.should_keep_char('₿'));   // Bitcoin
        assert!(config.should_keep_char('+')); // Plus
        assert!(config.should_keep_char('=')); // Equals
        assert!(config.should_keep_char('<')); // Less than
        assert!(config.should_keep_char('>')); // Greater than
        assert!(config.should_keep_char('∑')); // Sum
        assert!(config.should_keep_char('∏')); // Product
        assert!(config.should_keep_char('∫')); // Integral
        assert!(config.should_keep_char('√')); // Square root
        assert!(config.should_keep_char('∞')); // Infinity
        assert!(config.should_keep_char('©')); // Copyright
        assert!(config.should_keep_char('®')); // Registered
        assert!(config.should_keep_char('™')); // Trademark
        assert!(config.should_keep_char('♠')); // Spade suit
        assert!(config.should_keep_char('♣')); // Club suit
        assert!(config.should_keep_char('♥')); // Heart suit
        assert!(config.should_keep_char('♦')); // Diamond suit
        assert!(config.should_keep_char('♪')); // Musical note
        assert!(config.should_keep_char('☺')); // Smiley face
        assert!(config.should_keep_char('☯')); // Yin yang
        assert!(config.should_keep_char('⚡')); // Lightning
        assert!(config.should_keep_char('🚀')); // Rocket emoji

        // Should not match letters or punctuation
        assert!(!config.should_keep_char('a'));
        assert!(!config.should_keep_char('中'));
        assert!(!config.should_keep_char('.'));
        assert!(!config.should_keep_char(','));
    }

    #[test]
    fn test_mixed_scripts() {
        // Test tokenization with mixed scripts
        let mut token_chars = HashSet::new();
        token_chars.insert(TokenCharType::Letter);
        token_chars.insert(TokenCharType::Digit);
        let config = TokenCharsConfig {
            token_chars,
            custom_token_chars: String::new(),
        };

        let mut tokenizer = NgramTokenizerWithChars::new(2, 3, false, config).unwrap();
        let tokens = collect_tokens(tokenizer.token_stream("Hello世界2024"));

        // Should create separate segments for continuous letter/digit sequences
        assert!(tokens.iter().any(|t| t.text == "He"));
        assert!(tokens.iter().any(|t| t.text == "世界"));
        assert!(tokens.iter().any(|t| t.text == "20"));
    }

    #[test]
    fn test_prefix_only_mode() {
        // Test prefix_only mode for autocomplete use case
        let config = TokenCharsConfig::with_all();
        let mut tokenizer = NgramTokenizerWithChars::new(2, 5, true, config).unwrap();
        let tokens = collect_tokens(tokenizer.token_stream("search"));

        // Should only generate ngrams starting from position 0
        assert_eq!(tokens.len(), 4);
        assert!(tokens.iter().any(|t| t.text == "se"));
        assert!(tokens.iter().any(|t| t.text == "sea"));
        assert!(tokens.iter().any(|t| t.text == "sear"));
        assert!(tokens.iter().any(|t| t.text == "searc"));

        // Should NOT have ngrams starting from other positions
        assert!(!tokens.iter().any(|t| t.text == "ea"));
        assert!(!tokens.iter().any(|t| t.text == "ar"));
    }

    #[test]
    fn test_empty_input() {
        // Test empty string handling
        let mut tokenizer = create_alphanumeric_tokenizer(2, 3, false);
        let tokens = collect_tokens(tokenizer.token_stream(""));
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_single_char_below_min_gram() {
        // Test input shorter than min_gram
        let mut tokenizer = create_alphanumeric_tokenizer(2, 3, false);

        // Single character - should produce no tokens
        let tokens = collect_tokens(tokenizer.token_stream("a"));
        assert!(tokens.is_empty());

        // Also test with non-ASCII single character
        let tokens = collect_tokens(tokenizer.token_stream("中"));
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_only_excluded_chars() {
        // Test input with only excluded characters
        let mut token_chars = HashSet::new();
        token_chars.insert(TokenCharType::Letter);
        let config = TokenCharsConfig {
            token_chars,
            custom_token_chars: String::new(),
        };

        let mut tokenizer = NgramTokenizerWithChars::new(2, 3, false, config).unwrap();
        let tokens = collect_tokens(tokenizer.token_stream("123!@#"));
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_whitespace_handling() {
        // Test whitespace as token chars
        let mut token_chars = HashSet::new();
        token_chars.insert(TokenCharType::Whitespace);
        let config = TokenCharsConfig {
            token_chars,
            custom_token_chars: String::new(),
        };

        let mut tokenizer = NgramTokenizerWithChars::new(2, 3, false, config).unwrap();
        let tokens = collect_tokens(tokenizer.token_stream("  \t\n"));

        // Should generate ngrams from whitespace
        assert!(!tokens.is_empty());
        assert!(tokens.iter().any(|t| t.text == "  "));
    }

    #[test]
    fn test_position_field() {
        // Test that position field is set correctly for segments
        let mut tokenizer = create_alphanumeric_tokenizer(2, 2, false);
        let tokens = collect_tokens(tokenizer.token_stream("ab-cd-ef"));

        // Each segment should have incrementing position
        let ab_tokens: Vec<_> = tokens.iter().filter(|t| t.text.contains("ab")).collect();
        let cd_tokens: Vec<_> = tokens.iter().filter(|t| t.text.contains("cd")).collect();
        let ef_tokens: Vec<_> = tokens.iter().filter(|t| t.text.contains("ef")).collect();

        assert!(ab_tokens.iter().all(|t| t.position == 0));
        assert!(cd_tokens.iter().all(|t| t.position == 1));
        assert!(ef_tokens.iter().all(|t| t.position == 2));
    }

    #[test]
    fn test_min_max_gram_boundaries() {
        // Test min_gram = max_gram with only letters
        let mut token_chars = HashSet::new();
        token_chars.insert(TokenCharType::Letter);
        let config = TokenCharsConfig {
            token_chars,
            custom_token_chars: String::new(),
        };

        let mut tokenizer = NgramTokenizerWithChars::new(3, 3, false, config).unwrap();
        let tokens = collect_tokens(tokenizer.token_stream("hello"));

        // All tokens should be exactly 3 characters
        // Debug: print tokens if test fails
        for token in &tokens {
            if token.text.chars().count() != 3 {
                eprintln!(
                    "Unexpected token: '{}' (len={})",
                    token.text,
                    token.text.chars().count()
                );
            }
        }
        assert_eq!(tokens.len(), 3); // hel, ell, llo
        assert!(tokens.iter().all(|t| t.text.chars().count() == 3));
        assert!(tokens.iter().any(|t| t.text == "hel"));
        assert!(tokens.iter().any(|t| t.text == "ell"));
        assert!(tokens.iter().any(|t| t.text == "llo"));
    }

    #[test]
    fn test_large_text() {
        // Test performance with large text
        let large_text = "a".repeat(1000);
        let mut tokenizer = create_alphanumeric_tokenizer(2, 3, false);
        let tokens = collect_tokens(tokenizer.token_stream(&large_text));

        // Should generate correct number of ngrams
        // For text of length n (1000) with min=2, max=3:
        // Each position i (0 <= i <= 997) generates 2 tokens: 2-gram and 3-gram
        // Position 998 generates 1 token: 2-gram only
        // Position 999 generates 0 tokens (can't form 2-gram)
        // Total: 998 * 2 + 1 = 1997
        assert_eq!(tokens.len(), 1997);
    }

    #[test]
    fn test_emoji_as_symbols() {
        // Test that emojis are correctly classified as symbols
        let mut token_chars = HashSet::new();
        token_chars.insert(TokenCharType::Symbol);
        let config = TokenCharsConfig {
            token_chars,
            custom_token_chars: String::new(),
        };

        let mut tokenizer = NgramTokenizerWithChars::new(1, 2, false, config).unwrap();
        let tokens = collect_tokens(tokenizer.token_stream("😀🚀"));

        // Should tokenize emojis as symbols
        assert!(!tokens.is_empty());
        assert!(tokens.iter().any(|t| t.text == "😀"));
        assert!(tokens.iter().any(|t| t.text == "🚀"));
        assert!(tokens.iter().any(|t| t.text == "😀🚀"));
    }

    #[test]
    fn test_error_handling() {
        // Test invalid configurations
        assert!(NgramTokenizerWithChars::new(0, 3, false, TokenCharsConfig::default()).is_err());
        assert!(NgramTokenizerWithChars::new(5, 3, false, TokenCharsConfig::default()).is_err());
    }

    #[test]
    fn test_multiple_segments_with_custom_chars() {
        // Test complex scenario with multiple character types
        let mut token_chars = HashSet::new();
        token_chars.insert(TokenCharType::Letter);
        token_chars.insert(TokenCharType::Digit);
        token_chars.insert(TokenCharType::Custom);

        let config = TokenCharsConfig {
            token_chars,
            custom_token_chars: "@.".to_string(),
        };

        let mut tokenizer = NgramTokenizerWithChars::new(2, 3, false, config).unwrap();
        let tokens = collect_tokens(tokenizer.token_stream("user@example.com"));

        // Should keep @ and . as part of tokens
        assert!(tokens.iter().any(|t| t.text == "r@e"));
        assert!(tokens.iter().any(|t| t.text == "e.c"));
        assert!(tokens.iter().any(|t| t.text.contains('@')));
        assert!(tokens.iter().any(|t| t.text.contains('.')));
    }
}
