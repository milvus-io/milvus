use tantivy::tokenizer::{Token, TokenFilter, TokenStream, Tokenizer};

/// Converts all Unicode decimal digits (General Category Nd) to ASCII 0-9.
/// For example, Arabic-Indic digit ٣ (U+0663) becomes '3', Thai digit ๓ (U+0E53) becomes '3'.
#[derive(Clone)]
pub struct DecimalDigitFilter;

pub struct DecimalDigitFilterStream<T> {
    tail: T,
}

impl TokenFilter for DecimalDigitFilter {
    type Tokenizer<T: Tokenizer> = DecimalDigitFilterWrapper<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> DecimalDigitFilterWrapper<T> {
        DecimalDigitFilterWrapper(tokenizer)
    }
}

#[derive(Clone)]
pub struct DecimalDigitFilterWrapper<T>(T);

impl<T: Tokenizer> Tokenizer for DecimalDigitFilterWrapper<T> {
    type TokenStream<'a> = DecimalDigitFilterStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        DecimalDigitFilterStream {
            tail: self.0.token_stream(text),
        }
    }
}

/// Code points for digit '0' in each Unicode decimal digit block.
/// Unicode guarantees each script's 0-9 digits are contiguous.
const DECIMAL_DIGIT_ZEROS: &[u32] = &[
    0x0660, // Arabic-Indic
    0x06F0, // Extended Arabic-Indic
    0x0966, // Devanagari
    0x09E6, // Bengali
    0x0A66, // Gurmukhi
    0x0AE6, // Gujarati
    0x0B66, // Oriya
    0x0BE6, // Tamil
    0x0C66, // Telugu
    0x0CE6, // Kannada
    0x0D66, // Malayalam
    0x0E50, // Thai
    0x0ED0, // Lao
    0x0F20, // Tibetan
    0x1040, // Myanmar
    0x1090, // Myanmar Shan
    0x17E0, // Khmer
    0x1810, // Mongolian
    0x1946, // Limbu
    0x19D0, // New Tai Lue
    0x1A80, // Tai Tham Hora
    0x1A90, // Tai Tham Tham
    0x1B50, // Balinese
    0x1BB0, // Sundanese
    0x1C40, // Lepcha
    0x1C50, // Ol Chiki
    0xA620, // Vai
    0xA8D0, // Saurashtra
    0xA900, // Kayah Li
    0xA9D0, // Javanese
    0xA9F0, // Myanmar Tai Laing
    0xAA50, // Cham
    0xABF0, // Meetei Mayek
    0xFF10, // Fullwidth
];

/// Convert a Unicode decimal digit character to its ASCII equivalent.
/// Returns the character unchanged if it is not a recognized decimal digit.
fn normalize_decimal_digit(c: char) -> char {
    if c.is_ascii_digit() {
        return c;
    }
    let cp = c as u32;
    for &zero in DECIMAL_DIGIT_ZEROS {
        if cp >= zero && cp <= zero + 9 {
            return char::from(b'0' + (cp - zero) as u8);
        }
    }
    c
}

/// Check if text contains any non-ASCII decimal digit.
fn has_non_ascii_digit(text: &str) -> bool {
    text.chars().any(|c| {
        if c.is_ascii() {
            return false;
        }
        let cp = c as u32;
        DECIMAL_DIGIT_ZEROS
            .iter()
            .any(|&zero| cp >= zero && cp <= zero + 9)
    })
}

impl<T: TokenStream> TokenStream for DecimalDigitFilterStream<T> {
    fn advance(&mut self) -> bool {
        if !self.tail.advance() {
            return false;
        }

        let token = self.tail.token_mut();
        if has_non_ascii_digit(&token.text) {
            let new_text: String = token.text.chars().map(normalize_decimal_digit).collect();
            token.text = new_text;
        }

        true
    }

    fn token(&self) -> &Token {
        self.tail.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }
}

#[cfg(test)]
mod tests {
    use crate::analyzer::analyzer::create_analyzer;

    #[test]
    fn test_decimal_digit_filter() {
        let params = r#"{
            "tokenizer": "standard",
            "filter": ["decimaldigit"]
        }"#;

        let tokenizer = create_analyzer(&params.to_string(), "");
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());

        let mut binding = tokenizer.unwrap();
        let mut stream = binding.token_stream("price ١٢٣ baht ๔๕๖");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            let token = stream.token();
            results.push(token.text.clone());
        }

        assert!(
            results.contains(&"123".to_string()),
            "Arabic-Indic digits should be converted: {:?}",
            results
        );
        assert!(
            results.contains(&"456".to_string()),
            "Thai digits should be converted: {:?}",
            results
        );
    }

    #[test]
    fn test_decimal_digit_ascii_passthrough() {
        let params = r#"{
            "tokenizer": "standard",
            "filter": ["decimaldigit"]
        }"#;

        let tokenizer = create_analyzer(&params.to_string(), "");
        assert!(tokenizer.is_ok());

        let mut binding = tokenizer.unwrap();
        let mut stream = binding.token_stream("test 123 hello");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            let token = stream.token();
            results.push(token.text.clone());
        }

        assert!(
            results.contains(&"123".to_string()),
            "ASCII digits should pass through: {:?}",
            results
        );
    }
}
