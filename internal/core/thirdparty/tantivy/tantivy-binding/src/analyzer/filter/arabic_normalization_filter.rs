use tantivy::tokenizer::{Token, TokenFilter, TokenStream, Tokenizer};

/// Arabic normalization filter that performs the following transformations:
/// 1. Hamza variants with Alef seat (آ أ إ) → bare Alef (ا)
/// 2. Teh Marbuta (ة) → Heh (ه)
/// 3. Alef Maksura (ى) → Yeh (ي)
/// 4. Remove harakat (diacritics) U+064B..U+065F
/// 5. Remove tatweel (kashida) U+0640
#[derive(Clone)]
pub struct ArabicNormalizationFilter;

pub struct ArabicNormalizationFilterStream<T> {
    tail: T,
}

impl TokenFilter for ArabicNormalizationFilter {
    type Tokenizer<T: Tokenizer> = ArabicNormalizationFilterWrapper<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> ArabicNormalizationFilterWrapper<T> {
        ArabicNormalizationFilterWrapper(tokenizer)
    }
}

#[derive(Clone)]
pub struct ArabicNormalizationFilterWrapper<T>(T);

impl<T: Tokenizer> Tokenizer for ArabicNormalizationFilterWrapper<T> {
    type TokenStream<'a> = ArabicNormalizationFilterStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        ArabicNormalizationFilterStream {
            tail: self.0.token_stream(text),
        }
    }
}

const ALEF_MADDA: char = '\u{0622}'; // آ
const ALEF_HAMZA_ABOVE: char = '\u{0623}'; // أ
const ALEF_HAMZA_BELOW: char = '\u{0625}'; // إ
const ALEF: char = '\u{0627}'; // ا
const TEH_MARBUTA: char = '\u{0629}'; // ة
const HEH: char = '\u{0647}'; // ه
const ALEF_MAKSURA: char = '\u{0649}'; // ى
const YEH: char = '\u{064A}'; // ي
const TATWEEL: char = '\u{0640}'; // ـ

/// Check if a character is an Arabic haraka (diacritical mark).
/// Harakat range: U+064B (fathatan) to U+065F (waslah)
fn is_haraka(c: char) -> bool {
    c >= '\u{064B}' && c <= '\u{065F}'
}

fn normalize_arabic_char(c: char) -> Option<char> {
    match c {
        ALEF_MADDA | ALEF_HAMZA_ABOVE | ALEF_HAMZA_BELOW => Some(ALEF),
        TEH_MARBUTA => Some(HEH),
        ALEF_MAKSURA => Some(YEH),
        TATWEEL => None,           // remove
        c if is_haraka(c) => None, // remove
        _ => Some(c),
    }
}

impl<T: TokenStream> TokenStream for ArabicNormalizationFilterStream<T> {
    fn advance(&mut self) -> bool {
        if !self.tail.advance() {
            return false;
        }

        let token = self.tail.token_mut();
        if token.text.chars().any(|c| {
            matches!(
                c,
                ALEF_MADDA
                    | ALEF_HAMZA_ABOVE
                    | ALEF_HAMZA_BELOW
                    | TEH_MARBUTA
                    | ALEF_MAKSURA
                    | TATWEEL
            ) || is_haraka(c)
        }) {
            let new_text: String = token
                .text
                .chars()
                .filter_map(normalize_arabic_char)
                .collect();
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
    fn test_arabic_normalization_hamza() {
        let params = r#"{
            "tokenizer": "standard",
            "filter": ["arabic_normalization"]
        }"#;

        let tokenizer = create_analyzer(&params.to_string(), "");
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());

        let mut binding = tokenizer.unwrap();
        // أحمد (Ahmad with hamza) should normalize hamza-alef to bare alef
        let mut stream = binding.token_stream("أحمد إسلام آمن");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            let token = stream.token();
            results.push(token.text.clone());
        }

        // All alef variants should be normalized to bare alef
        assert!(
            results.iter().all(|t| !t.contains('\u{0623}')
                && !t.contains('\u{0625}')
                && !t.contains('\u{0622}')),
            "Hamza+Alef variants should be normalized: {:?}",
            results
        );
    }

    #[test]
    fn test_arabic_normalization_teh_marbuta() {
        let params = r#"{
            "tokenizer": "standard",
            "filter": ["arabic_normalization"]
        }"#;

        let tokenizer = create_analyzer(&params.to_string(), "");
        assert!(tokenizer.is_ok());

        let mut binding = tokenizer.unwrap();
        // مدرسة (school) has teh marbuta at the end
        let mut stream = binding.token_stream("مدرسة");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            results.push(stream.token().text.clone());
        }

        assert!(
            !results[0].contains('\u{0629}'),
            "Teh marbuta should be replaced with heh: {:?}",
            results
        );
        assert!(
            results[0].contains('\u{0647}'),
            "Should contain heh: {:?}",
            results
        );
    }

    #[test]
    fn test_arabic_normalization_diacritics_removal() {
        let params = r#"{
            "tokenizer": "standard",
            "filter": ["arabic_normalization"]
        }"#;

        let tokenizer = create_analyzer(&params.to_string(), "");
        assert!(tokenizer.is_ok());

        let mut binding = tokenizer.unwrap();
        // كِتَابٌ (kitaabun - "book" with full diacritics)
        let mut stream = binding.token_stream("كِتَابٌ");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            results.push(stream.token().text.clone());
        }

        // Should not contain any harakat
        assert!(
            results[0]
                .chars()
                .all(|c| !('\u{064B}'..='\u{065F}').contains(&c)),
            "Diacritics should be removed: {:?}",
            results
        );
    }

    #[test]
    fn test_arabic_normalization_tatweel_removal() {
        let params = r#"{
            "tokenizer": "standard",
            "filter": ["arabic_normalization"]
        }"#;

        let tokenizer = create_analyzer(&params.to_string(), "");
        assert!(tokenizer.is_ok());

        let mut binding = tokenizer.unwrap();
        // عـــربي (with tatweel stretching)
        let mut stream = binding.token_stream("عـــربي");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            results.push(stream.token().text.clone());
        }

        assert!(
            !results[0].contains('\u{0640}'),
            "Tatweel should be removed: {:?}",
            results
        );
        assert_eq!(
            results[0], "عربي",
            "Should be 'عربي' without tatweel: {:?}",
            results
        );
    }
}
