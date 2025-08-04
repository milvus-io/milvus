use std::collections::HashSet;
use std::str::CharIndices;
use std::sync::Arc;

use crate::error::{Result, TantivyBindingError};
use serde_json as json;
use tantivy::tokenizer::{Token, TokenStream, Tokenizer};
use unicode_general_category::{get_general_category, GeneralCategory};

#[derive(Clone, Hash, PartialEq, Eq)]
enum SystemCharGroup {
    Whitespace,
    Letter,
    Digit,
    Punctuation,
    Symbol,
    ASCIIWhitespace,
    ASCIILetter,
    ASCIIDigit,
    ASCIIPunctuation,
}

impl TryFrom<&str> for SystemCharGroup {
    type Error = TantivyBindingError;
    fn try_from(s: &str) -> std::result::Result<Self, Self::Error> {
        match s {
            "whitespace" => Ok(Self::Whitespace),
            "letter" => Ok(Self::Letter),
            "digit" => Ok(Self::Digit),
            "punctuation" => Ok(Self::Punctuation),
            "symbol" => Ok(Self::Symbol),
            "ascii_whitespace" => Ok(Self::ASCIIWhitespace),
            "ascii_letter" => Ok(Self::ASCIILetter),
            "ascii_digit" => Ok(Self::ASCIIDigit),
            "ascii_punctuation" => Ok(Self::ASCIIPunctuation),
            _ => Err(TantivyBindingError::InvalidArgument(format!(
                "{} not a char or name of build-in char group",
                s
            ))),
        }
    }
}

impl SystemCharGroup {
    fn in_group(&self, c: &char) -> bool {
        match self {
            Self::Whitespace => c.is_whitespace(),
            Self::Letter => c.is_alphabetic(),
            Self::Digit => c.is_digit(10),
            Self::Punctuation => is_unicode_punctuation(c),
            Self::Symbol => is_unicode_symbol(c),
            Self::ASCIIWhitespace => c.is_ascii_whitespace(),
            Self::ASCIILetter => c.is_ascii_alphabetic(),
            Self::ASCIIDigit => c.is_ascii_digit(),
            Self::ASCIIPunctuation => c.is_ascii_punctuation(),
        }
    }
}

/// Tokenize the text by splitting on char in char group.
#[derive(Clone, Default)]
pub struct CharGroupTokenizer {
    delimiters: Arc<HashSet<char>>,
    groups: Arc<HashSet<SystemCharGroup>>,
}

impl CharGroupTokenizer {
    pub(crate) fn from_json(params: &json::Map<String, json::Value>) -> Result<CharGroupTokenizer> {
        params.get("delimiters").map_or(
            Err(TantivyBindingError::InvalidArgument(
                "char group tokenizer delimiters can't be empty".to_string(),
            )),
            |v| {
                v.as_array().map_or(
                    Err(TantivyBindingError::InvalidArgument(
                        "char group tokenizer delimiters must be a vector".to_string(),
                    )),
                    |v| {
                        let mut delimiters = HashSet::new();
                        let mut groups = HashSet::new();
                        for s in v {
                            s.as_str().map_or(
                                Err(TantivyBindingError::InvalidArgument(
                                    "char group tokenizer delimiter in delimiters must be string"
                                        .to_string(),
                                )),
                                |v| {
                                    if v.len() == 1 {
                                        delimiters.insert(v.chars().next().unwrap());
                                        return Ok(());
                                    }
                                    groups.insert(SystemCharGroup::try_from(v)?);
                                    Ok(())
                                },
                            )?;
                        }
                        Ok(CharGroupTokenizer {
                            delimiters: Arc::new(delimiters),
                            groups: Arc::new(groups),
                        })
                    },
                )
            },
        )
    }
}

pub struct CharGroupTokenStream<'a> {
    text: &'a str,
    delimiters: Arc<HashSet<char>>,
    groups: Arc<HashSet<SystemCharGroup>>,
    chars: CharIndices<'a>,
    token: Token,
}

impl Tokenizer for CharGroupTokenizer {
    type TokenStream<'a> = CharGroupTokenStream<'a>;
    fn token_stream<'a>(&'a mut self, text: &'a str) -> CharGroupTokenStream<'a> {
        CharGroupTokenStream {
            text,
            delimiters: self.delimiters.clone(),
            groups: self.groups.clone(),
            chars: text.char_indices(),
            token: Token::default(),
        }
    }
}

impl<'a> CharGroupTokenStream<'a> {
    fn is_delimiter(
        c: &char,
        groups: &HashSet<SystemCharGroup>,
        delimiters: &HashSet<char>,
    ) -> bool {
        groups.iter().any(|g| g.in_group(c)) || delimiters.contains(c)
    }

    // search for the end of the current token.
    fn search_token_end(&mut self) -> usize {
        (&mut self.chars)
            .filter(|(_, c)| Self::is_delimiter(c, &self.groups, &self.delimiters))
            .map(|(offset, _)| offset)
            .next()
            .unwrap_or(self.text.len())
    }
}

impl<'a> TokenStream for CharGroupTokenStream<'a> {
    fn advance(&mut self) -> bool {
        self.token.text.clear();
        self.token.position = self.token.position.wrapping_add(1);
        while let Some((offset_from, c)) = self.chars.next() {
            if !Self::is_delimiter(&c, &self.groups, &self.delimiters) {
                let offset_to = self.search_token_end();
                self.token.offset_from = offset_from;
                self.token.offset_to = offset_to;
                self.token.text.push_str(&self.text[offset_from..offset_to]);
                return true;
            }
        }
        false
    }

    fn token(&self) -> &Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.token
    }
}

fn is_unicode_punctuation(c: &char) -> bool {
    matches!(
        get_general_category(c.clone()),
        GeneralCategory::ConnectorPunctuation
            | GeneralCategory::DashPunctuation
            | GeneralCategory::OpenPunctuation
            | GeneralCategory::ClosePunctuation
            | GeneralCategory::InitialPunctuation
            | GeneralCategory::FinalPunctuation
            | GeneralCategory::OtherPunctuation
    )
}

fn is_unicode_symbol(c: &char) -> bool {
    matches!(
        get_general_category(c.clone()),
        GeneralCategory::MathSymbol
            | GeneralCategory::CurrencySymbol
            | GeneralCategory::ModifierSymbol
            | GeneralCategory::OtherSymbol
    )
}

#[cfg(test)]
mod tests {
    use tantivy::tokenizer::{TokenStream, Tokenizer};

    use super::CharGroupTokenizer;
    use serde_json as json;

    #[test]
    fn test_char_group_tokenizer() {
        let params = r#"{
            "type": "chargroup",
            "delimiters": ["o", "punctuation","digit"]
        }"#;
        let json_param = json::from_str::<json::Map<String, json::Value>>(&params);
        assert!(json_param.is_ok());

        let tokenizer = CharGroupTokenizer::from_json(&json_param.unwrap());
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());
        let mut bining = tokenizer.unwrap();
        let mut stream = bining.token_stream("testotest,test1test");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            let token = stream.token();
            results.push(token.text.clone());
        }

        print!("test tokens :{:?}\n", results);
        assert_eq!(results, vec!["test", "test", "test", "test"])
    }
}
