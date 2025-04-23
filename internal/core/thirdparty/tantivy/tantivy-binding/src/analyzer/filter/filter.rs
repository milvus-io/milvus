use regex;
use serde_json as json;
use tantivy::tokenizer::*;

use super::util::*;
use super::{RegexFilter, RemovePunctFilter};
use crate::error::{Result, TantivyBindingError};

pub(crate) enum SystemFilter {
    Invalid,
    LowerCase(LowerCaser),
    AsciiFolding(AsciiFoldingFilter),
    AlphaNumOnly(AlphaNumOnlyFilter),
    CnCharOnly(CnCharOnlyFilter),
    CnAlphaNumOnly(CnAlphaNumOnlyFilter),
    Length(RemoveLongFilter),
    RemovePunct(RemovePunctFilter),
    Stop(StopWordFilter),
    Decompounder(SplitCompoundWords),
    Stemmer(Stemmer),
    Regex(RegexFilter),
}

impl SystemFilter {
    pub(crate) fn transform(self, builder: TextAnalyzerBuilder) -> TextAnalyzerBuilder {
        match self {
            Self::LowerCase(filter) => builder.filter(filter).dynamic(),
            Self::AsciiFolding(filter) => builder.filter(filter).dynamic(),
            Self::AlphaNumOnly(filter) => builder.filter(filter).dynamic(),
            Self::CnCharOnly(filter) => builder.filter(filter).dynamic(),
            Self::CnAlphaNumOnly(filter) => builder.filter(filter).dynamic(),
            Self::Length(filter) => builder.filter(filter).dynamic(),
            Self::Stop(filter) => builder.filter(filter).dynamic(),
            Self::Decompounder(filter) => builder.filter(filter).dynamic(),
            Self::Stemmer(filter) => builder.filter(filter).dynamic(),
            Self::RemovePunct(filter) => builder.filter(filter).dynamic(),
            Self::Regex(filter) => builder.filter(filter).dynamic(),
            Self::Invalid => builder,
        }
    }
}

//  create length filter from params
// {
//     "type": "length",
//     "max": 10, // length
// }
// TODO support min length
fn get_length_filter(params: &json::Map<String, json::Value>) -> Result<SystemFilter> {
    let limit_str = params.get("max");
    if limit_str.is_none() || !limit_str.unwrap().is_u64() {
        return Err(TantivyBindingError::InternalError(
            "lenth max param was none or not uint".to_string(),
        ));
    }
    let limit = limit_str.unwrap().as_u64().unwrap() as usize;
    Ok(SystemFilter::Length(RemoveLongFilter::limit(limit + 1)))
}

fn get_stop_words_filter(params: &json::Map<String, json::Value>) -> Result<SystemFilter> {
    let value = params.get("stop_words");
    if value.is_none() {
        return Err(TantivyBindingError::InternalError(
            "stop filter stop_words can't be empty".to_string(),
        ));
    }
    let str_list = get_string_list(value.unwrap(), "stop_words filter")?;
    Ok(SystemFilter::Stop(StopWordFilter::remove(
        get_stop_words_list(str_list),
    )))
}

fn get_decompounder_filter(params: &json::Map<String, json::Value>) -> Result<SystemFilter> {
    let value = params.get("word_list");
    if value.is_none() || !value.unwrap().is_array() {
        return Err(TantivyBindingError::InternalError(
            "decompounder word list should be array".to_string(),
        ));
    }

    let stop_words = value.unwrap().as_array().unwrap();
    let mut str_list = Vec::<String>::new();
    for element in stop_words {
        match element.as_str() {
            Some(word) => str_list.push(word.to_string()),
            _ => {
                return Err(TantivyBindingError::InternalError(
                    "decompounder word list item should be string".to_string(),
                ))
            }
        }
    }

    match SplitCompoundWords::from_dictionary(str_list) {
        Ok(f) => Ok(SystemFilter::Decompounder(f)),
        Err(e) => Err(TantivyBindingError::InternalError(format!(
            "create decompounder failed: {}",
            e.to_string()
        ))),
    }
}

fn get_stemmer_filter(params: &json::Map<String, json::Value>) -> Result<SystemFilter> {
    let value = params.get("language");
    if value.is_none() || !value.unwrap().is_string() {
        return Err(TantivyBindingError::InternalError(
            "stemmer language field should be string".to_string(),
        ));
    }

    match value.unwrap().as_str().unwrap().into_language() {
        Ok(language) => Ok(SystemFilter::Stemmer(Stemmer::new(language))),
        Err(e) => Err(TantivyBindingError::InternalError(format!(
            "create stemmer failed : {}",
            e.to_string()
        ))),
    }
}

trait LanguageParser {
    fn into_language(self) -> Result<Language>;
}

impl LanguageParser for &str {
    fn into_language(self) -> Result<Language> {
        match self.to_lowercase().as_str() {
            "arabig" => Ok(Language::Arabic),
            "danish" => Ok(Language::Danish),
            "dutch" => Ok(Language::Dutch),
            "english" => Ok(Language::English),
            "finnish" => Ok(Language::Finnish),
            "french" => Ok(Language::French),
            "german" => Ok(Language::German),
            "greek" => Ok(Language::Greek),
            "hungarian" => Ok(Language::Hungarian),
            "italian" => Ok(Language::Italian),
            "norwegian" => Ok(Language::Norwegian),
            "portuguese" => Ok(Language::Portuguese),
            "romanian" => Ok(Language::Romanian),
            "russian" => Ok(Language::Russian),
            "spanish" => Ok(Language::Spanish),
            "swedish" => Ok(Language::Swedish),
            "tamil" => Ok(Language::Tamil),
            "turkish" => Ok(Language::Turkish),
            other => Err(TantivyBindingError::InternalError(format!(
                "unsupport language: {}",
                other
            ))),
        }
    }
}

impl From<&str> for SystemFilter {
    fn from(value: &str) -> Self {
        match value {
            "lowercase" => Self::LowerCase(LowerCaser),
            "asciifolding" => Self::AsciiFolding(AsciiFoldingFilter),
            "alphanumonly" => Self::AlphaNumOnly(AlphaNumOnlyFilter),
            "cncharonly" => Self::CnCharOnly(CnCharOnlyFilter),
            "cnalphanumonly" => Self::CnAlphaNumOnly(CnAlphaNumOnlyFilter),
            "removepunct" => Self::RemovePunct(RemovePunctFilter),
            _ => Self::Invalid,
        }
    }
}

impl TryFrom<&json::Map<String, json::Value>> for SystemFilter {
    type Error = TantivyBindingError;

    fn try_from(params: &json::Map<String, json::Value>) -> Result<Self> {
        match params.get(&"type".to_string()) {
            Some(value) => {
                if !value.is_string() {
                    return Err(TantivyBindingError::InternalError(
                        "filter type should be string".to_string(),
                    ));
                };

                match value.as_str().unwrap() {
                    "length" => get_length_filter(params),
                    "stop" => get_stop_words_filter(params),
                    "decompounder" => get_decompounder_filter(params),
                    "stemmer" => get_stemmer_filter(params),
                    "regex" => RegexFilter::from_json(params).map(|f| SystemFilter::Regex(f)),
                    other => Err(TantivyBindingError::InternalError(format!(
                        "unsupport filter type: {}",
                        other
                    ))),
                }
            }
            None => Err(TantivyBindingError::InternalError(
                "no type field in filter params".to_string(),
            )),
        }
    }
}

pub struct CnCharOnlyFilter;

pub struct CnCharOnlyFilterStream<T> {
    regex: regex::Regex,
    tail: T,
}

impl TokenFilter for CnCharOnlyFilter {
    type Tokenizer<T: Tokenizer> = CnCharOnlyFilterWrapper<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> CnCharOnlyFilterWrapper<T> {
        CnCharOnlyFilterWrapper(tokenizer)
    }
}

#[derive(Clone)]
pub struct CnCharOnlyFilterWrapper<T>(T);

impl<T: Tokenizer> Tokenizer for CnCharOnlyFilterWrapper<T> {
    type TokenStream<'a> = CnCharOnlyFilterStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        CnCharOnlyFilterStream {
            regex: regex::Regex::new("\\p{Han}+").unwrap(),
            tail: self.0.token_stream(text),
        }
    }
}

impl<T: TokenStream> TokenStream for CnCharOnlyFilterStream<T> {
    fn advance(&mut self) -> bool {
        while self.tail.advance() {
            if self.regex.is_match(&self.tail.token().text) {
                return true;
            }
        }

        false
    }

    fn token(&self) -> &Token {
        self.tail.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }
}

pub struct CnAlphaNumOnlyFilter;

pub struct CnAlphaNumOnlyFilterStream<T> {
    regex: regex::Regex,
    tail: T,
}

impl TokenFilter for CnAlphaNumOnlyFilter {
    type Tokenizer<T: Tokenizer> = CnAlphaNumOnlyFilterWrapper<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> CnAlphaNumOnlyFilterWrapper<T> {
        CnAlphaNumOnlyFilterWrapper(tokenizer)
    }
}
#[derive(Clone)]
pub struct CnAlphaNumOnlyFilterWrapper<T>(T);

impl<T: Tokenizer> Tokenizer for CnAlphaNumOnlyFilterWrapper<T> {
    type TokenStream<'a> = CnAlphaNumOnlyFilterStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        CnAlphaNumOnlyFilterStream {
            regex: regex::Regex::new(r"[\p{Han}a-zA-Z0-9]+").unwrap(),
            tail: self.0.token_stream(text),
        }
    }
}

impl<T: TokenStream> TokenStream for CnAlphaNumOnlyFilterStream<T> {
    fn advance(&mut self) -> bool {
        while self.tail.advance() {
            if self.regex.is_match(&self.tail.token().text) {
                return true;
            }
        }

        false
    }

    fn token(&self) -> &Token {
        self.tail.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }
}
