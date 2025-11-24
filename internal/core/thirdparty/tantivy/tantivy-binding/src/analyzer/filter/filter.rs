use serde_json as json;
use tantivy::tokenizer::*;

use super::util::*;
use super::{
    CnAlphaNumOnlyFilter, CnCharOnlyFilter, PinyinFilter, RegexFilter, RemovePunctFilter,
    SynonymFilter,
};
use crate::analyzer::options::FileResourcePathHelper;
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
    Synonym(SynonymFilter),
    Pinyin(PinyinFilter),
}

pub(crate) trait FilterBuilder {
    fn from_json(
        params: &json::Map<String, json::Value>,
        helper: &mut FileResourcePathHelper,
    ) -> Result<Self>
    where
        Self: Sized;
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
            Self::Synonym(filter) => builder.filter(filter).dynamic(),
            Self::Pinyin(filter) => builder.filter(filter).dynamic(),
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
        if let Some(word) = element.as_str() {
            str_list.push(word.to_string());
        } else {
            return Err(TantivyBindingError::InternalError(
                "decompounder word list item should be string".to_string(),
            ));
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

// fetch build-in filter from string
impl From<&str> for SystemFilter {
    fn from(value: &str) -> Self {
        match value {
            "lowercase" => Self::LowerCase(LowerCaser),
            "asciifolding" => Self::AsciiFolding(AsciiFoldingFilter),
            "alphanumonly" => Self::AlphaNumOnly(AlphaNumOnlyFilter),
            "cncharonly" => Self::CnCharOnly(CnCharOnlyFilter),
            "cnalphanumonly" => Self::CnAlphaNumOnly(CnAlphaNumOnlyFilter),
            "removepunct" => Self::RemovePunct(RemovePunctFilter),
            "pinyin" => Self::Pinyin(PinyinFilter::default()),
            _ => Self::Invalid,
        }
    }
}

pub fn create_filter(
    params: &json::Map<String, json::Value>,
    helper: &mut FileResourcePathHelper,
) -> Result<SystemFilter> {
    match params.get(&"type".to_string()) {
        Some(value) => {
            if !value.is_string() {
                return Err(TantivyBindingError::InternalError(
                    "filter type should be string".to_string(),
                ));
            };

            match value.as_str().unwrap() {
                "length" => get_length_filter(params),
                "stop" => StopWordFilter::from_json(params, helper).map(|f| SystemFilter::Stop(f)),
                "decompounder" => SplitCompoundWords::from_json(params, helper)
                    .map(|f| SystemFilter::Decompounder(f)),
                "stemmer" => Stemmer::from_json(params, helper).map(|f| SystemFilter::Stemmer(f)),
                "regex" => RegexFilter::from_json(params).map(|f| SystemFilter::Regex(f)),
                "synonym" => {
                    SynonymFilter::from_json(params, helper).map(|f| SystemFilter::Synonym(f))
                }
                "pinyin" => PinyinFilter::from_json(params).map(|f| SystemFilter::Pinyin(f)),
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
