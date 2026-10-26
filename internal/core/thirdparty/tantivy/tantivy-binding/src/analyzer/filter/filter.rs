use serde_json as json;
use tantivy::tokenizer::*;

use super::util::*;
use super::{
    ArabicNormalizationFilter, CnAlphaNumOnlyFilter, CnCharOnlyFilter, DecimalDigitFilter,
    PinyinFilter, RegexFilter, RemovePunctFilter, Stemmer, SynonymFilter,
};
use crate::analyzer::options::FileResourcePathHelper;
use crate::error::{Result, TantivyBindingError};

pub(crate) enum SystemFilter {
    Invalid,
    LowerCase(LowerCaser),
    AsciiFolding(AsciiFoldingFilter),
    AlphaNumOnly(AlphaNumOnlyFilter),
    ArabicNormalization(ArabicNormalizationFilter),
    CnCharOnly(CnCharOnlyFilter),
    CnAlphaNumOnly(CnAlphaNumOnlyFilter),
    DecimalDigit(DecimalDigitFilter),
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
            Self::ArabicNormalization(filter) => builder.filter(filter).dynamic(),
            Self::CnCharOnly(filter) => builder.filter(filter).dynamic(),
            Self::CnAlphaNumOnly(filter) => builder.filter(filter).dynamic(),
            Self::DecimalDigit(filter) => builder.filter(filter).dynamic(),
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
        return Err(TantivyBindingError::InvalidArgument(
            "lenth max param was none or not uint".to_string(),
        ));
    }
    let limit = limit_str.unwrap().as_u64().unwrap() as usize;
    Ok(SystemFilter::Length(RemoveLongFilter::limit(limit + 1)))
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
            "decimaldigit" => Self::DecimalDigit(DecimalDigitFilter),
            "arabic_normalization" => Self::ArabicNormalization(ArabicNormalizationFilter),
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
                return Err(TantivyBindingError::InvalidArgument(
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
                other => Err(TantivyBindingError::InvalidArgument(format!(
                    "unsupport filter type: {}",
                    other
                ))),
            }
        }
        None => Err(TantivyBindingError::InvalidArgument(
            "no type field in filter params".to_string(),
        )),
    }
}
