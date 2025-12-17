use super::filter::FilterBuilder;
use crate::error::{Result, TantivyBindingError};
use serde_json as json;
use tantivy::tokenizer::{Language, Stemmer};

impl FilterBuilder for Stemmer {
    fn from_json(params: &json::Map<String, json::Value>) -> Result<Self> {
        let value = params.get("language");
        if value.is_none() || !value.unwrap().is_string() {
            return Err(TantivyBindingError::InternalError(
                "stemmer language field should be string".to_string(),
            ));
        }

        match value.unwrap().as_str().unwrap().into_language() {
            Ok(language) => Ok(Stemmer::new(language)),
            Err(e) => Err(TantivyBindingError::InternalError(format!(
                "create stemmer failed : {}",
                e.to_string()
            ))),
        }
    }
}

trait StemmerLanguageParser {
    fn into_language(self) -> Result<Language>;
}

impl StemmerLanguageParser for &str {
    fn into_language(self) -> Result<Language> {
        match self.to_lowercase().as_str() {
            "arabic" => Ok(Language::Arabic),
            "arabig" => Ok(Language::Arabic), // typo
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
