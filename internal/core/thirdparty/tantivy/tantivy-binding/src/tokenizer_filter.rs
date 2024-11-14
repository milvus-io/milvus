use tantivy::tokenizer::*;
use serde_json as json;
use regex;

use crate::error::TantivyError;
use crate::util::*;

pub(crate) enum SystemFilter{
    Invalid,
    LowerCase(LowerCaser),
    AsciiFolding(AsciiFoldingFilter),
    AlphaNumOnly(AlphaNumOnlyFilter),
    CnCharOnly(CnCharOnlyFilter),
    Length(RemoveLongFilter),
    Stop(StopWordFilter),
    Decompounder(SplitCompoundWords),
    Stemmer(Stemmer)
}

impl SystemFilter{
    pub(crate) fn transform(self, builder: TextAnalyzerBuilder) -> TextAnalyzerBuilder{
        match self{
            Self::LowerCase(filter) => builder.filter(filter).dynamic(),
            Self::AsciiFolding(filter) => builder.filter(filter).dynamic(),
            Self::AlphaNumOnly(filter) => builder.filter(filter).dynamic(),
            Self::CnCharOnly(filter)  => builder.filter(filter).dynamic(),
            Self::Length(filter) => builder.filter(filter).dynamic(),
            Self::Stop(filter) => builder.filter(filter).dynamic(),
            Self::Decompounder(filter) => builder.filter(filter).dynamic(),
            Self::Stemmer(filter) => builder.filter(filter).dynamic(),
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
fn get_length_filter(params: &json::Map<String, json::Value>) -> Result<SystemFilter, TantivyError>{
    let limit_str = params.get("max");
    if limit_str.is_none() || !limit_str.unwrap().is_u64(){
        return Err("lenth max param was none or not uint".into())
    }
    let limit = limit_str.unwrap().as_u64().unwrap() as usize;
    Ok(SystemFilter::Length(RemoveLongFilter::limit(limit)))
}

fn get_stop_words_filter(params: &json::Map<String, json::Value>)-> Result<SystemFilter, TantivyError>{
    let value = params.get("stop_words");
    if value.is_none(){
        return Err("stop filter stop_words can't be empty".into());
    }
    let str_list = get_string_list(value.unwrap(), "stop_words filter")?;
    Ok(SystemFilter::Stop(StopWordFilter::remove(get_stop_words_list(str_list))))
}

fn get_decompounder_filter(params: &json::Map<String, json::Value>)-> Result<SystemFilter, TantivyError>{
    let value = params.get("word_list");
    if value.is_none() || !value.unwrap().is_array(){
        return Err("decompounder word list should be array".into())
    }

    let stop_words = value.unwrap().as_array().unwrap();
    let mut str_list = Vec::<String>::new();
    for element in stop_words{
        match element.as_str(){
            Some(word) => str_list.push(word.to_string()),
            None => return Err("decompounder word list item should be string".into())
        }
    };

    match SplitCompoundWords::from_dictionary(str_list){
        Ok(f) => Ok(SystemFilter::Decompounder(f)),
        Err(e) => Err(format!("create decompounder failed: {}", e.to_string()).into())
    }
}

fn get_stemmer_filter(params: &json::Map<String, json::Value>)-> Result<SystemFilter, TantivyError>{
    let value = params.get("language");
    if value.is_none() || !value.unwrap().is_string(){
        return Err("stemmer language field should be string".into())
    }

    match value.unwrap().as_str().unwrap().into_language(){
        Ok(language) => Ok(SystemFilter::Stemmer(Stemmer::new(language))),
        Err(e) => Err(format!("create stemmer failed : {}", e.to_string()).into()),
    }
}

trait LanguageParser {
    type Error;
    fn into_language(self) -> Result<Language, Self::Error>;
}

impl LanguageParser for &str {   
    type Error = TantivyError;
    fn into_language(self) -> Result<Language, Self::Error> {
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
            other => Err(format!("unsupport language: {}", other).into()),
        }
    }
}

impl From<&str> for SystemFilter{
    fn from(value: &str) -> Self {
        match value{
            "lowercase" => Self::LowerCase(LowerCaser),
            "asciifolding" => Self::AsciiFolding(AsciiFoldingFilter),
            "alphanumonly" => Self::AlphaNumOnly(AlphaNumOnlyFilter),
            "cncharonly" => Self::CnCharOnly(CnCharOnlyFilter),
            _ => Self::Invalid,
        }
    }
}

impl TryFrom<&json::Map<String, json::Value>> for SystemFilter {
    type Error = TantivyError;

    fn try_from(params: &json::Map<String, json::Value>) -> Result<Self, Self::Error> {
        match params.get(&"type".to_string()){
            Some(value) =>{
                if !value.is_string(){
                    return Err("filter type should be string".into());
                };

                match value.as_str().unwrap(){
                    "length" => get_length_filter(params),
                    "stop" => get_stop_words_filter(params),
                    "decompounder" => get_decompounder_filter(params),
                    "stemmer" => get_stemmer_filter(params),
                    other=> Err(format!("unsupport filter type: {}", other).into()),
                }
            }
            None => Err("no type field in filter params".into()),
        }
    }
}

pub struct CnCharOnlyFilter;

pub struct CnCharOnlyFilterStream<T> {
    regex: regex::Regex,
    tail: T,
}

impl TokenFilter for CnCharOnlyFilter{
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