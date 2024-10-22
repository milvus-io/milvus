use tantivy::tokenizer::*;
use serde_json as json;

pub(crate) enum SystemFilter{
    Invalid,
    LowerCase(LowerCaser),
    AsciiFolding(AsciiFoldingFilter),
    AlphaNumOnly(AlphaNumOnlyFilter),
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
fn get_length_filter(params: &json::Map<String, json::Value>) -> Result<SystemFilter, ()>{
    let limit_str = params.get("max");
    if limit_str.is_none() || !limit_str.unwrap().is_u64(){
        return Err(())
    }
    let limit = limit_str.unwrap().as_u64().unwrap() as usize;
    Ok(SystemFilter::Length(RemoveLongFilter::limit(limit)))
}

fn get_stop_filter(params: &json::Map<String, json::Value>)-> Result<SystemFilter, ()>{
    let value = params.get("stop_words");
    if value.is_none() || !value.unwrap().is_array(){
        return Err(())
    }

    let stop_words= value.unwrap().as_array().unwrap();
    let mut str_list = Vec::<String>::new();
    for element in stop_words{
        match element.as_str(){
            Some(word) => str_list.push(word.to_string()),
            None => return Err(())
        }
    };
    Ok(SystemFilter::Stop(StopWordFilter::remove(str_list)))
}

fn get_decompounder_filter(params: &json::Map<String, json::Value>)-> Result<SystemFilter, ()>{
    let value = params.get("word_list");
    if value.is_none() || !value.unwrap().is_array(){
        return Err(())
    }

    let stop_words= value.unwrap().as_array().unwrap();
    let mut str_list = Vec::<String>::new();
    for element in stop_words{
        match element.as_str(){
            Some(word) => str_list.push(word.to_string()),
            None => return Err(())
        }
    };

    match SplitCompoundWords::from_dictionary(str_list){
        Ok(f) => Ok(SystemFilter::Decompounder(f)),
        Err(_e) => Err(())
    }
}

fn get_stemmer_filter(params: &json::Map<String, json::Value>)-> Result<SystemFilter, ()>{
    let value = params.get("language");
    if value.is_none() || !value.unwrap().is_string(){
        return Err(())
    }

    match value.unwrap().as_str().unwrap().into_language(){
        Ok(language) => Ok(SystemFilter::Stemmer(Stemmer::new(language))),
        Err(_e) => Err(()),
    }
}

trait LanguageParser {
    type Error;
    fn into_language(self) -> Result<Language, Self::Error>;
}

impl LanguageParser for &str {   
    type Error = ();
    fn into_language(self) -> Result<Language, Self::Error> {
        match self {
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
            _ => Err(()),
        }
    }
}

impl From<&str> for SystemFilter{
    fn from(value: &str) -> Self {
        match value{
            "lowercase" => Self::LowerCase(LowerCaser),
            "asciifolding" => Self::AsciiFolding(AsciiFoldingFilter),
            "alphanumonly" => Self::AlphaNumOnly(AlphaNumOnlyFilter),
            _ => Self::Invalid,
        }
    }
}

impl TryFrom<&json::Map<String, json::Value>> for SystemFilter {
    type Error = ();

    fn try_from(params: &json::Map<String, json::Value>) -> Result<Self, Self::Error> {
        match params.get(&"type".to_string()){
            Some(value) =>{
                if !value.is_string(){
                    return Err(());
                };

                match value.as_str().unwrap(){
                    "length" => get_length_filter(params),
                    "stop" => get_stop_filter(params),
                    "decompounder" => get_decompounder_filter(params),
                    "stemmer" => get_stemmer_filter(params),
                    _other=>Err(()),
                }
            }
            None => Err(()),
        }
    }
}
