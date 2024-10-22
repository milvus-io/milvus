use lazy_static::lazy_static;
use log::warn;
use std::collections::HashMap;
use tantivy::tokenizer::*;
use serde_json as json;

use crate::tokenizer_filter::*;
use crate::log::init_log;

lazy_static! {
    static ref DEFAULT_TOKENIZER_MANAGER: TokenizerManager = TokenizerManager::default();
}

pub(crate) fn default_tokenizer() -> TextAnalyzer {
    DEFAULT_TOKENIZER_MANAGER.get("default").unwrap()
}

struct TantivyBuilder<'a>{
    // builder: TextAnalyzerBuilder
    filters:HashMap<String, SystemFilter>,
    params:&'a json::Map<String, json::Value>
}

impl TantivyBuilder<'_>{
    fn new(params: &json::Map<String, json::Value>) -> TantivyBuilder{
        TantivyBuilder{
            filters: HashMap::new(),
            params:params,
        }
    }

    fn add_costom_filter(&mut self, name: &String, params: &json::Map<String, json::Value>){
        match SystemFilter::try_from(params){
            Ok(filter) => {self.filters.insert(name.to_string(), filter);},
            Err(_e) => {},
        };
    }

    fn add_costom_filters(&mut self, params:&json::Map<String, json::Value>){
        for (name, value) in params{
            if !value.is_object(){
                continue;
            }

            self.add_costom_filter(name, value.as_object().unwrap());
        }
    }

    fn build(mut self) -> Option<TextAnalyzer>{
        let tokenizer=self.params.get("tokenizer");
        if !tokenizer.is_none() && !tokenizer.unwrap().is_string(){
            return None;
        }

        let tokenizer_name = {
            if !tokenizer.is_none(){
                tokenizer.unwrap().as_str().unwrap()
            }else{
                "standard"
            }
        };

        match tokenizer_name {
            "standard" => {
                    let mut builder = TextAnalyzer::builder(SimpleTokenizer::default()).dynamic();
                    let filters= self.params.get("filter");
                    if !filters.is_none() && filters.unwrap().is_array(){
                        for filter in filters.unwrap().as_array().unwrap(){
                            if filter.is_string(){
                                let filter_name = filter.as_str().unwrap();
                                let costum = self.filters.remove(filter_name);
                                if !costum.is_none(){
                                    builder = costum.unwrap().transform(builder);
                                    continue;
                                }
                                // check if filter was system filter
                                let system = SystemFilter::from(filter_name);
                                match system {
                                    SystemFilter::Invalid => {
                                        log::warn!("build analyzer failed, filter not found :{}", filter_name);
                                        return None
                                    }
                                    other => {
                                        builder = other.transform(builder);
                                    },
                                }
                            }
                        }
                    }
                    Some(builder.build())
                }
            "jieba" => {
                Some(tantivy_jieba::JiebaTokenizer {}.into())
            }
            s => {
                warn!("unsupported tokenizer: {}", s);
                None
            }
        }
    }
}

pub(crate) fn create_tokenizer(params: &HashMap<String, String>) -> Option<TextAnalyzer> {
    init_log();

    let analyzer_json_value = match params.get("analyzer"){
        Some(value) => {
            let json_analyzer = json::from_str::<json::Value>(value);
            if json_analyzer.is_err() {
                return None;
            }
            let json_value = json_analyzer.unwrap();
            if !json_value.is_object(){
                return None
            }
            json_value
        }
        None => json::Value::Object(json::Map::<String, json::Value>::new()),
    };

    let analyzer_params= analyzer_json_value.as_object().unwrap();
    let mut builder = TantivyBuilder::new(analyzer_params);
    let str_filter=params.get("filter");
    if !str_filter.is_none(){
        let json_filter = json::from_str::<json::Value>(str_filter.unwrap());
        if json_filter.is_err(){
            return None
        }

        let filter_params = json_filter.unwrap();
        if !filter_params.is_object(){
            return None
        }

        builder.add_costom_filters(filter_params.as_object().unwrap());
    }
    builder.build()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use crate::tokenizer::create_tokenizer;

    #[test]
    fn test_create_tokenizer() {
        let mut params : HashMap<String, String> = HashMap::new();
        let analyzer_params = r#"
        {
            "tokenizer": "jieba"
        }"#;

        params.insert("analyzer".to_string(), analyzer_params.to_string());
        let tokenizer = create_tokenizer(&params);
        assert!(tokenizer.is_some());
    }
}
