use lazy_static::lazy_static;
use log::warn;
use std::collections::HashMap;
use tantivy::tokenizer::*;
use serde_json as json;

use crate::tokenizer_filter::*;
use crate::log::init_log;

pub(crate) fn standard_tokenizer() -> TextAnalyzer {
    TextAnalyzer::builder(SimpleTokenizer::default()).build()
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
            // TODO support jieba filter and use same builder with standard.
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

pub(crate) fn create_tokenizer(params: &String) -> Option<TextAnalyzer> {
    init_log();

    match json::from_str::<json::Value>(&params){
        Ok(value) =>{
            if value.is_null(){
                return Some(standard_tokenizer());
            }
            if !value.is_object(){
                return None;
            }
            let json_params = value.as_object().unwrap();
            // create builder
            let analyzer_params=json_params.get("analyzer");
            if analyzer_params.is_none(){
                return Some(standard_tokenizer());
            }
            if !analyzer_params.unwrap().is_object(){
                return None;
            }
            let mut builder = TantivyBuilder::new(analyzer_params.unwrap().as_object().unwrap());

            // build custom filter
            let filter_params=json_params.get("filter");
            if !filter_params.is_none() && filter_params.unwrap().is_object(){
                builder.add_costom_filters(filter_params.unwrap().as_object().unwrap());
            }

            // build analyzer
            builder.build()
        },
        Err(_e) => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::tokenizer::create_tokenizer;

    #[test]
    fn test_create_tokenizer() {
        let params = r#"
        {
            "analyzer": 
            {
                "tokenizer": "standard",
                "filter": [""],
            },
        }"#;

        let tokenizer = create_tokenizer(&params.to_string());
        assert!(tokenizer.is_some());
    }
}