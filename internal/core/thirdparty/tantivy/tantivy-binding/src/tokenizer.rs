use log::warn;
use std::collections::HashMap;
use tantivy::tokenizer::*;
use serde_json as json;

use crate::tokenizer_filter::*;
use crate::error::TantivyError;

pub(crate) fn standard_analyzer() -> TextAnalyzer {
    standard_builder().build()
}

fn standard_builder() -> TextAnalyzerBuilder{
    TextAnalyzer::builder(SimpleTokenizer::default()).dynamic()
}

fn whitespace_builder()-> TextAnalyzerBuilder{
    TextAnalyzer::builder(WhitespaceTokenizer::default()).dynamic()
}

fn get_builder_by_name(name:&String) -> Result<TextAnalyzerBuilder, TantivyError>{
    match name.as_str() {
        "standard" => Ok(standard_builder()),
        "whitespace" => Ok(whitespace_builder()),
        other => {
            warn!("unsupported tokenizer: {}", other);
            Err(format!("unsupported tokenizer: {}", other).into())
        }
    }
}

struct AnalyzerBuilder<'a>{
    // builder: TextAnalyzerBuilder
    filters:HashMap<String, SystemFilter>,
    params:&'a json::Map<String, json::Value>
}

impl AnalyzerBuilder<'_>{
    fn new(params: &json::Map<String, json::Value>) -> AnalyzerBuilder{
        AnalyzerBuilder{
            filters: HashMap::new(),
            params:params,
        }
    }

    fn get_tokenizer_name(&self) -> Result<String, TantivyError>{
        let tokenizer=self.params.get("tokenizer");
        if tokenizer.is_none(){
            return Ok("standard".to_string());
        }
        if !tokenizer.unwrap().is_string(){
            return Err(format!("tokenizer name should be string").into());
        }

        Ok(tokenizer.unwrap().as_str().unwrap().to_string())
    }

    fn add_custom_filter(&mut self, name: &String, params: &json::Map<String, json::Value>) -> Result<(),TantivyError>{
        match SystemFilter::try_from(params){
            Ok(filter) => {
                self.filters.insert(name.to_string(), filter);
                Ok(())
            },
            Err(e) => {Err(e)},
        }
    }

    fn add_custom_filters(&mut self, params:&json::Map<String, json::Value>) -> Result<(),TantivyError>{
        for (name, value) in params{
            if !value.is_object(){
                continue;
            }
            self.add_custom_filter(name, value.as_object().unwrap())?;
        }
        Ok(())
    }

    fn build_filter(&mut self,mut builder: TextAnalyzerBuilder, params: &json::Value) -> Result<TextAnalyzerBuilder, TantivyError>{
        if !params.is_array(){
            return Err("filter params should be array".into());
        }
    
        let filters = params.as_array().unwrap();
        for filter in filters{
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
                        return Err(format!("build analyzer failed, filter not found :{}", filter_name).into())
                    }
                    other => {
                        builder = other.transform(builder);
                    },
                }
            }else if filter.is_object(){
                let filter=SystemFilter::try_from(filter.as_object().unwrap())?;
                builder = filter.transform(builder);
            }
        };
        Ok(builder)
    }

    fn build_option(&mut self, mut builder: TextAnalyzerBuilder) -> Result<TextAnalyzerBuilder, TantivyError>{
        for (key, value) in self.params{
            match key.as_str(){
                "tokenizer" => {},
                "filter" => {
                    // build with filter if filter param exist
                    builder=self.build_filter(builder, value)?;
                },
                "max_token_length" => {
                    if !value.is_u64(){
                        return Err("max token length should be int type".into());
                    }
                    builder = builder.filter_dynamic(RemoveLongFilter::limit(value.as_u64().unwrap() as usize));
                }
                other => return Err(format!("unknown key of tokenizer option: {}", other).into()),
            }
        }
        Ok(builder)
    }

    fn build(mut self) -> Result<TextAnalyzer, TantivyError>{
        let tokenizer_name = self.get_tokenizer_name()?;      
        if tokenizer_name == "jieba"{
            return Ok(tantivy_jieba::JiebaTokenizer{}.into());
        }

        let mut builder=get_builder_by_name(&tokenizer_name)?;
        
        // build with option
        builder = self.build_option(builder)?;
        Ok(builder.build())
    }
}

pub(crate) fn create_tokenizer(params: &String) -> Result<TextAnalyzer, TantivyError> {
    if params.len()==0{
        return Ok(standard_analyzer());
    }

    match json::from_str::<json::Value>(&params){
        Ok(value) =>{
            if value.is_null(){
                return Ok(standard_analyzer());
            }
            if !value.is_object(){
                return Err("tokenizer params should be a json map".into());
            }
            let json_params = value.as_object().unwrap();

            // create builder
            let analyzer_params=json_params.get("analyzer");
            if analyzer_params.is_none(){
                return Ok(standard_analyzer());
            }
            if !analyzer_params.unwrap().is_object(){
                return Err("analyzer params should be a json map".into());
            }
            let mut builder = AnalyzerBuilder::new(analyzer_params.unwrap().as_object().unwrap());
    
            // build custom filter
            let filter_params=json_params.get("filter");
            if !filter_params.is_none() && filter_params.unwrap().is_object(){
                builder.add_custom_filters(filter_params.unwrap().as_object().unwrap())?;
            }

            // build analyzer
            builder.build()
        },
        Err(err) => Err(err.into()),
    }
}

#[cfg(test)]
mod tests {
    use crate::tokenizer::create_tokenizer;

    #[test]
    fn test_create_tokenizer() {
        let params = r#"{"analyzer": {"tokenizer": "standard"}}"#;

        let tokenizer = create_tokenizer(&params.to_string());
        assert!(tokenizer.is_ok());
    }

    #[test]
    fn test_jieba_tokenizer() {
        let params = r#"{"analyzer": {"tokenizer": "jieba"}}"#;

        let tokenizer = create_tokenizer(&params.to_string());
        assert!(tokenizer.is_ok());
        let mut bining = tokenizer.unwrap();

        let mut stream = bining.token_stream("系统安全");
        while stream.advance(){
            let token = stream.token();
            let text = token.text.clone();
            print!("test token :{}\n", text.as_str())
        }
    }
}