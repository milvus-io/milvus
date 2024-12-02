use log::warn;
use std::collections::HashMap;
use tantivy::tokenizer::*;
use tantivy::tokenizer::StopWordFilter;
use serde_json as json;

use crate::stop_words;
use crate::tokenizer_filter::*;
use crate::jieba_tokenizer::JiebaTokenizer;
use crate::error::TantivyError;
use crate::util::*;


// default build-in analyzer
pub(crate) fn standard_analyzer(stop_words: Vec<String>) -> TextAnalyzer {
    let builder = standard_builder()
        .filter(LowerCaser);

    if stop_words.len() > 0{
        return builder.filter(StopWordFilter::remove(stop_words)).build();
    }

    builder.build()
}

fn chinese_analyzer(stop_words: Vec<String>) -> TextAnalyzer{
    let builder = jieba_builder().filter(CnAlphaNumOnlyFilter);
    if stop_words.len() > 0{
        return builder.filter(StopWordFilter::remove(stop_words)).build();
    }

    builder.build()
}

fn english_analyzer(stop_words: Vec<String>) -> TextAnalyzer{
    let builder = standard_builder()
        .filter(LowerCaser)
        .filter(Stemmer::new(Language::English))
        .filter(StopWordFilter::remove(stop_words::ENGLISH.iter().map(|&word| word.to_owned())));

    if stop_words.len() > 0{
        return builder.filter(StopWordFilter::remove(stop_words)).build();
    }

    builder.build()
}

fn standard_builder() -> TextAnalyzerBuilder{
    TextAnalyzer::builder(SimpleTokenizer::default()).dynamic()
}

fn whitespace_builder()-> TextAnalyzerBuilder{
    TextAnalyzer::builder(WhitespaceTokenizer::default()).dynamic()
}

fn jieba_builder() -> TextAnalyzerBuilder{
    TextAnalyzer::builder(JiebaTokenizer::new()).dynamic()
}

fn get_builder_by_name(name:&String) -> Result<TextAnalyzerBuilder, TantivyError>{
    match name.as_str() {
        "standard" => Ok(standard_builder()),
        "whitespace" => Ok(whitespace_builder()),
        "jieba" => Ok(jieba_builder()),
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
            return Err(format!("tokenizer name or type must be set").into());
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
                other => return Err(format!("unknown analyzer option key: {}", other).into()),
            }
        }
        Ok(builder)
    }

    fn get_stop_words_option(&self) -> Result<Vec<String>, TantivyError>{
        let value = self.params.get("stop_words");
        match value{
            Some(value)=>{
                let str_list = get_string_list(value, "filter stop_words")?;
                Ok(get_stop_words_list(str_list))
            }
            None => Ok(vec![])
        }        
    }

    fn build_template(self, type_: &str)-> Result<TextAnalyzer, TantivyError>{
        match type_{
            "standard" => {
                Ok(standard_analyzer(self.get_stop_words_option()?))
            },
            "chinese" => {
                Ok(chinese_analyzer(self.get_stop_words_option()?))
            },
            "english" => {
                Ok(english_analyzer(self.get_stop_words_option()?))
            }
            other_ => Err(format!("unknown build-in analyzer type: {}", other_).into())
        }
    } 

    fn build(mut self) -> Result<TextAnalyzer, TantivyError>{
        // build base build-in analyzer
        match self.params.get("type"){
            Some(type_) =>{
                if !type_.is_string(){
                    return Err(format!("analyzer type shoud be string").into())
                }
                return self.build_template(type_.as_str().unwrap());
            },
            None => {}
        };

        //build custom analyzer
        let tokenizer_name = self.get_tokenizer_name()?;
        let mut builder=get_builder_by_name(&tokenizer_name)?;
        
        // build with option
        builder = self.build_option(builder)?;
        Ok(builder.build())
    }
}

pub(crate) fn create_tokenizer_with_filter(params: &String) -> Result<TextAnalyzer, TantivyError> {
    match json::from_str::<json::Value>(&params){
        Ok(value) =>{
            if value.is_null(){
                return Ok(standard_analyzer(vec![]));
            }
            if !value.is_object(){
                return Err("tokenizer params should be a json map".into());
            }
            let json_params = value.as_object().unwrap();

            // create builder
            let analyzer_params=json_params.get("analyzer");
            if analyzer_params.is_none(){
                return Ok(standard_analyzer(vec![]));
            }
            if !analyzer_params.unwrap().is_object(){
                return Err("analyzer params should be a json map".into());
            }

            let builder_params = analyzer_params.unwrap().as_object().unwrap();
            if builder_params.is_empty(){
                return Ok(standard_analyzer(vec![]));
            }

            let mut builder = AnalyzerBuilder::new(builder_params);
    
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

pub(crate) fn create_tokenizer(params: &String) -> Result<TextAnalyzer, TantivyError> {
    if params.len()==0{
        return Ok(standard_analyzer(vec![]));
    }
    create_tokenizer_with_filter(&format!("{{\"analyzer\":{}}}", params))
}

#[cfg(test)]
mod tests {
    use crate::tokenizer::create_tokenizer;

    #[test]
    fn test_standard_analyzer() {
        let params = r#"{
            "type": "standard",
            "stop_words": ["_english_"]
        }"#;

        let tokenizer = create_tokenizer(&params.to_string());
        assert!(tokenizer.is_ok(),  "error: {}", tokenizer.err().unwrap().reason());
    }

    #[test]
    fn test_chinese_analyzer() {
        let params = r#"{
            "type": "chinese"
        }"#;

        let tokenizer = create_tokenizer(&params.to_string());
        assert!(tokenizer.is_ok(),  "error: {}", tokenizer.err().unwrap().reason());
        let mut bining = tokenizer.unwrap();
        let mut stream = bining.token_stream("系统安全;,'';lxyz密码");
        
        let mut results = Vec::<String>::new();
        while stream.advance(){
            let token = stream.token();
            results.push(token.text.clone());
        }

        print!("test tokens :{:?}\n", results)
    }

}