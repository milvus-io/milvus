use crate::error::{Result, TantivyBindingError};
use lingua::{LanguageDetector, LanguageDetectorBuilder};
use serde_json as json;
use std::collections::HashMap;
use tantivy::tokenizer::{BoxTokenStream, TextAnalyzer, Tokenizer};
use whatlang::detect;

pub trait Identifier: Sync + Send {
    fn detect(&self, text: &str) -> String;
    fn box_clone(&self) -> Box<dyn Identifier>;
}

#[derive(Clone)]
pub struct WhatlangIdentifier {
    confidence: f64,
}

impl WhatlangIdentifier {
    pub fn new(confidence: f64) -> WhatlangIdentifier {
        WhatlangIdentifier { confidence }
    }

    pub fn from_json(params: &json::Map<String, json::Value>) -> Result<WhatlangIdentifier> {
        let confidence = params.get("confidence").map_or(Ok(0.0), |v| {
            v.as_f64().ok_or(TantivyBindingError::InternalError(
                "confidence must be float".to_string(),
            ))
        })?;

        Ok(WhatlangIdentifier { confidence })
    }
}

impl Identifier for WhatlangIdentifier {
    fn detect(&self, text: &str) -> String {
        detect(text)
            .map_or("default", |info| {
                if info.confidence() > self.confidence {
                    info.lang().code()
                } else {
                    "default"
                }
            })
            .to_string()
    }

    fn box_clone(&self) -> Box<dyn Identifier> {
        Box::new(self.clone())
    }
}

pub struct LinguaIdentifier {
    detector: LanguageDetector,
}

impl LinguaIdentifier {
    pub fn default() -> LinguaIdentifier {
        LinguaIdentifier {
            detector: LanguageDetectorBuilder::from_all_languages().build(),
        }
    }
}

impl Clone for LinguaIdentifier {
    fn clone(&self) -> Self {
        LinguaIdentifier {
            detector: LanguageDetectorBuilder::from_all_languages().build(),
        }
    }
}

impl Identifier for LinguaIdentifier {
    fn detect(&self, text: &str) -> String {
        self.detector
            .detect_language_of(text)
            .map_or("default".to_string(), |lang| lang.to_string())
    }

    fn box_clone(&self) -> Box<dyn Identifier> {
        Box::new(self.clone())
    }
}

pub struct BoxIdentifier<'a>(Box<dyn Identifier + 'a>);

impl<'a> BoxIdentifier<'a> {
    pub fn default() -> BoxIdentifier<'a> {
        BoxIdentifier::new(WhatlangIdentifier::new(0.0))
    }

    pub fn new<T: Identifier + 'a>(identifier: T) -> BoxIdentifier<'a> {
        BoxIdentifier(Box::new(identifier))
    }

    pub fn from_json<'b>(params: &'b json::Value) -> Result<BoxIdentifier<'a>> {
        if params.is_string() {
            return match params.as_str().unwrap() {
                "whatlang" => Ok(BoxIdentifier::new(WhatlangIdentifier::new(0.0))),
                "lingua" => Ok(BoxIdentifier::new(LinguaIdentifier::default())),
                s => Err(TantivyBindingError::InvalidArgument(
                    format!("unknown identifier name: {}", s).to_string(),
                )),
            };
        }

        let dict_params = params
            .as_object()
            .ok_or(TantivyBindingError::InvalidArgument(
                "identifier params must be string or dict".to_string(),
            ))?;

        let ident_type = dict_params
            .get("type")
            .ok_or(TantivyBindingError::InvalidArgument(
                "identifier type must be set".to_string(),
            ))?
            .as_str()
            .ok_or(TantivyBindingError::InvalidArgument(
                "identifier type must be string".to_string(),
            ))?;

        match ident_type {
            "whatlang" => {
                let ident = WhatlangIdentifier::from_json(dict_params)?;
                Ok(BoxIdentifier::new(ident))
            }
            "lingua" => Ok(BoxIdentifier::new(LinguaIdentifier::default())),
            s => Err(TantivyBindingError::InvalidArgument(
                format!("unknown identifier type: {}", s).to_string(),
            )),
        }
    }
}

impl Clone for BoxIdentifier<'_> {
    fn clone(&self) -> Self {
        BoxIdentifier(self.0.box_clone())
    }
}

#[derive(Clone)]
pub struct LangIdentTokenizer<'a> {
    identifier: BoxIdentifier<'a>,
    analyzers: HashMap<String, TextAnalyzer>,
    mapping: HashMap<String, String>, // language -> analyzer name
}

impl<'a> LangIdentTokenizer<'a> {
    pub fn new(ident: BoxIdentifier<'a>) -> LangIdentTokenizer<'a> {
        LangIdentTokenizer {
            identifier: ident,
            analyzers: HashMap::new(),
            mapping: HashMap::new(),
        }
    }

    pub fn default() -> LangIdentTokenizer<'a> {
        LangIdentTokenizer {
            identifier: BoxIdentifier::new(WhatlangIdentifier::new(0.0)),
            analyzers: HashMap::new(),
            mapping: HashMap::new(),
        }
    }

    pub fn from_json<'b>(
        params: &'b json::Map<String, json::Value>,
        fc: fn(&json::Map<String, json::Value>) -> Result<TextAnalyzer>,
    ) -> Result<LangIdentTokenizer<'a>> {
        // init identfier for tokenizer
        let identifier = params
            .get("identifier")
            .map_or(Ok(BoxIdentifier::default()), |v| {
                BoxIdentifier::from_json(v)
            })?;

        let mut analyzer = LangIdentTokenizer::new(identifier);
        let sub_analyzers = params
            .get("analyzers")
            .ok_or(TantivyBindingError::InvalidArgument(
                "analyzers must be set".to_string(),
            ))?
            .as_object()
            .ok_or(TantivyBindingError::InvalidArgument(
                "analyzers must be dict".to_string(),
            ))?;

        // init sub analyzers
        for (name, params) in sub_analyzers {
            println!("name = {:?}, params = {:?}", name, params);
            analyzer.add(
                name,
                fc(params.as_object().ok_or_else(|| {
                    TantivyBindingError::InvalidArgument(format!(
                        "sub analyzer \"{}\" params must be dict",
                        name
                    ))
                })?)?,
            );
        }

        if !analyzer.analyzers.contains_key("default") {
            return Err(TantivyBindingError::InvalidArgument(
                "default sub analyzer for language identifier analyzer must be set".to_string(),
            ));
        }

        // set analyzer maping
        if let Some(map) = params.get("mapping") {
            map.as_object()
                .ok_or(TantivyBindingError::InvalidArgument(
                    "mapping must be dict".to_string(),
                ))?
                .iter()
                .for_each(|(key, value)| {
                    analyzer
                        .mapping
                        .insert(key.to_string(), value.as_str().unwrap().to_string());
                })
        }

        Ok(analyzer)
    }

    pub fn add(&mut self, name: &str, analyzer: TextAnalyzer) {
        self.analyzers.insert(name.to_string(), analyzer);
    }

    fn get_by_language(&mut self, language: &str) -> &mut TextAnalyzer {
        let analyzer_name = self
            .mapping
            .get(language)
            .map(|v| v.as_str())
            .unwrap_or(language);

        if self.analyzers.contains_key(analyzer_name) {
            return self.analyzers.get_mut(analyzer_name).unwrap();
        } else {
            return self.analyzers.get_mut("default").unwrap();
        }
    }

    fn tokenize<'b>(&'b mut self, text: &'b str) -> BoxTokenStream<'b> {
        let language: String = self.identifier.0.detect(text);
        let analyzer = self.get_by_language(language.as_str());

        analyzer.token_stream(text)
    }
}

impl Tokenizer for LangIdentTokenizer<'static> {
    type TokenStream<'a> = BoxTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> BoxTokenStream<'a> {
        self.tokenize(text)
    }
}

#[cfg(test)]
mod tests {
    use serde_json as json;
    use tantivy::tokenizer::Tokenizer;

    use super::LangIdentTokenizer;
    use crate::analyzer::tokenizers::lang_ident_tokenizer::BoxIdentifier;
    use crate::analyzer::{create_analyzer, create_analyzer_by_json};
    use crate::error::Result;

    #[test]
    fn test_tokenizer() {
        let jieba_params = r#"{
            "tokenizer": "jieba"
        }"#;

        let standard_params = r#"{
            "tokenizer": "standard"
        }"#;

        let mut analyzer = LangIdentTokenizer::new(BoxIdentifier::default());
        let result = || -> Result<()> {
            analyzer.add("default", create_analyzer(standard_params)?);
            analyzer.add("cmn", create_analyzer(jieba_params)?);
            Ok(())
        }();

        assert!(result.is_ok(), "error: {}", result.err().unwrap());

        let mut stream = analyzer.token_stream("测试中文文本");
        stream.process(&mut |token| println!("token: {:?}", token.text));
    }

    #[test]
    fn test_with_json() {
        let params = r#"{
            "analyzers": {
                "default": {
                    "tokenizer": "standard"
                },
                "cmn": {
                    "tokenizer": "jieba"
                }
            }
        }"#;

        let json_params = json::from_str::<json::Value>(&params).unwrap();
        let builder: std::result::Result<LangIdentTokenizer, crate::error::TantivyBindingError> =
            LangIdentTokenizer::from_json(
                json_params.as_object().unwrap(),
                create_analyzer_by_json,
            );
        assert!(builder.is_ok(), "error: {}", builder.err().unwrap());

        let mut analyzer = builder.unwrap();
        let mut stream = analyzer.token_stream("测试中文文本");
        stream.process(&mut |token| println!("token: {:?}", token.text));
    }

    #[test]
    fn test_with_mapping() {
        let params = r#"{
            "analyzers": {
                "default": {
                    "tokenizer": "standard"
                },
                "en": {
                        "type": "english"
                    },
                "jieba": {
                    "tokenizer": "jieba"
                }
            },
            "mapping": {
                "cmn": "jieba",
                "eng": "en"
            }
        }"#;

        let json_params = json::from_str::<json::Value>(&params).unwrap();
        let builder: std::result::Result<LangIdentTokenizer, crate::error::TantivyBindingError> =
            LangIdentTokenizer::from_json(
                json_params.as_object().unwrap(),
                create_analyzer_by_json,
            );
        assert!(builder.is_ok(), "error: {}", builder.err().unwrap());

        let text = "This is a simple test for language identification.";
        let mut analyzer = builder.unwrap();
        let mut stream = analyzer.token_stream(text);
        stream.process(&mut |token| println!("token: {:?}", token.text));
    }

    #[test]
    fn test_with_lingua() {
        let params = r#"{
            "analyzers": {
                "default": {
                    "tokenizer": "standard"
                },
                "en": {
                        "type": "english"
                    },
                "jieba": {
                    "tokenizer": "jieba"
                }
            },
            "mapping": {
                "Chinese": "jieba",
                "English": "en"
            },
            "identifier": "lingua"
        }"#;

        let json_params = json::from_str::<json::Value>(&params).unwrap();
        let builder: std::result::Result<LangIdentTokenizer, crate::error::TantivyBindingError> =
            LangIdentTokenizer::from_json(
                json_params.as_object().unwrap(),
                create_analyzer_by_json,
            );
        assert!(builder.is_ok(), "error: {}", builder.err().unwrap());

        let text = "sort english test";
        let mut analyzer = builder.unwrap();
        let mut stream = analyzer.token_stream(text);
        stream.process(&mut |token| println!("token: {:?}", token.text));
    }
}
