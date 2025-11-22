use crate::error::{self, Result, TantivyBindingError};
use log::warn;
use serde_json as json;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tantivy::tokenizer::{Token, TokenFilter, TokenStream, Tokenizer};

pub struct SynonymDictBuilder {
    dict: HashMap<String, HashSet<String>>,
    expand: bool,
}

fn get_words(str: &str) -> Vec<String> {
    str.split(",")
        .into_iter()
        .map(|v| v.trim().to_string())
        .collect()
}

impl SynonymDictBuilder {
    fn new(expand: bool) -> SynonymDictBuilder {
        SynonymDictBuilder {
            dict: HashMap::new(),
            expand: expand,
        }
    }

    // TODO: Optimize memory usage when add group
    //       (forbid clone multiple times here)
    fn add_group(&mut self, words: Vec<String>) {
        for (i, key_word) in words.iter().enumerate() {
            let mut push_vec: Vec<String> = Vec::new();
            if self.expand {
                for (j, map_word) in words.iter().enumerate() {
                    if i != j {
                        push_vec.push(map_word.clone());
                    }
                }
            } else {
                push_vec.push(words.first().unwrap().clone());
            }

            self.add(key_word.clone(), push_vec);
        }
    }

    fn add_mapping(&mut self, keys: Vec<String>, words: Vec<String>) {
        for key in keys {
            self.add(key, words.clone());
        }
    }

    fn add(&mut self, key: String, words: Vec<String>) {
        if let Some(list) = self.dict.get_mut(&key) {
            list.extend(words);
        } else {
            let mut set: HashSet<_> = words.into_iter().collect();
            if self.expand {
                set.insert(key.clone());
            }
            self.dict.insert(key, set);
        }
    }

    fn add_row(&mut self, str: &str) -> Result<()> {
        let s1: Vec<&str> = str.split("=>").collect();
        match s1.len() {
            1 => {
                self.add_group(get_words(str));
                Ok(())
            }
            2 => {
                self.add_mapping(get_words(s1[0]), get_words(s1[1]));
                Ok(())
            }
            _ => Err(TantivyBindingError::InvalidArgument(format!(
                "read synonym dict failed, has more than one \"=>\" in {}",
                str
            ))),
        }
    }

    fn build(self) -> SynonymDict {
        SynonymDict::new(self.dict)
    }
}

pub struct SynonymDict {
    dict: HashMap<String, Box<[String]>>,
}

impl SynonymDict {
    fn new(dict: HashMap<String, HashSet<String>>) -> SynonymDict {
        let mut box_dict: HashMap<String, Box<[String]>> = HashMap::new();
        for (k, v) in dict {
            box_dict.insert(k, v.into_iter().collect::<Vec<_>>().into_boxed_slice());
        }
        return SynonymDict { dict: box_dict };
    }

    fn get(&self, k: &str) -> Option<&Box<[String]>> {
        self.dict.get(k)
    }
}

#[derive(Clone)]
pub struct SynonymFilter {
    dict: Arc<SynonymDict>,
    expand: bool,
}

impl SynonymFilter {
    pub fn from_json(params: &json::Map<String, json::Value>) -> Result<SynonymFilter> {
        let expand = params.get("expand").map_or(Ok(true), |v| {
            v.as_bool().ok_or(TantivyBindingError::InvalidArgument(
                "create synonym filter failed, `expand` must be bool".to_string(),
            ))
        })?;

        let mut builder = SynonymDictBuilder::new(expand);
        if let Some(dict) = params.get("synonyms") {
            dict.as_array()
                .ok_or(TantivyBindingError::InvalidArgument(
                    "create synonym filter failed, `synonyms` must be array".to_string(),
                ))?
                .iter()
                .try_for_each(|v| {
                    let s = v.as_str().ok_or(TantivyBindingError::InvalidArgument(
                        "create synonym filter failed, item in `synonyms` must be string"
                            .to_string(),
                    ))?;
                    builder.add_row(s)
                })?;
        }

        Ok(SynonymFilter {
            dict: Arc::new(builder.build()),
            expand: expand,
        })
    }
}

pub struct SynonymFilterStream<T> {
    dict: Arc<SynonymDict>,
    buffer: Vec<Token>,
    cursor: usize,
    tail: T,
}

impl TokenFilter for SynonymFilter {
    type Tokenizer<T: Tokenizer> = SynonymFilterWrapper<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> SynonymFilterWrapper<T> {
        SynonymFilterWrapper {
            dict: self.dict,
            inner: tokenizer,
        }
    }
}

#[derive(Clone)]
pub struct SynonymFilterWrapper<T> {
    dict: Arc<SynonymDict>,
    inner: T,
}

impl<T: Tokenizer> Tokenizer for SynonymFilterWrapper<T> {
    type TokenStream<'a> = SynonymFilterStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        SynonymFilterStream {
            dict: self.dict.clone(),
            buffer: vec![],
            cursor: 0,
            tail: self.inner.token_stream(text),
        }
    }
}

impl<T: TokenStream> SynonymFilterStream<T> {
    fn buffer_empty(&self) -> bool {
        return self.cursor >= self.buffer.len();
    }

    fn next_tail(&mut self) -> bool {
        if self.tail.advance() {
            let token = self.tail.token();
            if let Some(list) = self.dict.get(&token.text) {
                for s in list {
                    self.buffer.push(Token {
                        offset_from: token.offset_from,
                        offset_to: token.offset_to,
                        position: token.position,
                        text: s.clone(),
                        position_length: token.position_length,
                    });
                }
            }
            return true;
        }
        false
    }
}

impl<T: TokenStream> TokenStream for SynonymFilterStream<T> {
    fn advance(&mut self) -> bool {
        if !self.buffer_empty() {
            self.cursor += 1;
        }

        if self.buffer_empty() {
            return self.next_tail();
        }
        true
    }

    fn token(&self) -> &Token {
        if !self.buffer_empty() {
            return &self.buffer.get(self.cursor).unwrap();
        }
        self.tail.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::SynonymFilter;
    use crate::analyzer::tokenizers::standard_builder;
    use crate::log::init_log;
    use serde_json as json;

    #[test]
    fn test_synonym_filter() {
        init_log();
        let params = r#"{
            "type": "synonym",
            "expand": false,
            "synonyms": ["test, tests", "valid => val"]
        }"#;
        let json_params = json::from_str::<json::Value>(&params).unwrap();
        let filter = SynonymFilter::from_json(json_params.as_object().unwrap());
        assert!(filter.is_ok(), "error: {}", filter.err().unwrap());
        let builder = standard_builder().filter(filter.unwrap());
        let mut analyzer = builder.build();

        let mut stream = analyzer.token_stream("test valid token");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            let token = stream.token();
            results.push(token.text.clone());
        }

        print!("test tokens :{:?}\n", results)
    }
}
