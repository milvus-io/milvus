use crate::analyzer::options::{get_resource_path, read_resource_file, FileResourcePathHelper};
use crate::error::{Result, TantivyBindingError};
use serde_json as json;
use std::collections::{HashMap, HashSet};
use std::io::BufRead;
use std::sync::Arc;
use tantivy::tokenizer::{Token, TokenFilter, TokenStream, Tokenizer};

// Why a synonym rule could not be parsed. It carries no rule text, so a rule
// read from a file can be reported without echoing the file content.
#[derive(Debug, PartialEq)]
enum SynonymRowError {
    EscapeAtEnd,
    InvalidEscape(char),
    MultipleMapping,
    SpaceBetweenWords,
}

impl SynonymRowError {
    // Message for a rule given inline in the analyzer params.
    fn inline_message(&self, row: &str) -> String {
        match self {
            SynonymRowError::EscapeAtEnd => format!("invalid synonym escaped in the end: {}", row),
            SynonymRowError::InvalidEscape(c) => {
                format!("invalid synonym escaped: \\{} in {}", c, row)
            }
            SynonymRowError::MultipleMapping => format!(
                "read synonym dict failed, has more than one \"=>\" in {}",
                row
            ),
            SynonymRowError::SpaceBetweenWords => format!(
                "read synonym dict failed, has space between words {}, please use \\ to escape space",
                row
            ),
        }
    }

    // Message for a rule read from a file: line number only.
    fn file_message(&self, line: usize) -> String {
        let reason = match self {
            SynonymRowError::EscapeAtEnd => "invalid escape at the end of the rule",
            SynonymRowError::InvalidEscape(_) => "invalid escape sequence",
            SynonymRowError::MultipleMapping => "more than one \"=>\"",
            SynonymRowError::SpaceBetweenWords => {
                "space between words, please use \\ to escape space"
            }
        };
        format!(
            "read synonyms dict file failed at line {}: {}",
            line, reason
        )
    }
}

pub struct SynonymDictBuilder {
    dict: HashMap<String, HashSet<String>>,
    expand: bool,
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
        if words.is_empty() {
            return;
        }

        for (_, key_word) in words.iter().enumerate() {
            let push_vec = if self.expand {
                words.clone()
            } else {
                vec![words.first().cloned().unwrap_or_default()]
            };

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

    // read row from synonyms dict
    // use "A, B, C" to represent A, B, C was synonym for each other
    // use "A => B, C" to represent A will map to B and C
    // "=>", ",", " " are special characters, should be escaped with "\" if you want to use them as normal characters
    // synonyms dict don't support space between words, please use "\" to escape space
    // TODO: synonyms group support space between words
    fn add_row(&mut self, str: &str) -> std::result::Result<(), SynonymRowError> {
        let mut is_mapping = false;
        let mut space_flag = false;
        let mut left: Vec<String> = Vec::new();
        let mut right: Vec<String> = Vec::new();
        let mut current = String::new();

        let chars = str.chars().collect::<Vec<char>>();
        let mut i = 0;
        while i < chars.len() {
            // handle escape
            if chars[i] == '\\' {
                if i + 1 >= chars.len() {
                    return Err(SynonymRowError::EscapeAtEnd);
                }
                if chars[i + 1] == ',' || chars[i + 1] == '\\' || chars[i + 1] == ' ' {
                    current.push(chars[i + 1]);
                    i += 2;
                    continue;
                }

                if chars[i + 1] == '=' && i + 2 < chars.len() && chars[i + 2] == '>' {
                    current.push(chars[i + 1]);
                    current.push(chars[i + 2]);
                    i += 3;
                    continue;
                }

                return Err(SynonymRowError::InvalidEscape(chars[i + 1]));
            }

            // handle space
            if chars[i] == ' ' {
                if !current.is_empty() {
                    // skip space after words and set space flag
                    while i + 1 < chars.len() && chars[i + 1] == ' ' {
                        i += 1;
                    }
                    space_flag = true;
                }
                i += 1;
                continue;
            }

            // push current to left or right
            if chars[i] == ',' {
                if !current.is_empty() {
                    if is_mapping {
                        right.push(current);
                    } else {
                        left.push(current);
                    }
                }
                current = String::new();
                space_flag = false;
                i += 1;
                continue;
            }

            // handle mapping
            if chars[i] == '=' && i + 1 < chars.len() && chars[i + 1] == '>' {
                if is_mapping {
                    return Err(SynonymRowError::MultipleMapping);
                } else {
                    is_mapping = true;
                    if !current.is_empty() {
                        left.push(current);
                    }
                    current = String::new();
                    space_flag = false;
                }
                i += 2;
                continue;
            }

            if space_flag {
                return Err(SynonymRowError::SpaceBetweenWords);
            }

            current.push(chars[i]);
            i += 1;
        }

        // push remaining to left or right
        if !current.is_empty() {
            if is_mapping {
                right.push(current);
            } else {
                left.push(current);
            }
        }

        // add to dict
        if is_mapping {
            self.add_mapping(left, right);
        } else {
            self.add_group(left);
        }

        Ok(())
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
}

impl SynonymFilter {
    pub fn from_json(
        params: &json::Map<String, json::Value>,
        helper: &mut FileResourcePathHelper,
    ) -> Result<SynonymFilter> {
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
                    builder
                        .add_row(s)
                        .map_err(|e| TantivyBindingError::InvalidArgument(e.inline_message(s)))
                })?;
        }

        if let Some(file_params) = params.get("synonyms_file") {
            let path = get_resource_path(helper, file_params, "synonyms dict file")?;
            let data = read_resource_file(&path, "synonyms dict file")?;
            for (line_index, line) in data.lines().enumerate() {
                if let Ok(row_data) = line {
                    builder.add_row(&row_data).map_err(|e| {
                        TantivyBindingError::InvalidArgument(e.file_message(line_index + 1))
                    })?;
                } else {
                    return Err(TantivyBindingError::InternalError(format!(
                        "read synonyms dict file failed, error: {}",
                        line.unwrap_err().to_string()
                    )));
                }
            }
        }

        Ok(SynonymFilter {
            dict: Arc::new(builder.build()),
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
            self.buffer.clear();
            self.cursor = 0;
            if let Some(list) = self.dict.get(&token.text) {
                if list.is_empty() {
                    return true;
                }

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
    use crate::analyzer::options::test_util::{assert_invalid_argument, assert_not_allowed};
    use crate::analyzer::options::test_util::{helper_allowing, test_data_dir};
    use crate::analyzer::options::{FileResourcePathHelper, ResourceInfo};
    use crate::analyzer::tokenizers::standard_builder;
    use crate::log::init_log;

    use serde_json as json;
    use std::collections::HashSet;
    use std::sync::Arc;

    #[test]
    fn test_synonym_filter() {
        init_log();
        let params = r#"{
            "type": "synonym",
            "expand": false,
            "synonyms": ["trans => translate, \\=>", "\\\\test, test, tests"]
        }"#;
        let json_params = json::from_str::<json::Value>(&params).unwrap();
        let mut helper = FileResourcePathHelper::new(Arc::new(ResourceInfo::new()));
        let filter = SynonymFilter::from_json(json_params.as_object().unwrap(), &mut helper);
        assert!(filter.is_ok(), "error: {}", filter.err().unwrap());
        let builder = standard_builder().filter(filter.unwrap());
        let mut analyzer = builder.build();
        let mut stream = analyzer.token_stream("test trans synonym");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            let token = stream.token();
            results.push(token.text.clone());
        }

        assert_eq!(
            results
                .iter()
                .map(|s| s.as_str())
                .collect::<HashSet<&str>>(),
            HashSet::from(["\\test", "translate", "=>", "synonym"])
        );
    }

    #[test]
    fn test_synonym_filter_with_file() {
        init_log();
        let synonyms_path = test_data_dir().join("synonyms_dict.txt");
        let synonyms_path_str = synonyms_path.to_string_lossy().to_string();
        let params = format!(
            r#"{{
                "type": "synonym",
                "synonyms_file": {{
                    "type": "local",
                    "path": "{synonyms_path_str}"
                }}
            }}"#
        );
        let json_params = json::from_str::<json::Value>(&params).unwrap();
        let mut helper = helper_allowing(&[test_data_dir()]);
        let filter = SynonymFilter::from_json(json_params.as_object().unwrap(), &mut helper);
        assert!(filter.is_ok(), "error: {}", filter.err().unwrap());
        let builder = standard_builder().filter(filter.unwrap());
        let mut analyzer = builder.build();
        let mut stream = analyzer.token_stream("distance interval");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            let token = stream.token();
            results.push(token.text.clone());
        }

        assert_eq!(
            results
                .iter()
                .map(|s| s.as_str())
                .collect::<HashSet<&str>>(),
            HashSet::from(["distance", "range", "span", "length", "interval", "gap"])
        );
    }

    fn synonym_file_params(path: &str) -> json::Value {
        json::json!({
            "type": "synonym",
            "synonyms_file": {"type": "local", "path": path}
        })
    }

    #[test]
    fn test_synonym_file_errors_do_not_contain_file_content() {
        init_log();
        let dir = tempfile::tempdir().unwrap();
        let cases = [
            "good, rule\nroot:x:0:0:root:/root:/bin/bash secret-value\n",
            "good, rule\nsecret => a => b\n",
            "good, rule\nsecret\\x\n",
            "good, rule\nsecret\\\n",
        ];
        for (i, content) in cases.iter().enumerate() {
            let path = dir.path().join(format!("synonyms_{}.txt", i));
            std::fs::write(&path, content).unwrap();
            let params = synonym_file_params(path.to_str().unwrap());
            let mut helper = helper_allowing(&[dir.path()]);
            let err = SynonymFilter::from_json(params.as_object().unwrap(), &mut helper)
                .err()
                .unwrap();
            let msg = err.to_string();
            assert_invalid_argument(&err, &msg);
            assert!(msg.contains("line 2"), "{}", msg);
            assert!(!msg.contains("secret"), "{}", msg);
            assert!(!msg.contains("root"), "{}", msg);
        }
    }

    #[test]
    fn test_synonym_inline_errors_keep_rule_text() {
        init_log();
        let params = json::json!({"type": "synonym", "synonyms": ["a b, c"]});
        let mut helper = FileResourcePathHelper::new(Arc::new(ResourceInfo::new()));
        let err = SynonymFilter::from_json(params.as_object().unwrap(), &mut helper)
            .err()
            .unwrap();
        assert!(err.to_string().contains("a b, c"), "{}", err);
    }

    #[test]
    fn test_synonym_file_outside_allowed_dirs_is_rejected() {
        init_log();
        let allowed = tempfile::tempdir().unwrap();
        // A real dictionary outside the allowed directories is rejected too.
        let outside = test_data_dir().join("synonyms_dict.txt");
        for path in ["/etc/passwd", outside.to_str().unwrap(), "/dev/zero"] {
            let params = synonym_file_params(path);
            let mut helper = helper_allowing(&[allowed.path()]);
            let err = SynonymFilter::from_json(params.as_object().unwrap(), &mut helper)
                .err()
                .unwrap();
            assert_not_allowed(&err, path);
        }
    }
}
