use core::{option::Option::Some, result::Result::Ok};
use jieba_rs;
use lazy_static::lazy_static;
use serde_json as json;
use std::io::{BufRead, BufReader};
use std::{path::PathBuf, sync::Arc};
use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

use crate::analyzer::options::{get_resource_path, read_resource_file, FileResourcePathHelper};
use crate::error::{Result, TantivyBindingError};

const USER_DICT_KEY: &str = "jieba extra dict file";

lazy_static! {
    static ref JIEBA: Arc<jieba_rs::Jieba> = Arc::new(jieba_rs::Jieba::new());
}

static EXTEND_DEFAULT_DICT: &str = include_str!("../data/jieba/dict.txt.big");

#[allow(dead_code)]
#[derive(Clone)]
pub enum JiebaMode {
    Exact,
    Search,
}

#[derive(Clone)]
pub struct JiebaTokenizer {
    mode: JiebaMode,
    hmm: bool,
    tokenizer: Arc<jieba_rs::Jieba>,
}

pub struct JiebaTokenStream {
    tokens: Vec<Token>,
    index: usize,
}

impl TokenStream for JiebaTokenStream {
    fn advance(&mut self) -> bool {
        if self.index < self.tokens.len() {
            self.index += 1;
            true
        } else {
            false
        }
    }

    fn token(&self) -> &Token {
        &self.tokens[self.index - 1]
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.tokens[self.index - 1]
    }
}

fn get_jieba_dict(
    params: &json::Map<String, json::Value>,
    helper: &mut FileResourcePathHelper,
) -> Result<(Vec<String>, Option<String>, Option<PathBuf>)> {
    let mut words = Vec::<String>::new();
    let mut user_dict = None;
    // use default dict as default system dict
    let mut system_dict = Some("_default_".to_string());
    match params.get("dict") {
        Some(value) => {
            system_dict = None;
            if !value.is_array() {
                return Err(TantivyBindingError::InvalidArgument(format!(
                    "jieba tokenizer dict must be array"
                )));
            }

            for word in value.as_array().unwrap() {
                if !word.is_string() {
                    return Err(TantivyBindingError::InvalidArgument(format!(
                        "jieba tokenizer dict item must be string"
                    )));
                }
                let text = word.as_str().unwrap().to_string();

                // remove word if word is ""
                // add empty string to tokenizer will case panic
                if text.len() == 0 {
                    continue;
                }
                // The dictionary trie cannot store a NUL byte inside a word.
                if text.contains('\0') {
                    return Err(TantivyBindingError::InvalidArgument(format!(
                        "jieba tokenizer dict item must not contain NUL"
                    )));
                }

                if text == "_default_" || text == "_extend_default_" {
                    if system_dict.is_some() {
                        return Err(TantivyBindingError::InvalidArgument(format!(
                            "jieba tokenizer dict can only set one system dict"
                        )));
                    }
                    system_dict = Some(text)
                } else {
                    words.push(text);
                }
            }
        }
        _ => {}
    };

    match params.get("extra_dict_file") {
        Some(v) => {
            let path = get_resource_path(helper, v, USER_DICT_KEY)?;
            user_dict = Some(path)
        }
        _ => {}
    };

    Ok((words, system_dict, user_dict))
}

// Checks the user dictionary entries the way jieba-rs parses them
// ("word [freq [tag]]" per line), so that a bad entry is reported by line
// number only. jieba-rs' own error quotes the whole line.
fn check_user_dict(data: &[u8]) -> Result<()> {
    for (index, line) in data.lines().enumerate() {
        let line_no = index + 1;
        let line = line.map_err(|e| {
            TantivyBindingError::InvalidArgument(format!(
                "read {} failed at line {}: {}",
                USER_DICT_KEY, line_no, e
            ))
        })?;
        let mut fields = line.split_whitespace();
        if let Some(word) = fields.next() {
            // The dictionary trie cannot store a NUL byte inside a word.
            if word.contains('\0') {
                return Err(TantivyBindingError::InvalidArgument(format!(
                    "{} has an invalid word at line {}",
                    USER_DICT_KEY, line_no
                )));
            }
            if let Some(freq) = fields.next() {
                if freq.parse::<usize>().is_err() {
                    return Err(TantivyBindingError::InvalidArgument(format!(
                        "{} has an invalid frequency at line {}",
                        USER_DICT_KEY, line_no
                    )));
                }
            }
        }
    }
    Ok(())
}

fn load_user_dict(tokenizer: &mut jieba_rs::Jieba, path: &PathBuf) -> Result<()> {
    let data = read_resource_file(path, USER_DICT_KEY)?;
    check_user_dict(&data)?;
    tokenizer.load_dict(&mut &data[..]).map_err(|e| {
        // Never include the jieba-rs message: it quotes the dictionary line.
        let reason = match e {
            jieba_rs::Error::Io(e) => e.to_string(),
            jieba_rs::Error::InvalidDictEntry(_) => "invalid dictionary entry".to_string(),
        };
        TantivyBindingError::InvalidArgument(format!(
            "jieba tokenizer load dict file failed with error: {}",
            reason
        ))
    })
}

fn get_jieba_mode(params: &json::Map<String, json::Value>) -> Result<JiebaMode> {
    match params.get("mode") {
        Some(value) => {
            if !value.is_string() {
                return Err(TantivyBindingError::InvalidArgument(format!(
                    "jieba tokenizer mode must be string"
                )));
            }

            let mode = value.as_str().unwrap();
            match mode {
                "exact" => Ok(JiebaMode::Exact),
                "search" => Ok(JiebaMode::Search),
                _ => Err(TantivyBindingError::InvalidArgument(format!(
                    "jieba tokenizer mode must be \"exact\" or \"search\""
                ))),
            }
        }
        _ => Ok(JiebaMode::Search),
    }
}

fn get_jieba_hmm(params: &json::Map<String, json::Value>) -> Result<bool> {
    match params.get("hmm") {
        Some(value) => {
            if !value.is_boolean() {
                return Err(TantivyBindingError::InvalidArgument(format!(
                    "jieba tokenizer mode must be boolean"
                )));
            }

            return Ok(value.as_bool().unwrap());
        }
        _ => Ok(true),
    }
}

impl JiebaTokenizer {
    pub fn new() -> JiebaTokenizer {
        JiebaTokenizer {
            mode: JiebaMode::Search,
            hmm: true,
            tokenizer: JIEBA.clone(),
        }
    }

    pub fn from_json(
        params: &json::Map<String, json::Value>,
        helper: &mut FileResourcePathHelper,
    ) -> Result<JiebaTokenizer> {
        let (words, system_dict, user_dict) = get_jieba_dict(params, helper)?;

        let mut tokenizer =
            system_dict.map_or(Ok(jieba_rs::Jieba::empty()), |name| match name.as_str() {
                "_default_" => Ok(jieba_rs::Jieba::new()),
                "_extend_default_" => {
                    let mut buf = BufReader::new(EXTEND_DEFAULT_DICT.as_bytes());
                    jieba_rs::Jieba::with_dict(&mut buf).map_err(|e| {
                        TantivyBindingError::InternalError(format!(
                            "failed to load extend default system dict: {}",
                            e
                        ))
                    })
                }
                _ => Err(TantivyBindingError::InternalError(format!(
                    "invalid system dict name: {}",
                    name
                ))),
            })?;

        for word in words {
            tokenizer.add_word(word.as_str(), None, None);
        }

        if let Some(path) = user_dict {
            load_user_dict(&mut tokenizer, &path)?;
        }

        let mode = get_jieba_mode(params)?;
        let hmm = get_jieba_hmm(params)?;

        Ok(JiebaTokenizer {
            mode: mode,
            hmm: hmm,
            tokenizer: Arc::new(tokenizer),
        })
    }

    fn tokenize(&self, text: &str) -> Vec<Token> {
        let mut indices = text.char_indices().collect::<Vec<_>>();
        indices.push((text.len(), '\0'));
        let ori_tokens = match self.mode {
            JiebaMode::Exact => {
                self.tokenizer
                    .tokenize(text, jieba_rs::TokenizeMode::Default, self.hmm)
            }
            JiebaMode::Search => {
                self.tokenizer
                    .tokenize(text, jieba_rs::TokenizeMode::Search, self.hmm)
            }
        };

        let mut tokens = Vec::with_capacity(ori_tokens.len());
        for token in ori_tokens {
            tokens.push(Token {
                offset_from: indices[token.start].0,
                offset_to: indices[token.end].0,
                position: token.start,
                text: String::from(&text[(indices[token.start].0)..(indices[token.end].0)]),
                position_length: token.end - token.start,
            });
        }
        tokens
    }
}

impl Tokenizer for JiebaTokenizer {
    type TokenStream<'a> = JiebaTokenStream;

    fn token_stream(&mut self, text: &str) -> JiebaTokenStream {
        let tokens = self.tokenize(text);
        JiebaTokenStream { tokens, index: 0 }
    }
}

#[cfg(test)]
mod tests {
    use serde_json as json;
    use std::sync::Arc;

    use super::JiebaTokenizer;
    use crate::analyzer::options::test_util::helper_allowing;
    use crate::analyzer::options::test_util::{assert_invalid_argument, assert_not_allowed};
    use crate::analyzer::options::{FileResourcePathHelper, ResourceInfo};

    use tantivy::tokenizer::TokenStream;
    use tantivy::tokenizer::Tokenizer;

    #[test]
    fn test_jieba_tokenizer() {
        let params = r#"{
            "type": "jieba"
        }"#;
        let json_param = json::from_str::<json::Map<String, json::Value>>(&params);
        assert!(json_param.is_ok());

        let mut helper = FileResourcePathHelper::new(Arc::new(ResourceInfo::new()));
        let tokenizer = JiebaTokenizer::from_json(&json_param.unwrap(), &mut helper);
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());
        let mut bining = tokenizer.unwrap();
        let mut stream = bining.token_stream("结巴分词器");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            let token = stream.token();
            results.push(token.text.clone());
        }

        print!("test tokens :{:?}\n", results)
    }

    #[test]
    fn test_jieba_tokenizer_with_dict() {
        let params = r#"{
            "type": "jieba",
            "dict": ["结巴分词器"],
            "mode": "exact",
            "hmm": false
        }"#;
        let json_param = json::from_str::<json::Map<String, json::Value>>(&params);
        assert!(json_param.is_ok());

        let mut helper = FileResourcePathHelper::new(Arc::new(ResourceInfo::new()));
        let tokenizer = JiebaTokenizer::from_json(&json_param.unwrap(), &mut helper);
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());
        let mut bining = tokenizer.unwrap();
        let mut stream = bining.token_stream("milvus结巴分词器中文测试");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            let token = stream.token();
            results.push(token.text.clone());
        }

        print!("test tokens :{:?}\n", results)
    }

    #[test]
    fn test_jieba_tokenizer_with_extend_default_dict() {
        let params = r#"{
            "type": "jieba",
            "dict": ["_extend_default_"]
        }"#;
        let json_param = json::from_str::<json::Map<String, json::Value>>(&params);
        assert!(json_param.is_ok());

        let mut helper = FileResourcePathHelper::new(Arc::new(ResourceInfo::new()));
        let tokenizer = JiebaTokenizer::from_json(&json_param.unwrap(), &mut helper);
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());
        let mut bining = tokenizer.unwrap();
        let mut stream = bining.token_stream("milvus結巴分詞器中文測試");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            let token = stream.token();
            results.push(token.text.clone());
        }

        print!("test tokens :{:?}\n", results)
    }

    fn jieba_with_user_dict(
        path: &str,
        helper: &mut FileResourcePathHelper,
    ) -> crate::error::Result<JiebaTokenizer> {
        let params = json::json!({
            "type": "jieba",
            "extra_dict_file": {"type": "local", "path": path}
        });
        JiebaTokenizer::from_json(params.as_object().unwrap(), helper)
    }

    fn tokens(tokenizer: &mut JiebaTokenizer, text: &str) -> Vec<String> {
        let mut stream = tokenizer.token_stream(text);
        let mut results = Vec::new();
        while stream.advance() {
            results.push(stream.token().text.clone());
        }
        results
    }

    #[test]
    fn test_jieba_user_dict_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("user_dict.txt");
        std::fs::write(&path, "\u{6c34}\u{5e95}\u{6708}\u{5149} 100000 n\n").unwrap();
        let mut tokenizer =
            jieba_with_user_dict(path.to_str().unwrap(), &mut helper_allowing(&[dir.path()]))
                .unwrap();
        assert!(tokens(&mut tokenizer, "\u{6c34}\u{5e95}\u{6708}\u{5149}")
            .contains(&"\u{6c34}\u{5e95}\u{6708}\u{5149}".to_string()));
    }

    #[test]
    fn test_jieba_user_dict_errors_do_not_contain_file_content() {
        let dir = tempfile::tempdir().unwrap();
        let cases: [&[u8]; 3] = [
            b"word 1\nroot:x:0:0:root:/root:/bin/bash secret-value\n",
            b"word 1\nsecret\0value 1\n",
            b"word 1\nsecret \xff\n",
        ];
        for (i, content) in cases.iter().enumerate() {
            let path = dir.path().join(format!("user_dict_{}.txt", i));
            std::fs::write(&path, content).unwrap();
            let err =
                jieba_with_user_dict(path.to_str().unwrap(), &mut helper_allowing(&[dir.path()]))
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
    fn test_jieba_words_with_nul_are_rejected() {
        // Several such words corrupt the dictionary trie and panic.
        let words: Vec<String> = (0..50).map(|i| format!("k{}\0v{}", i, i)).collect();
        let mut dict = vec!["_default_".to_string()];
        dict.extend(words.iter().cloned());
        let params = json::json!({"type": "jieba", "dict": dict});
        let err = JiebaTokenizer::from_json(
            params.as_object().unwrap(),
            &mut FileResourcePathHelper::new(Arc::new(ResourceInfo::new())),
        )
        .err()
        .unwrap();
        assert_invalid_argument(&err, "inline dict");

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("user_dict.txt");
        std::fs::write(&path, words.join(" 1\n")).unwrap();
        let err = jieba_with_user_dict(path.to_str().unwrap(), &mut helper_allowing(&[dir.path()]))
            .err()
            .unwrap();
        assert_invalid_argument(&err, "user dict");
        assert!(err.to_string().contains("line 1"), "{}", err);
    }

    #[test]
    fn test_jieba_user_dict_outside_allowed_dirs_is_rejected() {
        let allowed = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let outside = other.path().join("user_dict.txt");
        std::fs::write(&outside, "root:x:0:0:root:/root:/bin/bash secret-value\n").unwrap();
        for path in [
            outside.to_str().unwrap(),
            "/etc/passwd",
            "/proc/self/environ",
            "relative/user_dict.txt",
            "/dev/zero",
        ] {
            let err = jieba_with_user_dict(path, &mut helper_allowing(&[allowed.path()]))
                .err()
                .unwrap();
            assert_not_allowed(&err, path);
            assert!(!err.to_string().contains("secret"), "{}", err);
        }
    }
}
