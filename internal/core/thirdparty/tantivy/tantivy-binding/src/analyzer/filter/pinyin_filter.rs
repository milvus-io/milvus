use crate::error::{Result, TantivyBindingError};
use pinyin::ToPinyin;
use serde_json as json;
use tantivy::tokenizer::{Token, TokenFilter, TokenStream, Tokenizer};

#[derive(Clone)]
pub struct PinyinOptions {
    // Whether to keep the original token
    keep_original: bool,
    // Whether to keep full pinyin tokens for each chinese character
    keep_full_pinyin: bool,
    // Whether to keep joined full pinyin token for the whole chinese word
    keep_joined_full_pinyin: bool,
    // Whether to keep separate first letter tokens for whole chinese word
    keep_separate_first_letter: bool,
}

impl PinyinOptions {
    pub fn default() -> Self {
        Self {
            keep_original: true,
            keep_full_pinyin: true,
            keep_joined_full_pinyin: false,
            keep_separate_first_letter: false,
        }
    }
}

#[derive(Clone)]
pub struct PinyinFilter {
    options: PinyinOptions,
}

impl Default for PinyinFilter {
    fn default() -> Self {
        Self {
            options: PinyinOptions::default(),
        }
    }
}

impl PinyinFilter {
    pub fn from_json(value: &json::Map<String, json::Value>) -> Result<PinyinFilter> {
        let mut options = PinyinOptions::default();

        if let Some(v) = value.get("keep_original") {
            options.keep_original = v.as_bool().ok_or_else(|| {
                TantivyBindingError::InternalError(
                    "keep_original must be a boolean value".to_string(),
                )
            })?;
        }

        if let Some(v) = value.get("keep_full_pinyin") {
            options.keep_full_pinyin = v.as_bool().ok_or_else(|| {
                TantivyBindingError::InternalError(
                    "keep_full_pinyin must be a boolean value".to_string(),
                )
            })?;
        }

        if let Some(v) = value.get("keep_joined_full_pinyin") {
            options.keep_joined_full_pinyin = v.as_bool().ok_or_else(|| {
                TantivyBindingError::InternalError(
                    "keep_joined_full_pinyin must be a boolean value".to_string(),
                )
            })?;
        }

        if let Some(v) = value.get("keep_separate_first_letter") {
            options.keep_separate_first_letter = v.as_bool().ok_or_else(|| {
                TantivyBindingError::InternalError(
                    "keep_separate_first_letter must be a boolean value".to_string(),
                )
            })?;
        }

        Ok(Self { options })
    }
}

impl TokenFilter for PinyinFilter {
    type Tokenizer<T: Tokenizer> = PinyinFilterWrapper<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> PinyinFilterWrapper<T> {
        PinyinFilterWrapper {
            inner: tokenizer,
            options: self.options,
        }
    }
}

#[derive(Clone)]
pub struct PinyinFilterWrapper<T> {
    inner: T,
    options: PinyinOptions,
}

impl<T: Tokenizer> Tokenizer for PinyinFilterWrapper<T> {
    type TokenStream<'a> = PinyinFilterStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        PinyinFilterStream {
            options: self.options.clone(),
            cache: Vec::new(),
            index: 0,
            tail: self.inner.token_stream(text),
        }
    }
}

pub struct PinyinFilterStream<T> {
    options: PinyinOptions,
    cache: Vec<Token>,
    index: usize,
    tail: T,
}

impl<T> PinyinFilterStream<T> {
    pub fn cache_clean(&mut self) {
        self.cache.clear();
        self.index = 0;
    }

    fn cache_valid(&self) -> bool {
        self.index <= self.cache.len()
    }
}

impl<T: TokenStream> TokenStream for PinyinFilterStream<T> {
    fn advance(&mut self) -> bool {
        self.index += 1;
        while !self.cache_valid() {
            if !self.tail.advance() {
                return false;
            }
            self.cache_clean();
            self.index += 1;

            if self.options.keep_original {
                self.cache.push(self.tail.token().clone());
            }

            let mut join_pinyin = String::new();
            let mut first_letter = String::new();
            for (index, char) in self
                .tail
                .token()
                .text
                .as_str()
                .to_pinyin()
                .flatten()
                .enumerate()
            {
                if self.options.keep_full_pinyin {
                    let mut start_position = self.tail.token().position;
                    if index <= self.tail.token().position_length {
                        start_position = start_position + index;
                    }
                    self.cache.push(Token {
                        text: char.plain().to_string(),
                        offset_from: self.tail.token().offset_from,
                        offset_to: self.tail.token().offset_to,
                        position: start_position,
                        position_length: 1,
                    })
                }

                if self.options.keep_joined_full_pinyin {
                    join_pinyin.push_str(char.plain());
                }

                if self.options.keep_separate_first_letter {
                    first_letter.push_str(char.first_letter());
                }
            }
            if self.options.keep_joined_full_pinyin && !join_pinyin.is_empty() {
                self.cache.push(Token {
                    text: join_pinyin,
                    offset_from: self.tail.token().offset_from,
                    offset_to: self.tail.token().offset_to,
                    position: self.tail.token().position,
                    position_length: self.tail.token().position_length,
                })
            }

            if self.options.keep_separate_first_letter && !first_letter.is_empty() {
                self.cache.push(Token {
                    text: first_letter,
                    offset_from: self.tail.token().offset_from,
                    offset_to: self.tail.token().offset_to,
                    position: self.tail.token().position,
                    position_length: self.tail.token().position_length,
                })
            }
        }

        return true;
    }

    fn token(&self) -> &Token {
        &self.cache[self.index - 1]
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.cache[self.index - 1]
    }
}

#[cfg(test)]
mod tests {
    use crate::analyzer::analyzer::create_analyzer;

    fn is_subset<T: PartialEq>(subset: &[T], superset: &[T]) -> bool {
        subset.iter().all(|item| superset.contains(item))
    }

    #[test]
    fn test_pinyin_filter_with_joined_full_pinyin() {
        let params = r#"{
            "tokenizer": "jieba",
            "filter": [{
                "type": "pinyin",
                "keep_original": true,
                "keep_full_pinyin": false,
                "keep_joined_full_pinyin": true,
                "keep_separate_first_letter": false
            }]
        }"#;

        let tokenizer = create_analyzer(&params.to_string(), "");
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());

        let mut bining = tokenizer.unwrap();
        let mut stream = bining.token_stream("中文测试");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            let token = stream.token();
            results.push(token.text.clone());
        }

        let expected: Vec<String> = vec!["zhongwen".to_string(), "ceshi".to_string()];
        print!("test tokens :{:?}\n", results);
        assert!(is_subset(&expected, &results));
    }

    #[test]
    fn test_pinyin_filter_with_full_pinyin() {
        let params = r#"{
            "tokenizer": "jieba",
            "filter": [{
                "type": "pinyin",
                "keep_original": true,
                "keep_full_pinyin": true,
                "keep_joined_full_pinyin": false,
                "keep_separate_first_letter": false
            }]
        }"#;

        let tokenizer = create_analyzer(&params.to_string(), "");
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());

        let mut bining = tokenizer.unwrap();
        let mut stream = bining.token_stream("中文测试");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            let token = stream.token();
            results.push(token.text.clone());
        }

        let expected: Vec<String> = vec![
            "zhong".to_string(),
            "wen".to_string(),
            "ce".to_string(),
            "shi".to_string(),
        ];
        println!("test tokens :{:?}", results);
        assert!(is_subset(&expected, &results));
    }

    #[test]
    fn test_pinyin_filter_with_first_letter() {
        let params = r#"{
            "tokenizer": "jieba",
            "filter": [{
                "type": "pinyin",
                "keep_original": true,
                "keep_full_pinyin": false,
                "keep_joined_full_pinyin": false,
                "keep_separate_first_letter": true
            }]
        }"#;

        let tokenizer = create_analyzer(&params.to_string(), "");
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());

        let mut bining = tokenizer.unwrap();
        let mut stream = bining.token_stream("中文测试");

        let mut results = Vec::<String>::new();
        while stream.advance() {
            let token = stream.token();
            results.push(token.text.clone());
        }

        let expected: Vec<String> = vec!["zw".to_string(), "cs".to_string()];
        print!("test tokens :{:?}\n", results);
        assert!(is_subset(&expected, &results));
    }
}
