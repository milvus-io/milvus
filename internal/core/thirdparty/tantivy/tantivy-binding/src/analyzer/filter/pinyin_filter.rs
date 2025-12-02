use crate::error::{Result, TantivyBindingError};
use pinyin::ToPinyin;
use serde_json as json;
use tantivy::tokenizer::{Token, TokenFilter, TokenStream, Tokenizer};

#[derive(Clone)]
pub struct PinyinOptions {
    keep_original: bool,
    keep_full_pinyin: bool,
    keep_joined_full_pinyin: bool,
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

impl PinyinFilter {
    pub fn default() -> PinyinFilter {
        Self {
            options: PinyinOptions::default(),
        }
    }

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
    pub fn clear_cache(&mut self) {
        self.cache.clear();
        self.index = 1;
    }

    fn cache_vaild(&self) -> bool {
        self.index <= self.cache.len()
    }
}

impl<T: TokenStream> TokenStream for PinyinFilterStream<T> {
    fn advance(&mut self) -> bool {
        self.index += 1;
        while !self.cache_vaild() {
            if !self.tail.advance() {
                return false;
            }
            self.clear_cache();

            if self.options.keep_original {
                self.cache.push(self.tail.token().clone());
            }

            let mut join_pinyin: String = "".to_string();
            let mut first_letter: String = "".to_string();
            for char in self.tail.token().text.as_str().to_pinyin().flatten() {
                if self.options.keep_full_pinyin {
                    self.cache.push(Token {
                        text: char.plain().to_string(),
                        offset_from: self.tail.token().offset_from,
                        offset_to: self.tail.token().offset_to,
                        position: self.tail.token().position,
                        position_length: self.tail.token().position_length,
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
        self.tail.token_mut()
    }
}
