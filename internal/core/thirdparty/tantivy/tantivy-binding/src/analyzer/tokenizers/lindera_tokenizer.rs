use core::result::Result::Err;
use std::collections::HashSet;

use lindera::dictionary::{load_dictionary_from_kind, DictionaryKind};
use lindera::mode::Mode;
use lindera::segmenter::Segmenter;
use lindera::token::Token as LToken;
use lindera::tokenizer::Tokenizer as LTokenizer;
use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

use lindera::token_filter::japanese_compound_word::JapaneseCompoundWordTokenFilter;
use lindera::token_filter::japanese_keep_tags::JapaneseKeepTagsTokenFilter;
use lindera::token_filter::japanese_stop_tags::JapaneseStopTagsTokenFilter;
use lindera::token_filter::korean_keep_tags::KoreanKeepTagsTokenFilter;
use lindera::token_filter::korean_stop_tags::KoreanStopTagsTokenFilter;
use lindera::token_filter::BoxTokenFilter as LTokenFilter;

use crate::error::{Result, TantivyBindingError};
use serde_json as json;

pub struct LinderaTokenStream<'a> {
    pub tokens: Vec<LToken<'a>>,
    pub token: &'a mut Token,
}

const DICTKINDKEY: &str = "dict_kind";
const FILTERKEY: &str = "filter";

impl<'a> TokenStream for LinderaTokenStream<'a> {
    fn advance(&mut self) -> bool {
        if self.tokens.is_empty() {
            return false;
        }
        let token = self.tokens.remove(0);
        self.token.text = token.text.to_string();
        self.token.offset_from = token.byte_start;
        self.token.offset_to = token.byte_end;
        self.token.position = token.position;
        self.token.position_length = token.position_length;

        true
    }

    fn token(&self) -> &Token {
        self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        self.token
    }
}

#[derive(Clone)]
pub struct LinderaTokenizer {
    tokenizer: LTokenizer,
    token: Token,
}

impl LinderaTokenizer {
    /// Create a new `LinderaTokenizer`.
    /// This function will create a new `LinderaTokenizer` with json parameters.
    pub fn from_json(params: &json::Map<String, json::Value>) -> Result<LinderaTokenizer> {
        let kind = fetch_lindera_kind(params)?;
        let dictionary = load_dictionary_from_kind(kind.clone()).map_err(|_| {
            TantivyBindingError::InvalidArgument(format!(
                "lindera tokenizer with invalid dict_kind"
            ))
        })?;

        let segmenter = Segmenter::new(Mode::Normal, dictionary, None);
        let mut tokenizer = LinderaTokenizer::from_segmenter(segmenter);

        // append lindera filter
        let filters = fetch_lindera_token_filters(&kind, params)?;
        for filter in filters {
            tokenizer.append_token_filter(filter)
        }

        Ok(tokenizer)
    }

    /// Create a new `LinderaTokenizer`.
    /// This function will create a new `LinderaTokenizer` with the specified `lindera::segmenter::Segmenter`.
    pub fn from_segmenter(segmenter: lindera::segmenter::Segmenter) -> LinderaTokenizer {
        LinderaTokenizer {
            tokenizer: LTokenizer::new(segmenter),
            token: Default::default(),
        }
    }

    pub fn append_token_filter(&mut self, filter: LTokenFilter) {
        self.tokenizer.append_token_filter(filter);
    }
}

impl Tokenizer for LinderaTokenizer {
    type TokenStream<'a> = LinderaTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> LinderaTokenStream<'a> {
        self.token.reset();
        LinderaTokenStream {
            tokens: self.tokenizer.tokenize(text).unwrap(),
            token: &mut self.token,
        }
    }
}

trait DictionaryKindParser {
    fn into_dict_kind(self) -> Result<DictionaryKind>;
}

impl DictionaryKindParser for &str {
    fn into_dict_kind(self) -> Result<DictionaryKind> {
        match self {
            "ipadic" => Ok(DictionaryKind::IPADIC),
            "ipadic-neologd" => Ok(DictionaryKind::IPADICNEologd),
            "unidic" => Ok(DictionaryKind::UniDic),
            "ko-dic" => Ok(DictionaryKind::KoDic),
            "cc-cedict" => Ok(DictionaryKind::CcCedict),
            other => Err(TantivyBindingError::InvalidArgument(format!(
                "unsupported lindera dict type: {}",
                other
            ))),
        }
    }
}

fn fetch_lindera_kind(params: &json::Map<String, json::Value>) -> Result<DictionaryKind> {
    params
        .get(DICTKINDKEY)
        .ok_or_else(|| {
            TantivyBindingError::InvalidArgument(format!("lindera tokenizer dict_kind must be set"))
        })?
        .as_str()
        .ok_or_else(|| {
            TantivyBindingError::InvalidArgument(format!(
                "lindera tokenizer dict kind should be string"
            ))
        })?
        .into_dict_kind()
}

fn fetch_lindera_tags_from_params(
    params: &json::Map<String, json::Value>,
) -> Result<HashSet<String>> {
    params
        .get("tags")
        .ok_or_else(|| {
            TantivyBindingError::InvalidArgument(format!(
                "lindera japanese stop tag filter tags must be set"
            ))
        })?
        .as_array()
        .ok_or_else(|| {
            TantivyBindingError::InvalidArgument(format!(
                "lindera japanese stop tags filter tags must be array"
            ))
        })?
        .iter()
        .map(|v| {
            v.as_str()
                .ok_or_else(|| {
                    TantivyBindingError::InvalidArgument(format!(
                        "lindera japanese stop tags filter tags must be string"
                    ))
                })
                .map(|s| s.to_string())
        })
        .collect::<Result<HashSet<String>>>()
}

fn fetch_japanese_compound_word_token_filter(
    kind: &DictionaryKind,
    params: Option<&json::Map<String, json::Value>>,
) -> Result<LTokenFilter> {
    let filter_param = params.ok_or_else(|| {
        TantivyBindingError::InvalidArgument(format!(
            "lindera japanese compound word filter must use with params"
        ))
    })?;

    let tags: HashSet<String> = fetch_lindera_tags_from_params(filter_param)?;

    let new_tag: Option<String> = filter_param
        .get("new_tag")
        .map(|v| {
            v.as_str()
                .ok_or_else(|| {
                    TantivyBindingError::InvalidArgument(format!(
                        "lindera japanese compound word filter new_tag must be string"
                    ))
                })
                .map(|s| s.to_string())
        })
        .transpose()?;
    Ok(JapaneseCompoundWordTokenFilter::new(kind.clone(), tags, new_tag).into())
}

fn fetch_japanese_keep_tags_token_filter(
    params: Option<&json::Map<String, json::Value>>,
) -> Result<LTokenFilter> {
    Ok(
        JapaneseKeepTagsTokenFilter::new(fetch_lindera_tags_from_params(params.ok_or_else(
            || {
                TantivyBindingError::InvalidArgument(format!(
                    "lindera japanese keep tags filter must use with params"
                ))
            },
        )?)?)
        .into(),
    )
}

fn fetch_japanese_stop_tags_token_filter(
    params: Option<&json::Map<String, json::Value>>,
) -> Result<LTokenFilter> {
    Ok(
        JapaneseStopTagsTokenFilter::new(fetch_lindera_tags_from_params(params.ok_or_else(
            || {
                TantivyBindingError::InvalidArgument(format!(
                    "lindera japanese stop tags filter must use with params"
                ))
            },
        )?)?)
        .into(),
    )
}

fn fetch_korean_keep_tags_token_filter(
    params: Option<&json::Map<String, json::Value>>,
) -> Result<LTokenFilter> {
    Ok(
        KoreanKeepTagsTokenFilter::new(fetch_lindera_tags_from_params(params.ok_or_else(
            || {
                TantivyBindingError::InvalidArgument(format!(
                    "lindera korean keep tags filter must use with params"
                ))
            },
        )?)?)
        .into(),
    )
}

fn fetch_korean_stop_tags_token_filter(
    params: Option<&json::Map<String, json::Value>>,
) -> Result<LTokenFilter> {
    Ok(
        KoreanStopTagsTokenFilter::new(fetch_lindera_tags_from_params(params.ok_or_else(
            || {
                TantivyBindingError::InvalidArgument(format!(
                    "lindera korean stop tags filter must use with params"
                ))
            },
        )?)?)
        .into(),
    )
}

fn fetch_lindera_token_filter_params(
    params: &json::Value,
) -> Result<(&str, Option<&json::Map<String, json::Value>>)> {
    if params.is_string() {
        return Ok((params.as_str().unwrap(), None));
    }

    let kind = params
        .as_object()
        .ok_or_else(|| {
            TantivyBindingError::InvalidArgument(format!(
                "lindera tokenizer filter params must be object"
            ))
        })?
        .get("kind")
        .ok_or_else(|| {
            TantivyBindingError::InvalidArgument(format!("lindera tokenizer filter must have type"))
        })?
        .as_str()
        .ok_or_else(|| {
            TantivyBindingError::InvalidArgument(format!(
                "lindera tokenizer filter type should be string"
            ))
        })?;

    Ok((kind, Some(params.as_object().unwrap())))
}

fn fetch_lindera_token_filter(
    type_name: &str,
    kind: &DictionaryKind,
    params: Option<&json::Map<String, json::Value>>,
) -> Result<LTokenFilter> {
    match type_name {
        "japanese_compound_word" => fetch_japanese_compound_word_token_filter(kind, params),
        "japanese_keep_tags" => fetch_japanese_keep_tags_token_filter(params),
        "japanese_stop_tags" => fetch_japanese_stop_tags_token_filter(params),
        "korean_keep_tags" => fetch_korean_keep_tags_token_filter(params),
        "korean_stop_tags" => fetch_korean_stop_tags_token_filter(params),
        _ => Err(TantivyBindingError::InvalidArgument(format!(
            "unknown lindera filter type"
        ))),
    }
}

fn fetch_lindera_token_filters(
    kind: &DictionaryKind,
    params: &json::Map<String, json::Value>,
) -> Result<Vec<LTokenFilter>> {
    let mut result: Vec<LTokenFilter> = vec![];

    match params.get(FILTERKEY) {
        Some(v) => {
            let filter_list = v.as_array().ok_or_else(|| {
                TantivyBindingError::InvalidArgument(format!("lindera filters should be array"))
            })?;

            for filter_params in filter_list {
                let (name, params) = fetch_lindera_token_filter_params(filter_params)?;
                let filter = fetch_lindera_token_filter(name, kind, params)?;
                result.push(filter);
            }
        }
        _ => {}
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use serde_json as json;
    use tantivy::tokenizer::Tokenizer;

    use crate::analyzer::tokenizers::lindera_tokenizer::LinderaTokenizer;

    #[test]
    fn test_lindera_tokenizer() {
        let params = r#"{
            "type": "lindera",
            "dict_kind": "ipadic",
            "filter": [{
                "kind": "japanese_stop_tags",
                "tags": ["接続詞", "助詞", "助詞,格助詞", "助詞,連体化"]
            }]
        }"#;
        let json_param = json::from_str::<json::Map<String, json::Value>>(&params);
        assert!(json_param.is_ok());

        let tokenizer = LinderaTokenizer::from_json(&json_param.unwrap());
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());

        let mut binding = tokenizer.unwrap();
        let stream =
            binding.token_stream("東京スカイツリーの最寄り駅はとうきょうスカイツリー駅です");
        let mut results = Vec::<String>::new();
        for token in stream.tokens {
            results.push(token.text.to_string());
        }

        print!("test tokens :{:?}\n", results)
    }

    #[test]
    #[cfg(feature = "lindera-cc-cedict")]
    fn test_lindera_tokenizer_cc() {
        let params = r#"{
            "type": "lindera",
            "dict_kind": "cc-cedict"
        }"#;
        let json_param = json::from_str::<json::Map<String, json::Value>>(&params);
        assert!(json_param.is_ok());

        let tokenizer = LinderaTokenizer::from_json(&json_param.unwrap());
        assert!(tokenizer.is_ok(), "error: {}", tokenizer.err().unwrap());
    }
}
