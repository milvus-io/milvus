use core::result::Result::Err;
use std::collections::HashSet;
use std::{borrow::Cow, sync::Arc};

use lindera::dictionary::DictionaryKind;
use lindera::mode::Mode;
use lindera::token::Token as LToken;
use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

use lindera::token_filter::japanese_compound_word::JapaneseCompoundWordTokenFilter;
use lindera::token_filter::japanese_keep_tags::JapaneseKeepTagsTokenFilter;
use lindera::token_filter::japanese_stop_tags::JapaneseStopTagsTokenFilter;
use lindera::token_filter::korean_keep_tags::KoreanKeepTagsTokenFilter;
use lindera::token_filter::korean_stop_tags::KoreanStopTagsTokenFilter;
use lindera::token_filter::BoxTokenFilter as LTokenFilter;

use lindera::dictionary::{Dictionary, UserDictionary};
use lindera_dictionary::viterbi::Lattice;

use crate::analyzer::dict::lindera::load_dictionary_from_kind;
use crate::analyzer::filter::get_string_list;
use crate::error::{Result, TantivyBindingError};
use serde_json as json;
/// Segmenter
#[derive(Clone)]
pub struct LinderaSegmenter {
    /// The segmentation mode to be used by the segmenter.
    /// This determines how the text will be split into segments.
    pub mode: Mode,

    /// The dictionary used for segmenting text. This dictionary contains the necessary
    /// data structures and algorithms to perform morphological analysis and tokenization.
    pub dictionary: Arc<Dictionary>,

    /// An optional user-defined dictionary that can be used to customize the segmentation process.
    /// If provided, this dictionary will be used in addition to the default dictionary to improve
    /// the accuracy of segmentation for specific words or phrases.
    pub user_dictionary: Option<Arc<UserDictionary>>,
}

impl LinderaSegmenter {
    /// Creates a new instance with the specified mode, dictionary, and optional user dictionary.
    pub fn new(
        mode: Mode,
        dictionary: Dictionary,
        user_dictionary: Option<UserDictionary>,
    ) -> Self {
        Self {
            mode,
            dictionary: Arc::new(dictionary),
            user_dictionary: user_dictionary.map(|d| Arc::new(d)),
        }
    }

    pub fn segment<'a>(&'a self, text: Cow<'a, str>) -> Result<Vec<LToken<'a>>> {
        let mut tokens: Vec<LToken> = Vec::new();
        let mut lattice = Lattice::default();

        let mut position = 0_usize;
        let mut byte_position = 0_usize;

        // Split text into sentences using Japanese punctuation.
        for sentence in text.split_inclusive(&['。', '、', '\n', '\t']) {
            if sentence.is_empty() {
                continue;
            }

            lattice.set_text(
                &self.dictionary.prefix_dictionary,
                &self.user_dictionary.as_ref().map(|d| &d.dict),
                &self.dictionary.character_definition,
                &self.dictionary.unknown_dictionary,
                sentence,
                &self.mode,
            );
            lattice.calculate_path_costs(&self.dictionary.connection_cost_matrix, &self.mode);

            let offsets = lattice.tokens_offset();

            for i in 0..offsets.len() {
                let (byte_start, word_id) = offsets[i];
                let byte_end = if i == offsets.len() - 1 {
                    sentence.len()
                } else {
                    let (next_start, _word_id) = offsets[i + 1];
                    next_start
                };

                // retrieve token from its sentence byte positions
                let surface = &sentence[byte_start..byte_end];

                // compute the token's absolute byte positions
                let token_start = byte_position;
                byte_position += surface.len();
                let token_end = byte_position;

                // Use Cow::Owned to ensure the token data can be returned safely
                tokens.push(LToken::new(
                    Cow::Owned(surface.to_string()), // Clone the string here
                    token_start,
                    token_end,
                    position,
                    word_id,
                    &self.dictionary,
                    self.user_dictionary.as_deref(),
                ));

                position += 1;
            }
        }

        Ok(tokens)
    }
}

pub struct LinderaTokenStream<'a> {
    pub tokens: Vec<LToken<'a>>,
    pub token: &'a mut Token,
}

const DICTKINDKEY: &str = "dict_kind";
const DICTBUILDDIRKEY: &str = "dict_build_dir";
const DICTDOWNLOADURLKEY: &str = "download_urls";
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

pub struct LinderaTokenizer {
    segmenter: LinderaSegmenter,
    lindera_filters: Vec<LTokenFilter>,
    token: Token,
}

impl Clone for LinderaTokenizer {
    fn clone(&self) -> Self {
        let mut token_filters: Vec<LTokenFilter> = Vec::new();
        for token_filter in self.lindera_filters.iter() {
            token_filters.push(token_filter.box_clone());
        }

        Self {
            segmenter: self.segmenter.clone(),
            lindera_filters: token_filters,
            token: self.token.clone(),
        }
    }
}

impl LinderaTokenizer {
    /// Create a new `LinderaTokenizer`.
    /// This function will create a new `LinderaTokenizer` with json parameters.
    pub fn from_json(params: &json::Map<String, json::Value>) -> Result<LinderaTokenizer> {
        let kind: DictionaryKind = fetch_lindera_kind(params)?;

        // for download dict online
        let build_dir = fetch_dict_build_dir(params)?;
        let download_urls = fetch_dict_download_urls(params)?;

        let dictionary = load_dictionary_from_kind(&kind, build_dir, download_urls)?;

        let segmenter = LinderaSegmenter::new(Mode::Normal, dictionary, None);
        let mut tokenizer = LinderaTokenizer::from_segmenter(segmenter);

        // append lindera filter
        tokenizer.append_token_filter(&kind, params)?;
        Ok(tokenizer)
    }

    /// Create a new `LinderaTokenizer`.
    /// This function will create a new `LinderaTokenizer` with the specified `lindera::segmenter::Segmenter`.
    pub fn from_segmenter(segmenter: LinderaSegmenter) -> LinderaTokenizer {
        LinderaTokenizer {
            segmenter: segmenter,
            lindera_filters: vec![],
            token: Default::default(),
        }
    }

    pub fn append_token_filter(
        &mut self,
        kind: &DictionaryKind,
        params: &json::Map<String, json::Value>,
    ) -> Result<()> {
        match params.get(FILTERKEY) {
            Some(v) => {
                let filter_list = v.as_array().ok_or_else(|| {
                    TantivyBindingError::InvalidArgument(format!("lindera filters should be array"))
                })?;

                for filter_params in filter_list {
                    let (name, params) = fetch_lindera_token_filter_params(filter_params)?;
                    let filter = fetch_lindera_token_filter(name, kind, params)?;
                    self.lindera_filters.push(filter);
                }
            }
            _ => {}
        }

        Ok(())
    }
}

impl Tokenizer for LinderaTokenizer {
    type TokenStream<'a> = LinderaTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> LinderaTokenStream<'a> {
        self.token.reset();
        // Segment a text.
        let mut tokens = self
            .segmenter
            .segment(Cow::<'a, str>::Borrowed(text))
            .unwrap();

        // Apply token filters to the tokens if they are not empty.
        for token_filter in &self.lindera_filters {
            token_filter.apply(&mut tokens).unwrap();
        }

        LinderaTokenStream {
            tokens: tokens,
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
        .ok_or(TantivyBindingError::InvalidArgument(format!(
            "lindera tokenizer dict_kind must be set"
        )))?
        .as_str()
        .ok_or(TantivyBindingError::InvalidArgument(format!(
            "lindera tokenizer dict kind should be string"
        )))?
        .into_dict_kind()
}

fn fetch_dict_build_dir(params: &json::Map<String, json::Value>) -> Result<String> {
    params
        .get(DICTBUILDDIRKEY)
        .map_or(Ok("/var/lib/milvus/dict/lindera".to_string()), |v| {
            v.as_str()
                .ok_or(TantivyBindingError::InvalidArgument(format!(
                    "dict build dir must be string"
                )))
                .map(|s| s.to_string())
        })
}

fn fetch_dict_download_urls(params: &json::Map<String, json::Value>) -> Result<Vec<String>> {
    params.get(DICTDOWNLOADURLKEY).map_or(Ok(vec![]), |v| {
        get_string_list(v, "lindera dict download urls")
    })
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

#[cfg(test)]
mod tests {
    use super::LinderaTokenizer;
    use serde_json as json;
    use tantivy::tokenizer::Tokenizer;

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
