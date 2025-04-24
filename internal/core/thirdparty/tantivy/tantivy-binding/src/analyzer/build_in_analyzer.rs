use tantivy::tokenizer::*;

use super::tokenizers::*;
use super::filter::*;
use super::filter::stop_words;

// default build-in analyzer
pub(crate) fn standard_analyzer(stop_words: Vec<String>) -> TextAnalyzer {
    let builder = standard_builder().filter(LowerCaser);

    if stop_words.len() > 0 {
        return builder.filter(StopWordFilter::remove(stop_words)).build();
    }

    builder.build()
}

pub fn chinese_analyzer(stop_words: Vec<String>) -> TextAnalyzer {
    let builder = jieba_builder(None).unwrap().filter(CnAlphaNumOnlyFilter);
    if stop_words.len() > 0 {
        return builder.filter(StopWordFilter::remove(stop_words)).build();
    }

    builder.build()
}

pub fn english_analyzer(stop_words: Vec<String>) -> TextAnalyzer {
    let builder = standard_builder()
        .filter(LowerCaser)
        .filter(Stemmer::new(Language::English))
        .filter(StopWordFilter::remove(
            stop_words::ENGLISH.iter().map(|&word| word.to_owned()),
        ));

    if stop_words.len() > 0 {
        return builder.filter(StopWordFilter::remove(stop_words)).build();
    }

    builder.build()
}