use std::ffi::c_void;

use tantivy::{
    query::{BooleanQuery, PhraseQuery},
    tokenizer::{TextAnalyzer, TokenStream},
    Term,
};

use crate::{
    analyzer::standard_analyzer, bitset_wrapper::BitsetWrapper, error::TantivyBindingError,
    index_reader::IndexReaderWrapper,
};
use crate::{direct_bitset_collector::DirectBitsetCollector, error::Result};

impl IndexReaderWrapper {
    // split the query string into multiple tokens using index's default tokenizer,
    // and then execute the disconjunction of term query.
    pub(crate) fn match_query(&self, q: &str, bitset: *mut c_void) -> Result<()> {
        // clone the tokenizer to make `match_query` thread-safe.
        let mut tokenizer = self
            .index
            .tokenizer_for_field(self.field)
            .unwrap_or(standard_analyzer(vec![]))
            .clone();
        let mut token_stream = tokenizer.token_stream(q);
        let mut terms: Vec<Term> = Vec::new();
        while token_stream.advance() {
            let token = token_stream.token();
            terms.push(Term::from_field_text(self.field, &token.text));
        }
        let query = BooleanQuery::new_multiterms_query(terms);
        self.search(&query, bitset)
    }

    // split the query string into multiple tokens using index's default tokenizer,
    // and then execute the disconjunction of term query.
    pub(crate) fn phrase_match_query(&self, q: &str, slop: u32, bitset: *mut c_void) -> Result<()> {
        // clone the tokenizer to make `match_query` thread-safe.
        let mut tokenizer = self
            .index
            .tokenizer_for_field(self.field)
            .unwrap_or(standard_analyzer(vec![]))
            .clone();
        let mut token_stream = tokenizer.token_stream(q);
        let mut terms: Vec<Term> = Vec::new();

        let mut positions = vec![];
        while token_stream.advance() {
            let token = token_stream.token();
            positions.push(token.position);
            terms.push(Term::from_field_text(self.field, &token.text));
        }
        if terms.len() <= 1 {
            // tantivy will panic when terms.len() <= 1, so we forward to text match instead.
            let query = BooleanQuery::new_multiterms_query(terms);
            return self.search(&query, bitset);
        }

        let terms_with_offset: Vec<_> = positions.into_iter().zip(terms.into_iter()).collect();
        let phrase_query = PhraseQuery::new_with_offset_and_slop(terms_with_offset, slop);
        self.search(&phrase_query, bitset)
    }

    pub(crate) fn register_tokenizer(&self, tokenizer_name: String, tokenizer: TextAnalyzer) {
        self.index.tokenizers().register(&tokenizer_name, tokenizer)
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::c_void;

    use tantivy::query::TermQuery;
    use tempfile::TempDir;

    use crate::{analyzer::create_analyzer, index_writer::IndexWriterWrapper, util::set_bitset};

    #[test]
    fn test_read() {
        let tokenizer = create_analyzer("").unwrap();
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::create_text_writer(
            "text".to_string(),
            dir.path().to_str().unwrap().to_string(),
            "default".to_string(),
            tokenizer,
            1,
            50_000_000,
            false,
        );

        for i in 0..10000 {
            writer.add_string("hello world", i).unwrap();
        }
        writer.commit().unwrap();

        let reader = writer.create_reader(set_bitset).unwrap();

        let query = TermQuery::new(
            tantivy::Term::from_field_text(reader.field.clone(), "hello"),
            tantivy::schema::IndexRecordOption::Basic,
        );

        let mut res: Vec<u32> = vec![];
        reader
            .search(&query, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res, (0..10000).collect::<Vec<u32>>());
        let res = reader.search_i64(&query).unwrap();
        assert_eq!(res, (0..10000).collect::<Vec<i64>>());
    }

    #[test]
    fn test_jeba() {
        let tokenizer = create_analyzer("{\"tokenizer\": \"jieba\"}").unwrap();
        let dir = TempDir::new().unwrap();

        let mut writer = IndexWriterWrapper::create_text_writer(
            "text".to_string(),
            dir.path().to_str().unwrap().to_string(),
            "jieba".to_string(),
            tokenizer,
            1,
            50_000_000,
            false,
        );

        writer.add_string("网球和滑雪", 0).unwrap();
        writer.add_string("网球以及滑雪", 1).unwrap();

        writer.commit().unwrap();

        let slop = 1;
        let reader = writer.create_reader(set_bitset).unwrap();
        let mut res: Vec<u32> = vec![];
        reader
            .phrase_match_query("网球滑雪", slop, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res, vec![0]);

        let slop = 2;
        let mut res: Vec<u32> = vec![];
        let reader = writer.create_reader(set_bitset).unwrap();
        reader
            .phrase_match_query("网球滑雪", slop, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res, vec![0, 1]);
    }
}
