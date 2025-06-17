use std::ffi::c_void;

use tantivy::{
    query::BooleanQuery,
    tokenizer::{TextAnalyzer, TokenStream},
    Term,
};

use crate::error::Result;
use crate::{analyzer::standard_analyzer, index_reader::IndexReaderWrapper};

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
}
