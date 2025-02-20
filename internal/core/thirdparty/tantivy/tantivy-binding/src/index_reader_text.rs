use tantivy::{
    query::{BooleanQuery, PhraseQuery},
    tokenizer::{TextAnalyzer, TokenStream},
    Term,
};

use crate::error::Result;
use crate::{index_reader::IndexReaderWrapper, tokenizer::standard_analyzer};

impl IndexReaderWrapper {
    // split the query string into multiple tokens using index's default tokenizer,
    // and then execute the disconjunction of term query.
    pub(crate) fn match_query(&self, q: &str) -> Result<Vec<u32>> {
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
        self.search(&query)
    }

    // split the query string into multiple tokens using index's default tokenizer,
    // and then execute the disconjunction of term query.
    pub(crate) fn phrase_match_query(&self, q: &str, slop: u32) -> Result<Vec<u32>> {
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
            return self.search(&query);
        }

        let terms_with_offset: Vec<_> = positions.into_iter().zip(terms.into_iter()).collect();
        let phrase_query = PhraseQuery::new_with_offset_and_slop(terms_with_offset, slop);
        self.search(&phrase_query)
    }

    pub(crate) fn register_tokenizer(&self, tokenizer_name: String, tokenizer: TextAnalyzer) {
        self.index.tokenizers().register(&tokenizer_name, tokenizer)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::{index_writer::IndexWriterWrapper, tokenizer::create_tokenizer};
    #[test]
    fn test_jeba() {
        let params = "{\"tokenizer\": \"jieba\"}".to_string();
        let tokenizer = create_tokenizer(&params).unwrap();
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

        writer.add_strings(&["网球和滑雪"], 0).unwrap();
        writer.add_strings(&["网球以及滑雪"], 1).unwrap();

        writer.commit().unwrap();

        let slop = 1;
        let reader = writer.create_reader().unwrap();
        let res = reader.phrase_match_query("网球滑雪", slop).unwrap();
        assert_eq!(res, vec![0]);

        let slop = 2;
        let reader = writer.create_reader().unwrap();
        let res = reader.phrase_match_query("网球滑雪", slop).unwrap();
        assert_eq!(res, vec![0, 1]);
    }
}
