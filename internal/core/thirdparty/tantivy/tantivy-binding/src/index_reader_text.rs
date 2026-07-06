use std::ffi::c_void;

use tantivy::{
    query::{BooleanQuery, PhraseQuery},
    tokenizer::{TextAnalyzer, TokenStream},
    Term,
};

use crate::{
    analyzer::standard_analyzer, error::TantivyBindingError, index_reader::IndexReaderWrapper,
};
use crate::{
    bitset_wrapper::BitsetWrapper, direct_bitset_collector::DirectBitsetCollector, error::Result,
};

impl IndexReaderWrapper {
    // Tokenize `q` with the field's analyzer (falling back to the standard one)
    // and return each token's position and term. A cloned tokenizer keeps the
    // text-match queries thread-safe; this is the one place that tokenizes.
    fn tokenize_terms(&self, q: &str) -> Vec<(usize, Term)> {
        let mut tokenizer = self
            .index
            .tokenizer_for_field(self.field)
            .unwrap_or_else(|_| standard_analyzer(vec![]));
        let mut token_stream = tokenizer.token_stream(q);
        let mut terms = Vec::new();
        while token_stream.advance() {
            let token = token_stream.token();
            terms.push((
                token.position,
                Term::from_field_text(self.field, &token.text),
            ));
        }
        terms
    }

    // split the query string into multiple tokens using index's default tokenizer,
    // and then execute the disconjunction of term query.
    pub(crate) fn match_query(&self, q: &str, bitset: *mut c_void) -> Result<()> {
        let terms: Vec<Term> = self.tokenize_terms(q).into_iter().map(|(_, t)| t).collect();
        let collector = DirectBitsetCollector {
            bitset_wrapper: BitsetWrapper::new(bitset, self.set_bitset),
            terms: terms.clone(),
        };
        let query = BooleanQuery::new_multiterms_query(terms);
        let searcher = self.reader.searcher();
        searcher
            .search(&query, &collector)
            .map_err(TantivyBindingError::TantivyError)
    }

    pub(crate) fn match_query_with_minimum(
        &self,
        q: &str,
        min_should_match: usize,
        bitset: *mut c_void,
    ) -> Result<()> {
        use tantivy::query::{Occur, TermQuery};
        use tantivy::schema::IndexRecordOption;
        let mut subqueries: Vec<(Occur, Box<dyn tantivy::query::Query>)> = Vec::new();
        for (_, term) in self.tokenize_terms(q) {
            subqueries.push((
                Occur::Should,
                Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
            ));
        }
        let effective_min = std::cmp::max(1, min_should_match);
        let query = BooleanQuery::with_minimum_required_clauses(subqueries, effective_min);
        self.search(&query, bitset)
    }

    // tokenize the query and OR a fuzzy (edit-distance) term query per token.
    pub(crate) fn fuzzy_match_query(
        &self,
        q: &str,
        max_edit_distance: u32,
        bitset: *mut c_void,
    ) -> Result<()> {
        // tantivy's fuzzy automaton caps the edit distance at 2. Reject larger
        // values up front: the `as u8` cast below would otherwise wrap (e.g.
        // 256 -> 0) and silently turn a fuzzy query into an exact one.
        if max_edit_distance > 2 {
            return Err(TantivyBindingError::InvalidArgument(format!(
                "max_edit_distance {} exceeds the fuzzy limit of 2",
                max_edit_distance
            )));
        }
        // An edit distance of 0 is exactly a term match. Take the cheaper
        // multiterms path instead of building a Levenshtein automaton per token.
        if max_edit_distance == 0 {
            return self.match_query(q, bitset);
        }
        use tantivy::query::FuzzyTermQuery;
        let distance = max_edit_distance as u8;
        let queries: Vec<Box<dyn tantivy::query::Query>> = self
            .tokenize_terms(q)
            .into_iter()
            .map(|(_, term)| {
                Box::new(FuzzyTermQuery::new(term, distance, true))
                    as Box<dyn tantivy::query::Query>
            })
            .collect();
        // A pure disjunction (OR) over the per-token fuzzy queries.
        let query = BooleanQuery::union(queries);
        self.search(&query, bitset)
    }

    // split the query string into multiple tokens using index's default tokenizer,
    // and then execute the disconjunction of term query.
    pub(crate) fn phrase_match_query(&self, q: &str, slop: u32, bitset: *mut c_void) -> Result<()> {
        let terms_with_offset = self.tokenize_terms(q);
        if terms_with_offset.len() <= 1 {
            // tantivy will panic when terms.len() <= 1, so we forward to text match instead.
            let terms: Vec<Term> = terms_with_offset.into_iter().map(|(_, t)| t).collect();
            let query = BooleanQuery::new_multiterms_query(terms);
            return self.search(&query, bitset);
        }

        let phrase_query = PhraseQuery::new_with_offset_and_slop(terms_with_offset, slop);
        self.search(&phrase_query, bitset)
    }

    pub(crate) fn register_tokenizer(&self, tokenizer_name: String, tokenizer: TextAnalyzer) {
        self.index.tokenizers().register(&tokenizer_name, tokenizer)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, ffi::c_void};

    use tempfile::TempDir;

    use crate::{index_writer::IndexWriterWrapper, util::set_bitset, TantivyIndexVersion};
    #[test]
    fn test_jeba() {
        let params = "{\"tokenizer\": \"jieba\"}".to_string();
        let dir = TempDir::new().unwrap();

        let mut writer = IndexWriterWrapper::create_text_writer(
            "text",
            dir.path().to_str().unwrap(),
            "jieba",
            &params,
            "",
            1,
            50_000_000,
            false,
            TantivyIndexVersion::default_version(),
        )
        .unwrap();

        writer.add("网球和滑雪", Some(0)).unwrap();
        writer.add("网球以及滑雪", Some(1)).unwrap();

        writer.commit().unwrap();

        let slop = 1;
        let reader = writer.create_reader(set_bitset).unwrap();
        let mut res: HashSet<u32> = HashSet::new();
        reader
            .phrase_match_query("网球滑雪", slop, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res, vec![0].into_iter().collect::<HashSet<u32>>());

        let slop = 2;
        let mut res: HashSet<u32> = HashSet::new();
        let reader = writer.create_reader(set_bitset).unwrap();
        reader
            .phrase_match_query("网球滑雪", slop, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res, vec![0, 1].into_iter().collect::<HashSet<u32>>());
    }

    #[test]
    fn test_read() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::create_text_writer(
            "text",
            dir.path().to_str().unwrap(),
            "default",
            "",
            "",
            1,
            50_000_000,
            false,
            TantivyIndexVersion::default_version(),
        )
        .unwrap();

        for i in 0..100000 {
            writer.add("hello world", Some(i)).unwrap();
        }
        writer.commit().unwrap();

        let reader = writer.create_reader(set_bitset).unwrap();

        let mut res: HashSet<u32> = HashSet::new();
        reader
            .match_query("hello world", &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res, (0..100000).collect::<HashSet<u32>>());
    }

    #[test]
    fn test_min_should_match_match_query() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::create_text_writer(
            "text",
            dir.path().to_str().unwrap(),
            "default",
            "",
            "",
            1,
            50_000_000,
            false,
            TantivyIndexVersion::default_version(),
        )
        .unwrap();

        // doc ids: 0..4
        writer.add("a b", Some(0)).unwrap();
        writer.add("a c", Some(1)).unwrap();
        writer.add("b c", Some(2)).unwrap();
        writer.add("c", Some(3)).unwrap();
        writer.add("a b c", Some(4)).unwrap();
        writer.commit().unwrap();

        let reader = writer.create_reader(set_bitset).unwrap();

        // min=1 behaves like union of tokens
        let mut res: HashSet<u32> = HashSet::new();
        reader
            .match_query_with_minimum("a b", 1, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res, vec![0, 1, 2, 4].into_iter().collect::<HashSet<u32>>());

        // min=2 requires at least two tokens
        res.clear();
        reader
            .match_query_with_minimum("a b c", 2, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res, vec![0, 1, 2, 4].into_iter().collect::<HashSet<u32>>());

        // min=3 requires all three tokens
        res.clear();
        reader
            .match_query_with_minimum("a b c", 3, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res, vec![4].into_iter().collect::<HashSet<u32>>());

        // large min should yield empty
        res.clear();
        reader
            .match_query_with_minimum("a b c", 10, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert!(res.is_empty());
    }

    #[test]
    fn test_fuzzy_match_query() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::create_text_writer(
            "text",
            dir.path().to_str().unwrap(),
            "default",
            "",
            "",
            1,
            50_000_000,
            false,
            TantivyIndexVersion::default_version(),
        )
        .unwrap();

        writer.add("allergy", Some(0)).unwrap();
        writer.add("allergic", Some(1)).unwrap();
        writer.add("apple", Some(2)).unwrap();
        writer.commit().unwrap();

        let reader = writer.create_reader(set_bitset).unwrap();

        // "alergy" is one edit away from "allergy" only.
        let mut res: HashSet<u32> = HashSet::new();
        reader
            .fuzzy_match_query("alergy", 1, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res, vec![0].into_iter().collect::<HashSet<u32>>());

        // distance 0 is an exact term match, so a typo finds nothing.
        res.clear();
        reader
            .fuzzy_match_query("alergy", 0, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert!(res.is_empty());

        // distance 2 still matches only "allergy"; "allergic" is distance 3 away.
        res.clear();
        reader
            .fuzzy_match_query("alergy", 2, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res, vec![0].into_iter().collect::<HashSet<u32>>());

        // a multi-token query ORs the per-token fuzzy matches.
        res.clear();
        reader
            .fuzzy_match_query("alergy aple", 1, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res, vec![0, 2].into_iter().collect::<HashSet<u32>>());

        // an out-of-range distance is rejected. 256 is chosen deliberately:
        // `256 as u8 == 0`, so without the guard this would wrap to an exact
        // match and return Ok; the guard must turn it into an error.
        res.clear();
        assert!(reader
            .fuzzy_match_query("alergy", 256, &mut res as *mut _ as *mut c_void)
            .is_err());
    }
}
