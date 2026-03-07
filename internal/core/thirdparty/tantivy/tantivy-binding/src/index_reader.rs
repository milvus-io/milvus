use std::ffi::c_void;
use std::ops::Bound;
use std::sync::Arc;

use libc::c_char;
use tantivy::fastfield::FastValue;
use tantivy::query::{
    BooleanQuery, ExistsQuery, Query, RangeQuery, RegexQuery, TermQuery, TermSetQuery,
};
use tantivy::schema::{Field, IndexRecordOption};
use tantivy::tokenizer::{NgramTokenizer, TokenStream, Tokenizer};
use tantivy::{Directory, DocSet, HasLen, Index, IndexReader, ReloadPolicy, Term, TERMINATED};

use crate::bitset_wrapper::BitsetWrapper;
use crate::docid_collector::{DocIdCollector, DocIdCollectorI64};
use crate::index_reader_c::SetBitsetFn;
use crate::log::init_log;
use crate::milvus_id_collector::MilvusIdCollector;
use crate::util::{c_ptr_to_str, make_bounds};
use crate::vec_collector::VecCollector;

use crate::error::{Result, TantivyBindingError};

// Threshold for batch-in query. Less than this threshold, we use term_query one by one and use
// TermSetQuery when larger than this threshold. This value is based on some experiments.
const BATCH_THRESHOLD: usize = 10000;

#[allow(dead_code)]
pub(crate) struct IndexReaderWrapper {
    pub(crate) field_name: String,
    pub(crate) field: Field,
    pub(crate) reader: IndexReader,
    pub(crate) index: Arc<Index>,
    pub(crate) id_field: Option<Field>,
    pub(crate) user_specified_doc_id: bool,
    pub(crate) set_bitset: SetBitsetFn,
}

impl IndexReaderWrapper {
    pub fn load(
        path: &str,
        load_in_mmap: bool,
        set_bitset: SetBitsetFn,
    ) -> Result<IndexReaderWrapper> {
        init_log();

        let index = if load_in_mmap {
            Index::open_in_dir(path)?
        } else {
            Index::open_in_dir_in_ram(path)?
        };

        IndexReaderWrapper::from_index(Arc::new(index), set_bitset)
    }

    pub fn from_index(index: Arc<Index>, set_bitset: SetBitsetFn) -> Result<IndexReaderWrapper> {
        let field = index.schema().fields().next().unwrap().0;
        let schema = index.schema();
        let field_name = String::from(schema.get_field_name(field));
        let id_field: Option<Field> = match schema.get_field("doc_id") {
            Ok(field) => Some(field),
            Err(_) => None,
        };

        assert!(!schema.user_specified_doc_id() || id_field.is_none());

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay) // OnCommitWithDelay serve for growing segment.
            .try_into()?;
        reader.reload()?;

        Ok(IndexReaderWrapper {
            field_name,
            field,
            reader,
            index,
            id_field,
            user_specified_doc_id: schema.user_specified_doc_id(),
            set_bitset,
        })
    }

    pub fn reload(&self) -> Result<()> {
        self.reader.reload()?;
        Ok(())
    }

    pub fn count(&self) -> Result<u32> {
        let metas = self.index.searchable_segment_metas()?;
        let mut sum: u32 = 0;
        for meta in metas {
            if self.user_specified_doc_id {
                sum = std::cmp::max(sum, meta.max_doc());
            } else {
                sum += meta.max_doc();
            }
        }
        Ok(sum)
    }

    pub fn index_size_bytes(&self) -> Result<u64> {
        let dir = self.index.directory();
        let mut total: u64 = 0;
        for path in dir.list_managed_files() {
            match dir.open_read(&path) {
                Ok(file_slice) => {
                    total += file_slice.len() as u64;
                }
                Err(_e) => {
                    // Some legacy/unsupported files may fail to open (e.g., old tantivy versions).
                    // Skip unreadable files to provide a best-effort byte-size estimation instead of failing hard.
                    continue;
                }
            }
        }
        Ok(total)
    }

    pub(crate) fn search(&self, q: &dyn Query, bitset: *mut c_void) -> Result<()> {
        let searcher = self.reader.searcher();
        match self.id_field {
            Some(_) => {
                // newer version with doc_id.
                searcher
                    .search(
                        q,
                        &DocIdCollector {
                            bitset_wrapper: BitsetWrapper::new(bitset, self.set_bitset),
                        },
                    )
                    .map_err(TantivyBindingError::TantivyError)
            }
            None => {
                if self.user_specified_doc_id {
                    // newer version with user specified doc id.
                    searcher
                        .search(
                            q,
                            &MilvusIdCollector {
                                bitset_wrapper: BitsetWrapper::new(bitset, self.set_bitset),
                            },
                        )
                        .map_err(TantivyBindingError::TantivyError)
                } else {
                    // older version without doc_id, only one segment.
                    searcher
                        .search(
                            q,
                            &VecCollector {
                                bitset_wrapper: BitsetWrapper::new(bitset, self.set_bitset),
                            },
                        )
                        .map_err(TantivyBindingError::TantivyError)
                }
            }
        }
    }

    // Generally, we should use [`crate::search`], except for some special senarios where the doc_id could beyound
    // the scope of u32 such as json key stats offset.
    #[allow(dead_code)]
    pub(crate) fn search_i64(&self, q: &dyn Query) -> Result<Vec<i64>> {
        assert!(self.id_field.is_some());
        let searcher = self.reader.searcher();
        searcher
            .search(q, &DocIdCollectorI64::default())
            .map_err(TantivyBindingError::TantivyError)
    }

    /// Returns true when direct posting-list reads are safe:
    /// id_field is None (raw DocId == bitset offset), and either single-segment
    /// or user_specified_doc_id (globally unique doc IDs).
    #[inline]
    fn can_use_direct_posting(&self) -> bool {
        if self.id_field.is_some() {
            return false;
        }
        if !self.user_specified_doc_id {
            // Legacy auto-assigned IDs are segment-local; only safe with one segment.
            let n_segments = self.reader.searcher().segment_readers().len();
            if n_segments > 1 {
                return false;
            }
        }
        true
    }

    /// Reads posting lists directly for multiple terms, bypassing Query/Weight/Scorer.
    /// Caller must verify [`can_use_direct_posting`] before calling.
    fn direct_terms_posting(&self, terms: &[Term], bitset: *mut c_void) -> Result<()> {
        let searcher = self.reader.searcher();
        let bw = BitsetWrapper::new(bitset, self.set_bitset);
        let mut buffer = [0u32; 4096];

        for segment_reader in searcher.segment_readers() {
            let inv_index = segment_reader
                .inverted_index(self.field)
                .map_err(TantivyBindingError::TantivyError)?;
            let alive_bitset = segment_reader.alive_bitset();
            for term in terms {
                if let Some(mut posting) =
                    inv_index.read_postings(term, IndexRecordOption::Basic)?
                {
                    let mut len = 0;
                    while posting.doc() != TERMINATED {
                        let doc = posting.doc();
                        if alive_bitset.map_or(true, |bs| bs.is_alive(doc)) {
                            buffer[len] = doc;
                            len += 1;
                            if len == 4096 {
                                bw.batch_set(&buffer[..len]);
                                len = 0;
                            }
                        }
                        posting.advance();
                    }
                    if len > 0 {
                        bw.batch_set(&buffer[..len]);
                    }
                }
            }
        }
        Ok(())
    }

    #[inline]
    fn single_term_query<F>(&self, term_builder: F, bitset: *mut c_void) -> Result<()>
    where
        F: FnOnce(Field) -> Term,
    {
        // Single-term queries go through the normal pipeline — the overhead of
        // TermQuery/Weight/Scorer is negligible for one term and tantivy's scorer
        // already batches docs via collect_block(64).
        let q = TermQuery::new(term_builder(self.field), IndexRecordOption::Basic);
        self.search(&q, bitset)
    }

    // Due to overhead, `TermSetQuery` is not efficient for small number of terms. So we execute term query one by one
    // when the terms number is less than `BATCH_THRESHOLD`.
    #[inline]
    fn batch_terms_query<T, F>(
        &self,
        terms: &[T],
        term_builder: F,
        bitset: *mut c_void,
    ) -> Result<()>
    where
        T: Copy,
        F: Fn(Field, T) -> Term,
    {
        if terms.len() < BATCH_THRESHOLD {
            if self.can_use_direct_posting() {
                let term_vec: Vec<Term> = terms
                    .iter()
                    .map(|&term| term_builder(self.field, term))
                    .collect();
                return self.direct_terms_posting(&term_vec, bitset);
            }
            return terms.iter().try_for_each(|term| {
                self.single_term_query(|field| term_builder(field, *term), bitset)
            });
        }

        let term_vec: Vec<_> = terms
            .iter()
            .map(|&term| term_builder(self.field, term))
            .collect();
        let q = TermSetQuery::new(term_vec);
        self.search(&q, bitset)
    }

    pub fn terms_query_bool(&self, terms: &[bool], bitset: *mut c_void) -> Result<()> {
        self.batch_terms_query(terms, Term::from_field_bool, bitset)
    }

    pub fn terms_query_i64(&self, terms: &[i64], bitset: *mut c_void) -> Result<()> {
        self.batch_terms_query(terms, Term::from_field_i64, bitset)
    }

    pub fn terms_query_f64(&self, terms: &[f64], bitset: *mut c_void) -> Result<()> {
        self.batch_terms_query(terms, Term::from_field_f64, bitset)
    }

    #[inline]
    fn term_query_keyword(&self, term: &str, bitset: *mut c_void) -> Result<()> {
        let q = TermQuery::new(
            Term::from_field_text(self.field, term),
            IndexRecordOption::Basic,
        );
        self.search(&q, bitset)
    }

    pub fn terms_query_keyword(&self, terms: &[*const c_char], bitset: *mut c_void) -> Result<()> {
        if terms.len() < BATCH_THRESHOLD {
            if self.can_use_direct_posting() {
                let term_vec: Vec<Term> = terms
                    .iter()
                    .map(|term| -> Result<Term> {
                        Ok(Term::from_field_text(self.field, c_ptr_to_str(*term)?))
                    })
                    .collect::<Result<Vec<_>>>()?;
                return self.direct_terms_posting(&term_vec, bitset);
            }
            return terms
                .iter()
                .try_for_each(|term| self.term_query_keyword(c_ptr_to_str(*term)?, bitset));
        }

        let mut term_strs = Vec::with_capacity(terms.len());
        for term in terms {
            let term_str = c_ptr_to_str(*term)?;
            term_strs.push(Term::from_field_text(self.field, term_str));
        }
        let q = TermSetQuery::new(term_strs);

        self.search(&q, bitset)
    }

    pub fn term_query_keyword_i64(&self, term: &str) -> Result<Vec<i64>> {
        let q = TermQuery::new(
            Term::from_field_text(self.field, term),
            IndexRecordOption::Basic,
        );
        self.search_i64(&q)
    }

    pub fn lower_bound_range_query_i64(
        &self,
        lower_bound: i64,
        inclusive: bool,
        bitset: *mut c_void,
    ) -> Result<()> {
        let term = Term::from_field_i64(self.field, lower_bound);

        let q = RangeQuery::new(make_bounds(term, inclusive), Bound::Unbounded);
        self.search(&q, bitset)
    }

    pub fn upper_bound_range_query_i64(
        &self,
        upper_bound: i64,
        inclusive: bool,
        bitset: *mut c_void,
    ) -> Result<()> {
        let term = Term::from_field_i64(self.field, upper_bound);
        let q = RangeQuery::new(Bound::Unbounded, make_bounds(term, inclusive));
        self.search(&q, bitset)
    }

    pub fn lower_bound_range_query_bool(
        &self,
        lower_bound: bool,
        inclusive: bool,
        bitset: *mut c_void,
    ) -> Result<()> {
        let lower_bound = make_bounds(Term::from_field_bool(self.field, lower_bound), inclusive);
        let upper_bound = Bound::Unbounded;
        let q = RangeQuery::new(lower_bound, upper_bound);
        self.search(&q, bitset)
    }

    pub fn upper_bound_range_query_bool(
        &self,
        upper_bound: bool,
        inclusive: bool,
        bitset: *mut c_void,
    ) -> Result<()> {
        let lower_bound = Bound::Unbounded;
        let upper_bound = make_bounds(Term::from_field_bool(self.field, upper_bound), inclusive);
        let q = RangeQuery::new(lower_bound, upper_bound);
        self.search(&q, bitset)
    }

    pub fn range_query_i64(
        &self,
        lower_bound: i64,
        upper_bound: i64,
        lb_inclusive: bool,
        ub_inclusive: bool,
        bitset: *mut c_void,
    ) -> Result<()> {
        let lb = make_bounds(Term::from_field_i64(self.field, lower_bound), lb_inclusive);
        let ub = make_bounds(Term::from_field_i64(self.field, upper_bound), ub_inclusive);
        let q = RangeQuery::new(lb, ub);
        self.search(&q, bitset)
    }

    pub fn range_query_bool(
        &self,
        lower_bound: bool,
        upper_bound: bool,
        lb_inclusive: bool,
        ub_inclusive: bool,
        bitset: *mut c_void,
    ) -> Result<()> {
        let lower_bound = make_bounds(Term::from_field_bool(self.field, lower_bound), lb_inclusive);
        let upper_bound = make_bounds(Term::from_field_bool(self.field, upper_bound), ub_inclusive);
        let q = RangeQuery::new(lower_bound, upper_bound);
        self.search(&q, bitset)
    }

    pub fn lower_bound_range_query_f64(
        &self,
        lower_bound: f64,
        inclusive: bool,
        bitset: *mut c_void,
    ) -> Result<()> {
        let q = RangeQuery::new(
            make_bounds(Term::from_field_f64(self.field, lower_bound), inclusive),
            Bound::Unbounded,
        );
        self.search(&q, bitset)
    }

    pub fn upper_bound_range_query_f64(
        &self,
        upper_bound: f64,
        inclusive: bool,
        bitset: *mut c_void,
    ) -> Result<()> {
        let q = RangeQuery::new(
            Bound::Unbounded,
            make_bounds(Term::from_field_f64(self.field, upper_bound), inclusive),
        );
        self.search(&q, bitset)
    }

    pub fn range_query_f64(
        &self,
        lower_bound: f64,
        upper_bound: f64,
        lb_inclusive: bool,
        ub_inclusive: bool,
        bitset: *mut c_void,
    ) -> Result<()> {
        let lb = make_bounds(Term::from_field_f64(self.field, lower_bound), lb_inclusive);
        let ub = make_bounds(Term::from_field_f64(self.field, upper_bound), ub_inclusive);
        let q = RangeQuery::new(lb, ub);
        self.search(&q, bitset)
    }

    pub fn lower_bound_range_query_keyword(
        &self,
        lower_bound: &str,
        inclusive: bool,
        bitset: *mut c_void,
    ) -> Result<()> {
        let q = RangeQuery::new(
            make_bounds(Term::from_field_text(self.field, lower_bound), inclusive),
            Bound::Unbounded,
        );
        self.search(&q, bitset)
    }

    pub fn upper_bound_range_query_keyword(
        &self,
        upper_bound: &str,
        inclusive: bool,
        bitset: *mut c_void,
    ) -> Result<()> {
        let q = RangeQuery::new(
            Bound::Unbounded,
            make_bounds(Term::from_field_text(self.field, upper_bound), inclusive),
        );
        self.search(&q, bitset)
    }

    pub fn range_query_keyword(
        &self,
        lower_bound: &str,
        upper_bound: &str,
        lb_inclusive: bool,
        ub_inclusive: bool,
        bitset: *mut c_void,
    ) -> Result<()> {
        let lb = make_bounds(Term::from_field_text(self.field, lower_bound), lb_inclusive);
        let ub = make_bounds(Term::from_field_text(self.field, upper_bound), ub_inclusive);
        let q = RangeQuery::new(lb, ub);
        self.search(&q, bitset)
    }

    pub fn prefix_query_keyword(&self, prefix: &str, bitset: *mut c_void) -> Result<()> {
        let escaped = regex::escape(prefix);
        let pattern = format!("{}(.|\n)*", escaped);
        self.regex_query(&pattern, bitset)
    }

    pub fn regex_query(&self, pattern: &str, bitset: *mut c_void) -> Result<()> {
        let q = RegexQuery::from_pattern(&pattern, self.field)?;
        self.search(&q, bitset)
    }

    // JSON related query methods
    // These methods support querying JSON fields with different data types

    pub fn json_term_query_i64(
        &self,
        json_path: &str,
        term: i64,
        bitset: *mut c_void,
    ) -> Result<()> {
        let mut json_term = Term::from_field_json_path(self.field, json_path, false);
        json_term.append_type_and_fast_value(term);
        let q = TermQuery::new(json_term, IndexRecordOption::Basic);
        self.search(&q, bitset)
    }

    pub fn json_term_query_f64(
        &self,
        json_path: &str,
        term: f64,
        bitset: *mut c_void,
    ) -> Result<()> {
        let mut json_term = Term::from_field_json_path(self.field, json_path, false);
        json_term.append_type_and_fast_value(term);
        let q = TermQuery::new(json_term, IndexRecordOption::Basic);
        self.search(&q, bitset)
    }

    pub fn json_term_query_bool(
        &self,
        json_path: &str,
        term: bool,
        bitset: *mut c_void,
    ) -> Result<()> {
        let mut json_term = Term::from_field_json_path(self.field, json_path, false);
        json_term.append_type_and_fast_value(term);
        let q = TermQuery::new(json_term, IndexRecordOption::Basic);
        self.search(&q, bitset)
    }

    pub fn json_term_query_keyword(
        &self,
        json_path: &str,
        term: &str,
        bitset: *mut c_void,
    ) -> Result<()> {
        let mut json_term = Term::from_field_json_path(self.field, json_path, false);
        json_term.append_type_and_str(term);
        let q = TermQuery::new(json_term, IndexRecordOption::Basic);
        self.search(&q, bitset)
    }

    pub fn json_exist_query(&self, json_path: &str, bitset: *mut c_void) -> Result<()> {
        let full_json_path = if json_path == "" {
            self.field_name.clone()
        } else {
            format!("{}.{}", self.field_name, json_path)
        };
        let q = ExistsQuery::new(full_json_path, true);
        self.search(&q, bitset)
    }

    pub fn json_range_query<T: FastValue>(
        &self,
        json_path: &str,
        lower_bound: T,
        higher_bound: T,
        lb_unbounded: bool,
        up_unbounded: bool,
        lb_inclusive: bool,
        ub_inclusive: bool,
        bitset: *mut c_void,
    ) -> Result<()> {
        let lb = if lb_unbounded {
            Bound::Unbounded
        } else {
            let mut term = Term::from_field_json_path(self.field, json_path, false);
            term.append_type_and_fast_value::<T>(lower_bound);
            make_bounds(term, lb_inclusive)
        };
        let ub = if up_unbounded {
            Bound::Unbounded
        } else {
            let mut term = Term::from_field_json_path(self.field, json_path, false);
            term.append_type_and_fast_value::<T>(higher_bound);
            make_bounds(term, ub_inclusive)
        };
        let q = RangeQuery::new(lb, ub);
        self.search(&q, bitset)
    }

    pub fn json_range_query_keyword(
        &self,
        json_path: &str,
        lower_bound: &str,
        higher_bound: &str,
        lb_unbounded: bool,
        up_unbounded: bool,
        lb_inclusive: bool,
        ub_inclusive: bool,
        bitset: *mut c_void,
    ) -> Result<()> {
        let lb = if lb_unbounded {
            Bound::Unbounded
        } else {
            let mut term = Term::from_field_json_path(self.field, json_path, false);
            term.append_type_and_str(lower_bound);
            make_bounds(term, lb_inclusive)
        };
        let ub = if up_unbounded {
            Bound::Unbounded
        } else {
            let mut term = Term::from_field_json_path(self.field, json_path, false);
            term.append_type_and_str(higher_bound);
            make_bounds(term, ub_inclusive)
        };
        let q = RangeQuery::new(lb, ub);
        self.search(&q, bitset)
    }

    pub fn json_regex_query(
        &self,
        json_path: &str,
        pattern: &str,
        bitset: *mut c_void,
    ) -> Result<()> {
        let q = RegexQuery::from_pattern_with_json_path(pattern, self.field, json_path)?;
        self.search(&q, bitset)
    }

    pub fn json_prefix_query(
        &self,
        json_path: &str,
        prefix: &str,
        bitset: *mut c_void,
    ) -> Result<()> {
        let escaped = regex::escape(prefix);
        let pattern = format!("{}(.|\n)*", escaped);
        self.json_regex_query(json_path, &pattern, bitset)
    }

    // **Note**: literal length must be larger or equal to min_gram.
    pub fn ngram_match_query(
        &self,
        literal: &str,
        min_gram: usize,
        max_gram: usize,
        bitset: *mut c_void,
    ) -> Result<()> {
        // literal length should be larger or equal to min_gram.
        assert!(
            literal.chars().count() >= min_gram,
            "literal length should be larger or equal to min_gram. literal: {}, min_gram: {}",
            literal,
            min_gram
        );

        if literal.chars().count() <= max_gram {
            return self.term_query_keyword(literal, bitset);
        }

        let mut terms = vec![];
        // So, str length is larger than 'max_gram' parse 'str' by 'max_gram'-gram and search all of them with boolean intersection
        // nivers
        let mut term_queries: Vec<Box<dyn Query>> = vec![];
        let mut tokenizer = NgramTokenizer::new(max_gram, max_gram, false).unwrap();
        let mut token_stream = tokenizer.token_stream(literal);
        token_stream.process(&mut |token| {
            let term = Term::from_field_text(self.field, &token.text);
            term_queries.push(Box::new(TermQuery::new(term, IndexRecordOption::Basic)));
            terms.push(token.text.clone());
        });
        let query = BooleanQuery::intersection(term_queries);
        self.search(&query, bitset)
    }

    /// Tokenize literals into ngram terms and return them sorted by doc_freq (ascending).
    ///
    /// For Match type queries like `%xxx%yyy%`, literals = ["xxx", "yyy"].
    /// For InnerMatch/PrefixMatch/PostfixMatch type queries like `%xxx%`, literals = ["xxx"].
    ///
    /// Returns: Vec of term strings sorted by doc_freq ascending (rarest first).
    pub fn ngram_tokenize(
        &self,
        literals: &[&str],
        min_gram: usize,
        max_gram: usize,
    ) -> Result<Vec<String>> {
        // Collect (term_text, Term) pairs to track text alongside terms
        let mut all_term_pairs: Vec<(String, Term)> = vec![];
        let mut tokenizer = NgramTokenizer::new(max_gram, max_gram, false).unwrap();

        for literal in literals {
            assert!(
                literal.chars().count() >= min_gram,
                "literal '{}' must be >= min_gram {}",
                literal,
                min_gram
            );

            if literal.chars().count() <= max_gram {
                all_term_pairs.push((
                    literal.to_string(),
                    Term::from_field_text(self.field, literal),
                ));
            } else {
                let mut token_stream = tokenizer.token_stream(literal);
                token_stream.process(&mut |token| {
                    all_term_pairs.push((
                        token.text.clone(),
                        Term::from_field_text(self.field, &token.text),
                    ));
                });
            }
        }

        assert!(
            !all_term_pairs.is_empty(),
            "ngram_tokenize should not produce empty terms for valid literals"
        );

        // Get doc_freq for each term and sort
        let searcher = self.reader.searcher();
        let mut term_with_freq: Vec<(String, u64)> = Vec::with_capacity(all_term_pairs.len());

        for (text, term) in all_term_pairs.iter() {
            let mut total_doc_freq: u64 = 0;
            for segment_reader in searcher.segment_readers() {
                let inv_index = segment_reader.inverted_index(term.field())?;
                total_doc_freq += inv_index.doc_freq(term)? as u64;
            }
            term_with_freq.push((text.clone(), total_doc_freq));
        }

        // Sort by doc_freq ascending (rarest first)
        term_with_freq.sort_by_key(|(_, freq)| *freq);

        // Remove duplicates
        let mut seen = std::collections::HashSet::new();
        Ok(term_with_freq
            .into_iter()
            .filter_map(|(text, _)| {
                if seen.insert(text.clone()) {
                    Some(text)
                } else {
                    None
                }
            })
            .collect())
    }

    /// Get the posting list for a single ngram term.
    /// Sets bits in the bitset for all documents containing the term.
    pub fn ngram_term_posting_list(&self, term_str: &str, bitset: *mut c_void) -> Result<()> {
        self.term_query_keyword(term_str, bitset)
    }
}

#[cfg(test)]
mod test {
    use std::{
        collections::HashSet,
        ffi::{c_void, CString},
        sync::Arc,
    };

    use tantivy::{
        doc,
        schema::{Schema, STORED, STRING, TEXT_WITH_DOC_ID},
        Index,
    };

    use crate::util::set_bitset;

    use super::IndexReaderWrapper;

    #[test]
    pub fn test_escape_regex() {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("title", STRING | STORED);

        let schema = schema_builder.build();
        let title = schema.get_field("title").unwrap();

        let index = Index::create_in_ram(schema.clone());
        let mut index_writer = index.writer(50000000).unwrap();

        index_writer.add_document(doc!(title => "^abc")).unwrap();
        index_writer.add_document(doc!(title => "$abc")).unwrap();
        index_writer.commit().unwrap();

        let index_shared = Arc::new(index);
        let mut res: HashSet<u32> = HashSet::new();
        let index_reader_wrapper =
            IndexReaderWrapper::from_index(index_shared, set_bitset).unwrap();
        index_reader_wrapper
            .prefix_query_keyword("^", &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res.len(), 1);
        res.clear();
        index_reader_wrapper
            .prefix_query_keyword("$", &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res.len(), 1);
    }

    #[test]
    fn test_count() {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("title", TEXT_WITH_DOC_ID);
        schema_builder.enable_user_specified_doc_id();
        let schema = schema_builder.build();
        let title = schema.get_field("title").unwrap();

        let index = Index::create_in_ram(schema.clone());
        let mut index_writer = index.writer(50000000).unwrap();

        for i in 0..10_000 {
            index_writer
                .add_document_with_doc_id(i, doc!(title => format!("abc{}", i)))
                .unwrap();
        }
        index_writer.commit().unwrap();

        let index_shared = Arc::new(index);
        let index_reader_wrapper =
            IndexReaderWrapper::from_index(index_shared, set_bitset).unwrap();
        let count = index_reader_wrapper.count().unwrap();
        assert_eq!(count, 10000);

        let batch: Vec<_> = (0..10_000)
            .into_iter()
            .map(|i| doc!(title => format!("hello{}", i)))
            .collect();
        index_writer
            .add_documents_with_doc_id(10_000, batch)
            .unwrap();
        index_writer.commit().unwrap();
        index_reader_wrapper.reload().unwrap();
        let count = index_reader_wrapper.count().unwrap();
        assert_eq!(count, 20000);
    }

    #[test]
    fn test_batch_terms_query() {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("content", TEXT_WITH_DOC_ID);
        schema_builder.enable_user_specified_doc_id();
        let schema = schema_builder.build();
        let content = schema.get_field("content").unwrap();

        let index = Index::create_in_ram(schema.clone());
        let mut index_writer = index.writer(50000000).unwrap();

        for i in 0..30_000 {
            index_writer
                .add_document_with_doc_id(i, doc!(content => format!("key{:010}", i)))
                .unwrap();
        }
        index_writer.commit().unwrap();

        let reader_wrapper = IndexReaderWrapper::from_index(Arc::new(index), set_bitset).unwrap();
        let arrays = (0..1000)
            .map(|i| CString::new(format!("key{:010}", i)).unwrap())
            .collect::<Vec<_>>();
        let arrays: Vec<*const libc::c_char> =
            arrays.iter().map(|s| s.as_ptr()).collect::<Vec<_>>();

        let mut res: HashSet<u32> = HashSet::new();
        reader_wrapper
            .terms_query_keyword(&arrays, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res.len(), 1000);
        for i in 0..1000 {
            assert!(res.contains(&(i as u32)));
        }

        let arrays = (0..20000)
            .map(|i| CString::new(format!("key{:010}", i)).unwrap())
            .collect::<Vec<_>>();
        let arrays: Vec<*const libc::c_char> =
            arrays.iter().map(|s| s.as_ptr()).collect::<Vec<_>>();
        let mut res: HashSet<u32> = HashSet::new();
        reader_wrapper
            .terms_query_keyword(&arrays, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res.len(), 20000);
    }

    /// Regression test for `direct_terms_posting`:
    /// - Exercises the direct posting-list bypass (user_specified_doc_id, id_field=None)
    /// - Verifies terms not present in the index are silently skipped
    /// - Verifies duplicate terms don't produce duplicate bits
    /// - Verifies multi-segment correctness with user-specified doc IDs
    #[test]
    fn test_direct_terms_posting_keyword() {
        let mut schema_builder = Schema::builder();
        // TEXT_WITH_DOC_ID is required when user_specified_doc_id is enabled
        // (disables fieldnorms which are incompatible with user-specified doc IDs).
        schema_builder.add_text_field("tag", TEXT_WITH_DOC_ID);
        schema_builder.enable_user_specified_doc_id();
        let schema = schema_builder.build();
        let tag = schema.get_field("tag").unwrap();

        let index = Index::create_in_ram(schema.clone());
        let mut index_writer = index.writer(50000000).unwrap();

        // Insert 500 docs with known tags across two commits (potentially two segments).
        for i in 0u32..250 {
            index_writer
                .add_document_with_doc_id(i, doc!(tag => format!("val{}", i)))
                .unwrap();
        }
        index_writer.commit().unwrap();
        for i in 250u32..500 {
            index_writer
                .add_document_with_doc_id(i, doc!(tag => format!("val{}", i)))
                .unwrap();
        }
        index_writer.commit().unwrap();

        let reader = IndexReaderWrapper::from_index(Arc::new(index), set_bitset).unwrap();
        assert!(reader.can_use_direct_posting());

        // --- Case 1: basic multi-term query, all terms present ---
        let terms: Vec<CString> = (0..100)
            .map(|i| CString::new(format!("val{}", i)).unwrap())
            .collect();
        let ptrs: Vec<*const libc::c_char> = terms.iter().map(|s| s.as_ptr()).collect();
        let mut res: HashSet<u32> = HashSet::new();
        reader
            .terms_query_keyword(&ptrs, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res.len(), 100);
        for i in 0u32..100 {
            assert!(res.contains(&i), "missing doc_id {}", i);
        }

        // --- Case 2: some terms not in the index (should be silently skipped) ---
        let terms: Vec<CString> = (0..10)
            .map(|i| CString::new(format!("val{}", i)).unwrap())
            .chain(
                (0..5).map(|i| CString::new(format!("nonexistent{}", i)).unwrap()),
            )
            .collect();
        let ptrs: Vec<*const libc::c_char> = terms.iter().map(|s| s.as_ptr()).collect();
        let mut res: HashSet<u32> = HashSet::new();
        reader
            .terms_query_keyword(&ptrs, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res.len(), 10);

        // --- Case 3: duplicate terms should not produce duplicate bits ---
        let terms: Vec<CString> = vec![
            CString::new("val0").unwrap(),
            CString::new("val0").unwrap(),
            CString::new("val1").unwrap(),
        ];
        let ptrs: Vec<*const libc::c_char> = terms.iter().map(|s| s.as_ptr()).collect();
        let mut res: HashSet<u32> = HashSet::new();
        reader
            .terms_query_keyword(&ptrs, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res.len(), 2);
        assert!(res.contains(&0));
        assert!(res.contains(&1));

        // --- Case 4: empty terms list ---
        let ptrs: Vec<*const libc::c_char> = vec![];
        let mut res: HashSet<u32> = HashSet::new();
        reader
            .terms_query_keyword(&ptrs, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res.len(), 0);

        // --- Case 5: terms spanning both segments ---
        let terms: Vec<CString> = vec![
            CString::new("val0").unwrap(),
            CString::new("val249").unwrap(),
            CString::new("val250").unwrap(),
            CString::new("val499").unwrap(),
        ];
        let ptrs: Vec<*const libc::c_char> = terms.iter().map(|s| s.as_ptr()).collect();
        let mut res: HashSet<u32> = HashSet::new();
        reader
            .terms_query_keyword(&ptrs, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res.len(), 4);
        assert!(res.contains(&0));
        assert!(res.contains(&249));
        assert!(res.contains(&250));
        assert!(res.contains(&499));
    }

    /// Verifies that `can_use_direct_posting` returns false for legacy indexes
    /// (no user_specified_doc_id) with multiple segments, preventing segment-local
    /// doc ID collisions in the direct posting path.
    #[test]
    fn test_can_use_direct_posting_gate() {
        // --- Legacy format: no user_specified_doc_id → multi-segment is unsafe ---
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("color", STRING);
        let schema = schema_builder.build();
        let color = schema.get_field("color").unwrap();

        let index = Index::create_in_ram(schema.clone());
        let mut index_writer = index.writer(50000000).unwrap();
        index_writer
            .add_document(doc!(color => "red"))
            .unwrap();
        index_writer
            .add_document(doc!(color => "blue"))
            .unwrap();
        index_writer.commit().unwrap();

        let reader = IndexReaderWrapper::from_index(Arc::new(index), set_bitset).unwrap();
        let n_segments = reader.reader.searcher().segment_readers().len();

        if n_segments > 1 {
            // Legacy multi-segment: guard must block the direct posting path.
            assert!(
                !reader.can_use_direct_posting(),
                "must NOT use direct posting with legacy multi-segment"
            );
        } else {
            // Legacy single-segment: safe to use direct posting.
            assert!(reader.can_use_direct_posting());
        }

        // --- user_specified_doc_id format: multi-segment is always safe ---
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("tag", TEXT_WITH_DOC_ID);
        schema_builder.enable_user_specified_doc_id();
        let schema = schema_builder.build();
        let tag = schema.get_field("tag").unwrap();

        let index = Index::create_in_ram(schema.clone());
        let mut index_writer = index.writer(50000000).unwrap();
        // Two commits to potentially create multiple segments.
        index_writer
            .add_document_with_doc_id(0, doc!(tag => "red"))
            .unwrap();
        index_writer.commit().unwrap();
        index_writer
            .add_document_with_doc_id(1, doc!(tag => "blue"))
            .unwrap();
        index_writer.commit().unwrap();

        let reader = IndexReaderWrapper::from_index(Arc::new(index), set_bitset).unwrap();
        // user_specified_doc_id: always safe regardless of segment count.
        assert!(
            reader.can_use_direct_posting(),
            "user_specified_doc_id should always allow direct posting"
        );
    }
}
