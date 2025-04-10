use std::ops::Bound;
use std::sync::Arc;

use tantivy::query::{Query, RangeQuery, RegexQuery, TermQuery};
use tantivy::schema::{Field, IndexRecordOption};
use tantivy::{Index, IndexReader, ReloadPolicy, Term};

use crate::docid_collector::DocIdCollector;
use crate::log::init_log;
use crate::milvus_id_collector::MilvusIdCollector;
use crate::util::make_bounds;
use crate::vec_collector::VecCollector;

use crate::error::{Result, TantivyBindingError};

#[allow(dead_code)]
pub(crate) struct IndexReaderWrapper {
    pub(crate) field_name: String,
    pub(crate) field: Field,
    pub(crate) reader: IndexReader,
    pub(crate) index: Arc<Index>,
    pub(crate) id_field: Option<Field>,
    pub(crate) user_specified_doc_id: bool,
}

impl IndexReaderWrapper {
    pub fn load(path: &str) -> Result<IndexReaderWrapper> {
        init_log();

        let index = Index::open_in_dir(path)?;

        IndexReaderWrapper::from_index(Arc::new(index))
    }

    pub fn from_index(index: Arc<Index>) -> Result<IndexReaderWrapper> {
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

    pub(crate) fn search(&self, q: &dyn Query) -> Result<Vec<u32>> {
        let searcher = self.reader.searcher();
        match self.id_field {
            Some(_) => {
                // newer version with doc_id.
                searcher
                    .search(q, &DocIdCollector::<u32>::default())
                    .map_err(TantivyBindingError::TantivyError)
            }
            None => {
                if self.user_specified_doc_id {
                    // newer version with user specified doc id.
                    searcher
                        .search(q, &MilvusIdCollector::default())
                        .map_err(TantivyBindingError::TantivyError)
                } else {
                    // older version without doc_id, only one segment.
                    searcher
                        .search(q, &VecCollector {})
                        .map_err(TantivyBindingError::TantivyError)
                }
            }
        }
    }

    // Generally, we should use [`crate::search`], except for some special senarios where the doc_id could beyound
    // the score of u32.
    #[allow(dead_code)]
    pub(crate) fn search_i64(&self, q: &dyn Query) -> Result<Vec<i64>> {
        assert!(self.id_field.is_some());
        let searcher = self.reader.searcher();
        searcher
            .search(q, &DocIdCollector::<i64>::default())
            .map_err(TantivyBindingError::TantivyError)
    }

    pub fn term_query_i64(&self, term: i64) -> Result<Vec<u32>> {
        let q = TermQuery::new(
            Term::from_field_i64(self.field, term),
            IndexRecordOption::Basic,
        );
        self.search(&q)
    }

    pub fn lower_bound_range_query_i64(
        &self,
        lower_bound: i64,
        inclusive: bool,
    ) -> Result<Vec<u32>> {
        let term = Term::from_field_i64(self.field, lower_bound);

        let q = RangeQuery::new(make_bounds(term, inclusive), Bound::Unbounded);
        self.search(&q)
    }

    pub fn upper_bound_range_query_i64(
        &self,
        upper_bound: i64,
        inclusive: bool,
    ) -> Result<Vec<u32>> {
        let term = Term::from_field_i64(self.field, upper_bound);
        let q = RangeQuery::new(Bound::Unbounded, make_bounds(term, inclusive));
        self.search(&q)
    }

    pub fn lower_bound_range_query_bool(
        &self,
        lower_bound: bool,
        inclusive: bool,
    ) -> Result<Vec<u32>> {
        let lower_bound = make_bounds(Term::from_field_bool(self.field, lower_bound), inclusive);
        let upper_bound = Bound::Unbounded;
        let q = RangeQuery::new(lower_bound, upper_bound);
        self.search(&q)
    }

    pub fn upper_bound_range_query_bool(
        &self,
        upper_bound: bool,
        inclusive: bool,
    ) -> Result<Vec<u32>> {
        let lower_bound = Bound::Unbounded;
        let upper_bound = make_bounds(Term::from_field_bool(self.field, upper_bound), inclusive);
        let q = RangeQuery::new(lower_bound, upper_bound);
        self.search(&q)
    }

    pub fn range_query_i64(
        &self,
        lower_bound: i64,
        upper_bound: i64,
        lb_inclusive: bool,
        ub_inclusive: bool,
    ) -> Result<Vec<u32>> {
        let lb = make_bounds(Term::from_field_i64(self.field, lower_bound), lb_inclusive);
        let ub = make_bounds(Term::from_field_i64(self.field, upper_bound), ub_inclusive);
        let q = RangeQuery::new(lb, ub);
        self.search(&q)
    }

    pub fn term_query_f64(&self, term: f64) -> Result<Vec<u32>> {
        let q: TermQuery = TermQuery::new(
            Term::from_field_f64(self.field, term),
            IndexRecordOption::Basic,
        );
        self.search(&q)
    }

    pub fn range_query_bool(
        &self,
        lower_bound: bool,
        upper_bound: bool,
        lb_inclusive: bool,
        ub_inclusive: bool,
    ) -> Result<Vec<u32>> {
        let lower_bound = make_bounds(Term::from_field_bool(self.field, lower_bound), lb_inclusive);
        let upper_bound = make_bounds(Term::from_field_bool(self.field, upper_bound), ub_inclusive);
        let q = RangeQuery::new(lower_bound, upper_bound);
        self.search(&q)
    }

    pub fn lower_bound_range_query_f64(
        &self,
        lower_bound: f64,
        inclusive: bool,
    ) -> Result<Vec<u32>> {
        let q = RangeQuery::new(
            make_bounds(Term::from_field_f64(self.field, lower_bound), inclusive),
            Bound::Unbounded,
        );
        self.search(&q)
    }

    pub fn upper_bound_range_query_f64(
        &self,
        upper_bound: f64,
        inclusive: bool,
    ) -> Result<Vec<u32>> {
        let q = RangeQuery::new(
            Bound::Unbounded,
            make_bounds(Term::from_field_f64(self.field, upper_bound), inclusive),
        );
        self.search(&q)
    }

    pub fn range_query_f64(
        &self,
        lower_bound: f64,
        upper_bound: f64,
        lb_inclusive: bool,
        ub_inclusive: bool,
    ) -> Result<Vec<u32>> {
        let lb = make_bounds(Term::from_field_f64(self.field, lower_bound), lb_inclusive);
        let ub = make_bounds(Term::from_field_f64(self.field, upper_bound), ub_inclusive);
        let q = RangeQuery::new(lb, ub);
        self.search(&q)
    }

    pub fn term_query_bool(&self, term: bool) -> Result<Vec<u32>> {
        let q = TermQuery::new(
            Term::from_field_bool(self.field, term),
            IndexRecordOption::Basic,
        );
        self.search(&q)
    }

    pub fn term_query_keyword(&self, term: &str) -> Result<Vec<u32>> {
        let q = TermQuery::new(
            Term::from_field_text(self.field, term),
            IndexRecordOption::Basic,
        );
        self.search(&q)
    }

    pub fn term_query_keyword_i64(&self, term: &str) -> Result<Vec<i64>> {
        let q = TermQuery::new(
            Term::from_field_text(self.field, term),
            IndexRecordOption::Basic,
        );
        self.search_i64(&q)
    }

    pub fn lower_bound_range_query_keyword(
        &self,
        lower_bound: &str,
        inclusive: bool,
    ) -> Result<Vec<u32>> {
        let q = RangeQuery::new(
            make_bounds(Term::from_field_text(self.field, lower_bound), inclusive),
            Bound::Unbounded,
        );
        self.search(&q)
    }

    pub fn upper_bound_range_query_keyword(
        &self,
        upper_bound: &str,
        inclusive: bool,
    ) -> Result<Vec<u32>> {
        let q = RangeQuery::new(
            Bound::Unbounded,
            make_bounds(Term::from_field_text(self.field, upper_bound), inclusive),
        );
        self.search(&q)
    }

    pub fn range_query_keyword(
        &self,
        lower_bound: &str,
        upper_bound: &str,
        lb_inclusive: bool,
        ub_inclusive: bool,
    ) -> Result<Vec<u32>> {
        let lb = make_bounds(Term::from_field_text(self.field, lower_bound), lb_inclusive);
        let ub = make_bounds(Term::from_field_text(self.field, upper_bound), ub_inclusive);
        let q = RangeQuery::new(lb, ub);
        self.search(&q)
    }

    pub fn prefix_query_keyword(&self, prefix: &str) -> Result<Vec<u32>> {
        let escaped = regex::escape(prefix);
        let pattern = format!("{}(.|\n)*", escaped);
        self.regex_query(&pattern)
    }

    pub fn regex_query(&self, pattern: &str) -> Result<Vec<u32>> {
        let q = RegexQuery::from_pattern(&pattern, self.field)?;
        self.search(&q)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use tantivy::{
        doc,
        schema::{Schema, STORED, STRING, TEXT_WITH_DOC_ID},
        Index,
    };

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
        let index_reader_wrapper = IndexReaderWrapper::from_index(index_shared).unwrap();
        let mut res = index_reader_wrapper.prefix_query_keyword("^").unwrap();
        assert_eq!(res.len(), 1);
        res = index_reader_wrapper.prefix_query_keyword("$").unwrap();
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
        let index_reader_wrapper = IndexReaderWrapper::from_index(index_shared).unwrap();
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
}
