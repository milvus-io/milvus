use std::ops::Bound;
use std::sync::Arc;

use tantivy::query::{Query, RangeQuery, RegexQuery, TermQuery};
use tantivy::schema::{Field, IndexRecordOption};
use tantivy::{Index, IndexReader, ReloadPolicy, Term};

use crate::docid_collector::DocIdCollector;
use crate::log::init_log;
use crate::util::make_bounds;
use crate::vec_collector::VecCollector;

pub(crate) struct IndexReaderWrapper {
    pub(crate) field_name: String,
    pub(crate) field: Field,
    pub(crate) reader: IndexReader,
    pub(crate) index: Arc<Index>,
    pub(crate) id_field: Option<Field>,
}

impl IndexReaderWrapper {
    pub fn load(path: &str) -> IndexReaderWrapper {
        init_log();

        let index = Index::open_in_dir(path).unwrap();

        IndexReaderWrapper::from_index(Arc::new(index))
    }

    pub fn from_index(index: Arc<Index>) -> IndexReaderWrapper {
        let field = index.schema().fields().next().unwrap().0;
        let schema = index.schema();
        let field_name = String::from(schema.get_field_name(field));
        let id_field: Option<Field> = match schema.get_field("doc_id") {
            Ok(field) => Some(field),
            Err(_) => None,
        };

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit) // OnCommit serve for growing segment.
            .try_into()
            .unwrap();
        reader.reload().unwrap();

        IndexReaderWrapper {
            field_name,
            field,
            reader,
            index,
            id_field,
        }
    }

    pub fn reload(&self) {
        self.reader.reload().unwrap();
    }

    pub fn count(&self) -> u32 {
        let metas = self.index.searchable_segment_metas().unwrap();
        let mut sum: u32 = 0;
        for meta in metas {
            sum += meta.max_doc();
        }
        sum
    }

    pub(crate) fn search(&self, q: &dyn Query) -> Vec<u32> {
        let searcher = self.reader.searcher();
        match self.id_field {
            Some(_) => {
                // newer version with doc_id.
                searcher.search(q, &DocIdCollector {}).unwrap()
            }
            None => {
                // older version without doc_id, only one segment.
                searcher.search(q, &VecCollector {}).unwrap()
            }
        }
    }

    pub fn term_query_i64(&self, term: i64) -> Vec<u32> {
        let q = TermQuery::new(
            Term::from_field_i64(self.field, term),
            IndexRecordOption::Basic,
        );
        self.search(&q)
    }

    pub fn lower_bound_range_query_i64(&self, lower_bound: i64, inclusive: bool) -> Vec<u32> {
        let q = RangeQuery::new_i64_bounds(
            self.field_name.to_string(),
            make_bounds(lower_bound, inclusive),
            Bound::Unbounded,
        );
        self.search(&q)
    }

    pub fn upper_bound_range_query_i64(&self, upper_bound: i64, inclusive: bool) -> Vec<u32> {
        let q = RangeQuery::new_i64_bounds(
            self.field_name.to_string(),
            Bound::Unbounded,
            make_bounds(upper_bound, inclusive),
        );
        self.search(&q)
    }

    pub fn range_query_i64(
        &self,
        lower_bound: i64,
        upper_bound: i64,
        lb_inclusive: bool,
        ub_inclusive: bool,
    ) -> Vec<u32> {
        let lb = make_bounds(lower_bound, lb_inclusive);
        let ub = make_bounds(upper_bound, ub_inclusive);
        let q = RangeQuery::new_i64_bounds(self.field_name.to_string(), lb, ub);
        self.search(&q)
    }

    pub fn term_query_f64(&self, term: f64) -> Vec<u32> {
        let q = TermQuery::new(
            Term::from_field_f64(self.field, term),
            IndexRecordOption::Basic,
        );
        self.search(&q)
    }

    pub fn lower_bound_range_query_f64(&self, lower_bound: f64, inclusive: bool) -> Vec<u32> {
        let q = RangeQuery::new_f64_bounds(
            self.field_name.to_string(),
            make_bounds(lower_bound, inclusive),
            Bound::Unbounded,
        );
        self.search(&q)
    }

    pub fn upper_bound_range_query_f64(&self, upper_bound: f64, inclusive: bool) -> Vec<u32> {
        let q = RangeQuery::new_f64_bounds(
            self.field_name.to_string(),
            Bound::Unbounded,
            make_bounds(upper_bound, inclusive),
        );
        self.search(&q)
    }

    pub fn range_query_f64(
        &self,
        lower_bound: f64,
        upper_bound: f64,
        lb_inclusive: bool,
        ub_inclusive: bool,
    ) -> Vec<u32> {
        let lb = make_bounds(lower_bound, lb_inclusive);
        let ub = make_bounds(upper_bound, ub_inclusive);
        let q = RangeQuery::new_f64_bounds(self.field_name.to_string(), lb, ub);
        self.search(&q)
    }

    pub fn term_query_bool(&self, term: bool) -> Vec<u32> {
        let q = TermQuery::new(
            Term::from_field_bool(self.field, term),
            IndexRecordOption::Basic,
        );
        self.search(&q)
    }

    pub fn term_query_keyword(&self, term: &str) -> Vec<u32> {
        let q = TermQuery::new(
            Term::from_field_text(self.field, term),
            IndexRecordOption::Basic,
        );
        self.search(&q)
    }

    pub fn lower_bound_range_query_keyword(&self, lower_bound: &str, inclusive: bool) -> Vec<u32> {
        let q = RangeQuery::new_str_bounds(
            self.field_name.to_string(),
            make_bounds(lower_bound, inclusive),
            Bound::Unbounded,
        );
        self.search(&q)
    }

    pub fn upper_bound_range_query_keyword(&self, upper_bound: &str, inclusive: bool) -> Vec<u32> {
        let q = RangeQuery::new_str_bounds(
            self.field_name.to_string(),
            Bound::Unbounded,
            make_bounds(upper_bound, inclusive),
        );
        self.search(&q)
    }

    pub fn range_query_keyword(
        &self,
        lower_bound: &str,
        upper_bound: &str,
        lb_inclusive: bool,
        ub_inclusive: bool,
    ) -> Vec<u32> {
        let lb = make_bounds(lower_bound, lb_inclusive);
        let ub = make_bounds(upper_bound, ub_inclusive);
        let q = RangeQuery::new_str_bounds(self.field_name.to_string(), lb, ub);
        self.search(&q)
    }

    pub fn prefix_query_keyword(&self, prefix: &str) -> Vec<u32> {
        let pattern = format!("{}(.|\n)*", prefix);
        self.regex_query(&pattern)
    }

    pub fn regex_query(&self, pattern: &str) -> Vec<u32> {
        let q = RegexQuery::from_pattern(&pattern, self.field).unwrap();
        self.search(&q)
    }
}
