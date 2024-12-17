use std::ops::Bound;
use std::str::FromStr;

use tantivy::directory::MmapDirectory;
use tantivy::query::{Query, RangeQuery, RegexQuery, TermQuery};
use tantivy::schema::{Field, IndexRecordOption};
use tantivy::{Index, IndexReader, ReloadPolicy, Term};

use crate::log::init_log;
use crate::util::make_bounds;
use crate::vec_collector::VecCollector;

pub struct IndexReaderWrapper {
    pub field_name: String,
    pub field: Field,
    pub reader: IndexReader,
    pub cnt: u32,
}

impl IndexReaderWrapper {
    pub fn new(index: &Index, field_name: &String, field: Field) -> IndexReaderWrapper {
        init_log();

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .unwrap();
        let metas = index.searchable_segment_metas().unwrap();
        let mut sum: u32 = 0;
        for meta in metas {
            sum += meta.max_doc();
        }
        reader.reload().unwrap();
        IndexReaderWrapper {
            field_name: field_name.to_string(),
            field,
            reader,
            cnt: sum,
        }
    }

    pub fn load(path: &str) -> IndexReaderWrapper {
        let dir = MmapDirectory::open(path).unwrap();
        let index = Index::open(dir).unwrap();
        let field = index.schema().fields().next().unwrap().0;
        let schema = index.schema();
        let field_name = schema.get_field_name(field);
        IndexReaderWrapper::new(&index, &String::from_str(field_name).unwrap(), field)
    }

    pub fn count(&self) -> u32 {
        self.cnt
    }

    fn search(&self, q: &dyn Query) -> Vec<u32> {
        let searcher = self.reader.searcher();
        let hits = searcher.search(q, &VecCollector).unwrap();
        hits
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
        let escaped = regex::escape(prefix);
        let pattern = format!("{}(.|\n)*", escaped);
        self.regex_query(&pattern)
    }

    pub fn regex_query(&self, pattern: &str) -> Vec<u32> {
        let q = RegexQuery::from_pattern(&pattern, self.field).unwrap();
        self.search(&q)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use tantivy::{
        doc,
        schema::{self, Schema, STORED, STRING, TEXT},
        Index, IndexWriter,
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
        let index_reader_wrapper = IndexReaderWrapper::from_index(index_shared);
        let mut res = index_reader_wrapper.prefix_query_keyword("^");
        assert_eq!(res.len(), 1);
        res = index_reader_wrapper.prefix_query_keyword("$");
        assert_eq!(res.len(), 1);
    }
}
