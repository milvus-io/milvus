use std::ops::Bound;
use std::str::FromStr;
use std::sync::Arc;

use tantivy::directory::MmapDirectory;
use tantivy::query::{Query, RangeQuery, RegexQuery, TermQuery};
use tantivy::schema::{Field, IndexRecordOption};
use tantivy::{Index, IndexReader, ReloadPolicy, Term};

use crate::docid_collector::DocIdCollector;
use crate::log::init_log;
use crate::util::make_bounds;
use crate::vec_collector::VecCollector;

use crate::error::{Result, TantivyBindingError};

pub(crate) struct IndexReaderWrapper {
    pub(crate) field_name: String,
    pub(crate) field: Field,
    pub(crate) reader: IndexReader,
    pub(crate) index: Arc<Index>,
    pub(crate) id_field: Option<Field>,
}

impl IndexReaderWrapper {
    pub fn load(path: &str) -> Result<IndexReaderWrapper> {
        init_log();

        let index = Index::open_in_dir(path)?;

        IndexReaderWrapper::from_index(Arc::new(index))
    }

    pub fn from_index(index: Arc<Index>) -> Result<IndexReaderWrapper> {
        let schema = index.schema();
        let field = schema.fields().next().unwrap().0;
        let field_name = String::from(schema.get_field_name(field));
        let id_field = schema.get_field("doc_id").ok();

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit) // OnCommit serve for growing segment.
            .try_into()?;
        reader.reload()?;

        Ok(IndexReaderWrapper {
            field_name,
            field,
            reader,
            index,
            id_field,
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
            sum += meta.max_doc();
        }
        Ok(sum)
    }

    pub(crate) fn search(&self, q: &dyn Query) -> Result<Vec<u32>> {
        let searcher = self.reader.searcher();
        // We set `id_field` when "doc_id" exists in schema which means
        // there may be multiple segments within the index. If only one segment is allowed
        // in the index, we can trivially know which rows of milvus relate to which
        // docs in tantivy. So `VecCollector` is enough in such casese.
        // When more than one segments are allowed, we use doc_id field to build the relationship
        // between milvus and tantivy. In this case, we use `DocIdCollector`.
        match self.id_field {
            Some(_) => {
                // newer version with doc_id.
                searcher
                    .search(q, &DocIdCollector {})
                    .map_err(TantivyBindingError::TantivyError)
            }
            None => {
                // older version without doc_id, only one segment.
                searcher
                    .search(q, &VecCollector {})
                    .map_err(TantivyBindingError::TantivyError)
            }
        }
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
        let q = RangeQuery::new_i64_bounds(
            self.field_name.to_string(),
            make_bounds(lower_bound, inclusive),
            Bound::Unbounded,
        );
        self.search(&q)
    }

    pub fn upper_bound_range_query_i64(
        &self,
        upper_bound: i64,
        inclusive: bool,
    ) -> Result<Vec<u32>> {
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
    ) -> Result<Vec<u32>> {
        let lb = make_bounds(lower_bound, lb_inclusive);
        let ub = make_bounds(upper_bound, ub_inclusive);
        let q = RangeQuery::new_i64_bounds(self.field_name.to_string(), lb, ub);
        self.search(&q)
    }

    pub fn term_query_f64(&self, term: f64) -> Result<Vec<u32>> {
        let q = TermQuery::new(
            Term::from_field_f64(self.field, term),
            IndexRecordOption::Basic,
        );
        self.search(&q)
    }

    pub fn lower_bound_range_query_f64(
        &self,
        lower_bound: f64,
        inclusive: bool,
    ) -> Result<Vec<u32>> {
        let q = RangeQuery::new_f64_bounds(
            self.field_name.to_string(),
            make_bounds(lower_bound, inclusive),
            Bound::Unbounded,
        );
        self.search(&q)
    }

    pub fn upper_bound_range_query_f64(
        &self,
        upper_bound: f64,
        inclusive: bool,
    ) -> Result<Vec<u32>> {
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
    ) -> Result<Vec<u32>> {
        let lb = make_bounds(lower_bound, lb_inclusive);
        let ub = make_bounds(upper_bound, ub_inclusive);
        let q = RangeQuery::new_f64_bounds(self.field_name.to_string(), lb, ub);
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

    pub fn lower_bound_range_query_keyword(
        &self,
        lower_bound: &str,
        inclusive: bool,
    ) -> Result<Vec<u32>> {
        let q = RangeQuery::new_str_bounds(
            self.field_name.to_string(),
            make_bounds(lower_bound, inclusive),
            Bound::Unbounded,
        );
        self.search(&q)
    }

    pub fn upper_bound_range_query_keyword(
        &self,
        upper_bound: &str,
        inclusive: bool,
    ) -> Result<Vec<u32>> {
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
    ) -> Result<Vec<u32>> {
        let lb = make_bounds(lower_bound, lb_inclusive);
        let ub = make_bounds(upper_bound, ub_inclusive);
        let q = RangeQuery::new_str_bounds(self.field_name.to_string(), lb, ub);
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
    use std::{
        ops::{Bound, Range},
        sync::Arc,
    };
    use tempfile::Builder;

    use tantivy::{
        doc,
        query::RangeQuery,
        schema::{self, Schema, FAST, INDEXED, STORED, STRING, TEXT},
        Index, IndexWriter, SingleSegmentIndexWriter, Term,
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
    fn test_load_without_doc_id() {
        let temp_dir = Builder::new()
            .prefix("test_index_load_without_doc_id")
            .tempdir()
            .unwrap();

        {
            let mut schema_builder = Schema::builder();
            let f64_field = schema_builder.add_f64_field("f64", INDEXED);
            let schema = schema_builder.build();
            let index = Index::create_in_dir(&temp_dir, schema).unwrap();
            let mut index_writer = SingleSegmentIndexWriter::new(index, 100_000_000).unwrap();

            for i in 0..4 {
                for j in 0..10 {
                    let n = i * 10 + j;
                    index_writer
                        .add_document(doc!(
                            f64_field => n as f64,
                        ))
                        .unwrap();
                }
            }
            index_writer.finalize().unwrap();
        }

        let index = IndexReaderWrapper::load(temp_dir.path().to_str().unwrap());
        let query = RangeQuery::new_f64(
            index.field_name.clone(),
            Range {
                start: 0.0,
                end: 101.0,
            },
        );
        let mut res = index.search(&query);
        res.sort();

        let expected = (0..40).collect::<Vec<u32>>();
        assert_eq!(res, expected);
    }

    #[test]
    fn test_load_with_doc_id() {
        let temp_dir = Builder::new()
            .prefix("test_index_load_with_doc_id")
            .tempdir()
            .unwrap();

        {
            let mut schema_builder = Schema::builder();
            let f64_field = schema_builder.add_f64_field("f64", INDEXED);
            let dock_id_field = schema_builder.add_i64_field("doc_id", FAST);
            let schema = schema_builder.build();
            let index = Index::create_in_dir(&temp_dir, schema).unwrap();
            let mut index_writer = index.writer_with_num_threads(4, 100_000_000).unwrap();

            for i in 0..4 {
                for j in 0..10 {
                    let n = i * 10 + j;
                    index_writer
                        .add_document(doc!(
                            f64_field => n as f64,
                            dock_id_field => n as i64,
                        ))
                        .unwrap();
                }
                index_writer.commit().unwrap();
            }
        }

        let index = IndexReaderWrapper::load(temp_dir.path().to_str().unwrap());
        let query = RangeQuery::new_f64(
            index.field_name.clone(),
            Range {
                start: 0.0,
                end: 101.0,
            },
        );
        let mut res = index.search(&query);
        res.sort();

        let expected = (0..40).collect::<Vec<u32>>();
        assert_eq!(res, expected);
    }
}
