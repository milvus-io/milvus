use std::fmt;
use std::ops::Bound;
use std::str::FromStr;

use futures::executor::block_on;

use tantivy::collector::TopDocs;
use tantivy::directory::MmapDirectory;
use tantivy::query::{Query, RangeQuery, TermQuery, RegexQuery};
use tantivy::schema::{Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions, INDEXED};
use tantivy::{doc, tokenizer, DocAddress, Index, IndexReader, IndexWriter, ReloadPolicy, Term};

use crate::data_type::TantivyDataType;
use crate::util::make_bounds;

pub struct IndexWrapper {
    pub field_name: Option<String>,
    pub field: Option<Field>,
    pub data_type: Option<TantivyDataType>,
    pub path: Option<String>,
    pub index: Option<Index>,
    pub index_writer: Option<IndexWriter>,
    pub reader: Option<IndexReader>,
}

impl IndexWrapper {
    fn create_reader(&mut self) {
        let reader = self
            .index
            .as_mut()
            .unwrap()
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .unwrap();
        self.reader = Some(reader)
    }

    fn reload(&mut self) {
        self.reader.as_mut().unwrap().reload().unwrap();
    }

    pub fn new(field_name: String, data_type: TantivyDataType, path: String) -> IndexWrapper {
        let field: Field;
        let mut schema_builder = Schema::builder();
        let mut use_raw_tokenizer = false;
        match data_type {
            TantivyDataType::I64 => {
                field = schema_builder.add_i64_field(&field_name, INDEXED);
            }
            TantivyDataType::F64 => {
                field = schema_builder.add_f64_field(&field_name, INDEXED);
            }
            TantivyDataType::Bool => {
                field = schema_builder.add_bool_field(&field_name, INDEXED);
            }
            TantivyDataType::Keyword => {
                let text_field_indexing = TextFieldIndexing::default()
                    .set_tokenizer("raw_tokenizer")
                    .set_index_option(IndexRecordOption::Basic);
                let text_options = TextOptions::default().set_indexing_options(text_field_indexing);
                field = schema_builder.add_text_field(&field_name, text_options);
                use_raw_tokenizer = true;
            }
            _ => {
                panic!("not supported");
            }
        }
        let schema = schema_builder.build();
        let index = Index::create_in_dir(path.clone(), schema).unwrap();
        if use_raw_tokenizer {
            index
                .tokenizers()
                .register("raw_tokenizer", tokenizer::RawTokenizer::default());
        }
        let index_writer = index.writer_with_num_threads(1, 15_000_000).unwrap();
        let mut ret = IndexWrapper {
            field_name: Some(field_name),
            field: Some(field),
            data_type: Some(data_type),
            path: Some(path),
            index: Some(index),
            index_writer: Some(index_writer),
            reader: None,
        };
        ret.create_reader();
        ret
    }

    pub fn load(path: &str) -> IndexWrapper {
        let dir = MmapDirectory::open(path).unwrap();
        let index = Index::open(dir).unwrap();
        let field = index.schema().fields().next().unwrap().0;
        let schema = index.schema();
        let field_name = schema.get_field_name(field);
        let mut ret = IndexWrapper {
            field_name: Some(String::from_str(field_name).unwrap()),
            field: Some(field),
            data_type: None,
            path: None,
            index: Some(index),
            index_writer: None,
            reader: None,
        };
        ret.create_reader();
        ret.reload();
        ret
    }

    pub fn add_i8(&mut self, data: i8) {
        self.add_i64(data.into())
    }

    pub fn add_i16(&mut self, data: i16) {
        self.add_i64(data.into())
    }

    pub fn add_i32(&mut self, data: i32) {
        self.add_i64(data.into())
    }

    pub fn add_i64(&mut self, data: i64) {
        // println!("{} was input", data);
        self.index_writer
            .as_mut()
            .unwrap()
            .add_document(doc!(self.field.unwrap() => data))
            .unwrap();
    }

    pub fn add_f32(&mut self, data: f32) {
        self.add_f64(data.into())
    }

    pub fn add_f64(&mut self, data: f64) {
        // println!("{} was input", data);
        self.index_writer
            .as_mut()
            .unwrap()
            .add_document(doc!(self.field.unwrap() => data))
            .unwrap();
    }

    pub fn add_bool(&mut self, data: bool) {
        // println!("{} was input", data);
        self.index_writer
            .as_mut()
            .unwrap()
            .add_document(doc!(self.field.unwrap() => data))
            .unwrap();
    }

    pub fn add_keyword(&mut self, data: &str) {
        // println!("{} was input", data);
        self.index_writer
            .as_mut()
            .unwrap()
            .add_document(doc!(self.field.unwrap() => data))
            .unwrap();
    }

    pub fn finish(&mut self) {
        self.index_writer.as_mut().unwrap().commit().unwrap();
        block_on(self.index_writer.as_mut().unwrap().garbage_collect_files()).unwrap();
        self.reload();
    }

    pub fn count(&mut self) -> u32 {
        let metas = self
            .index
            .as_mut()
            .unwrap()
            .searchable_segment_metas()
            .unwrap();
        let mut sum: u32 = 0;
        for meta in metas {
            sum += meta.max_doc();
        }
        sum
    }

    fn search(&mut self, q: &dyn Query) -> Vec<u32> {
        let searcher = self.reader.as_mut().unwrap().searcher();
        let cnt = self.count();
        let hits = searcher
            .search(q, &TopDocs::with_limit(cnt as usize))
            .unwrap();
        let mut ret = Vec::new();
        for (_, address) in hits {
            // println!("id: {}", address.doc_id);
            ret.push(address.doc_id);
        }
        ret
    }

    pub fn term_query_i64(&mut self, term: i64) -> Vec<u32> {
        let q = TermQuery::new(
            Term::from_field_i64(*self.field.as_mut().unwrap(), term),
            IndexRecordOption::Basic,
        );
        self.search(&q)
    }

    pub fn lower_bound_range_query_i64(&mut self, lower_bound: i64, inclusive: bool) -> Vec<u32> {
        let q = RangeQuery::new_i64_bounds(
            self.field_name.as_mut().unwrap().to_string(),
            make_bounds(lower_bound, inclusive),
            Bound::Unbounded,
        );
        self.search(&q)
    }

    pub fn upper_bound_range_query_i64(&mut self, upper_bound: i64, inclusive: bool) -> Vec<u32> {
        let q = RangeQuery::new_i64_bounds(
            self.field_name.as_mut().unwrap().to_string(),
            Bound::Unbounded,
            make_bounds(upper_bound, inclusive),
        );
        self.search(&q)
    }

    pub fn range_query_i64(
        &mut self,
        lower_bound: i64,
        upper_bound: i64,
        lb_inclusive: bool,
        ub_inclusive: bool,
    ) -> Vec<u32> {
        let lb = make_bounds(lower_bound, lb_inclusive);
        let ub = make_bounds(upper_bound, ub_inclusive);
        let q = RangeQuery::new_i64_bounds(self.field_name.as_mut().unwrap().to_string(), lb, ub);
        self.search(&q)
    }

    pub fn term_query_f64(&mut self, term: f64) -> Vec<u32> {
        let q = TermQuery::new(
            Term::from_field_f64(*self.field.as_mut().unwrap(), term),
            IndexRecordOption::Basic,
        );
        self.search(&q)
    }

    pub fn lower_bound_range_query_f64(&mut self, lower_bound: f64, inclusive: bool) -> Vec<u32> {
        let q = RangeQuery::new_f64_bounds(
            self.field_name.as_mut().unwrap().to_string(),
            make_bounds(lower_bound, inclusive),
            Bound::Unbounded,
        );
        self.search(&q)
    }

    pub fn upper_bound_range_query_f64(&mut self, upper_bound: f64, inclusive: bool) -> Vec<u32> {
        let q = RangeQuery::new_f64_bounds(
            self.field_name.as_mut().unwrap().to_string(),
            Bound::Unbounded,
            make_bounds(upper_bound, inclusive),
        );
        self.search(&q)
    }

    pub fn range_query_f64(
        &mut self,
        lower_bound: f64,
        upper_bound: f64,
        lb_inclusive: bool,
        ub_inclusive: bool,
    ) -> Vec<u32> {
        let lb = make_bounds(lower_bound, lb_inclusive);
        let ub = make_bounds(upper_bound, ub_inclusive);
        let q = RangeQuery::new_f64_bounds(self.field_name.as_mut().unwrap().to_string(), lb, ub);
        self.search(&q)
    }

    pub fn term_query_bool(&mut self, term: bool) -> Vec<u32> {
        let q = TermQuery::new(
            Term::from_field_bool(*self.field.as_mut().unwrap(), term),
            IndexRecordOption::Basic,
        );
        self.search(&q)
    }

    pub fn term_query_keyword(&mut self, term: &str) -> Vec<u32> {
        let q = TermQuery::new(
            Term::from_field_text(*self.field.as_mut().unwrap(), term),
            IndexRecordOption::Basic,
        );
        self.search(&q)
    }

    pub fn lower_bound_range_query_keyword(&mut self, lower_bound: &str, inclusive: bool) -> Vec<u32> {
        let q = RangeQuery::new_str_bounds(
            self.field_name.as_mut().unwrap().to_string(),
            make_bounds(lower_bound, inclusive),
            Bound::Unbounded,
        );
        self.search(&q)
    }

    pub fn upper_bound_range_query_keyword(&mut self, upper_bound: &str, inclusive: bool) -> Vec<u32> {
        let q = RangeQuery::new_str_bounds(
            self.field_name.as_mut().unwrap().to_string(),
            Bound::Unbounded,
            make_bounds(upper_bound, inclusive),
        );
        self.search(&q)
    }

    pub fn range_query_keyword(
        &mut self,
        lower_bound: &str,
        upper_bound: &str,
        lb_inclusive: bool,
        ub_inclusive: bool,
    ) -> Vec<u32> {
        let lb = make_bounds(lower_bound, lb_inclusive);
        let ub = make_bounds(upper_bound, ub_inclusive);
        let q = RangeQuery::new_str_bounds(self.field_name.as_mut().unwrap().to_string(), lb, ub);
        self.search(&q)
    }

    pub fn prefix_query_keyword(
        &mut self,
        prefix: &str,
    ) -> Vec<u32> {
        let pattern = format!("{}(.|\n)*", prefix);
        let q = RegexQuery::from_pattern(&pattern, *self.field.as_mut().unwrap()).unwrap();
        self.search(&q)
    }
}
