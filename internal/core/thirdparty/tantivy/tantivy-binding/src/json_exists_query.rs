use core::fmt::Debug;

use tantivy::columnar::{ColumnIndex, ColumnType, DynamicColumn};
use tantivy::index::SegmentReader;
use tantivy::query::{ConstScorer, EmptyScorer, EnableScoring, Explanation, Query, Scorer, Weight};
use tantivy::schema::Type;
use tantivy::{DocId, DocSet, Score, TantivyError, TERMINATED};

use crate::data_type::JsonExistValueType;

#[derive(Clone, Debug)]
pub struct JsonExistsQuery {
    field_name: String,
    json_subpaths: bool,
    value_type: JsonExistValueType,
}

impl JsonExistsQuery {
    pub fn new(field_name: String, json_subpaths: bool, value_type: JsonExistValueType) -> Self {
        Self {
            field_name,
            json_subpaths,
            value_type,
        }
    }
}

impl Query for JsonExistsQuery {
    fn weight(&self, enable_scoring: EnableScoring) -> tantivy::Result<Box<dyn Weight>> {
        let schema = enable_scoring.schema();
        let Some((field, _path)) = schema.find_field(&self.field_name) else {
            return Err(TantivyError::FieldNotFound(self.field_name.clone()));
        };
        let field_type = schema.get_field_entry(field).field_type();
        if !field_type.is_fast() {
            return Err(TantivyError::SchemaError(format!(
                "Field {} is not a fast field.",
                self.field_name
            )));
        }
        Ok(Box::new(JsonExistsWeight {
            field_name: self.field_name.clone(),
            field_type: field_type.value_type(),
            json_subpaths: self.json_subpaths,
            value_type: self.value_type,
        }))
    }
}

struct JsonExistsWeight {
    field_name: String,
    field_type: Type,
    json_subpaths: bool,
    value_type: JsonExistValueType,
}

impl Weight for JsonExistsWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> tantivy::Result<Box<dyn Scorer>> {
        let fast_field_reader = reader.fast_fields();
        let mut column_handles = fast_field_reader.dynamic_column_handles(&self.field_name)?;
        if self.field_type == Type::Json && self.json_subpaths {
            let mut sub_columns =
                fast_field_reader.dynamic_subpath_column_handles(&self.field_name)?;
            column_handles.append(&mut sub_columns);
        }

        let dynamic_columns: tantivy::Result<Vec<DynamicColumn>> = column_handles
            .into_iter()
            .filter(|handle| self.value_type.matches(handle.column_type()))
            .map(|handle| handle.open().map_err(Into::into))
            .collect();
        let non_empty_columns: Vec<DynamicColumn> = dynamic_columns?
            .into_iter()
            .filter(|column| !matches!(column.column_index(), ColumnIndex::Empty { .. }))
            .collect();

        if non_empty_columns.is_empty() {
            Ok(Box::new(EmptyScorer))
        } else {
            let docset = JsonExistsDocSet::new(non_empty_columns, reader.max_doc());
            Ok(Box::new(ConstScorer::new(docset, boost)))
        }
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> tantivy::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) != doc {
            return Err(TantivyError::InvalidArgument(format!(
                "Document #{doc} does not match"
            )));
        }
        Ok(Explanation::new("JsonExistsQuery", 1.0))
    }
}

impl JsonExistValueType {
    fn matches(self, column_type: ColumnType) -> bool {
        match self {
            Self::Any => true,
            Self::Numeric => matches!(
                column_type,
                ColumnType::I64 | ColumnType::U64 | ColumnType::F64
            ),
            Self::String => column_type == ColumnType::Str,
            Self::Bool => column_type == ColumnType::Bool,
        }
    }
}

struct JsonExistsDocSet {
    columns: Vec<DynamicColumn>,
    doc: DocId,
    max_doc: DocId,
}

impl JsonExistsDocSet {
    fn new(columns: Vec<DynamicColumn>, max_doc: DocId) -> Self {
        let mut set = Self {
            columns,
            doc: 0,
            max_doc,
        };
        set.find_next();
        set
    }

    fn find_next(&mut self) -> DocId {
        while self.doc < self.max_doc {
            if self
                .columns
                .iter()
                .any(|column| column.column_index().has_value(self.doc))
            {
                return self.doc;
            }
            self.doc += 1;
        }
        self.doc = TERMINATED;
        TERMINATED
    }
}

impl DocSet for JsonExistsDocSet {
    fn advance(&mut self) -> DocId {
        self.seek(self.doc + 1)
    }

    fn size_hint(&self) -> u32 {
        0
    }

    fn doc(&self) -> DocId {
        self.doc
    }

    #[inline(always)]
    fn seek(&mut self, target: DocId) -> DocId {
        self.doc = target;
        self.find_next()
    }
}
