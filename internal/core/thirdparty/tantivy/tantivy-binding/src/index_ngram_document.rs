use std::sync::Arc;

use tantivy::schema::{Document, Field};

use crate::error::{Result, TantivyBindingError};

const ABSENT_START: u32 = u32::MAX;

pub(crate) struct NgramBatchData {
    field: Field,
    text: String,
}

impl NgramBatchData {
    pub(crate) fn new(field: Field, text: String) -> Result<Self> {
        u32::try_from(text.len()).map_err(|_| {
            TantivyBindingError::InvalidArgument(
                "ngram batch text exceeds the u32 document range".to_string(),
            )
        })?;
        Ok(Self { field, text })
    }
}

#[derive(Clone)]
pub(crate) struct NgramDocument {
    batch: Arc<NgramBatchData>,
    start: u32,
    len: u32,
}

impl NgramDocument {
    pub(crate) fn present(batch: Arc<NgramBatchData>, start: u32, len: u32) -> Result<Self> {
        if start == ABSENT_START {
            return Err(TantivyBindingError::InternalError(
                "ngram document range uses the reserved absent offset".to_string(),
            ));
        }
        let end = start.checked_add(len).ok_or_else(|| {
            TantivyBindingError::InternalError("ngram document range overflows u32".to_string())
        })?;
        if batch.text.get(start as usize..end as usize).is_none() {
            return Err(TantivyBindingError::InternalError(
                "ngram document range is outside the shared UTF-8 buffer".to_string(),
            ));
        }
        Ok(Self { batch, start, len })
    }

    pub(crate) fn absent(batch: Arc<NgramBatchData>) -> Self {
        Self {
            batch,
            start: ABSENT_START,
            len: 0,
        }
    }

    pub(crate) fn from_validated_range(batch: Arc<NgramBatchData>, start: u32, len: u32) -> Self {
        Self { batch, start, len }
    }
}

impl Document for NgramDocument {
    type Value<'a> = &'a str;
    type FieldsValuesIter<'a> = std::option::IntoIter<(Field, &'a str)>;

    fn iter_fields_and_values(&self) -> Self::FieldsValuesIter<'_> {
        if self.start == ABSENT_START {
            return None.into_iter();
        }

        let end = self.start.saturating_add(self.len);
        self.batch
            .text
            .get(self.start as usize..end as usize)
            .map(|value| (self.batch.field, value))
            .into_iter()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tantivy::schema::{Document, Field};

    use super::{NgramBatchData, NgramDocument};

    fn test_field() -> Field {
        Field::from_field_id(3)
    }

    fn field_values(document: &NgramDocument) -> Vec<(Field, &str)> {
        document.iter_fields_and_values().collect()
    }

    #[test]
    fn present_document_yields_one_string_value() {
        let batch = Arc::new(NgramBatchData::new(test_field(), "prefixvalue".to_string()).unwrap());
        let document = NgramDocument::present(batch, 6, 5).unwrap();

        assert_eq!(field_values(&document), vec![(test_field(), "value")]);
    }

    #[test]
    fn empty_string_is_distinct_from_absent() {
        let batch = Arc::new(NgramBatchData::new(test_field(), String::new()).unwrap());
        let empty = NgramDocument::present(batch.clone(), 0, 0).unwrap();
        let absent = NgramDocument::absent(batch);

        assert_eq!(field_values(&empty), vec![(test_field(), "")]);
        assert!(field_values(&absent).is_empty());
    }

    #[test]
    fn document_preserves_embedded_nul_and_utf8_boundaries() {
        let text = "nul\0byte测试tail".to_string();
        let value_len = "nul\0byte测试".len() as u32;
        let batch = Arc::new(NgramBatchData::new(test_field(), text).unwrap());
        let document = NgramDocument::present(batch, 0, value_len).unwrap();

        assert_eq!(
            field_values(&document),
            vec![(test_field(), "nul\0byte测试")]
        );
    }

    #[test]
    fn cloned_documents_share_the_same_batch() {
        let batch = Arc::new(NgramBatchData::new(test_field(), "shared".to_string()).unwrap());
        let document = NgramDocument::present(batch, 0, 6).unwrap();
        let clone = document.clone();

        assert!(Arc::ptr_eq(&document.batch, &clone.batch));
    }

    #[test]
    fn document_remains_valid_after_source_and_siblings_are_dropped() {
        let document = {
            let source = String::from("owned测试");
            let mut flat = String::new();
            flat.push_str(&source);
            let batch = Arc::new(NgramBatchData::new(test_field(), flat).unwrap());
            let sibling = NgramDocument::absent(batch.clone());
            let document = NgramDocument::present(batch, 0, source.len() as u32).unwrap();
            drop(sibling);
            drop(source);
            document
        };

        assert_eq!(field_values(&document), vec![(test_field(), "owned测试")]);
    }
}
