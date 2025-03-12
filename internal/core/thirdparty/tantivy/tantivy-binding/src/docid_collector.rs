use tantivy::{
    collector::{Collector, SegmentCollector},
    fastfield::Column,
    DocId, Score, SegmentOrdinal, SegmentReader,
};

const BATCH_SIZE: usize = 4096;

#[derive(Default)]
pub(crate) struct DocIdCollector<T> {
    _phantom: std::marker::PhantomData<T>,
}

pub(crate) struct DocIdChildCollector<T> {
    milvus_doc_ids: Vec<T>,
    column: Column<i64>,
    // Batch tantivy doc ids to get milvus doc ids in batch.
    tantivy_doc_ids: Vec<DocId>,
}

impl DocIdChildCollector<u32> {
    fn collect_values_u32(&mut self) {
        self.milvus_doc_ids.extend(
            self.column
                .values_for_docs_flatten(&self.tantivy_doc_ids)
                .into_iter()
                .map(|val| val as u32),
        );
        self.tantivy_doc_ids.clear();
    }
}

impl DocIdChildCollector<i64> {
    fn collect_values_i64(&mut self) {
        self.milvus_doc_ids
            .extend(self.column.values_for_docs_flatten(&self.tantivy_doc_ids));
        self.tantivy_doc_ids.clear();
    }
}

impl Collector for DocIdCollector<u32> {
    type Fruit = Vec<u32>;
    type Child = DocIdChildCollector<u32>;

    fn for_segment(
        &self,
        _segment_local_id: SegmentOrdinal,
        segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(DocIdChildCollector {
            milvus_doc_ids: Vec::new(),
            column: segment.fast_fields().i64("doc_id").unwrap(),
            tantivy_doc_ids: Vec::with_capacity(BATCH_SIZE),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        let len: usize = segment_fruits.iter().map(|docset| docset.len()).sum();
        let mut result = Vec::with_capacity(len);
        for docs in segment_fruits {
            for doc in docs {
                result.push(doc);
            }
        }
        Ok(result)
    }
}

impl SegmentCollector for DocIdChildCollector<u32> {
    type Fruit = Vec<u32>;

    fn collect(&mut self, doc: DocId, _score: Score) {
        if self.tantivy_doc_ids.len() == BATCH_SIZE {
            self.collect_values_u32();
        }
        self.tantivy_doc_ids.push(doc);
    }

    fn harvest(mut self) -> Self::Fruit {
        if !self.tantivy_doc_ids.is_empty() {
            self.collect_values_u32();
        }
        self.milvus_doc_ids
    }
}

impl Collector for DocIdCollector<i64> {
    type Fruit = Vec<i64>;
    type Child = DocIdChildCollector<i64>;

    fn for_segment(
        &self,
        _segment_local_id: SegmentOrdinal,
        segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(DocIdChildCollector {
            milvus_doc_ids: Vec::new(),
            column: segment.fast_fields().i64("doc_id").unwrap(),
            tantivy_doc_ids: Vec::with_capacity(BATCH_SIZE),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        let len: usize = segment_fruits.iter().map(|docset| docset.len()).sum();
        let mut result = Vec::with_capacity(len);
        for docs in segment_fruits {
            for doc in docs {
                result.push(doc);
            }
        }
        Ok(result)
    }
}

impl SegmentCollector for DocIdChildCollector<i64> {
    type Fruit = Vec<i64>;

    fn collect(&mut self, doc: DocId, _score: Score) {
        if self.tantivy_doc_ids.len() == BATCH_SIZE {
            self.collect_values_i64();
        }
        self.tantivy_doc_ids.push(doc);
    }

    fn harvest(mut self) -> Self::Fruit {
        if !self.tantivy_doc_ids.is_empty() {
            self.collect_values_i64();
        }
        self.milvus_doc_ids
    }
}
