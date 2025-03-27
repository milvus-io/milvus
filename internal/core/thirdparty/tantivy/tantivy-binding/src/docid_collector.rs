use tantivy::{
    collector::{Collector, SegmentCollector},
    fastfield::Column,
    DocId, Score, SegmentOrdinal, SegmentReader,
};

#[derive(Default)]
pub(crate) struct DocIdCollector<T> {
    _phantom: std::marker::PhantomData<T>,
}

pub(crate) struct DocIdChildCollector<T> {
    milvus_doc_ids: Vec<T>,
    column: Column<i64>,
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

    fn collect_block(&mut self, docs: &[DocId]) {
        self.milvus_doc_ids.extend(
            self.column
                .values_for_docs_flatten(docs)
                .into_iter()
                .map(|val| val as u32),
        );
    }

    fn collect(&mut self, doc: DocId, _score: Score) {
        // Unreachable code actually.
        self.collect_block(&[doc]);
    }

    fn harvest(self) -> Self::Fruit {
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

    fn collect_block(&mut self, docs: &[DocId]) {
        self.milvus_doc_ids
            .extend(self.column.values_for_docs_flatten(docs));
    }

    fn collect(&mut self, doc: DocId, _score: Score) {
        // Unreachable code actually.
        self.collect_block(&[doc]);
    }

    fn harvest(self) -> Self::Fruit {
        self.milvus_doc_ids
    }
}
