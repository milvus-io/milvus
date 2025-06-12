use tantivy::{
    collector::{Collector, SegmentCollector},
    fastfield::Column,
    DocId, Score, SegmentOrdinal, SegmentReader,
};

use crate::bitset_wrapper::BitsetWrapper;

// "warning": bitset_wrapper has no guarantee for thread safety, so `DocIdChildCollector`
// should be handled serializely which means we should only use single thread
// for executing query.
pub(crate) struct DocIdCollector {
    pub(crate) bitset_wrapper: BitsetWrapper,
}

impl Collector for DocIdCollector {
    type Fruit = ();
    type Child = DocIdChildCollector;

    fn for_segment(
        &self,
        _segment_local_id: SegmentOrdinal,
        segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(DocIdChildCollector {
            bitset_wrapper: self.bitset_wrapper.clone(),
            column: segment.fast_fields().i64("doc_id").unwrap(),
        })
    }

    #[inline]
    fn requires_scoring(&self) -> bool {
        false
    }

    #[inline]
    fn merge_fruits(
        &self,
        _: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        Ok(())
    }
}

pub(crate) struct DocIdChildCollector {
    bitset_wrapper: BitsetWrapper,
    column: Column<i64>,
}

impl SegmentCollector for DocIdChildCollector {
    type Fruit = ();

    #[inline]
    fn collect_block(&mut self, docs: &[DocId]) {
        let docs: Vec<_> = self
            .column
            .values_for_docs_flatten(docs)
            .into_iter()
            .map(|doc_id| doc_id as u32)
            .collect();
        self.bitset_wrapper.batch_set(&docs);
    }

    fn collect(&mut self, doc: DocId, _score: Score) {
        // Unreachable code actually.
        self.collect_block(&[doc]);
    }

    #[inline]
    fn harvest(self) -> Self::Fruit {}
}

#[derive(Default)]
pub(crate) struct DocIdCollectorI64;

impl Collector for DocIdCollectorI64 {
    type Fruit = Vec<i64>;
    type Child = DocIdChildCollectorI64;

    fn for_segment(
        &self,
        _segment_local_id: SegmentOrdinal,
        segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(DocIdChildCollectorI64 {
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

pub(crate) struct DocIdChildCollectorI64 {
    milvus_doc_ids: Vec<i64>,
    column: Column<i64>,
}

impl SegmentCollector for DocIdChildCollectorI64 {
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
