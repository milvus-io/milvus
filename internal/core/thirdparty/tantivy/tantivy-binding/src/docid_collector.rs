use std::ffi::c_void;

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
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
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
    fn collect(&mut self, doc: DocId, _score: Score) {
        self.column.values_for_doc(doc).for_each(|doc_id| {
            self.bitset_wrapper.set(doc_id as u32);
        })
    }

    #[inline]
    fn harvest(self) -> Self::Fruit {}
}
