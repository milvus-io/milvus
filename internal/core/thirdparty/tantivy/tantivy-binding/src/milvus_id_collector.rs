use tantivy::{
    collector::{Collector, SegmentCollector},
    DocId, Score, SegmentOrdinal, SegmentReader,
};

use crate::bitset_wrapper::BitsetWrapper;

pub(crate) struct MilvusIdCollector {
    pub(crate) bitset_wrapper: BitsetWrapper,
}

pub(crate) struct MilvusIdChildCollector {
    pub(crate) bitset_wrapper: BitsetWrapper,
}

impl Collector for MilvusIdCollector {
    type Fruit = ();
    type Child = MilvusIdChildCollector;

    fn for_segment(
        &self,
        _segment_local_id: SegmentOrdinal,
        _segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(MilvusIdChildCollector {
            bitset_wrapper: self.bitset_wrapper.clone(),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        _segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        Ok(())
    }
}

impl SegmentCollector for MilvusIdChildCollector {
    type Fruit = ();

    #[inline]
    fn collect_block(&mut self, docs: &[DocId]) {
        self.bitset_wrapper.batch_set(docs);
    }

    fn collect(&mut self, doc: DocId, _score: Score) {
        // Unreachable code actually
        self.collect_block(&[doc]);
    }

    #[inline]
    fn harvest(self) -> Self::Fruit {}
}
