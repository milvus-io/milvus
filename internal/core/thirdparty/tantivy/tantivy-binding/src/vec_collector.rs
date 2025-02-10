use tantivy::{
    collector::{Collector, SegmentCollector},
    DocId,
};

use crate::bitset_wrapper::BitsetWrapper;

// "warning": bitset_wrapper has no guarantee for thread safety, so `DocIdChildCollector`
// should be handled serializely which means we should only use single thread
// for executing query.
pub struct VecCollector {
    pub(crate) bitset_wrapper: BitsetWrapper,
}

impl Collector for VecCollector {
    type Fruit = ();

    type Child = VecChildCollector;

    fn for_segment(
        &self,
        _segment_local_id: tantivy::SegmentOrdinal,
        _segment: &tantivy::SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(VecChildCollector {
            bitset_wrapper: self.bitset_wrapper.clone(),
        })
    }

    #[inline]
    fn requires_scoring(&self) -> bool {
        false
    }

    #[inline]
    fn merge_fruits(&self, _: Vec<()>) -> tantivy::Result<()> {
        Ok(())
    }
}

pub struct VecChildCollector {
    bitset_wrapper: BitsetWrapper,
}

impl SegmentCollector for VecChildCollector {
    type Fruit = ();

    #[inline]
    fn collect(&mut self, doc: DocId, _score: tantivy::Score) {
        self.bitset_wrapper.set(doc);
    }

    #[inline]
    fn harvest(self) -> Self::Fruit {}
}
