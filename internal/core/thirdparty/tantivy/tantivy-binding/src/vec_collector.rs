use log::warn;
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

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> tantivy::Result<()> {
        if segment_fruits.len() == 1 {
            Ok(())
        } else {
            warn!(
                "inverted index should have only one segment, but got {} segments",
                segment_fruits.len()
            );
            Ok(())
        }
    }
}

pub struct VecChildCollector {
    bitset_wrapper: BitsetWrapper,
}

impl SegmentCollector for VecChildCollector {
    type Fruit = ();

    #[inline]
    fn collect(&mut self, doc: DocId, _score: tantivy::Score) {
        self.bitset_wrapper.batch_set(&[doc]);
    }

    #[inline]
    fn harvest(self) -> Self::Fruit {}
}
