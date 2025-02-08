use log::warn;
use tantivy::{
    collector::{Collector, SegmentCollector},
    DocId,
};

use crate::bitset_wrapper::BitsetWrapper;

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

    fn merge_fruits(&self, segment_fruits: Vec<()>) -> tantivy::Result<()> {
        Ok(())
    }
}

pub struct VecChildCollector {
    bitset_wrapper: BitsetWrapper,
}

impl SegmentCollector for VecChildCollector {
    type Fruit = ();

    fn collect(&mut self, doc: DocId, _score: tantivy::Score) {
        self.bitset_wrapper.set(doc);
    }

    fn harvest(self) -> Self::Fruit {}
}
