use log::warn;
use tantivy::{
    collector::{Collector, SegmentCollector},
    DocId,
};

pub struct VecCollector;

impl Collector for VecCollector {
    type Fruit = Vec<DocId>;

    type Child = VecChildCollector;

    fn for_segment(
        &self,
        _segment_local_id: tantivy::SegmentOrdinal,
        _segment: &tantivy::SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(VecChildCollector { docs: Vec::new() })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segment_fruits: Vec<Vec<DocId>>) -> tantivy::Result<Vec<DocId>> {
        if segment_fruits.len() == 1 {
            Ok(segment_fruits.into_iter().next().unwrap())
        } else {
            warn!(
                "inverted index should have only one segment, but got {} segments",
                segment_fruits.len()
            );
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
}

pub struct VecChildCollector {
    docs: Vec<DocId>,
}

impl SegmentCollector for VecChildCollector {
    type Fruit = Vec<DocId>;

    fn collect(&mut self, doc: DocId, _score: tantivy::Score) {
        self.docs.push(doc);
    }

    fn harvest(self) -> Self::Fruit {
        self.docs
    }
}
