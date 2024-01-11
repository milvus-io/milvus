use std::collections::HashSet;

use tantivy::{
    collector::{Collector, SegmentCollector},
    DocId,
};

pub struct HashSetCollector;

impl Collector for HashSetCollector {
    type Fruit = HashSet<DocId>;

    type Child = HashSetChildCollector;

    fn for_segment(
        &self,
        _segment_local_id: tantivy::SegmentOrdinal,
        _segment: &tantivy::SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(HashSetChildCollector {
            docs: HashSet::new(),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segment_fruits: Vec<HashSet<DocId>>) -> tantivy::Result<HashSet<DocId>> {
        if segment_fruits.len() == 1 {
            Ok(segment_fruits.into_iter().next().unwrap())
        } else {
            let len: usize = segment_fruits.iter().map(|docset| docset.len()).sum();
            let mut result = HashSet::with_capacity(len);
            for docs in segment_fruits {
                for doc in docs {
                    result.insert(doc);
                }
            }
            Ok(result)
        }
    }
}

pub struct HashSetChildCollector {
    docs: HashSet<DocId>,
}

impl SegmentCollector for HashSetChildCollector {
    type Fruit = HashSet<DocId>;

    fn collect(&mut self, doc: DocId, _score: tantivy::Score) {
        self.docs.insert(doc);
    }

    fn harvest(self) -> Self::Fruit {
        self.docs
    }
}
