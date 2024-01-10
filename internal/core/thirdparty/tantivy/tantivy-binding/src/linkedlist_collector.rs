use std::collections::LinkedList;

use tantivy::{
    collector::{Collector, SegmentCollector},
    DocId,
};

pub struct LinkedListCollector;

impl Collector for LinkedListCollector {
    type Fruit = LinkedList<DocId>;

    type Child = LinkedListChildCollector;

    fn for_segment(
        &self,
        _segment_local_id: tantivy::SegmentOrdinal,
        _segment: &tantivy::SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(LinkedListChildCollector {
            docs: LinkedList::new(),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<LinkedList<DocId>>,
    ) -> tantivy::Result<LinkedList<DocId>> {
        if segment_fruits.len() == 1 {
            Ok(segment_fruits.into_iter().next().unwrap())
        } else {
            let mut result = LinkedList::new();
            for docs in segment_fruits {
                for doc in docs {
                    result.push_front(doc);
                }
            }
            Ok(result)
        }
    }
}

pub struct LinkedListChildCollector {
    docs: LinkedList<DocId>,
}

impl SegmentCollector for LinkedListChildCollector {
    type Fruit = LinkedList<DocId>;

    fn collect(&mut self, doc: DocId, _score: tantivy::Score) {
        self.docs.push_front(doc);
    }

    fn harvest(self) -> Self::Fruit {
        self.docs
    }
}
