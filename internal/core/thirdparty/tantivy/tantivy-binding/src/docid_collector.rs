use tantivy::{
    collector::{Collector, SegmentCollector},
    fastfield::Column,
    DocId, Score, SegmentOrdinal, SegmentReader,
};

pub(crate) struct DocIdCollector;

impl Collector for DocIdCollector {
    type Fruit = Vec<DocId>;
    type Child = DocIdChildCollector;

    fn for_segment(
        &self,
        segment_local_id: SegmentOrdinal,
        segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(DocIdChildCollector {
            docs: Vec::new(),
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
        Ok(segment_fruits.into_iter().flatten().collect::<Vec<_>>())
    }
}

pub(crate) struct DocIdChildCollector {
    docs: Vec<u32>,
    column: Column<i64>,
}

impl SegmentCollector for DocIdChildCollector {
    type Fruit = Vec<u32>;

    fn collect(&mut self, doc: DocId, score: Score) {
        self.column
            .values_for_doc(doc)
            .for_each(|doc_id| self.docs.push(doc_id as u32));
    }

    fn harvest(self) -> Self::Fruit {
        self.docs
    }
}
