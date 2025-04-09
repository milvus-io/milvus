use tantivy::{
    collector::{Collector, SegmentCollector},
    DocId, Score, SegmentOrdinal, SegmentReader,
};

#[derive(Default)]
pub(crate) struct MilvusIdCollector {}

pub(crate) struct MilvusIdChildCollector {
    milvus_doc_ids: Vec<u32>,
}

impl Collector for MilvusIdCollector {
    type Fruit = Vec<u32>;
    type Child = MilvusIdChildCollector;

    fn for_segment(
        &self,
        _segment_local_id: SegmentOrdinal,
        _segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(MilvusIdChildCollector {
            milvus_doc_ids: Vec::new(),
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

impl SegmentCollector for MilvusIdChildCollector {
    type Fruit = Vec<u32>;

    fn collect_block(&mut self, docs: &[DocId]) {
        self.milvus_doc_ids.extend(docs);
    }

    fn collect(&mut self, doc: DocId, _score: Score) {
        // Unreachable code actually.
        self.collect_block(&[doc]);
    }

    fn harvest(self) -> Self::Fruit {
        self.milvus_doc_ids
    }
}
