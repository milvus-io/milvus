use tantivy::{
    collector::{Collector, SegmentCollector},
    fastfield::Column,
    DocId, Score, SegmentOrdinal, SegmentReader,
};

#[derive(Default)]
pub(crate) struct DocIdCollector<T> {
    _phantom: std::marker::PhantomData<T>,
}

pub(crate) struct DocIdChildCollector<T> {
    docs: Vec<T>,
    column: Column<i64>,
}

impl Collector for DocIdCollector<u32> {
    type Fruit = Vec<u32>;
    type Child = DocIdChildCollector<u32>;

    fn for_segment(
        &self,
        _segment_local_id: SegmentOrdinal,
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

impl SegmentCollector for DocIdChildCollector<u32> {
    type Fruit = Vec<u32>;

    fn collect(&mut self, doc: DocId, _score: Score) {
        self.column.values_for_doc(doc).for_each(|doc_id| {
            self.docs.push(doc_id as u32);
        })
    }

    fn harvest(self) -> Self::Fruit {
        self.docs
    }
}

impl Collector for DocIdCollector<i64> {
    type Fruit = Vec<i64>;
    type Child = DocIdChildCollector<i64>;

    fn for_segment(
        &self,
        _segment_local_id: SegmentOrdinal,
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

impl SegmentCollector for DocIdChildCollector<i64> {
    type Fruit = Vec<i64>;

    fn collect(&mut self, doc: DocId, _score: Score) {
        self.column.values_for_doc(doc).for_each(|doc_id| {
            self.docs.push(doc_id);
        })
    }

    fn harvest(self) -> Self::Fruit {
        self.docs
    }
}