use tantivy::{
    collector::{Collector, SegmentCollector},
    schema::IndexRecordOption,
    DocId, DocSet, Score, SegmentOrdinal, SegmentReader, Term, COLLECT_BLOCK_BUFFER_LEN,
    TERMINATED,
};

use crate::bitset_wrapper::BitsetWrapper;

// only support for text match query.
pub(crate) struct DirectBitsetCollector {
    pub(crate) bitset_wrapper: BitsetWrapper,
    pub(crate) terms: Vec<Term>,
}

pub(crate) struct DirectBitsetChildCollector {}

impl Collector for DirectBitsetCollector {
    type Fruit = ();
    type Child = DirectBitsetChildCollector;

    fn collect_segment(
        &self,
        weight: &dyn tantivy::query::Weight,
        _segment_ord: u32,
        reader: &SegmentReader,
    ) -> tantivy::Result<<Self::Child as SegmentCollector>::Fruit> {
        let mut buffer = [0u32; 4096];

        for term in self.terms.iter() {
            let inv_index = reader.inverted_index(term.field())?;
            if let Some(mut posting) = inv_index.read_postings(term, IndexRecordOption::Basic)? {
                while posting.doc() != TERMINATED {
                    let mut len = 0;
                    while posting.doc() != TERMINATED && len < 4096 {
                        buffer[len] = posting.doc();
                        len += 1;
                        posting.advance();
                    }
                    self.bitset_wrapper.batch_set(&buffer[..len]);
                }
            }
        }

        Ok(())
    }

    fn for_segment(
        &self,
        _segment_local_id: SegmentOrdinal,
        _segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(DirectBitsetChildCollector {})
    }

    fn merge_fruits(
        &self,
        _segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        Ok(())
    }

    fn requires_scoring(&self) -> bool {
        false
    }
}

impl SegmentCollector for DirectBitsetChildCollector {
    type Fruit = ();

    fn collect(&mut self, _doc: DocId, _score: Score) {
        unreachable!();
    }

    fn collect_block(&mut self, _docs: &[DocId]) {
        unreachable!();
    }

    fn harvest(self) -> Self::Fruit {}
}
