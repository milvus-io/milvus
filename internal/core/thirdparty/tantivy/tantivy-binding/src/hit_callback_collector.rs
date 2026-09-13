use std::{ffi::c_void, ptr::NonNull};

use log::warn;
use tantivy::{
    collector::{Collector, SegmentCollector},
    fastfield::Column,
    DocId, Score, SegmentOrdinal, SegmentReader,
};

use crate::index_reader_c::SetBitsetFn;

#[derive(Clone)]
struct HitBatchCallback {
    context: NonNull<c_void>,
    callback: SetBitsetFn,
}

// IndexReaderWrapper keeps Tantivy's default single-thread search executor,
// and the FFI entry point does not return until collection finishes. Therefore
// each callback runs synchronously while the caller-owned context is alive.
unsafe impl Send for HitBatchCallback {}
unsafe impl Sync for HitBatchCallback {}

impl HitBatchCallback {
    fn new(context: *mut c_void, callback: SetBitsetFn) -> Self {
        Self {
            context: NonNull::new(context).expect("hit callback context must not be null"),
            callback,
        }
    }

    #[inline]
    fn emit(&self, doc_ids: &[u32]) {
        if !doc_ids.is_empty() {
            (self.callback)(self.context.as_ptr(), doc_ids.as_ptr(), doc_ids.len());
        }
    }
}

pub(crate) struct HitCallbackCollector {
    callback: HitBatchCallback,
    read_doc_id: bool,
    single_segment_expected: bool,
}

impl HitCallbackCollector {
    pub(crate) fn new(
        context: *mut c_void,
        callback: SetBitsetFn,
        read_doc_id: bool,
        single_segment_expected: bool,
    ) -> Self {
        Self {
            callback: HitBatchCallback::new(context, callback),
            read_doc_id,
            single_segment_expected,
        }
    }
}

impl Collector for HitCallbackCollector {
    type Fruit = ();
    type Child = HitCallbackChildCollector;

    fn for_segment(
        &self,
        _segment_local_id: SegmentOrdinal,
        segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(HitCallbackChildCollector {
            callback: self.callback.clone(),
            doc_id_column: self
                .read_doc_id
                .then(|| segment.fast_fields().i64("doc_id").unwrap()),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        if self.single_segment_expected && segment_fruits.len() != 1 {
            warn!(
                "inverted index should have only one segment, but got {} segments",
                segment_fruits.len()
            );
        }
        Ok(())
    }
}

pub(crate) struct HitCallbackChildCollector {
    callback: HitBatchCallback,
    doc_id_column: Option<Column<i64>>,
}

impl SegmentCollector for HitCallbackChildCollector {
    type Fruit = ();

    #[inline]
    fn collect_block(&mut self, docs: &[DocId]) {
        if let Some(column) = &self.doc_id_column {
            let doc_ids: Vec<_> = column
                .values_for_docs_flatten(docs)
                .into_iter()
                .map(|doc_id| doc_id as u32)
                .collect();
            self.callback.emit(&doc_ids);
        } else {
            self.callback.emit(docs);
        }
    }

    fn collect(&mut self, doc: DocId, _score: Score) {
        self.collect_block(&[doc]);
    }

    fn harvest(self) -> Self::Fruit {}
}
