use std::{ffi::c_void, ptr::NonNull};

use crate::index_reader::SetBitsetFn;

#[derive(Clone)]
pub struct BitsetWrapper {
    bitset: NonNull<c_void>,
    set_bitset: SetBitsetFn,
}

unsafe impl Send for BitsetWrapper {}

unsafe impl Sync for BitsetWrapper {}

impl BitsetWrapper {
    pub fn new(bitset: *mut c_void, set_bitset: SetBitsetFn) -> Self {
        let bitset = NonNull::new(bitset).expect("bitset pointer must not be null");
        BitsetWrapper { bitset, set_bitset }
    }

    pub fn set(&self, doc_id: u32) {
        (self.set_bitset)(self.bitset.as_ptr(), doc_id);
    }
}
