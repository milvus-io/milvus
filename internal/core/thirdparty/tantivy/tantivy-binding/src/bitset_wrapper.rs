use std::ffi::c_void;

use crate::index_reader::SetBitsetFn;

#[derive(Clone)]
pub struct BitsetWrapper {
    bitset: *mut c_void,
    set_bitset: SetBitsetFn,
}

unsafe impl Send for BitsetWrapper {}

unsafe impl Sync for BitsetWrapper {}

impl BitsetWrapper {
    pub fn new(bitset: *mut c_void, set_bitset: SetBitsetFn) -> Self {
        assert!(!bitset.is_null());
        BitsetWrapper { bitset, set_bitset }
    }

    pub fn set(&self, doc_id: u32) -> c_void {
        (self.set_bitset)(self.bitset, doc_id)
    }
}
