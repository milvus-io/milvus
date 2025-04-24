use std::ffi::c_void;
use std::ops::Bound;
use tantivy::{directory::MmapDirectory, Index};

pub fn index_exist(path: &str) -> bool {
    let dir = MmapDirectory::open(path).unwrap();
    Index::exists(&dir).unwrap()
}

pub fn make_bounds<T>(bound: T, inclusive: bool) -> Bound<T> {
    if inclusive {
        Bound::Included(bound)
    } else {
        Bound::Excluded(bound)
    }
}

pub fn create_binding<T>(wrapper: T) -> *mut c_void {
    let bp = Box::new(wrapper);
    let p_heap: *mut T = Box::into_raw(bp);
    p_heap as *mut c_void
}

pub fn free_binding<T>(ptr: *mut c_void) {
    let real = ptr as *mut T;
    unsafe {
        drop(Box::from_raw(real));
    }
}

#[cfg(test)]
pub extern "C" fn set_bitset(bitset: *mut c_void, doc_id: *const u32, len: usize) {
    let bitset = unsafe { &mut *(bitset as *mut Vec<u32>) };
    let docs = unsafe { std::slice::from_raw_parts(doc_id, len) };
    bitset.extend_from_slice(docs);
}
