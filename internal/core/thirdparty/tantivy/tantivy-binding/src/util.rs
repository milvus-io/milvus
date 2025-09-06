use crate::convert_to_rust_slice;
use crate::error::Result;
use core::slice;
use std::collections::HashSet;
use std::ffi::CStr;
use std::ffi::{c_char, c_void};
use std::ops::Bound;
use tantivy::{directory::MmapDirectory, Index};

#[inline]
pub fn c_ptr_to_str(ptr: *const c_char) -> Result<&'static str> {
    Ok(unsafe { CStr::from_ptr(ptr) }.to_str()?)
}

pub fn index_exist(path: &str) -> bool {
    let Ok(dir) = MmapDirectory::open(path) else {
        return false;
    };
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
    let bitset = unsafe { &mut *(bitset as *mut HashSet<u32>) };
    let docs = unsafe { convert_to_rust_slice!(doc_id, len) };
    for doc in docs {
        bitset.insert(*doc);
    }
}
