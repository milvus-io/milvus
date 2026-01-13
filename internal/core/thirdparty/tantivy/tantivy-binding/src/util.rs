use crate::error::Result;
use crate::log::init_log;
use core::slice;
use log::info;
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
        init_log();
        info!(
            "tantivy index_exist: failed to open directory: {}, (used for debug now)",
            path
        );
        return false;
    };
    let exists = Index::exists(&dir).unwrap();
    if !exists {
        init_log();
        let files: Vec<_> = std::fs::read_dir(path)
            .map(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .map(|e| e.file_name().to_string_lossy().to_string())
                    .collect()
            })
            .unwrap_or_default();
        info!(
            "tantivy index_exist: meta.json not found at {}, files: {:?}, (used for debug now)",
            path, files
        );
    }
    exists
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
    let docs = unsafe { crate::convert_to_rust_slice!(doc_id, len) };
    for doc in docs {
        bitset.insert(*doc);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_index_exist_directory_not_exist() {
        let result = index_exist("/nonexistent/path/to/index");
        assert!(!result);
    }

    #[test]
    fn test_index_exist_empty_directory() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let result = index_exist(path);
        assert!(!result);
    }

    #[test]
    fn test_index_exist_directory_without_meta_json() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        // Create some dummy files but no meta.json
        fs::write(path.join("dummy.txt"), "test").unwrap();
        let result = index_exist(path.to_str().unwrap());
        assert!(!result);
    }
}
