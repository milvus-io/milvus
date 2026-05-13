use crate::convert_to_rust_slice;
use core::slice;
use std::fs;
use std::ffi::c_void;
use std::ops::Bound;
use std::path::Path;
use tantivy::directory::{Directory, MmapDirectory, RamDirectory};
use tantivy::Index;

use crate::error::{Result, TantivyBindingError};

pub fn index_exist(path: &str) -> bool {
    let Ok(dir) = MmapDirectory::open(path) else {
        return false;
    };
    Index::exists(&dir).unwrap()
}

pub fn load_fs_index_into_ram_directory(path: &str) -> Result<RamDirectory> {
    let ram_dir = RamDirectory::create();
    let dir_path = Path::new(path);

    if !dir_path.is_dir() {
        return Err(TantivyBindingError::InvalidArgument(format!(
            "index path is not a directory: {}",
            path
        )));
    }

    for entry in fs::read_dir(dir_path)
        .map_err(|e| TantivyBindingError::InternalError(e.to_string()))?
    {
        let entry = entry.map_err(|e| TantivyBindingError::InternalError(e.to_string()))?;
        let entry_path = entry.path();

        if !entry_path.is_file() {
            continue;
        }

        let file_name = entry_path
            .file_name()
            .ok_or_else(|| {
                TantivyBindingError::InternalError(format!(
                    "failed to get file name from path: {}",
                    entry_path.display()
                ))
            })?;

        let data = fs::read(&entry_path)
            .map_err(|e| TantivyBindingError::InternalError(e.to_string()))?;

        ram_dir
            .atomic_write(Path::new(file_name), &data)
            .map_err(|e| TantivyBindingError::InternalError(e.to_string()))?;
    }

    Ok(ram_dir)
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
    let docs = unsafe { convert_to_rust_slice!(doc_id, len) };
    bitset.extend_from_slice(docs);
}
