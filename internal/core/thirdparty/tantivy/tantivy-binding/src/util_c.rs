use std::ffi::{c_char, CStr};

use crate::util::index_exist;

#[no_mangle]
pub extern "C" fn tantivy_index_exist(path: *const c_char) -> bool {
    let path_str = unsafe { CStr::from_ptr(path) };
    index_exist(path_str.to_str().unwrap())
}
