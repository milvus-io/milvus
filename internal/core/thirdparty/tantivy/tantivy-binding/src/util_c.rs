use std::ffi::c_char;

use crate::util::{c_ptr_to_str, index_exist};

#[no_mangle]
pub extern "C" fn tantivy_index_exist(path: *const c_char) -> bool {
    let path_str = c_ptr_to_str(path).unwrap();
    index_exist(path_str)
}
