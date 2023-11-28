use std::ffi::{c_char, CStr, c_void};

use crate::{util::index_exist, index::IndexWrapper};

#[no_mangle]
pub extern "C" fn tantivy_index_exist(path: *const c_char) -> bool {
	let path_str = unsafe { CStr::from_ptr(path) };
	index_exist(path_str.to_str().unwrap())
}

pub fn create_tantivy_binding(wrapper: IndexWrapper) -> *mut c_void {
	let bp = Box::new(wrapper);
	let p_heap  : *mut IndexWrapper = Box::into_raw(bp);
	p_heap as *mut c_void
}

#[no_mangle]
pub extern "C" fn tantivy_load_index(path: *const c_char) -> *mut c_void {
	assert!(tantivy_index_exist(path));
	let path_str = unsafe { CStr::from_ptr(path) };
	let wrapper = IndexWrapper::load(path_str.to_str().unwrap());
	create_tantivy_binding(wrapper)
}
