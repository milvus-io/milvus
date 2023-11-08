use core::slice;
use std::ffi::{c_void, c_char, CStr};


use crate::{data_type::TantivyDataType, index::IndexWrapper};

#[no_mangle]
pub extern "C" fn tantivy_create_index(field_name: *const c_char, data_type: TantivyDataType, path: *const c_char) -> *mut c_void {
	let field_name_str = unsafe { CStr::from_ptr(field_name) };
	let path_str = unsafe { CStr::from_ptr(path) };
	let p = IndexWrapper::new(String::from(field_name_str.to_str().unwrap()), data_type, String::from(path_str.to_str().unwrap()));
	let bp = Box::new(p);
	let p_heap  : *mut IndexWrapper = Box::into_raw(bp);
	p_heap as *mut c_void
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_int64s(ptr: *mut c_void, array: *const i64, len: usize) {
	let real = ptr as *mut IndexWrapper;
	let arr = unsafe {
		slice::from_raw_parts(array, len)
	};
	unsafe {
		for data in arr {
			(*real).add_i64(*data);
		}
	}
}

#[no_mangle]
pub extern "C" fn tantivy_finish_index(ptr: *mut c_void) {
	let real = ptr as *mut IndexWrapper;
	unsafe {
		(*real).finish();
	}
}

#[no_mangle]
pub extern "C" fn tantivy_free_index(ptr: *mut c_void) {
	let real = ptr as *mut IndexWrapper;
	unsafe {
		drop(Box::from_raw(real));
	}
}

