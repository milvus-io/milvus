use std::collections::HashMap;
use std::ffi::c_char;
use std::ffi::c_void;
use std::ffi::CStr;

use crate::index_writer::IndexWriterWrapper;
use crate::tokenizer::create_tokenizer;
use crate::util::create_binding;

#[no_mangle]
pub extern "C" fn tantivy_create_text_writer(
    field_name: *const c_char,
    path: *const c_char,
    tokenizer_name: *const c_char,
    tokenizer_params: *mut c_void,
    num_threads: usize,
    overall_memory_budget_in_bytes: usize,
    in_ram: bool,
) -> *mut c_void {
    let field_name_str = unsafe { CStr::from_ptr(field_name).to_str().unwrap() };
    let path_str = unsafe { CStr::from_ptr(path).to_str().unwrap() };
    let tokenizer_name_str = unsafe { CStr::from_ptr(tokenizer_name).to_str().unwrap() };
    let analyzer = unsafe {
        let m = tokenizer_params as *const HashMap<String, String>;
        create_tokenizer(&(*m))
    };
    match analyzer {
        Some(text_analyzer) => {
            let wrapper = IndexWriterWrapper::create_text_writer(
                String::from(field_name_str),
                String::from(path_str),
                String::from(tokenizer_name_str),
                text_analyzer,
                num_threads,
                overall_memory_budget_in_bytes,
                in_ram,
            );
            create_binding(wrapper)
        }
        None => {
            std::ptr::null_mut()
        }
    }
}
