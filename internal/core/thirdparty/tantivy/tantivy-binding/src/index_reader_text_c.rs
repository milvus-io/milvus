use std::{collections::HashMap, ffi::CStr};

use libc::{c_char, c_void};

use crate::{array::RustArray, index_reader::IndexReaderWrapper, tokenizer::create_tokenizer};

#[no_mangle]
pub extern "C" fn tantivy_match_query(ptr: *mut c_void, query: *const c_char) -> RustArray {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        let c_str = CStr::from_ptr(query);
        let hits = (*real).match_query(c_str.to_str().unwrap());
        RustArray::from_vec(hits)
    }
}

#[no_mangle]
pub extern "C" fn tantivy_register_tokenizer(
    ptr: *mut c_void,
    tokenizer_name: *const c_char,
    tokenizer_params: *mut c_void,
) {
    let real = ptr as *mut IndexReaderWrapper;
    let tokenizer_name_str = unsafe { CStr::from_ptr(tokenizer_name) };
    let analyzer = unsafe {
        let m = tokenizer_params as *const HashMap<String, String>;
        create_tokenizer(&(*m))
    };
    match analyzer {
        Some(text_analyzer) => unsafe {
            (*real).register_tokenizer(
                String::from(tokenizer_name_str.to_str().unwrap()),
                text_analyzer,
            );
        },
        None => {
            panic!("unsupported tokenizer");
        }
    }
}
