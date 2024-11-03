use std::{ffi::CStr};

use libc::{c_char, c_void};

use crate::{
    array::RustArray,
    string_c::c_str_to_str,
    index_reader::IndexReaderWrapper,
    tokenizer::create_tokenizer,
    log::init_log,
};

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
    tokenizer_params: *const c_char,
) {
    init_log();
    let real = ptr as *mut IndexReaderWrapper;
    let tokenizer_name_str = unsafe { CStr::from_ptr(tokenizer_name) };
    let params = unsafe{c_str_to_str(tokenizer_params).to_string()};
    let analyzer = create_tokenizer(&params);
    match analyzer {
        Ok(text_analyzer) => unsafe {
            (*real).register_tokenizer(
                String::from(tokenizer_name_str.to_str().unwrap()),
                text_analyzer,
            );
        },
        Err(err) => {
            panic!("create tokenizer failed with error: {} param: {}", err.to_string(), params);
        },
    }
}
