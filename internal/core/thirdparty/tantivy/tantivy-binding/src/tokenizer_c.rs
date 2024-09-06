use std::collections::HashMap;

use libc::c_void;
use tantivy::tokenizer::TextAnalyzer;

use crate::{
    tokenizer::create_tokenizer,
    util::{create_binding, free_binding},
};

#[no_mangle]
pub extern "C" fn tantivy_create_tokenizer(tokenizer_params: *mut c_void) -> *mut c_void {
    let analyzer = unsafe {
        let m = tokenizer_params as *const HashMap<String, String>;
        create_tokenizer(&(*m))
    };
    match analyzer {
        Some(text_analyzer) => create_binding(text_analyzer),
        None => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn tantivy_free_tokenizer(tokenizer: *mut c_void) {
    free_binding::<TextAnalyzer>(tokenizer);
}
