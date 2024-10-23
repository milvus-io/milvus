use libc::{c_void,c_char};
use tantivy::tokenizer::TextAnalyzer;

use crate::{
    string_c::c_str_to_str,
    tokenizer::create_tokenizer,
    util::{create_binding, free_binding},
};

#[no_mangle]
pub extern "C" fn tantivy_create_tokenizer(tokenizer_params: *const c_char) -> *mut c_void {
    let analyzer = unsafe {
        let params = c_str_to_str(tokenizer_params).to_string();
        create_tokenizer(&params)
    };
    match analyzer {
        Some(text_analyzer) => create_binding(text_analyzer),
        None => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn tantivy_clone_tokenizer(ptr: *mut c_void) -> *mut c_void {
    let analyzer=ptr as *mut TextAnalyzer;
    let clone = unsafe {(*analyzer).clone()};
    create_binding(clone)
}

#[no_mangle]
pub extern "C" fn tantivy_free_tokenizer(tokenizer: *mut c_void) {
    free_binding::<TextAnalyzer>(tokenizer);
}
