use std::ffi::{c_char, CStr};

use libc::c_void;
use tantivy::tokenizer::{BoxTokenStream, TextAnalyzer};

use crate::{
    string_c::create_string,
    util::{create_binding, free_binding},
};

// Note: the tokenizer and text must be released after the token_stream.
#[no_mangle]
pub extern "C" fn tantivy_create_token_stream(
    tokenizer: *mut c_void,
    text: *const c_char,
) -> *mut c_void {
    let text_str = unsafe { CStr::from_ptr(text) };
    let analyzer = tokenizer as *mut TextAnalyzer;
    let token_stream = unsafe { (*analyzer).token_stream(text_str.to_str().unwrap()) };
    create_binding(token_stream)
}

#[no_mangle]
pub extern "C" fn tantivy_free_token_stream(token_stream: *mut c_void) {
    free_binding::<BoxTokenStream<'_>>(token_stream);
}

#[no_mangle]
pub extern "C" fn tantivy_token_stream_advance(token_stream: *mut c_void) -> bool {
    let real = token_stream as *mut BoxTokenStream<'_>;
    unsafe { (*real).advance() }
}

// Note: the returned token should be released by calling `free_string` after use.
#[no_mangle]
pub extern "C" fn tantivy_token_stream_get_token(token_stream: *mut c_void) -> *const c_char {
    let real = token_stream as *mut BoxTokenStream<'_>;
    let token = unsafe { (*real).token().text.to_string() };
    create_string(token)
}
