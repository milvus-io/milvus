use std::ffi::c_char;

use libc::c_void;
use tantivy::tokenizer::{BoxTokenStream, TextAnalyzer, Token};

use crate::string_c::c_str_to_str;
use crate::{
    string_c::create_string,
    util::{create_binding, free_binding},
};

#[repr(C)]
pub struct TantivyToken{
    pub token: *const c_char,
    pub start_offset: i64,
    pub end_offset:i64,
    pub position:i64,
    pub position_length:i64,
}

impl TantivyToken{
    pub fn from_token(token: &Token) -> Self{
        TantivyToken{
            token: create_string(&token.text),
            start_offset: token.offset_from as i64,
            end_offset: token.offset_to as i64,
            position: token.position as i64,
            position_length: token.position_length as i64,
        }
    }
}


// Note: the tokenizer and text must be released after the token_stream.
#[no_mangle]
pub extern "C" fn tantivy_create_token_stream(
    tokenizer: *mut c_void,
    text: *const c_char,
) -> *mut c_void {
    let analyzer = tokenizer as *mut TextAnalyzer;
    let token_stream = unsafe { (*analyzer).token_stream(c_str_to_str(text)) };
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
    let token = unsafe { (*real).token().text.as_str() };
    create_string(token)
}

#[no_mangle]
pub extern "C" fn tantivy_token_stream_get_detailed_token(token_stream: *mut c_void) -> TantivyToken {
    let real = token_stream as *mut BoxTokenStream<'_>;
    TantivyToken::from_token(unsafe { (*real).token()})
}
