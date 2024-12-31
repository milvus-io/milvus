use libc::{c_char, c_void};
use tantivy::tokenizer::TextAnalyzer;

use crate::{
    array::RustResult,
    log::init_log,
    string_c::c_str_to_str,
    tokenizer::create_tokenizer,
    util::{create_binding, free_binding},
};

#[no_mangle]
pub extern "C" fn tantivy_create_tokenizer(analyzer_params: *const c_char) -> RustResult {
    init_log();
    let params = unsafe { c_str_to_str(analyzer_params).to_string() };
    let analyzer = create_tokenizer(&params);
    match analyzer {
        Ok(text_analyzer) => RustResult::from_ptr(create_binding(text_analyzer)),
        Err(err) => RustResult::from_error(format!(
            "create tokenizer failed with error: {} param: {}",
            err.to_string(),
            params,
        )),
    }
}

#[no_mangle]
pub extern "C" fn tantivy_clone_tokenizer(ptr: *mut c_void) -> *mut c_void {
    let analyzer = ptr as *mut TextAnalyzer;
    let clone = unsafe { (*analyzer).clone() };
    create_binding(clone)
}

#[no_mangle]
pub extern "C" fn tantivy_free_tokenizer(tokenizer: *mut c_void) {
    free_binding::<TextAnalyzer>(tokenizer);
}
