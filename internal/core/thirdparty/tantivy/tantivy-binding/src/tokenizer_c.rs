use libc::{c_char, c_void};
use tantivy::tokenizer::TextAnalyzer;

use crate::analyzer::{create_analyzer, set_options};
use crate::{
    array::RustResult,
    log::init_log,
    string_c::c_str_to_str,
    util::{create_binding, free_binding},
};

#[no_mangle]
pub extern "C" fn tantivy_create_analyzer(analyzer_params: *const c_char) -> RustResult {
    init_log();
    let params = unsafe { c_str_to_str(analyzer_params).to_string() };
    let analyzer = create_analyzer(&params);
    match analyzer {
        Ok(text_analyzer) => RustResult::from_ptr(create_binding(text_analyzer)),
        Err(err) => RustResult::from_error(format!(
            "create tokenizer failed with error: {} param: {}",
            err, params,
        )),
    }
}

#[no_mangle]
pub extern "C" fn tantivy_clone_analyzer(ptr: *mut c_void) -> *mut c_void {
    let analyzer = ptr as *mut TextAnalyzer;
    let clone = unsafe { (*analyzer).clone() };
    create_binding(clone)
}

#[no_mangle]
pub extern "C" fn tantivy_free_analyzer(tokenizer: *mut c_void) {
    free_binding::<TextAnalyzer>(tokenizer);
}

#[no_mangle]
pub extern "C" fn tantivy_set_analyzer_options(params: *const c_char) -> RustResult {
    init_log();
    let json_str = unsafe { c_str_to_str(params).to_string() };

    set_options(&json_str).map_or_else(
        |e| {
            RustResult::from_error(format!(
                "set analyzer option failed: {}, params: {}",
                e, json_str
            ))
        },
        |_| RustResult::from_success(),
    )
}
