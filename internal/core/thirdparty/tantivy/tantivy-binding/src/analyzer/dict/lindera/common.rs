pub const CHAR_DEFINITION_DATA: &'static str = "char_def.bin";
pub const CONNECTION_DATA: &'static str = "matrix.mtx";
pub const DA_DATA: &'static str = "dict.da";
pub const VALS_DATA: &'static str = "dict.vals";
pub const UNKNOWN_DATA: &'static str = "unk.bin";
pub const WORDS_IDX_DATA: &'static str = "dict.wordsidx";
pub const WORDS_DATA: &'static str = "dict.words";

pub const BUILD_DIR_KEY: &'static str = "build_dir";

#[cfg(not(feature = "analyzer-remote-tls"))]
pub fn download_disabled_error(dict: &str) -> crate::error::TantivyBindingError {
    crate::error::TantivyBindingError::InvalidArgument(format!(
        "Lindera {dict} dictionary not found under build_dir and online download is disabled in this build (analyzer-remote-tls off); prebuild the dictionary there or compile with the matching lindera-* feature"
    ))
}
