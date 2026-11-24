mod common;
mod resource_file;
mod resource_info;
mod runtime_option;
mod util;

pub use self::resource_file::{
    check_resource_file, read_resource_file, LocalPathPolicy, MAX_RESOURCE_FILE_BYTES,
};
pub use self::resource_info::{FileResourcePathHelper, ResourceInfo};
pub use self::runtime_option::{
    get_global_file_resource_helper, get_lindera_download_url, get_options, set_options,
};

pub use self::util::get_resource_path;

pub use self::common::{DEFAULT_DICT_PATH_KEY, RESOURCE_PATH_KEY};

#[cfg(test)]
pub(crate) mod test_util {
    use super::{FileResourcePathHelper, LocalPathPolicy, ResourceInfo};
    use crate::error::TantivyBindingError;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    pub(crate) fn assert_invalid_argument(
        err: &TantivyBindingError,
        context: impl std::fmt::Display,
    ) {
        assert!(
            matches!(err, TantivyBindingError::InvalidArgument(_)),
            "{}: {}",
            context,
            err
        );
    }

    /// The error a local path outside the allowed directories gets.
    pub(crate) fn assert_not_allowed(err: &TantivyBindingError, context: impl std::fmt::Display) {
        assert_invalid_argument(err, &context);
        assert!(
            err.to_string().contains("allowed_local_resource_dirs"),
            "{}: {}",
            context,
            err
        );
    }

    pub(crate) fn assert_not_found(err: &TantivyBindingError) {
        assert!(
            matches!(err, TantivyBindingError::IOError(e) if e.kind() == std::io::ErrorKind::NotFound),
            "{}",
            err
        );
    }

    /// A helper whose `local` paths may point into `dirs` only.
    pub(crate) fn helper_allowing<P: AsRef<Path>>(dirs: &[P]) -> FileResourcePathHelper {
        FileResourcePathHelper::new(Arc::new(ResourceInfo::new()))
            .with_local_policy(LocalPathPolicy::new(dirs.iter().map(AsRef::<Path>::as_ref)))
    }

    /// Absolute path of the analyzer test data directory.
    pub(crate) fn test_data_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/analyzer/data/test")
    }
}
