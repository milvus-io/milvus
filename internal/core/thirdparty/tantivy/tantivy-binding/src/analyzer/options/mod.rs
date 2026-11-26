mod common;
mod resource_info;
mod runtime_option;
mod util;

pub use self::resource_info::{FileResourcePathHelper, ResourceInfo};
pub use self::runtime_option::{
    get_global_file_resource_helper, get_lindera_download_url, get_options, set_options,
};

pub(crate) use self::util::with_resource_file;
pub use self::util::{get_resource_path, read_resource_file};

pub use self::common::{DEFAULT_DICT_PATH_KEY, RESOURCE_PATH_KEY};
