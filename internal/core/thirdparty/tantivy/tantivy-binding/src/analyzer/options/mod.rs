mod runtime_option;
mod util;

pub use self::runtime_option::{get_lindera_download_url, get_options, set_options};

pub use self::util::get_resource_path;

pub use self::runtime_option::DEFAULT_DICT_PATH_KEY;
