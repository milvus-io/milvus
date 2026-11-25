use serde_json as json;
use std::ffi::CString;
use std::fs::File;
use std::io::Read;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path, PathBuf};

use super::resource_info::FileResourcePathHelper;
use crate::error::{Result, TantivyBindingError};

// Walk directory descriptors instead of checking a canonical path and reopening
// it: a concurrent symlink replacement must never redirect a dictionary read.
fn read_file_in_root(root: &Path, path: &Path, limit: u64) -> Result<Vec<u8>> {
    let denied = || {
        TantivyBindingError::InvalidArgument(
            "dictionary path is outside the configured directory or contains a symbolic link"
                .into(),
        )
    };
    if !root.is_absolute() || limit == 0 || limit == u64::MAX {
        return Err(TantivyBindingError::InternalError(
            "invalid dictionary directory or read limit configuration".into(),
        ));
    }
    let relative = if path.is_absolute() {
        path.strip_prefix(root).map_err(|_| denied())?
    } else {
        path
    };
    let parts: Vec<_> = relative.components().collect();
    if parts.is_empty()
        || parts
            .iter()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(denied());
    }
    let mut file = File::open(root)?;
    for (index, part) in parts.iter().enumerate() {
        let name = CString::new(part.as_os_str().as_bytes()).map_err(|_| denied())?;
        let flags = libc::O_RDONLY
            | libc::O_CLOEXEC
            | libc::O_NOFOLLOW
            | libc::O_NONBLOCK
            | if index + 1 < parts.len() {
                libc::O_DIRECTORY
            } else {
                0
            };
        let fd = unsafe { libc::openat(file.as_raw_fd(), name.as_ptr(), flags) };
        if fd < 0 {
            let err = std::io::Error::last_os_error();
            if matches!(err.raw_os_error(), Some(libc::ELOOP) | Some(libc::ENOTDIR)) {
                return Err(denied());
            }
            return Err(err.into());
        }
        file = unsafe { File::from_raw_fd(fd) };
    }
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(TantivyBindingError::InvalidArgument(
            "dictionary must be a regular file".into(),
        ));
    }
    let too_large = || {
        TantivyBindingError::InvalidArgument("dictionary exceeds the configured read limit".into())
    };
    if metadata.len() > limit {
        return Err(too_large());
    }
    let mut bytes = Vec::new();
    file.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(too_large());
    }
    Ok(bytes)
}

pub fn read_resource_file(
    helper: &mut FileResourcePathHelper,
    params: &json::Value,
    key: &str,
) -> Result<Vec<u8>> {
    let path = get_resource_path(helper, params, key)?;
    let root_key = if params.get("type").and_then(json::Value::as_str) == Some("local") {
        super::DEFAULT_DICT_PATH_KEY
    } else {
        super::RESOURCE_PATH_KEY
    };
    let root = super::get_options(root_key)
        .and_then(|v| v.as_str().map(PathBuf::from))
        .unwrap_or_default();
    let root = if root_key == super::RESOURCE_PATH_KEY && root.is_relative() {
        std::env::current_dir()?.join(root)
    } else {
        root
    };
    #[cfg(test)]
    let root = helper.test_root.clone().unwrap_or(root);
    let limit = super::get_options("max_dict_size")
        .and_then(|v| v.as_u64())
        .unwrap_or(64 * 1024 * 1024);
    read_file_in_root(&root, &path, limit)
}

pub fn get_resource_path(
    helper: &mut FileResourcePathHelper,
    v: &json::Value,
    resource_key: &str,
) -> Result<PathBuf> {
    if !v.is_object() {
        return Err(TantivyBindingError::InvalidArgument(format!(
            "file config of {} must be object",
            resource_key
        )));
    }

    let params = v.as_object().unwrap();

    let file_type = params.get("type").ok_or_else(|| {
        TantivyBindingError::InvalidArgument(format!("file type of {} must be set", resource_key))
    })?;

    if !file_type.is_string() {
        return Err(TantivyBindingError::InvalidArgument(format!(
            "file type of {} must be string",
            resource_key
        )));
    }

    match file_type.as_str().unwrap() {
        "local" => {
            let path = params.get("path").ok_or_else(|| {
                TantivyBindingError::InvalidArgument(format!(
                    "file path of local file `{}` must be set",
                    resource_key
                ))
            })?;

            if !path.is_string() {
                return Err(TantivyBindingError::InvalidArgument(format!(
                    "file path of local file `{}` must be string",
                    resource_key
                )));
            }

            let path_str = path.as_str().unwrap();
            Ok(Path::new(path_str).to_path_buf())
        }
        "remote" => {
            let resource_name = params
                .get("resource_name")
                .ok_or_else(|| {
                    TantivyBindingError::InvalidArgument(format!(
                        "resource name of remote file `{}` must be set",
                        resource_key
                    ))
                })?
                .as_str()
                .ok_or(TantivyBindingError::InvalidArgument(format!(
                    "remote file resource name of remote file `{}` must be string",
                    resource_key
                )))?;

            let file_name = params
                .get("file_name")
                .ok_or_else(|| {
                    TantivyBindingError::InvalidArgument(format!(
                        "file name of remote file `{}` must be set",
                        resource_key
                    ))
                })?
                .as_str()
                .ok_or(TantivyBindingError::InvalidArgument(format!(
                    "remote file resource name of {} must be string",
                    resource_key
                )))?;

            if file_name.is_empty()
                || file_name.contains(['/', '\\'])
                || matches!(file_name, "." | "..")
            {
                return Err(TantivyBindingError::InvalidArgument(
                    "file resource name must be a single filename".into(),
                ));
            }
            helper.get_resource_file_path(resource_name, file_name)
        }
        other => Err(TantivyBindingError::InvalidArgument(format!(
            "unsupported file type {} of {}",
            other, resource_key
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    #[test]
    fn dictionary_reads_are_confined_and_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("allowed");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("dict"), b"test").unwrap();
        std::fs::write(dir.path().join("secret"), b"SECRET_MARKER").unwrap();
        assert_eq!(
            read_file_in_root(&root, Path::new("dict"), 4).unwrap(),
            b"test"
        );
        assert!(read_file_in_root(&root, &root.join("dict"), 3)
            .unwrap_err()
            .to_string()
            .contains("read limit"));
        for path in [
            dir.path().join("secret"),
            dir.path().join("absent"),
            root.join("../secret"),
            PathBuf::from("../secret"),
        ] {
            let err = read_file_in_root(&root, &path, 64).unwrap_err().to_string();
            assert!(err.contains("outside the configured directory"));
            assert!(!err.contains("SECRET_MARKER"));
        }
        symlink(dir.path().join("secret"), root.join("link")).unwrap();
        symlink(dir.path(), root.join("directory_link")).unwrap();
        for path in ["link", "directory_link/secret"] {
            assert!(read_file_in_root(&root, Path::new(path), 64).is_err());
        }
        let fifo = CString::new(root.join("fifo").as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
        assert!(read_file_in_root(&root, Path::new("fifo"), 64)
            .unwrap_err()
            .to_string()
            .contains("regular file"));
    }

    #[test]
    fn dictionary_errors_do_not_echo_file_contents() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dictionary.txt");
        let mut helper =
            FileResourcePathHelper::new(std::sync::Arc::new(super::super::ResourceInfo::new()));
        helper.test_root = Some(dir.path().to_path_buf());
        let file = json::json!({"type":"local", "path":path});
        for (kind, text) in [
            ("synonym", "SECRET_MARKER private value"),
            ("synonym", "SECRET_MARKER=>a=>b"),
            ("synonym", "SECRET_MARKER\\q"),
            ("synonym", "SECRET_MARKER\\"),
            ("jieba", "SECRET_MARKER invalid_frequency"),
        ] {
            std::fs::write(&path, text).unwrap();
            let params = if kind == "synonym" {
                json::json!({"tokenizer":"standard", "filter":[{"type":"synonym", "synonyms_file":file}]})
            } else {
                json::json!({"tokenizer":{"type":"jieba", "dict":[], "extra_dict_file":file}})
            };
            let error = match crate::analyzer::create_analyzer_by_json(
                params.as_object().unwrap(),
                &mut helper,
            ) {
                Ok(_) => panic!("invalid dictionary accepted"),
                Err(error) => error.to_string(),
            };
            assert!(!error.contains("SECRET_MARKER"), "{}", error);
            assert!(!error.contains(text), "{}", error);
        }
    }

    #[test]
    fn remote_filename_cannot_select_another_resource() {
        let mut helper =
            FileResourcePathHelper::new(std::sync::Arc::new(super::super::ResourceInfo::new()));
        for name in [
            "../secret",
            "/secret",
            "..",
            "",
            "nested/file",
            "nested\\file",
        ] {
            let params =
                json::json!({"type":"remote", "resource_name":"dictionary", "file_name":name});
            let err = get_resource_path(&mut helper, &params, "test")
                .unwrap_err()
                .to_string();
            assert!(err.contains("single filename"), "{}", err);
        }
    }
}
