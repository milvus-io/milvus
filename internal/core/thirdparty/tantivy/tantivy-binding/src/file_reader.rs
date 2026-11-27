use crate::error::{Result, TantivyBindingError};
use std::ffi::CString;
use std::fs::File;
use std::io::Read;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path};

// Walk directory descriptors instead of checking a canonical path and reopening
// it: a concurrent symlink replacement must never redirect a file read.
pub(crate) fn read_file_in_root(root: &Path, path: &Path, limit: u64) -> Result<Vec<u8>> {
    let denied = || {
        TantivyBindingError::InvalidArgument(
            "file path is outside the configured directory or contains a symbolic link".into(),
        )
    };
    if !root.is_absolute() || limit == 0 || limit == u64::MAX {
        return Err(TantivyBindingError::InternalError(
            "invalid file directory or read limit configuration".into(),
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
            "file must be a regular file".into(),
        ));
    }
    let too_large =
        || TantivyBindingError::InvalidArgument("file exceeds the configured read limit".into());
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
