use crate::error::{Result, TantivyBindingError};
use std::ffi::CString;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path};

// Walk directory descriptors instead of checking a canonical path and reopening
// it: a concurrent symlink replacement must never redirect a file read.
pub(crate) fn with_file_in_root<T>(
    root: &Path,
    path: &Path,
    limit: u64,
    parse: impl FnOnce(&mut dyn BufRead) -> Result<T>,
) -> Result<T> {
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
    let mut reader = BufReader::new(file.take(limit + 1));
    let result = parse(&mut reader);
    if result.is_ok() {
        // Parsers may stop early; still enforce the limit if the file grew after stat.
        std::io::copy(&mut reader, &mut std::io::sink())?;
    }
    if reader.get_ref().limit() == 0 {
        return Err(too_large());
    }
    result
}

pub(crate) fn read_file_in_root(root: &Path, path: &Path, limit: u64) -> Result<Vec<u8>> {
    with_file_in_root(root, path, limit, |reader| {
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;
        Ok(bytes)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn streaming_reads_detect_growth_and_early_parser_return() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dict");
        for consume in [false, true] {
            std::fs::write(&path, b"word\n").unwrap();
            let err = with_file_in_root(dir.path(), Path::new("dict"), 5, |reader| {
                File::options()
                    .append(true)
                    .open(&path)?
                    .write_all(b"extra")?;
                if consume {
                    let mut bytes = Vec::new();
                    reader.read_to_end(&mut bytes)?;
                    assert_eq!(bytes.len(), 6);
                }
                Ok(())
            })
            .unwrap_err();
            assert!(matches!(err, TantivyBindingError::InvalidArgument(_)));
            assert!(err.to_string().contains("read limit"));
        }
    }

    #[test]
    fn streaming_reads_handle_exact_limit_long_lines_and_invalid_utf8() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dict");
        let text = "a".repeat(20000) + "\nlast";
        std::fs::write(&path, &text).unwrap();
        let lines = with_file_in_root(dir.path(), Path::new("dict"), text.len() as u64, |reader| {
            Ok(reader.lines().collect::<std::io::Result<Vec<_>>>()?)
        })
        .unwrap();
        assert_eq!(lines, vec!["a".repeat(20000), "last".into()]);
        std::fs::write(&path, b"SECRET_MARKER\xff").unwrap();
        let error = with_file_in_root(dir.path(), Path::new("dict"), 64, |reader| {
            Ok(reader.lines().collect::<std::io::Result<Vec<_>>>()?)
        })
        .unwrap_err()
        .to_string();
        assert!(!error.contains("SECRET_MARKER"));
    }
}
