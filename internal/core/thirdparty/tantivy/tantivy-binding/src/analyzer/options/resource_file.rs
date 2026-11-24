// Access to files that analyzer parameters refer to (dictionaries, stop words,
// synonyms, user dictionaries, certificates).
//
// Analyzer parameters come from requests, so a path in them must not reach
// arbitrary files on the node:
// - a `local` path must resolve, after following symlinks, to a regular file
//   under one of the directories the operator allows
//   (`function.analyzer.allowed_local_resource_dirs` and
//   `function.analyzer.local_resource_path`);
// - a `remote` file name must be a plain file name inside the resource
//   directory Milvus downloads to;
// - reads are limited to regular files of at most MAX_RESOURCE_FILE_BYTES.
//
// Errors returned from here and from the parsers that use these files must not
// contain file content, because they are returned to the caller.
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::path::{Component, Path, PathBuf};

use serde_json as json;

use crate::error::{Result, TantivyBindingError};

/// Upper bound for a single analyzer resource file.
pub const MAX_RESOURCE_FILE_BYTES: u64 = 1 << 30;

const ALLOWED_DIRS_CONFIG: &str = "function.analyzer.allowed_local_resource_dirs";

/// Directories that `local` resource paths may point into.
#[derive(Clone, Debug, Default)]
pub struct LocalPathPolicy {
    roots: Vec<PathBuf>,
}

impl LocalPathPolicy {
    pub fn new<I, P>(roots: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: AsRef<Path>,
    {
        Self {
            roots: roots
                .into_iter()
                .map(|root| root.as_ref().to_path_buf())
                .filter(|root| root.is_absolute())
                .map(|root| normalize(&root))
                .collect(),
        }
    }

    /// Builds the policy from the runtime option value (a JSON array of paths).
    /// A missing or malformed value allows no directory.
    pub fn from_option(value: Option<&json::Value>) -> Self {
        let roots = value
            .and_then(|v| v.as_array())
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item.as_str())
                    .filter(|item| !item.is_empty())
                    .map(PathBuf::from)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Self::new(roots)
    }

    /// Resolves a `local` resource path and checks it against the policy.
    /// Returns the canonical path of a regular file under an allowed directory.
    pub fn resolve(&self, path_str: &str, resource_key: &str) -> Result<PathBuf> {
        if path_str.is_empty() {
            return Err(TantivyBindingError::InvalidArgument(format!(
                "local file path of {} must not be empty",
                resource_key
            )));
        }
        // A relative path keeps resolving against the working directory, and
        // the result must still be under an allowed directory.
        let requested = Path::new(path_str);
        let absolute = if requested.is_absolute() {
            requested.to_path_buf()
        } else {
            std::env::current_dir()?.join(requested)
        };
        let normalized = normalize(&absolute);

        let not_allowed = || {
            TantivyBindingError::InvalidArgument(format!(
                "local file path {:?} of {} is not under an allowed local resource directory, see {}",
                path_str, resource_key, ALLOWED_DIRS_CONFIG
            ))
        };

        // Reject paths that are lexically outside the allowed directories
        // before touching the requested path.
        let canonical_roots: Vec<PathBuf> = self
            .roots
            .iter()
            .filter_map(|root| std::fs::canonicalize(root).ok())
            .collect();
        let lexically_allowed = self
            .roots
            .iter()
            .chain(canonical_roots.iter())
            .any(|root| normalized.starts_with(root));
        if !lexically_allowed {
            return Err(not_allowed());
        }

        // A missing file keeps the NotFound I/O classification.
        let canonical = std::fs::canonicalize(&normalized)?;
        // Symlinks must not lead out of the allowed directories.
        if !canonical_roots
            .iter()
            .any(|root| canonical.starts_with(root))
        {
            return Err(not_allowed());
        }

        if !std::fs::metadata(&canonical)?.is_file() {
            return Err(TantivyBindingError::InvalidArgument(format!(
                "local file path {:?} of {} must be a regular file",
                path_str, resource_key
            )));
        }
        Ok(canonical)
    }
}

/// Checks that a remote resource file name stays inside its resource directory.
/// Downloaded resources are stored under their base name, so anything other than
/// a single file name component can never refer to a downloaded file.
pub fn check_remote_file_name(file_name: &str, resource_key: &str) -> Result<()> {
    let mut components = Path::new(file_name).components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(name)), None) if name == file_name => Ok(()),
        _ => Err(TantivyBindingError::InvalidArgument(format!(
            "file name of remote file {} must be a plain file name",
            resource_key
        ))),
    }
}

/// Checks that `path` is a regular file no larger than MAX_RESOURCE_FILE_BYTES,
/// for readers that open the file themselves.
pub fn check_resource_file(path: &Path, resource_key: &str) -> Result<()> {
    let metadata = std::fs::metadata(path)?;
    if !metadata.is_file() {
        return Err(not_regular_file(resource_key));
    }
    if metadata.len() > MAX_RESOURCE_FILE_BYTES {
        return Err(too_large(resource_key));
    }
    Ok(())
}

/// Reads a whole resource file. The file must be a regular file of at most
/// MAX_RESOURCE_FILE_BYTES; a file that grows past the limit while being read
/// is rejected as well.
pub fn read_resource_file(path: &Path, resource_key: &str) -> Result<Vec<u8>> {
    check_resource_file(path, resource_key)?;
    let file = open_regular_file(path, resource_key)?;
    let len = file.metadata()?.len();
    if len > MAX_RESOURCE_FILE_BYTES {
        return Err(too_large(resource_key));
    }
    let mut data = Vec::with_capacity(len as usize);
    file.take(MAX_RESOURCE_FILE_BYTES + 1)
        .read_to_end(&mut data)?;
    if data.len() as u64 > MAX_RESOURCE_FILE_BYTES {
        return Err(too_large(resource_key));
    }
    Ok(data)
}

fn open_regular_file(path: &Path, resource_key: &str) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        // Do not block on a FIFO or acquire a terminal if the path was replaced
        // after it was checked; the type is checked again on the open handle.
        options.custom_flags(libc::O_NONBLOCK | libc::O_NOCTTY);
    }
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(not_regular_file(resource_key));
    }
    Ok(file)
}

fn not_regular_file(resource_key: &str) -> TantivyBindingError {
    TantivyBindingError::InvalidArgument(format!("{} must be a regular file", resource_key))
}

fn too_large(resource_key: &str) -> TantivyBindingError {
    TantivyBindingError::InvalidArgument(format!(
        "{} exceeds the maximum size of {} bytes",
        resource_key, MAX_RESOURCE_FILE_BYTES
    ))
}

/// Normalizes `.` and `..` of an absolute path without touching the filesystem.
fn normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(name) => normalized.push(name),
        }
    }
    normalized
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyzer::options::test_util::{assert_invalid_argument, assert_not_found};

    fn policy(dir: &Path) -> LocalPathPolicy {
        LocalPathPolicy::new([dir])
    }

    #[test]
    fn test_resolve_allows_file_under_allowed_dir() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("dict.txt");
        std::fs::write(&file, "word\n").unwrap();

        let resolved = policy(dir.path())
            .resolve(file.to_str().unwrap(), "test dict file")
            .unwrap();
        assert_eq!(resolved, std::fs::canonicalize(&file).unwrap());

        // `..` that stays inside the allowed directory is fine.
        let sub = dir.path().join("sub");
        std::fs::create_dir(&sub).unwrap();
        let dotted = format!("{}/../dict.txt", sub.to_str().unwrap());
        assert!(policy(dir.path())
            .resolve(&dotted, "test dict file")
            .is_ok());
    }

    #[test]
    fn test_resolve_rejects_paths_outside_allowed_dirs() {
        let allowed = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let outside = other.path().join("secret.txt");
        std::fs::write(&outside, "secret\n").unwrap();

        let traversal = format!(
            "{}/../{}/secret.txt",
            allowed.path().to_str().unwrap(),
            other.path().file_name().unwrap().to_str().unwrap()
        );
        for path in [
            outside.to_str().unwrap(),
            traversal.as_str(),
            "/etc/passwd",
            "/proc/self/environ",
            "/dev/zero",
        ] {
            let err = policy(allowed.path())
                .resolve(path, "test dict file")
                .unwrap_err();
            assert_invalid_argument(&err, path);
            assert!(
                err.to_string().contains(ALLOWED_DIRS_CONFIG),
                "{}: {}",
                path,
                err
            );
        }

        // A missing file outside the allowed directories is rejected the same
        // way, without revealing whether it exists.
        let missing = other.path().join("missing.txt");
        let err = policy(allowed.path())
            .resolve(missing.to_str().unwrap(), "test dict file")
            .unwrap_err();
        assert_invalid_argument(&err, "");

        // A missing file inside an allowed directory keeps the I/O NotFound code.
        let missing = allowed.path().join("missing.txt");
        let err = policy(allowed.path())
            .resolve(missing.to_str().unwrap(), "test dict file")
            .unwrap_err();
        assert_not_found(&err);
    }

    #[test]
    fn test_resolve_relative_and_empty_paths() {
        let dir = tempfile::tempdir().unwrap();
        for path in ["", "dict.txt", "./dict.txt", "../etc/passwd"] {
            let err = policy(dir.path())
                .resolve(path, "test dict file")
                .unwrap_err();
            assert_invalid_argument(&err, path);
        }

        // Relative paths resolve against the working directory, which cargo
        // sets to the package root, and still need an allowed directory.
        let data_dir = crate::analyzer::options::test_util::test_data_dir();
        let relative = "src/analyzer/data/test/stop_words_dict.txt";
        assert_eq!(
            policy(&data_dir)
                .resolve(relative, "test dict file")
                .unwrap(),
            std::fs::canonicalize(data_dir.join("stop_words_dict.txt")).unwrap()
        );
        let err = policy(dir.path())
            .resolve(relative, "test dict file")
            .unwrap_err();
        assert_invalid_argument(&err, relative);
    }

    #[test]
    fn test_resolve_rejects_everything_without_allowed_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("dict.txt");
        std::fs::write(&file, "word\n").unwrap();
        for policy in [
            LocalPathPolicy::default(),
            LocalPathPolicy::from_option(None),
            LocalPathPolicy::from_option(Some(&json::json!("not an array"))),
            LocalPathPolicy::from_option(Some(&json::json!(["", "relative/dir"]))),
        ] {
            let err = policy
                .resolve(file.to_str().unwrap(), "test dict file")
                .unwrap_err();
            assert_invalid_argument(&err, "");
        }
        let from_option =
            LocalPathPolicy::from_option(Some(&json::json!([dir.path().to_str().unwrap()])));
        assert!(from_option
            .resolve(file.to_str().unwrap(), "test dict file")
            .is_ok());
    }

    #[test]
    fn test_resolve_rejects_prefix_that_is_not_a_parent() {
        let parent = tempfile::tempdir().unwrap();
        let allowed = parent.path().join("dicts");
        let sibling = parent.path().join("dicts2");
        std::fs::create_dir(&allowed).unwrap();
        std::fs::create_dir(&sibling).unwrap();
        let file = sibling.join("dict.txt");
        std::fs::write(&file, "word\n").unwrap();
        assert!(policy(&allowed)
            .resolve(file.to_str().unwrap(), "test dict file")
            .is_err());
    }

    #[cfg(unix)]
    #[test]
    fn test_resolve_rejects_symlink_escape() {
        let allowed = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let target = other.path().join("secret.txt");
        std::fs::write(&target, "secret\n").unwrap();

        let file_link = allowed.path().join("dict.txt");
        std::os::unix::fs::symlink(&target, &file_link).unwrap();
        let dir_link = allowed.path().join("dir");
        std::os::unix::fs::symlink(other.path(), &dir_link).unwrap();

        for path in [file_link.clone(), dir_link.join("secret.txt")] {
            let err = policy(allowed.path())
                .resolve(path.to_str().unwrap(), "test dict file")
                .unwrap_err();
            assert_invalid_argument(&err, "");
        }

        // A symlink that stays inside the allowed directory is fine.
        let real = allowed.path().join("real.txt");
        std::fs::write(&real, "word\n").unwrap();
        let inner_link = allowed.path().join("inner.txt");
        std::os::unix::fs::symlink(&real, &inner_link).unwrap();
        assert!(policy(allowed.path())
            .resolve(inner_link.to_str().unwrap(), "test dict file")
            .is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn test_resolve_allows_symlinked_allowed_dir() {
        // The allowed directory itself may be a symlink, e.g. a mounted volume.
        let real = tempfile::tempdir().unwrap();
        let parent = tempfile::tempdir().unwrap();
        let link = parent.path().join("dicts");
        std::os::unix::fs::symlink(real.path(), &link).unwrap();
        std::fs::write(real.path().join("dict.txt"), "word\n").unwrap();

        let via_link = link.join("dict.txt");
        let via_real = real.path().join("dict.txt");
        for path in [via_link, via_real] {
            assert!(policy(&link)
                .resolve(path.to_str().unwrap(), "test dict file")
                .is_ok());
        }
    }

    #[cfg(unix)]
    #[test]
    fn test_non_regular_files_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("sub");
        std::fs::create_dir(&sub).unwrap();
        let err = policy(dir.path())
            .resolve(sub.to_str().unwrap(), "test dict file")
            .unwrap_err();
        assert_invalid_argument(&err, "");

        // Even when a device is allowed by configuration it is not read.
        let dev = LocalPathPolicy::new(["/dev"]);
        let err = dev.resolve("/dev/zero", "test dict file").unwrap_err();
        assert_invalid_argument(&err, "");
        let err = read_resource_file(Path::new("/dev/zero"), "test dict file").unwrap_err();
        assert_invalid_argument(&err, "");
        let err = check_resource_file(Path::new("/dev/zero"), "test dict file").unwrap_err();
        assert_invalid_argument(&err, "");

        // A FIFO is rejected without blocking.
        let fifo = dir.path().join("fifo");
        let c_path = std::ffi::CString::new(fifo.to_str().unwrap()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);
        let err = policy(dir.path())
            .resolve(fifo.to_str().unwrap(), "test dict file")
            .unwrap_err();
        assert_invalid_argument(&err, "");
        let err = read_resource_file(&fifo, "test dict file").unwrap_err();
        assert_invalid_argument(&err, "");
    }

    #[test]
    fn test_read_resource_file_is_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("dict.txt");
        std::fs::write(&file, "word\n").unwrap();
        assert_eq!(
            read_resource_file(&file, "test dict file").unwrap(),
            b"word\n"
        );

        // A sparse file larger than the limit is rejected without reading it.
        let large = dir.path().join("large.txt");
        let f = std::fs::File::create(&large).unwrap();
        f.set_len(MAX_RESOURCE_FILE_BYTES + 1).unwrap();
        let err = read_resource_file(&large, "test dict file").unwrap_err();
        assert_invalid_argument(&err, "");
        assert!(err.to_string().contains("maximum size"));
        let err = check_resource_file(&large, "test dict file").unwrap_err();
        assert_invalid_argument(&err, "");
    }

    #[test]
    fn test_check_remote_file_name() {
        for name in ["dict.txt", "a..b", ".hidden"] {
            assert!(
                check_remote_file_name(name, "test dict file").is_ok(),
                "{}",
                name
            );
        }
        for name in [
            "",
            ".",
            "..",
            "../dict.txt",
            "sub/dict.txt",
            "/etc/passwd",
            "./dict.txt",
            "dict.txt/",
        ] {
            let err = check_remote_file_name(name, "test dict file").unwrap_err();
            assert_invalid_argument(&err, name);
        }
    }
}
