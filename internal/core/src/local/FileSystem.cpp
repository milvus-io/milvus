// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "local/FileSystem.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>
#include <utility>

#include "common/EasyAssert.h"
#include "local/ManagedSubtree.h"

#ifndef MAP_POPULATE
#define MAP_POPULATE 0
#endif

namespace milvus::local {
namespace {

namespace fs = std::filesystem;

[[noreturn]] void
ThrowFileSystemError(ErrorCode code,
                     const std::string& operation,
                     const fs::path& path,
                     const std::error_code& error) {
    ThrowInfo(code,
              "{} local path {} failed, error: {}",
              operation,
              path.string(),
              error.message());
}

bool
IsWithin(const fs::path& root, const fs::path& candidate) {
    auto root_it = root.begin();
    auto candidate_it = candidate.begin();
    for (; root_it != root.end(); ++root_it, ++candidate_it) {
        if (candidate_it == candidate.end() || *root_it != *candidate_it) {
            return false;
        }
    }
    return true;
}

fs::path
Canonicalize(const fs::path& path, ErrorCode code) {
    std::error_code error;
    auto result = fs::weakly_canonical(path, error);
    if (error) {
        ThrowFileSystemError(code, "canonicalize", path, error);
    }
    return result;
}

class UniqueFd final {
 public:
    explicit UniqueFd(int fd) noexcept : fd_(fd) {
    }

    UniqueFd(const UniqueFd&) = delete;
    UniqueFd&
    operator=(const UniqueFd&) = delete;

    UniqueFd(UniqueFd&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {
    }

    ~UniqueFd() {
        if (fd_ != -1) {
            close(fd_);
        }
    }

    int
    Get() const noexcept {
        return fd_;
    }

    int
    Release() noexcept {
        return std::exchange(fd_, -1);
    }

 private:
    int fd_;
};

UniqueFd
OpenFile(const fs::path& path, int flags, ErrorCode code) {
    auto fd = open(path.c_str(), flags, S_IRUSR | S_IWUSR);
    if (fd == -1) {
        ThrowInfo(code,
                  "failed to open local file {}, error: {}",
                  path.string(),
                  strerror(errno));
    }
    return UniqueFd(fd);
}

}  // namespace

struct FileSystem::RootState {
    explicit RootState(fs::path root) : root(std::move(root)) {
    }

    const fs::path root;
};

Path::Path(std::string value) {
    if (value.empty() || value.find('\0') != std::string::npos) {
        ThrowInfo(ErrorCode::InvalidParameter,
                  "local path must be non-empty and contain no NUL bytes");
    }

    fs::path parsed(value);
    if (parsed.is_absolute() || parsed.has_root_name() ||
        parsed.has_root_directory()) {
        ThrowInfo(ErrorCode::InvalidParameter,
                  "local path must be relative, got: {}",
                  value);
    }
    for (const auto& component : parsed) {
        if (component == "..") {
            ThrowInfo(ErrorCode::InvalidParameter,
                      "local path must not contain parent traversal, got: {}",
                      value);
        }
    }

    auto normalized = parsed.lexically_normal();
    if (normalized.empty() || normalized == ".") {
        ThrowInfo(ErrorCode::InvalidParameter,
                  "local path resolves to an empty path: {}",
                  value);
    }
    value_ = normalized.generic_string();
    while (!value_.empty() && value_.back() == '/') {
        value_.pop_back();
    }
}

FileSystem::FileSystem(std::shared_ptr<const RootState> root, fs::path prefix)
    : root_(std::move(root)), prefix_(std::move(prefix)) {
}

FileSystem
FileSystem::Open(fs::path absolute_root) {
    if (absolute_root.empty() || !absolute_root.is_absolute()) {
        ThrowInfo(ErrorCode::InvalidParameter,
                  "local filesystem root must be an absolute path, got: {}",
                  absolute_root.string());
    }

    absolute_root = absolute_root.lexically_normal();
    std::error_code error;
    fs::create_directories(absolute_root, error);
    if (error) {
        ThrowFileSystemError(ErrorCode::FileCreateFailed,
                             "create root directory",
                             absolute_root,
                             error);
    }
    if (!fs::is_directory(absolute_root, error) || error) {
        if (error) {
            ThrowFileSystemError(ErrorCode::FileReadFailed,
                                 "inspect root directory",
                                 absolute_root,
                                 error);
        }
        ThrowInfo(ErrorCode::InvalidParameter,
                  "local filesystem root is not a directory: {}",
                  absolute_root.string());
    }

    auto canonical_root =
        Canonicalize(absolute_root, ErrorCode::FileOpenFailed);
    return FileSystem(std::make_shared<RootState>(std::move(canonical_root)),
                      {});
}

FileSystem
FileSystem::Subtree(const Path& path) const {
    auto prefix = (prefix_ / std::string(path.String())).lexically_normal();
    return FileSystem(root_, std::move(prefix));
}

std::shared_ptr<ManagedSubtree>
FileSystem::ManageSubtree(const Path& path) const {
    return std::shared_ptr<ManagedSubtree>(new ManagedSubtree(*this, path));
}

fs::path
FileSystem::ScopedRoot() const {
    return (root_->root / prefix_).lexically_normal();
}

fs::path
FileSystem::CheckedNativePath(const Path& path) const {
    auto scoped_root = ScopedRoot();
    auto canonical_scope = Canonicalize(scoped_root, ErrorCode::FileOpenFailed);
    if (!IsWithin(root_->root, canonical_scope)) {
        ThrowInfo(ErrorCode::FileOpenFailed,
                  "local filesystem subtree {} escapes root {}",
                  scoped_root.string(),
                  root_->root.string());
    }

    auto candidate =
        (scoped_root / std::string(path.String())).lexically_normal();
    auto canonical_candidate =
        Canonicalize(candidate, ErrorCode::FileOpenFailed);
    if (!IsWithin(canonical_scope, canonical_candidate)) {
        ThrowInfo(ErrorCode::FileOpenFailed,
                  "local path {} escapes filesystem subtree {}",
                  candidate.string(),
                  scoped_root.string());
    }
    return candidate;
}

fs::path
FileSystem::ResolveNativePath(const Path& path) const {
    return CheckedNativePath(path);
}

fs::path
FileSystem::NativeRoot() const {
    return ScopedRoot();
}

Path
FileSystem::PathFromNativePath(fs::path native_path) const {
    if (native_path.empty() || !native_path.is_absolute()) {
        ThrowInfo(ErrorCode::InvalidParameter,
                  "native local path must be absolute, got: {}",
                  native_path.string());
    }

    native_path = native_path.lexically_normal();
    auto scoped_root = ScopedRoot().lexically_normal();
    auto relative = native_path.lexically_relative(scoped_root);
    if (relative.empty() || relative == "." || relative.is_absolute()) {
        ThrowInfo(ErrorCode::InvalidParameter,
                  "native local path must name an entry below root {}, got: {}",
                  scoped_root.string(),
                  native_path.string());
    }
    for (const auto& component : relative) {
        if (component == "..") {
            ThrowInfo(ErrorCode::InvalidParameter,
                      "native local path escapes root {}, got: {}",
                      scoped_root.string(),
                      native_path.string());
        }
    }
    return Path(relative.generic_string());
}

bool
FileSystem::Exists(const Path& path) const {
    auto native_path = CheckedNativePath(path);
    std::error_code error;
    auto exists = fs::exists(native_path, error);
    if (error) {
        ThrowFileSystemError(
            ErrorCode::FileReadFailed, "check", native_path, error);
    }
    return exists;
}

uint64_t
FileSystem::FileSize(const Path& path) const {
    auto native_path = CheckedNativePath(path);
    std::error_code error;
    auto size = fs::file_size(native_path, error);
    if (error) {
        if (error == std::errc::no_such_file_or_directory) {
            ThrowInfo(ErrorCode::PathNotExist,
                      "local path does not exist: {}",
                      native_path.string());
        }
        ThrowFileSystemError(
            ErrorCode::FileReadFailed, "get size of", native_path, error);
    }
    return size;
}

uint64_t
FileSystem::UsedSize() const {
    uint64_t total = 0;
    auto root = ScopedRoot();
    std::error_code error;
    fs::recursive_directory_iterator iterator(root, error);
    if (error) {
        ThrowFileSystemError(
            ErrorCode::FileReadFailed, "list directory", root, error);
    }
    for (const auto& entry : iterator) {
        if (!entry.is_regular_file(error)) {
            if (error) {
                ThrowFileSystemError(ErrorCode::FileReadFailed,
                                     "inspect directory entry",
                                     entry.path(),
                                     error);
            }
            continue;
        }
        auto size = entry.file_size(error);
        if (error) {
            ThrowFileSystemError(
                ErrorCode::FileReadFailed, "get size of", entry.path(), error);
        }
        total += size;
    }
    return total;
}

std::vector<Path>
FileSystem::List(const Path& directory, bool recursive) const {
    auto native_directory = CheckedNativePath(directory);
    std::error_code error;
    if (!fs::is_directory(native_directory, error)) {
        if (error) {
            ThrowFileSystemError(ErrorCode::FileReadFailed,
                                 "inspect directory",
                                 native_directory,
                                 error);
        }
        ThrowInfo(ErrorCode::PathNotExist,
                  "local directory does not exist: {}",
                  native_directory.string());
    }

    std::vector<Path> files;
    auto add_file = [&](const fs::directory_entry& entry) {
        std::error_code status_error;
        if (!entry.is_regular_file(status_error)) {
            if (status_error) {
                ThrowFileSystemError(ErrorCode::FileReadFailed,
                                     "inspect directory entry",
                                     entry.path(),
                                     status_error);
            }
            return;
        }
        auto relative = fs::relative(entry.path(), ScopedRoot(), status_error);
        if (status_error) {
            ThrowFileSystemError(ErrorCode::FileReadFailed,
                                 "resolve relative path for",
                                 entry.path(),
                                 status_error);
        }
        files.emplace_back(relative.generic_string());
    };

    if (recursive) {
        fs::recursive_directory_iterator iterator(native_directory, error);
        if (error) {
            ThrowFileSystemError(ErrorCode::FileReadFailed,
                                 "list directory",
                                 native_directory,
                                 error);
        }
        for (const auto& entry : iterator) {
            add_file(entry);
        }
    } else {
        fs::directory_iterator iterator(native_directory, error);
        if (error) {
            ThrowFileSystemError(ErrorCode::FileReadFailed,
                                 "list directory",
                                 native_directory,
                                 error);
        }
        for (const auto& entry : iterator) {
            add_file(entry);
        }
    }

    std::sort(
        files.begin(), files.end(), [](const auto& left, const auto& right) {
            return left.String() < right.String();
        });
    return files;
}

void
FileSystem::CreateDirectories(const Path& path) const {
    auto native_path = CheckedNativePath(path);
    std::error_code error;
    fs::create_directories(native_path, error);
    if (error) {
        ThrowFileSystemError(ErrorCode::FileCreateFailed,
                             "create directories",
                             native_path,
                             error);
    }
}

void
FileSystem::RemoveFile(const Path& path) const {
    auto native_path = CheckedNativePath(path);
    std::error_code error;
    fs::remove(native_path, error);
    if (error) {
        ThrowFileSystemError(
            ErrorCode::FileWriteFailed, "remove file", native_path, error);
    }
}

void
FileSystem::RemoveAll(const Path& path) const {
    auto native_path = CheckedNativePath(path);
    std::error_code error;
    fs::remove_all(native_path, error);
    if (error) {
        ThrowFileSystemError(
            ErrorCode::FileWriteFailed, "remove subtree", native_path, error);
    }
}

void
FileSystem::Clear() const {
    auto root = ScopedRoot();
    std::error_code error;
    for (fs::directory_iterator it(root, error), end; it != end && !error;
         it.increment(error)) {
        fs::remove_all(it->path(), error);
        if (error) {
            ThrowFileSystemError(
                ErrorCode::FileWriteFailed, "clear", it->path(), error);
        }
    }
    if (error) {
        ThrowFileSystemError(ErrorCode::FileReadFailed, "list", root, error);
    }
}

void
FileSystem::Rename(const Path& from, const Path& to) const {
    auto native_from = CheckedNativePath(from);
    auto native_to = CheckedNativePath(to);
    std::error_code error;
    fs::rename(native_from, native_to, error);
    if (error) {
        ThrowInfo(ErrorCode::FileWriteFailed,
                  "rename local path {} to {} failed, error: {}",
                  native_from.string(),
                  native_to.string(),
                  error.message());
    }
}

io::RandomAccessFile
FileSystem::OpenForRead(const Path& path) const {
    auto native_path = CheckedNativePath(path);
    auto fd = OpenFile(native_path, O_RDONLY, ErrorCode::FileOpenFailed);
    return io::RandomAccessFile(fd.Release(), std::move(native_path));
}

io::WritableFile
FileSystem::OpenForWrite(const Path& path, const WriteOptions& options) const {
    auto native_path = CheckedNativePath(path);
    if (options.create_parent) {
        std::error_code error;
        fs::create_directories(native_path.parent_path(), error);
        if (error) {
            ThrowFileSystemError(ErrorCode::FileCreateFailed,
                                 "create parent directory",
                                 native_path.parent_path(),
                                 error);
        }
    }

    auto flags = O_RDWR;
    if (options.create) {
        flags |= O_CREAT;
    }
    if (options.truncate) {
        flags |= O_TRUNC;
    }
#if !defined(__APPLE__) && defined(O_DIRECT)
    if (options.direct_io) {
        flags |= O_DIRECT;
    }
#endif

    auto fd = OpenFile(native_path,
                       flags,
                       options.create ? ErrorCode::FileCreateFailed
                                      : ErrorCode::FileOpenFailed);
#ifdef __APPLE__
    if (options.direct_io && fcntl(fd.Get(), F_NOCACHE, 1) == -1) {
        auto direct_io_error = errno;
        ThrowInfo(ErrorCode::FileOpenFailed,
                  "failed to enable direct IO for local file {}, error: {}",
                  native_path.string(),
                  strerror(direct_io_error));
    }
#endif
    return io::WritableFile(
        fd.Release(), std::move(native_path), options.direct_io);
}

io::MappedRegion
FileSystem::OpenMappedRegion(const Path& path,
                             const MapOptions& options) const {
    auto native_path = CheckedNativePath(path);
    auto fd = OpenFile(native_path, O_RDONLY, ErrorCode::FileOpenFailed);

    struct stat status {};
    if (fstat(fd.Get(), &status) != 0) {
        auto stat_error = errno;
        ThrowInfo(ErrorCode::FileReadFailed,
                  "failed to inspect local file {} for mmap, error: {}",
                  native_path.string(),
                  strerror(stat_error));
    }
    auto file_size = static_cast<uint64_t>(status.st_size);
    if (options.offset > file_size) {
        ThrowInfo(ErrorCode::InvalidParameter,
                  "mmap offset {} exceeds local file {} size {}",
                  options.offset,
                  native_path.string(),
                  file_size);
    }

    auto visible_size = options.length == 0
                            ? file_size - options.offset
                            : static_cast<uint64_t>(options.length);
    if (visible_size > file_size - options.offset) {
        ThrowInfo(ErrorCode::InvalidParameter,
                  "mmap range [{}, {}) exceeds local file {} size {}",
                  options.offset,
                  options.offset + visible_size,
                  native_path.string(),
                  file_size);
    }
    if (visible_size == 0) {
        return {};
    }

    auto page_size = sysconf(_SC_PAGESIZE);
    if (page_size <= 0) {
        auto page_error = errno;
        ThrowInfo(ErrorCode::MmapError,
                  "failed to determine mmap page size, error: {}",
                  strerror(page_error));
    }
    auto page_size_u64 = static_cast<uint64_t>(page_size);
    auto aligned_offset = options.offset - (options.offset % page_size_u64);
    auto displacement = options.offset - aligned_offset;
    if (visible_size > std::numeric_limits<size_t>::max() - displacement) {
        ThrowInfo(ErrorCode::InvalidParameter,
                  "mmap range for local file {} exceeds platform limit",
                  native_path.string());
    }
    auto mapping_size = static_cast<size_t>(visible_size + displacement);

    auto flags = MAP_SHARED;
    if (options.populate) {
        flags |= MAP_POPULATE;
    }
    auto mapping = mmap(nullptr,
                        mapping_size,
                        PROT_READ,
                        flags,
                        fd.Get(),
                        static_cast<off_t>(aligned_offset));
    auto mmap_error = errno;
    if (mapping == MAP_FAILED) {
        ThrowInfo(ErrorCode::MmapError,
                  "mmap local file {} failed, error: {}",
                  native_path.string(),
                  strerror(mmap_error));
    }

    auto* data = static_cast<const std::byte*>(mapping) + displacement;
    return io::MappedRegion(
        mapping, mapping_size, data, static_cast<size_t>(visible_size));
}

}  // namespace milvus::local
