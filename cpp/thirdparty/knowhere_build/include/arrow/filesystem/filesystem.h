// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/status.h"
#include "arrow/util/compression.h"
#include "arrow/util/visibility.h"

// The Windows API defines macros from *File resolving to either
// *FileA or *FileW.  Need to undo them.
#ifdef _WIN32
#ifdef DeleteFile
#undef DeleteFile
#endif
#ifdef CopyFile
#undef CopyFile
#endif
#endif

namespace arrow {

namespace io {

class InputStream;
class OutputStream;
class RandomAccessFile;

}  // namespace io

namespace fs {

// A system clock time point expressed as a 64-bit (or more) number of
// nanoseconds since the epoch.
using TimePoint =
    std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>;

/// \brief EXPERIMENTAL: FileSystem entry type
enum class ARROW_EXPORT FileType {
  // Target does not exist
  NonExistent,
  // Target exists but its type is unknown (could be a special file such
  // as a Unix socket or character device, or Windows NUL / CON / ...)
  Unknown,
  // Target is a regular file
  File,
  // Target is a directory
  Directory
};

ARROW_EXPORT std::string ToString(FileType);

static const int64_t kNoSize = -1;
static const TimePoint kNoTime = TimePoint(TimePoint::duration(-1));

/// \brief EXPERIMENTAL: FileSystem entry stats
struct ARROW_EXPORT FileStats {
  FileStats() = default;
  FileStats(FileStats&&) = default;
  FileStats& operator=(FileStats&&) = default;
  FileStats(const FileStats&) = default;
  FileStats& operator=(const FileStats&) = default;

  // The file type.
  FileType type() const { return type_; }
  void set_type(FileType type) { type_ = type; }

  // The full file path in the filesystem.
  std::string path() const { return path_; }
  void set_path(const std::string& path) { path_ = path; }

  // The file base name (component after the last directory separator).
  std::string base_name() const;

  // The size in bytes, if available.  Only regular files are guaranteed
  // to have a size.
  int64_t size() const { return size_; }
  void set_size(int64_t size) { size_ = size; }

  // The time of last modification, if available.
  TimePoint mtime() const { return mtime_; }
  void set_mtime(TimePoint mtime) { mtime_ = mtime; }

 protected:
  FileType type_ = FileType::Unknown;
  std::string path_;
  int64_t size_ = kNoSize;
  TimePoint mtime_ = kNoTime;
};

/// \brief EXPERIMENTAL: file selector
struct ARROW_EXPORT Selector {
  // The directory in which to select files.
  // If the path exists but doesn't point to a directory, this should be an error.
  std::string base_dir;
  // The behavior if `base_dir` doesn't exist in the filesystem.  If false,
  // an error is returned.  If true, an empty selection is returned.
  bool allow_non_existent = false;
  // Whether to recurse into subdirectories.
  bool recursive = false;

  Selector() {}
};

/// \brief EXPERIMENTAL: abstract file system API
class ARROW_EXPORT FileSystem {
 public:
  virtual ~FileSystem();

  /// Get statistics for the given target.
  ///
  /// Any symlink is automatically dereferenced, recursively.
  /// A non-existing or unreachable file returns an Ok status and
  /// has a FileType of value NonExistent.  An error status indicates
  /// a truly exceptional condition (low-level I/O error, etc.).
  virtual Status GetTargetStats(const std::string& path, FileStats* out) = 0;
  /// Same, for many targets at once.
  virtual Status GetTargetStats(const std::vector<std::string>& paths,
                                std::vector<FileStats>* out);
  /// Same, according to a selector.
  ///
  /// The selector's base directory will not be part of the results, even if
  /// it exists.
  /// If it doesn't exist, see `Selector::allow_non_existent`.
  virtual Status GetTargetStats(const Selector& select, std::vector<FileStats>* out) = 0;

  /// Create a directory and subdirectories.
  ///
  /// This function succeeds if the directory already exists.
  virtual Status CreateDir(const std::string& path, bool recursive = true) = 0;

  /// Delete a directory and its contents, recursively.
  virtual Status DeleteDir(const std::string& path) = 0;

  /// Delete a file.
  virtual Status DeleteFile(const std::string& path) = 0;
  /// Delete many files.
  ///
  /// The default implementation issues individual delete operations in sequence.
  virtual Status DeleteFiles(const std::vector<std::string>& paths);

  /// Move / rename a file or directory.
  ///
  /// If the destination exists:
  /// - if it is a non-empty directory, an error is returned
  /// - otherwise, if it has the same type as the source, it is replaced
  /// - otherwise, behavior is unspecified (implementation-dependent).
  virtual Status Move(const std::string& src, const std::string& dest) = 0;

  /// Copy a file.
  ///
  /// If the destination exists and is a directory, an error is returned.
  /// Otherwise, it is replaced.
  virtual Status CopyFile(const std::string& src, const std::string& dest) = 0;

  /// Open an input stream for sequential reading.
  virtual Status OpenInputStream(const std::string& path,
                                 std::shared_ptr<io::InputStream>* out) = 0;

  /// Open an input file for random access reading.
  virtual Status OpenInputFile(const std::string& path,
                               std::shared_ptr<io::RandomAccessFile>* out) = 0;

  /// Open an output stream for sequential writing.
  ///
  /// If the target already exists, existing data is truncated.
  virtual Status OpenOutputStream(const std::string& path,
                                  std::shared_ptr<io::OutputStream>* out) = 0;

  /// Open an output stream for appending.
  ///
  /// If the target doesn't exist, a new empty file is created.
  virtual Status OpenAppendStream(const std::string& path,
                                  std::shared_ptr<io::OutputStream>* out) = 0;
};

/// \brief EXPERIMENTAL: a FileSystem implementation that delegates to another
/// implementation after prepending a fixed base path.
///
/// This is useful to expose a logical view of a subtree of a filesystem,
/// for example a directory in a LocalFileSystem.
/// This makes no security guarantee.  For example, symlinks may allow to
/// "escape" the subtree and access other parts of the underlying filesystem.
class ARROW_EXPORT SubTreeFileSystem : public FileSystem {
 public:
  explicit SubTreeFileSystem(const std::string& base_path,
                             std::shared_ptr<FileSystem> base_fs);
  ~SubTreeFileSystem() override;

  using FileSystem::GetTargetStats;
  Status GetTargetStats(const std::string& path, FileStats* out) override;
  Status GetTargetStats(const Selector& select, std::vector<FileStats>* out) override;

  Status CreateDir(const std::string& path, bool recursive = true) override;

  Status DeleteDir(const std::string& path) override;

  Status DeleteFile(const std::string& path) override;

  Status Move(const std::string& src, const std::string& dest) override;

  Status CopyFile(const std::string& src, const std::string& dest) override;

  Status OpenInputStream(const std::string& path,
                         std::shared_ptr<io::InputStream>* out) override;

  Status OpenInputFile(const std::string& path,
                       std::shared_ptr<io::RandomAccessFile>* out) override;

  Status OpenOutputStream(const std::string& path,
                          std::shared_ptr<io::OutputStream>* out) override;

  Status OpenAppendStream(const std::string& path,
                          std::shared_ptr<io::OutputStream>* out) override;

 protected:
  const std::string base_path_;
  std::shared_ptr<FileSystem> base_fs_;

  std::string PrependBase(const std::string& s) const;
  Status PrependBaseNonEmpty(std::string* s) const;
  Status StripBase(const std::string& s, std::string* out) const;
  Status FixStats(FileStats* st) const;
};

}  // namespace fs
}  // namespace arrow
