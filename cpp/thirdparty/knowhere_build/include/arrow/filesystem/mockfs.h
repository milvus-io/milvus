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

#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "arrow/filesystem/filesystem.h"

namespace arrow {
namespace fs {
namespace internal {

struct DirInfo {
  std::string full_path;
  TimePoint mtime;

  bool operator==(const DirInfo& other) const {
    return mtime == other.mtime && full_path == other.full_path;
  }

  friend ARROW_EXPORT std::ostream& operator<<(std::ostream&, const DirInfo&);
};

struct FileInfo {
  std::string full_path;
  TimePoint mtime;
  std::string data;

  bool operator==(const FileInfo& other) const {
    return mtime == other.mtime && full_path == other.full_path && data == other.data;
  }

  friend ARROW_EXPORT std::ostream& operator<<(std::ostream&, const FileInfo&);
};

/// A mock FileSystem implementation that holds its contents in memory.
///
/// Useful for validating the FileSystem API, writing conformance suite,
/// and bootstrapping FileSystem-based APIs.
class ARROW_EXPORT MockFileSystem : public FileSystem {
 public:
  explicit MockFileSystem(TimePoint current_time);
  ~MockFileSystem() override;

  // XXX It's not very practical to have to explicitly declare inheritance
  // of default overrides.
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

  // Contents-dumping helpers to ease testing.
  // Output is lexicographically-ordered by full path.
  std::vector<DirInfo> AllDirs();
  std::vector<FileInfo> AllFiles();

  class Impl;

 protected:
  std::unique_ptr<Impl> impl_;
};

}  // namespace internal
}  // namespace fs
}  // namespace arrow
