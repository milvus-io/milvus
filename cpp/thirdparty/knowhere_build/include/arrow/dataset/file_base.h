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

#include <memory>
#include <string>
#include <utility>

#include "arrow/dataset/scanner.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/dataset/writer.h"
#include "arrow/util/compression.h"

namespace arrow {
namespace dataset {

/// \brief Contains the location of a file to be read
class ARROW_DS_EXPORT FileSource {
 public:
  enum SourceType { PATH, BUFFER };

  FileSource(std::string path, fs::FileSystem* filesystem,
             Compression::type compression = Compression::UNCOMPRESSED)
      : FileSource(FileSource::PATH, compression) {
    path_ = std::move(path);
    filesystem_ = filesystem;
  }

  FileSource(std::shared_ptr<Buffer> buffer,
             Compression::type compression = Compression::UNCOMPRESSED)
      : FileSource(FileSource::BUFFER, compression) {
    buffer_ = std::move(buffer);
  }

  bool operator==(const FileSource& other) const {
    if (type_ != other.type_) {
      return false;
    } else if (type_ == FileSource::PATH) {
      return path_ == other.path_ && filesystem_ == other.filesystem_;
    } else {
      return buffer_->Equals(*other.buffer_);
    }
  }

  /// \brief The kind of file, whether stored in a filesystem, memory
  /// resident, or other
  SourceType type() const { return type_; }

  /// \brief Return the type of raw compression on the file, if any
  Compression::type compression() const { return compression_; }

  /// \brief Return the file path, if any. Only valid when file source
  /// type is PATH
  std::string path() const { return path_; }

  /// \brief Return the filesystem, if any. Only valid when file
  /// source type is PATH
  fs::FileSystem* filesystem() const { return filesystem_; }

  /// \brief Return the buffer containing the file, if any. Only value
  /// when file source type is BUFFER
  std::shared_ptr<Buffer> buffer() const { return buffer_; }

 private:
  explicit FileSource(SourceType type,
                      Compression::type compression = Compression::UNCOMPRESSED)
      : type_(type), compression_(compression) {}
  SourceType type_;
  Compression::type compression_;

  // PATH-based source
  std::string path_;
  fs::FileSystem* filesystem_;

  // BUFFER-based source
  std::shared_ptr<Buffer> buffer_;
};

/// \brief Base class for file scanning options
class ARROW_DS_EXPORT FileScanOptions : public ScanOptions {
 public:
  /// \brief The name of the file format this options corresponds to
  virtual std::string file_type() const = 0;
};

/// \brief Base class for file writing options
class ARROW_DS_EXPORT FileWriteOptions : public WriteOptions {
 public:
  virtual ~FileWriteOptions() = default;

  /// \brief The name of the file format this options corresponds to
  virtual std::string file_type() const = 0;
};

/// \brief Base class for file format implementation
class ARROW_DS_EXPORT FileFormat {
 public:
  virtual ~FileFormat() = default;

  virtual std::string name() const = 0;

  /// \brief Return true if the given file extension
  virtual bool IsKnownExtension(const std::string& ext) const = 0;

  /// \brief Open a file for scanning
  virtual Status ScanFile(const FileSource& location,
                          std::shared_ptr<ScanOptions> scan_options,
                          std::shared_ptr<ScanContext> scan_context,
                          std::unique_ptr<ScanTaskIterator>* out) const = 0;
};

/// \brief A DataFragment that is stored in a file with a known format
class ARROW_DS_EXPORT FileBasedDataFragment : public DataFragment {
 public:
  FileBasedDataFragment(const FileSource& location, std::shared_ptr<FileFormat> format,
                        std::shared_ptr<ScanOptions>);

  const FileSource& location() const { return location_; }
  std::shared_ptr<FileFormat> format() const { return format_; }

 protected:
  FileSource location_;
  std::shared_ptr<FileFormat> format_;
  std::shared_ptr<ScanOptions> scan_options_;
};

}  // namespace dataset
}  // namespace arrow
