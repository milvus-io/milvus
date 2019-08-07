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

#ifndef ARROW_IO_INTERFACES_H
#define ARROW_IO_INTERFACES_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/util/macros.h"
#include "arrow/util/string_view.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;
class Status;

namespace io {

struct FileMode {
  enum type { READ, WRITE, READWRITE };
};

struct ObjectType {
  enum type { FILE, DIRECTORY };
};

struct ARROW_EXPORT FileStatistics {
  /// Size of file, -1 if finding length is unsupported
  int64_t size;
  ObjectType::type kind;
};

class ARROW_EXPORT FileSystem {
 public:
  virtual ~FileSystem() = default;

  virtual Status MakeDirectory(const std::string& path) = 0;

  virtual Status DeleteDirectory(const std::string& path) = 0;

  virtual Status GetChildren(const std::string& path,
                             std::vector<std::string>* listing) = 0;

  virtual Status Rename(const std::string& src, const std::string& dst) = 0;

  virtual Status Stat(const std::string& path, FileStatistics* stat) = 0;
};

class ARROW_EXPORT FileInterface {
 public:
  virtual ~FileInterface() = 0;
  virtual Status Close() = 0;
  virtual Status Tell(int64_t* position) const = 0;
  virtual bool closed() const = 0;

  FileMode::type mode() const { return mode_; }

 protected:
  FileInterface() : mode_(FileMode::READ) {}
  FileMode::type mode_;
  void set_mode(FileMode::type mode) { mode_ = mode; }

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(FileInterface);
};

class ARROW_EXPORT Seekable {
 public:
  virtual ~Seekable() = default;
  virtual Status Seek(int64_t position) = 0;
};

class ARROW_EXPORT Writable {
 public:
  virtual ~Writable() = default;

  virtual Status Write(const void* data, int64_t nbytes) = 0;

  /// \brief Flush buffered bytes, if any
  virtual Status Flush();

  Status Write(const std::string& data);
};

class ARROW_EXPORT Readable {
 public:
  virtual ~Readable() = default;

  virtual Status Read(int64_t nbytes, int64_t* bytes_read, void* out) = 0;

  // Does not copy if not necessary
  virtual Status Read(int64_t nbytes, std::shared_ptr<Buffer>* out) = 0;
};

class ARROW_EXPORT OutputStream : virtual public FileInterface, public Writable {
 protected:
  OutputStream() = default;
};

class ARROW_EXPORT InputStream : virtual public FileInterface, virtual public Readable {
 public:
  /// \brief Advance or skip stream indicated number of bytes
  /// \param[in] nbytes the number to move forward
  /// \return Status
  Status Advance(int64_t nbytes);

  /// \brief Return zero-copy string_view to upcoming bytes in the
  /// stream but do not modify stream position. View becomes invalid
  /// after any operation on file. If the InputStream is unbuffered,
  /// returns 0-length string_view. May trigger buffering if the
  /// requested size is larger than the number of buffered bytes
  /// \param[in] nbytes the maximum number of bytes to see
  /// \param[out] out the returned arrow::util::string_view
  /// \return Status
  virtual Status Peek(int64_t nbytes, util::string_view* out);

  /// \brief Return true if InputStream is capable of zero copy Buffer reads
  virtual bool supports_zero_copy() const;

 protected:
  InputStream() = default;
};

class ARROW_EXPORT RandomAccessFile : public InputStream, public Seekable {
 public:
  /// Necessary because we hold a std::unique_ptr
  ~RandomAccessFile() override;

  virtual Status GetSize(int64_t* size) = 0;

  /// \brief Read nbytes at position, provide default implementations using
  /// Read(...), but can be overridden. The default implementation is
  /// thread-safe. It is unspecified whether this method updates the file
  /// position or not.
  ///
  /// \param[in] position Where to read bytes from
  /// \param[in] nbytes The number of bytes to read
  /// \param[out] bytes_read The number of bytes read
  /// \param[out] out The buffer to read bytes into
  /// \return Status
  virtual Status ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read, void* out);

  /// \brief Read nbytes at position, provide default implementations using
  /// Read(...), but can be overridden. The default implementation is
  /// thread-safe. It is unspecified whether this method updates the file
  /// position or not.
  ///
  /// \param[in] position Where to read bytes from
  /// \param[in] nbytes The number of bytes to read
  /// \param[out] out The buffer to read bytes into. The number of bytes read can be
  /// retrieved by calling Buffer::size().
  virtual Status ReadAt(int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out);

 protected:
  RandomAccessFile();

 private:
  struct ARROW_NO_EXPORT RandomAccessFileImpl;
  std::unique_ptr<RandomAccessFileImpl> interface_impl_;
};

class ARROW_EXPORT WritableFile : public OutputStream, public Seekable {
 public:
  virtual Status WriteAt(int64_t position, const void* data, int64_t nbytes) = 0;

 protected:
  WritableFile() = default;
};

class ARROW_EXPORT ReadWriteFileInterface : public RandomAccessFile, public WritableFile {
 protected:
  ReadWriteFileInterface() { RandomAccessFile::set_mode(FileMode::READWRITE); }
};

// TODO(kszucs): remove this after 0.13
#ifndef _MSC_VER
using WriteableFile ARROW_DEPRECATED("Use WritableFile") = WritableFile;
using ReadableFileInterface ARROW_DEPRECATED("Use RandomAccessFile") = RandomAccessFile;
#else
// MSVC does not like using ARROW_DEPRECATED with using declarations
using WriteableFile = WritableFile;
using ReadableFileInterface = RandomAccessFile;
#endif

}  // namespace io
}  // namespace arrow

#endif  // ARROW_IO_INTERFACES_H
