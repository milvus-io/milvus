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

// Public API for different memory sharing / IO mechanisms

#pragma once

#include <cstdint>
#include <memory>

#include "arrow/buffer.h"
#include "arrow/io/interfaces.h"
#include "arrow/memory_pool.h"
#include "arrow/util/string_view.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;
class ResizableBuffer;
class Status;

namespace io {

// \brief An output stream that writes to a resizable buffer
class ARROW_EXPORT BufferOutputStream : public OutputStream {
 public:
  explicit BufferOutputStream(const std::shared_ptr<ResizableBuffer>& buffer);

  /// \brief Create in-memory output stream with indicated capacity using a
  /// memory pool
  /// \param[in] initial_capacity the initial allocated internal capacity of
  /// the OutputStream
  /// \param[in,out] pool a MemoryPool to use for allocations
  /// \param[out] out the created stream
  static Status Create(int64_t initial_capacity, MemoryPool* pool,
                       std::shared_ptr<BufferOutputStream>* out);

  ~BufferOutputStream() override;

  // Implement the OutputStream interface
  Status Close() override;
  bool closed() const override;
  Status Tell(int64_t* position) const override;
  Status Write(const void* data, int64_t nbytes) override;

  using OutputStream::Write;

  /// Close the stream and return the buffer
  Status Finish(std::shared_ptr<Buffer>* result);

  /// \brief Initialize state of OutputStream with newly allocated memory and
  /// set position to 0
  /// \param[in] initial_capacity the starting allocated capacity
  /// \param[in,out] pool the memory pool to use for allocations
  /// \return Status
  Status Reset(int64_t initial_capacity = 1024, MemoryPool* pool = default_memory_pool());

  int64_t capacity() const { return capacity_; }

 private:
  BufferOutputStream();

  // Ensures there is sufficient space available to write nbytes
  Status Reserve(int64_t nbytes);

  std::shared_ptr<ResizableBuffer> buffer_;
  bool is_open_;
  int64_t capacity_;
  int64_t position_;
  uint8_t* mutable_data_;
};

// \brief A helper class to tracks the size of allocations
class ARROW_EXPORT MockOutputStream : public OutputStream {
 public:
  MockOutputStream() : extent_bytes_written_(0), is_open_(true) {}

  // Implement the OutputStream interface
  Status Close() override;
  bool closed() const override;
  Status Tell(int64_t* position) const override;
  Status Write(const void* data, int64_t nbytes) override;

  int64_t GetExtentBytesWritten() const { return extent_bytes_written_; }

 private:
  int64_t extent_bytes_written_;
  bool is_open_;
};

/// \brief Enables random writes into a fixed-size mutable buffer
class ARROW_EXPORT FixedSizeBufferWriter : public WritableFile {
 public:
  /// Input buffer must be mutable, will abort if not
  explicit FixedSizeBufferWriter(const std::shared_ptr<Buffer>& buffer);
  ~FixedSizeBufferWriter() override;

  Status Close() override;
  bool closed() const override;
  Status Seek(int64_t position) override;
  Status Tell(int64_t* position) const override;
  Status Write(const void* data, int64_t nbytes) override;
  Status WriteAt(int64_t position, const void* data, int64_t nbytes) override;

  void set_memcopy_threads(int num_threads);
  void set_memcopy_blocksize(int64_t blocksize);
  void set_memcopy_threshold(int64_t threshold);

 protected:
  class FixedSizeBufferWriterImpl;
  std::unique_ptr<FixedSizeBufferWriterImpl> impl_;
};

/// \class BufferReader
/// \brief Random access zero-copy reads on an arrow::Buffer
class ARROW_EXPORT BufferReader : public RandomAccessFile {
 public:
  explicit BufferReader(const std::shared_ptr<Buffer>& buffer);
  explicit BufferReader(const Buffer& buffer);
  BufferReader(const uint8_t* data, int64_t size);

  /// \brief Instantiate from std::string or arrow::util::string_view. Does not
  /// own data
  explicit BufferReader(const util::string_view& data);

  Status Close() override;
  bool closed() const override;
  Status Tell(int64_t* position) const override;
  Status Read(int64_t nbytes, int64_t* bytes_read, void* buffer) override;
  // Zero copy read
  Status Read(int64_t nbytes, std::shared_ptr<Buffer>* out) override;

  Status Peek(int64_t nbytes, util::string_view* out) override;

  bool supports_zero_copy() const override;

  Status ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read,
                void* out) override;
  Status ReadAt(int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out) override;

  Status GetSize(int64_t* size) override;
  Status Seek(int64_t position) override;

  std::shared_ptr<Buffer> buffer() const { return buffer_; }

 protected:
  inline Status CheckClosed() const;

  std::shared_ptr<Buffer> buffer_;
  const uint8_t* data_;
  int64_t size_;
  int64_t position_;
  bool is_open_;
};

}  // namespace io
}  // namespace arrow
