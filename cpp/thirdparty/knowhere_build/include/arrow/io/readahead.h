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

#ifndef ARROW_IO_READAHEAD_H
#define ARROW_IO_READAHEAD_H

#include <cstdint>
#include <memory>

#include "arrow/util/visibility.h"

namespace arrow {

class MemoryPool;
class ResizableBuffer;
class Status;

namespace io {

class InputStream;

namespace internal {

struct ARROW_EXPORT ReadaheadBuffer {
  std::shared_ptr<ResizableBuffer> buffer;
  int64_t left_padding;
  int64_t right_padding;
};

class ARROW_EXPORT ReadaheadSpooler {
 public:
  /// \brief EXPERIMENTAL: Create a readahead spooler wrapping the given input stream.
  ///
  /// The spooler launches a background thread that reads up to a given number
  /// of fixed-size blocks in advance from the underlying stream.
  /// The buffers returned by Read() will be padded at the beginning and the end
  /// with the configured amount of (zeroed) bytes.
  ReadaheadSpooler(MemoryPool* pool, std::shared_ptr<InputStream> raw,
                   int64_t read_size = kDefaultReadSize, int32_t readahead_queue_size = 1,
                   int64_t left_padding = 0, int64_t right_padding = 0);

  explicit ReadaheadSpooler(std::shared_ptr<InputStream> raw,
                            int64_t read_size = kDefaultReadSize,
                            int32_t readahead_queue_size = 1, int64_t left_padding = 0,
                            int64_t right_padding = 0);

  ~ReadaheadSpooler();

  /// Configure zero-padding at beginning and end of buffers (default 0 bytes).
  /// The buffers returned by Read() will be padded at the beginning and the end
  /// with the configured amount of (zeroed) bytes.
  /// Note that, as reading happens in background and in advance, changing the
  /// configured values might not affect Read() results immediately.
  int64_t GetLeftPadding();
  void SetLeftPadding(int64_t size);

  int64_t GetRightPadding();
  void SetRightPadding(int64_t size);

  /// \brief Close the spooler.  This implicitly closes the underlying input stream.
  Status Close();

  /// \brief Read a buffer from the queue.
  ///
  /// If the buffer pointer in the ReadaheadBuffer is null, then EOF was
  /// reached and/or the spooler was explicitly closed.
  /// Otherwise, the buffer will contain at most read_size bytes in addition
  /// to the configured padding (short reads are possible at the end of a file).
  // How do we allow reusing the buffer in ReadaheadBuffer? perhaps by using
  // a caching memory pool?
  Status Read(ReadaheadBuffer* out);

 private:
  static constexpr int64_t kDefaultReadSize = 1 << 20;  // 1 MB

  class ARROW_NO_EXPORT Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace internal
}  // namespace io
}  // namespace arrow

#endif  // ARROW_IO_READAHEAD_H
