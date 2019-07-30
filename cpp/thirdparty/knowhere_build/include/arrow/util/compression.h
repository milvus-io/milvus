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

#ifndef ARROW_UTIL_COMPRESSION_H
#define ARROW_UTIL_COMPRESSION_H

#include <cstdint>
#include <memory>

#include "arrow/util/visibility.h"

namespace arrow {

class Status;

struct Compression {
  enum type { UNCOMPRESSED, SNAPPY, GZIP, BROTLI, ZSTD, LZ4, LZO, BZ2 };
};

namespace util {

/// \brief Streaming compressor interface
///
class ARROW_EXPORT Compressor {
 public:
  virtual ~Compressor();

  /// \brief Compress some input.
  ///
  /// If bytes_read is 0 on return, then a larger output buffer should be supplied.
  virtual Status Compress(int64_t input_len, const uint8_t* input, int64_t output_len,
                          uint8_t* output, int64_t* bytes_read,
                          int64_t* bytes_written) = 0;

  /// \brief Flush part of the compressed output.
  ///
  /// If should_retry is true on return, Flush() should be called again
  /// with a larger buffer.
  virtual Status Flush(int64_t output_len, uint8_t* output, int64_t* bytes_written,
                       bool* should_retry) = 0;

  /// \brief End compressing, doing whatever is necessary to end the stream.
  ///
  /// If should_retry is true on return, End() should be called again
  /// with a larger buffer.  Otherwise, the Compressor should not be used anymore.
  ///
  /// End() implies Flush().
  virtual Status End(int64_t output_len, uint8_t* output, int64_t* bytes_written,
                     bool* should_retry) = 0;

  // XXX add methods for buffer size heuristics?
};

/// \brief Streaming decompressor interface
///
class ARROW_EXPORT Decompressor {
 public:
  virtual ~Decompressor();

  /// \brief Decompress some input.
  ///
  /// If need_more_output is true on return, a larger output buffer needs
  /// to be supplied.
  /// XXX is need_more_output necessary? (Brotli?)
  virtual Status Decompress(int64_t input_len, const uint8_t* input, int64_t output_len,
                            uint8_t* output, int64_t* bytes_read, int64_t* bytes_written,
                            bool* need_more_output) = 0;

  /// \brief Return whether the compressed stream is finished.
  ///
  /// This is a heuristic.  If true is returned, then it is guaranteed
  /// that the stream is finished.  If false is returned, however, it may
  /// simply be that the underlying library isn't able to provide the information.
  virtual bool IsFinished() = 0;

  // XXX add methods for buffer size heuristics?
};

class ARROW_EXPORT Codec {
 public:
  virtual ~Codec();

  static Status Create(Compression::type codec, std::unique_ptr<Codec>* out);

  /// \brief One-shot decompression function
  ///
  /// output_buffer_len must be correct and therefore be obtained in advance.
  ///
  /// \note One-shot decompression is not always compatible with streaming
  /// compression.  Depending on the codec (e.g. LZ4), different formats may
  /// be used.
  virtual Status Decompress(int64_t input_len, const uint8_t* input,
                            int64_t output_buffer_len, uint8_t* output_buffer) = 0;

  /// \brief One-shot decompression function that also returns the
  /// actual decompressed size.
  ///
  /// \param[in] input_len the number of bytes of compressed data.
  /// \param[in] input the compressed data.
  /// \param[in] output_buffer_len the number of bytes of buffer for
  /// decompressed data.
  /// \param[in] output_buffer the buffer for decompressed data.
  /// \param[out] output_len the actual decompressed size.
  ///
  /// \note One-shot decompression is not always compatible with streaming
  /// compression.  Depending on the codec (e.g. LZ4), different formats may
  /// be used.
  virtual Status Decompress(int64_t input_len, const uint8_t* input,
                            int64_t output_buffer_len, uint8_t* output_buffer,
                            int64_t* output_len) = 0;

  /// \brief One-shot compression function
  ///
  /// output_buffer_len must first have been computed using MaxCompressedLen().
  ///
  /// \note One-shot compression is not always compatible with streaming
  /// decompression.  Depending on the codec (e.g. LZ4), different formats may
  /// be used.
  virtual Status Compress(int64_t input_len, const uint8_t* input,
                          int64_t output_buffer_len, uint8_t* output_buffer,
                          int64_t* output_len) = 0;

  virtual int64_t MaxCompressedLen(int64_t input_len, const uint8_t* input) = 0;

  // XXX Should be able to choose compression level, or presets? ("fast", etc.)

  /// \brief Create a streaming compressor instance
  virtual Status MakeCompressor(std::shared_ptr<Compressor>* out) = 0;

  /// \brief Create a streaming decompressor instance
  virtual Status MakeDecompressor(std::shared_ptr<Decompressor>* out) = 0;

  virtual const char* name() const = 0;
};

}  // namespace util
}  // namespace arrow

#endif
