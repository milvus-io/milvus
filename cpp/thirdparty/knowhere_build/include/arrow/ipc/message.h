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

// C++ object model and user API for interprocess schema messaging

#ifndef ARROW_IPC_MESSAGE_H
#define ARROW_IPC_MESSAGE_H

#include <cstdint>
#include <memory>
#include <string>

#include "arrow/status.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;

namespace io {

class FileInterface;
class InputStream;
class OutputStream;
class RandomAccessFile;

}  // namespace io

namespace ipc {

enum class MetadataVersion : char {
  /// 0.1.0
  V1,

  /// 0.2.0
  V2,

  /// 0.3.0 to 0.7.1
  V3,

  /// >= 0.8.0
  V4
};

// ARROW-109: We set this number arbitrarily to help catch user mistakes. For
// deeply nested schemas, it is expected the user will indicate explicitly the
// maximum allowed recursion depth
constexpr int kMaxNestingDepth = 64;

// Read interface classes. We do not fully deserialize the flatbuffers so that
// individual fields metadata can be retrieved from very large schema without
//

/// \class Message
/// \brief An IPC message including metadata and body
class ARROW_EXPORT Message {
 public:
  enum Type { NONE, SCHEMA, DICTIONARY_BATCH, RECORD_BATCH, TENSOR, SPARSE_TENSOR };

  /// \brief Construct message, but do not validate
  ///
  /// Use at your own risk; Message::Open has more metadata validation
  Message(const std::shared_ptr<Buffer>& metadata, const std::shared_ptr<Buffer>& body);

  ~Message();

  /// \brief Create and validate a Message instance from two buffers
  ///
  /// \param[in] metadata a buffer containing the Flatbuffer metadata
  /// \param[in] body a buffer containing the message body, which may be null
  /// \param[out] out the created message
  /// \return Status
  static Status Open(const std::shared_ptr<Buffer>& metadata,
                     const std::shared_ptr<Buffer>& body, std::unique_ptr<Message>* out);

  /// \brief Read message body and create Message given Flatbuffer metadata
  /// \param[in] metadata containing a serialized Message flatbuffer
  /// \param[in] stream an InputStream
  /// \param[out] out the created Message
  /// \return Status
  ///
  /// \note If stream supports zero-copy, this is zero-copy
  static Status ReadFrom(const std::shared_ptr<Buffer>& metadata, io::InputStream* stream,
                         std::unique_ptr<Message>* out);

  /// \brief Read message body from position in file, and create Message given
  /// the Flatbuffer metadata
  /// \param[in] offset the position in the file where the message body starts.
  /// \param[in] metadata containing a serialized Message flatbuffer
  /// \param[in] file the seekable file interface to read from
  /// \param[out] out the created Message
  /// \return Status
  ///
  /// \note If file supports zero-copy, this is zero-copy
  static Status ReadFrom(const int64_t offset, const std::shared_ptr<Buffer>& metadata,
                         io::RandomAccessFile* file, std::unique_ptr<Message>* out);

  /// \brief Return true if message type and contents are equal
  ///
  /// \param other another message
  /// \return true if contents equal
  bool Equals(const Message& other) const;

  /// \brief the Message metadata
  ///
  /// \return buffer
  std::shared_ptr<Buffer> metadata() const;

  /// \brief the Message body, if any
  ///
  /// \return buffer is null if no body
  std::shared_ptr<Buffer> body() const;

  /// \brief The expected body length according to the metadata, for
  /// verification purposes
  int64_t body_length() const;

  /// \brief The Message type
  Type type() const;

  /// \brief The Message metadata version
  MetadataVersion metadata_version() const;

  const void* header() const;

  /// \brief Write length-prefixed metadata and body to output stream
  ///
  /// \param[in] file output stream to write to
  /// \param[in] alignment byte alignment for metadata, usually 8 or
  /// 64. Whether the body is padded depends on the metadata; if the body
  /// buffer is smaller than the size indicated in the metadata, then extra
  /// padding bytes will be written
  /// \param[out] output_length the number of bytes written
  /// \return Status
  Status SerializeTo(io::OutputStream* file, int32_t alignment,
                     int64_t* output_length) const;

  /// \brief Return true if the Message metadata passes Flatbuffer validation
  bool Verify() const;

  /// \brief Whether a given message type needs a body.
  static bool HasBody(Type type) { return type != NONE && type != SCHEMA; }

 private:
  // Hide serialization details from user API
  class MessageImpl;
  std::unique_ptr<MessageImpl> impl_;

  ARROW_DISALLOW_COPY_AND_ASSIGN(Message);
};

ARROW_EXPORT std::string FormatMessageType(Message::Type type);

/// \brief Abstract interface for a sequence of messages
/// \since 0.5.0
class ARROW_EXPORT MessageReader {
 public:
  virtual ~MessageReader() = default;

  /// \brief Create MessageReader that reads from InputStream
  static std::unique_ptr<MessageReader> Open(io::InputStream* stream);

  /// \brief Create MessageReader that reads from owned InputStream
  static std::unique_ptr<MessageReader> Open(
      const std::shared_ptr<io::InputStream>& owned_stream);

  /// \brief Read next Message from the interface
  ///
  /// \param[out] message an arrow::ipc::Message instance
  /// \return Status
  virtual Status ReadNextMessage(std::unique_ptr<Message>* message) = 0;
};

/// \brief Read encapsulated RPC message from position in file
///
/// Read a length-prefixed message flatbuffer starting at the indicated file
/// offset. If the message has a body with non-zero length, it will also be
/// read
///
/// The metadata_length includes at least the length prefix and the flatbuffer
///
/// \param[in] offset the position in the file where the message starts. The
/// first 4 bytes after the offset are the message length
/// \param[in] metadata_length the total number of bytes to read from file
/// \param[in] file the seekable file interface to read from
/// \param[out] message the message read
/// \return Status success or failure
ARROW_EXPORT
Status ReadMessage(const int64_t offset, const int32_t metadata_length,
                   io::RandomAccessFile* file, std::unique_ptr<Message>* message);

/// \brief Advance stream to an 8-byte offset if its position is not a multiple
/// of 8 already
/// \param[in] stream an input stream
/// \param[in] alignment the byte multiple for the metadata prefix, usually 8
/// or 64, to ensure the body starts on a multiple of that alignment
/// \return Status
ARROW_EXPORT
Status AlignStream(io::InputStream* stream, int32_t alignment = 8);

/// \brief Advance stream to an 8-byte offset if its position is not a multiple
/// of 8 already
/// \param[in] stream an output stream
/// \param[in] alignment the byte multiple for the metadata prefix, usually 8
/// or 64, to ensure the body starts on a multiple of that alignment
/// \return Status
ARROW_EXPORT
Status AlignStream(io::OutputStream* stream, int32_t alignment = 8);

/// \brief Return error Status if file position is not a multiple of the
/// indicated alignment
ARROW_EXPORT
Status CheckAligned(io::FileInterface* stream, int32_t alignment = 8);

/// \brief Read encapsulated RPC message (metadata and body) from InputStream
///
/// Read length-prefixed message with as-yet unknown length. Returns null if
/// there are not enough bytes available or the message length is 0 (e.g. EOS
/// in a stream)
ARROW_EXPORT
Status ReadMessage(io::InputStream* stream, std::unique_ptr<Message>* message);

}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_MESSAGE_H
