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

// Implement Arrow streaming binary format

#ifndef ARROW_IPC_WRITER_H
#define ARROW_IPC_WRITER_H

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/ipc/message.h"
#include "arrow/result.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class Buffer;
class MemoryPool;
class RecordBatch;
class Schema;
class Status;
class Table;
class Tensor;
class SparseTensor;

namespace io {

class OutputStream;

}  // namespace io

namespace ipc {

class DictionaryMemo;

/// \class RecordBatchWriter
/// \brief Abstract interface for writing a stream of record batches
class ARROW_EXPORT RecordBatchWriter {
 public:
  virtual ~RecordBatchWriter();

  /// \brief Write a record batch to the stream
  ///
  /// \param[in] batch the record batch to write to the stream
  /// \param[in] allow_64bit if true, allow field lengths that don't fit
  ///    in a signed 32-bit int
  /// \return Status
  virtual Status WriteRecordBatch(const RecordBatch& batch, bool allow_64bit = false) = 0;

  /// \brief Write possibly-chunked table by creating sequence of record batches
  /// \param[in] table table to write
  /// \return Status
  Status WriteTable(const Table& table);

  /// \brief Write Table with a particular chunksize
  /// \param[in] table table to write
  /// \param[in] max_chunksize maximum chunk size for table chunks
  /// \return Status
  Status WriteTable(const Table& table, int64_t max_chunksize);

  /// \brief Perform any logic necessary to finish the stream
  ///
  /// \return Status
  virtual Status Close() = 0;

  /// In some cases, writing may require memory allocation. We use the default
  /// memory pool, but provide the option to override
  ///
  /// \param pool the memory pool to use for required allocations
  virtual void set_memory_pool(MemoryPool* pool) = 0;
};

/// \class RecordBatchStreamWriter
/// \brief Synchronous batch stream writer that writes the Arrow streaming
/// format
class ARROW_EXPORT RecordBatchStreamWriter : public RecordBatchWriter {
 public:
  ~RecordBatchStreamWriter() override;

  /// Create a new writer from stream sink and schema. User is responsible for
  /// closing the actual OutputStream.
  ///
  /// \param[in] sink output stream to write to
  /// \param[in] schema the schema of the record batches to be written
  /// \param[out] out the created stream writer
  /// \return Status
  static Status Open(io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
                     std::shared_ptr<RecordBatchWriter>* out);

  /// Create a new writer from stream sink and schema. User is responsible for
  /// closing the actual OutputStream.
  ///
  /// \param[in] sink output stream to write to
  /// \param[in] schema the schema of the record batches to be written
  /// \return Result<std::shared_ptr<RecordBatchWriter>>
  static Result<std::shared_ptr<RecordBatchWriter>> Open(
      io::OutputStream* sink, const std::shared_ptr<Schema>& schema);

  /// \brief Write a record batch to the stream
  ///
  /// \param[in] batch the record batch to write
  /// \param[in] allow_64bit allow array lengths over INT32_MAX - 1
  /// \return Status
  Status WriteRecordBatch(const RecordBatch& batch, bool allow_64bit = false) override;

  /// \brief Close the stream by writing a 4-byte int32 0 EOS market
  /// \return Status
  Status Close() override;

  void set_memory_pool(MemoryPool* pool) override;

 protected:
  RecordBatchStreamWriter();
  class ARROW_NO_EXPORT RecordBatchStreamWriterImpl;
  std::unique_ptr<RecordBatchStreamWriterImpl> impl_;
};

/// \brief Creates the Arrow record batch file format
///
/// Implements the random access file format, which structurally is a record
/// batch stream followed by a metadata footer at the end of the file. Magic
/// numbers are written at the start and end of the file
class ARROW_EXPORT RecordBatchFileWriter : public RecordBatchStreamWriter {
 public:
  ~RecordBatchFileWriter() override;

  /// Create a new writer from stream sink and schema
  ///
  /// \param[in] sink output stream to write to
  /// \param[in] schema the schema of the record batches to be written
  /// \param[out] out the created stream writer
  /// \return Status
  static Status Open(io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
                     std::shared_ptr<RecordBatchWriter>* out);

  /// Create a new writer from stream sink and schema
  ///
  /// \param[in] sink output stream to write to
  /// \param[in] schema the schema of the record batches to be written
  /// \return Status
  static Result<std::shared_ptr<RecordBatchWriter>> Open(
      io::OutputStream* sink, const std::shared_ptr<Schema>& schema);

  /// \brief Write a record batch to the file
  ///
  /// \param[in] batch the record batch to write
  /// \param[in] allow_64bit allow array lengths over INT32_MAX - 1
  /// \return Status
  Status WriteRecordBatch(const RecordBatch& batch, bool allow_64bit = false) override;

  /// \brief Close the file stream by writing the file footer and magic number
  /// \return Status
  Status Close() override;

 private:
  RecordBatchFileWriter();
  class ARROW_NO_EXPORT RecordBatchFileWriterImpl;
  std::unique_ptr<RecordBatchFileWriterImpl> file_impl_;
};

/// \brief Low-level API for writing a record batch (without schema) to an OutputStream
///
/// \param[in] batch the record batch to write
/// \param[in] buffer_start_offset the start offset to use in the buffer metadata,
/// generally should be 0
/// \param[in] dst an OutputStream
/// \param[out] metadata_length the size of the length-prefixed flatbuffer
/// including padding to a 64-byte boundary
/// \param[out] body_length the size of the contiguous buffer block plus
/// \param[in] pool the memory pool to allocate memory from
/// \param[in] max_recursion_depth the maximum permitted nesting schema depth
/// \param[in] allow_64bit permit field lengths exceeding INT32_MAX. May not be
/// readable by other Arrow implementations
/// padding bytes
/// \return Status
///
/// Write the RecordBatch (collection of equal-length Arrow arrays) to the
/// output stream in a contiguous block. The record batch metadata is written as
/// a flatbuffer (see format/Message.fbs -- the RecordBatch message type)
/// prefixed by its size, followed by each of the memory buffers in the batch
/// written end to end (with appropriate alignment and padding):
///
/// \code
/// <int32: metadata size> <uint8*: metadata> <buffers ...>
/// \endcode
///
/// Finally, the absolute offsets (relative to the start of the output stream)
/// to the end of the body and end of the metadata / data header (suffixed by
/// the header size) is returned in out-variables
ARROW_EXPORT
Status WriteRecordBatch(const RecordBatch& batch, int64_t buffer_start_offset,
                        io::OutputStream* dst, int32_t* metadata_length,
                        int64_t* body_length, MemoryPool* pool,
                        int max_recursion_depth = kMaxNestingDepth,
                        bool allow_64bit = false);

/// \brief Serialize record batch as encapsulated IPC message in a new buffer
///
/// \param[in] batch the record batch
/// \param[in] pool a MemoryPool to allocate memory from
/// \param[out] out the serialized message
/// \return Status
ARROW_EXPORT
Status SerializeRecordBatch(const RecordBatch& batch, MemoryPool* pool,
                            std::shared_ptr<Buffer>* out);

/// \brief Write record batch to OutputStream
///
/// \param[in] batch the record batch to write
/// \param[in] pool a MemoryPool to use for temporary allocations, if needed
/// \param[in] out the OutputStream to write the output to
/// \return Status
///
/// If writing to pre-allocated memory, you can use
/// arrow::ipc::GetRecordBatchSize to compute how much space is required
ARROW_EXPORT
Status SerializeRecordBatch(const RecordBatch& batch, MemoryPool* pool,
                            io::OutputStream* out);

/// \brief Serialize schema as encapsulated IPC message
///
/// \param[in] schema the schema to write
/// \param[in] dictionary_memo a DictionaryMemo for recording dictionary ids
/// \param[in] pool a MemoryPool to allocate memory from
/// \param[out] out the serialized schema
/// \return Status
ARROW_EXPORT
Status SerializeSchema(const Schema& schema, DictionaryMemo* dictionary_memo,
                       MemoryPool* pool, std::shared_ptr<Buffer>* out);

/// \brief Write multiple record batches to OutputStream, including schema
/// \param[in] batches a vector of batches. Must all have same schema
/// \param[out] dst an OutputStream
/// \return Status
ARROW_EXPORT
Status WriteRecordBatchStream(const std::vector<std::shared_ptr<RecordBatch>>& batches,
                              io::OutputStream* dst);

/// \brief Compute the number of bytes needed to write a record batch including metadata
///
/// \param[in] batch the record batch to write
/// \param[out] size the size of the complete encapsulated message
/// \return Status
ARROW_EXPORT
Status GetRecordBatchSize(const RecordBatch& batch, int64_t* size);

/// \brief Compute the number of bytes needed to write a tensor including metadata
///
/// \param[in] tensor the tenseor to write
/// \param[out] size the size of the complete encapsulated message
/// \return Status
ARROW_EXPORT
Status GetTensorSize(const Tensor& tensor, int64_t* size);

/// \brief EXPERIMENTAL: Convert arrow::Tensor to a Message with minimal memory
/// allocation
///
/// \param[in] tensor the Tensor to write
/// \param[in] pool MemoryPool to allocate space for metadata
/// \param[out] out the resulting Message
/// \return Status
ARROW_EXPORT
Status GetTensorMessage(const Tensor& tensor, MemoryPool* pool,
                        std::unique_ptr<Message>* out);

/// \brief Write arrow::Tensor as a contiguous message.
///
/// The metadata and body are written assuming 64-byte alignment. It is the
/// user's responsibility to ensure that the OutputStream has been aligned
/// to a 64-byte multiple before writing the message.
///
/// The message is written out as followed:
/// \code
/// <metadata size> <metadata> <tensor data>
/// \endcode
///
/// \param[in] tensor the Tensor to write
/// \param[in] dst the OutputStream to write to
/// \param[out] metadata_length the actual metadata length, including padding
/// \param[out] body_length the acutal message body length
/// \return Status
ARROW_EXPORT
Status WriteTensor(const Tensor& tensor, io::OutputStream* dst, int32_t* metadata_length,
                   int64_t* body_length);

// \brief EXPERIMENTAL: Write arrow::SparseTensor as a contiguous mesasge. The metadata,
// sparse index, and body are written assuming 64-byte alignment. It is the
// user's responsibility to ensure that the OutputStream has been aligned
// to a 64-byte multiple before writing the message.
//
// \param[in] tensor the SparseTensor to write
// \param[in] dst the OutputStream to write to
// \param[out] metadata_length the actual metadata length, including padding
// \param[out] body_length the actual message body length
ARROW_EXPORT
Status WriteSparseTensor(const SparseTensor& sparse_tensor, io::OutputStream* dst,
                         int32_t* metadata_length, int64_t* body_length,
                         MemoryPool* pool);

namespace internal {

// These internal APIs may change without warning or deprecation

// Intermediate data structure with metadata header, and zero or more buffers
// for the message body.
struct IpcPayload {
  Message::Type type = Message::NONE;
  std::shared_ptr<Buffer> metadata;
  std::vector<std::shared_ptr<Buffer>> body_buffers;
  int64_t body_length = 0;
};

class ARROW_EXPORT IpcPayloadWriter {
 public:
  virtual ~IpcPayloadWriter();

  // Default implementation is a no-op
  virtual Status Start();

  virtual Status WritePayload(const IpcPayload& payload) = 0;

  virtual Status Close() = 0;
};

/// Create a new RecordBatchWriter from IpcPayloadWriter and schema.
///
/// \param[in] sink the IpcPayloadWriter to write to
/// \param[in] schema the schema of the record batches to be written
/// \param[out] out the created RecordBatchWriter
/// \return Status
ARROW_EXPORT
Status OpenRecordBatchWriter(std::unique_ptr<IpcPayloadWriter> sink,
                             const std::shared_ptr<Schema>& schema,
                             std::unique_ptr<RecordBatchWriter>* out);

/// Create a new RecordBatchWriter from IpcPayloadWriter and schema.
///
/// \param[in] sink the IpcPayloadWriter to write to
/// \param[in] schema the schema of the record batches to be written
/// \return Result<std::unique_ptr<RecordBatchWriter>>
ARROW_EXPORT
Result<std::unique_ptr<RecordBatchWriter>> OpenRecordBatchWriter(
    std::unique_ptr<IpcPayloadWriter> sink, const std::shared_ptr<Schema>& schema);

/// \brief Compute IpcPayload for the given schema
/// \param[in] schema the Schema that is being serialized
/// \param[in,out] dictionary_memo class to populate with assigned dictionary ids
/// \param[out] out the returned vector of IpcPayloads
/// \return Status
ARROW_EXPORT
Status GetSchemaPayload(const Schema& schema, DictionaryMemo* dictionary_memo,
                        IpcPayload* out);

/// \brief Compute IpcPayload for a dictionary
/// \param[in] id the dictionary id
/// \param[in] dictionary the dictionary values
/// \param[out] payload the output IpcPayload
/// \return Status
ARROW_EXPORT
Status GetDictionaryPayload(int64_t id, const std::shared_ptr<Array>& dictionary,
                            MemoryPool* pool, IpcPayload* payload);

/// \brief Compute IpcPayload for the given record batch
/// \param[in] batch the RecordBatch that is being serialized
/// \param[in,out] pool for any required temporary memory allocations
/// \param[out] out the returned IpcPayload
/// \return Status
ARROW_EXPORT
Status GetRecordBatchPayload(const RecordBatch& batch, MemoryPool* pool, IpcPayload* out);

}  // namespace internal

}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_WRITER_H
