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

// Read Arrow files and streams

#ifndef ARROW_IPC_READER_H
#define ARROW_IPC_READER_H

#include <cstdint>
#include <memory>

#include "arrow/ipc/dictionary.h"
#include "arrow/ipc/message.h"
#include "arrow/record_batch.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;
class Schema;
class Status;
class Tensor;
class SparseTensor;

namespace io {

class InputStream;
class RandomAccessFile;

}  // namespace io

namespace ipc {

using RecordBatchReader = ::arrow::RecordBatchReader;

/// \class RecordBatchStreamReader
/// \brief Synchronous batch stream reader that reads from io::InputStream
///
/// This class reads the schema (plus any dictionaries) as the first messages
/// in the stream, followed by record batches. For more granular zero-copy
/// reads see the ReadRecordBatch functions
class ARROW_EXPORT RecordBatchStreamReader : public RecordBatchReader {
 public:
  ~RecordBatchStreamReader() override;

  /// Create batch reader from generic MessageReader.
  /// This will take ownership of the given MessageReader.
  ///
  /// \param[in] message_reader a MessageReader implementation
  /// \param[out] out the created RecordBatchReader object
  /// \return Status
  static Status Open(std::unique_ptr<MessageReader> message_reader,
                     std::shared_ptr<RecordBatchReader>* out);
  static Status Open(std::unique_ptr<MessageReader> message_reader,
                     std::unique_ptr<RecordBatchReader>* out);

  /// \brief Record batch stream reader from InputStream
  ///
  /// \param[in] stream an input stream instance. Must stay alive throughout
  /// lifetime of stream reader
  /// \param[out] out the created RecordBatchStreamReader object
  /// \return Status
  static Status Open(io::InputStream* stream, std::shared_ptr<RecordBatchReader>* out);

  /// \brief Open stream and retain ownership of stream object
  /// \param[in] stream the input stream
  /// \param[out] out the batch reader
  /// \return Status
  static Status Open(const std::shared_ptr<io::InputStream>& stream,
                     std::shared_ptr<RecordBatchReader>* out);

  /// \brief Returns the schema read from the stream
  std::shared_ptr<Schema> schema() const override;

  Status ReadNext(std::shared_ptr<RecordBatch>* batch) override;

 private:
  RecordBatchStreamReader();

  class ARROW_NO_EXPORT RecordBatchStreamReaderImpl;
  std::unique_ptr<RecordBatchStreamReaderImpl> impl_;
};

/// \brief Reads the record batch file format
class ARROW_EXPORT RecordBatchFileReader {
 public:
  ~RecordBatchFileReader();

  /// \brief Open a RecordBatchFileReader
  ///
  /// Open a file-like object that is assumed to be self-contained; i.e., the
  /// end of the file interface is the end of the Arrow file. Note that there
  /// can be any amount of data preceding the Arrow-formatted data, because we
  /// need only locate the end of the Arrow file stream to discover the metadata
  /// and then proceed to read the data into memory.
  static Status Open(io::RandomAccessFile* file,
                     std::shared_ptr<RecordBatchFileReader>* reader);

  /// \brief Open a RecordBatchFileReader
  /// If the file is embedded within some larger file or memory region, you can
  /// pass the absolute memory offset to the end of the file (which contains the
  /// metadata footer). The metadata must have been written with memory offsets
  /// relative to the start of the containing file
  ///
  /// \param[in] file the data source
  /// \param[in] footer_offset the position of the end of the Arrow file
  /// \param[out] reader the returned reader
  /// \return Status
  static Status Open(io::RandomAccessFile* file, int64_t footer_offset,
                     std::shared_ptr<RecordBatchFileReader>* reader);

  /// \brief Version of Open that retains ownership of file
  ///
  /// \param[in] file the data source
  /// \param[out] reader the returned reader
  /// \return Status
  static Status Open(const std::shared_ptr<io::RandomAccessFile>& file,
                     std::shared_ptr<RecordBatchFileReader>* reader);

  /// \brief Version of Open that retains ownership of file
  ///
  /// \param[in] file the data source
  /// \param[in] footer_offset the position of the end of the Arrow file
  /// \param[out] reader the returned reader
  /// \return Status
  static Status Open(const std::shared_ptr<io::RandomAccessFile>& file,
                     int64_t footer_offset,
                     std::shared_ptr<RecordBatchFileReader>* reader);

  /// \brief The schema read from the file
  std::shared_ptr<Schema> schema() const;

  /// \brief Returns the number of record batches in the file
  int num_record_batches() const;

  /// \brief Return the metadata version from the file metadata
  MetadataVersion version() const;

  /// \brief Read a particular record batch from the file. Does not copy memory
  /// if the input source supports zero-copy.
  ///
  /// \param[in] i the index of the record batch to return
  /// \param[out] batch the read batch
  /// \return Status
  Status ReadRecordBatch(int i, std::shared_ptr<RecordBatch>* batch);

 private:
  RecordBatchFileReader();

  class ARROW_NO_EXPORT RecordBatchFileReaderImpl;
  std::unique_ptr<RecordBatchFileReaderImpl> impl_;
};

// Generic read functions; does not copy data if the input supports zero copy reads

/// \brief Read Schema from stream serialized as a single IPC message
/// and populate any dictionary-encoded fields into a DictionaryMemo
///
/// \param[in] stream an InputStream
/// \param[in] dictionary_memo for recording dictionary-encoded fields
/// \param[out] out the output Schema
/// \return Status
///
/// If record batches follow the schema, it is better to use
/// RecordBatchStreamReader
ARROW_EXPORT
Status ReadSchema(io::InputStream* stream, DictionaryMemo* dictionary_memo,
                  std::shared_ptr<Schema>* out);

/// \brief Read Schema from encapsulated Message
///
/// \param[in] message a message instance containing metadata
/// \param[in] dictionary_memo DictionaryMemo for recording dictionary-encoded
/// fields. Can be nullptr if you are sure there are no
/// dictionary-encoded fields
/// \param[out] out the resulting Schema
/// \return Status
ARROW_EXPORT
Status ReadSchema(const Message& message, DictionaryMemo* dictionary_memo,
                  std::shared_ptr<Schema>* out);

/// Read record batch as encapsulated IPC message with metadata size prefix and
/// header
///
/// \param[in] schema the record batch schema
/// \param[in] dictionary_memo DictionaryMemo which has any
/// dictionaries. Can be nullptr if you are sure there are no
/// dictionary-encoded fields
/// \param[in] stream the file where the batch is located
/// \param[out] out the read record batch
/// \return Status
ARROW_EXPORT
Status ReadRecordBatch(const std::shared_ptr<Schema>& schema,
                       const DictionaryMemo* dictionary_memo, io::InputStream* stream,
                       std::shared_ptr<RecordBatch>* out);

/// \brief Read record batch from file given metadata and schema
///
/// \param[in] metadata a Message containing the record batch metadata
/// \param[in] schema the record batch schema
/// \param[in] dictionary_memo DictionaryMemo which has any
/// dictionaries. Can be nullptr if you are sure there are no
/// dictionary-encoded fields
/// \param[in] file a random access file
/// \param[out] out the read record batch
/// \return Status
ARROW_EXPORT
Status ReadRecordBatch(const Buffer& metadata, const std::shared_ptr<Schema>& schema,
                       const DictionaryMemo* dictionary_memo, io::RandomAccessFile* file,
                       std::shared_ptr<RecordBatch>* out);

/// \brief Read record batch from encapsulated Message
///
/// \param[in] message a message instance containing metadata and body
/// \param[in] schema the record batch schema
/// \param[in] dictionary_memo DictionaryMemo which has any
/// dictionaries. Can be nullptr if you are sure there are no
/// dictionary-encoded fields
/// \param[out] out the resulting RecordBatch
/// \return Status
ARROW_EXPORT
Status ReadRecordBatch(const Message& message, const std::shared_ptr<Schema>& schema,
                       const DictionaryMemo* dictionary_memo,
                       std::shared_ptr<RecordBatch>* out);

/// Read record batch from file given metadata and schema
///
/// \param[in] metadata a Message containing the record batch metadata
/// \param[in] schema the record batch schema
/// \param[in] dictionary_memo DictionaryMemo which has any
/// dictionaries. Can be nullptr if you are sure there are no
/// dictionary-encoded fields
/// \param[in] file a random access file
/// \param[in] max_recursion_depth the maximum permitted nesting depth
/// \param[out] out the read record batch
/// \return Status
ARROW_EXPORT
Status ReadRecordBatch(const Buffer& metadata, const std::shared_ptr<Schema>& schema,
                       const DictionaryMemo* dictionary_memo, int max_recursion_depth,
                       io::RandomAccessFile* file, std::shared_ptr<RecordBatch>* out);

/// \brief Read arrow::Tensor as encapsulated IPC message in file
///
/// \param[in] file an InputStream pointed at the start of the message
/// \param[out] out the read tensor
/// \return Status
ARROW_EXPORT
Status ReadTensor(io::InputStream* file, std::shared_ptr<Tensor>* out);

/// \brief EXPERIMENTAL: Read arrow::Tensor from IPC message
///
/// \param[in] message a Message containing the tensor metadata and body
/// \param[out] out the read tensor
/// \return Status
ARROW_EXPORT
Status ReadTensor(const Message& message, std::shared_ptr<Tensor>* out);

/// \brief EXPERIMETNAL: Read arrow::SparseTensor as encapsulated IPC message in file
///
/// \param[in] file an InputStream pointed at the start of the message
/// \param[out] out the read sparse tensor
/// \return Status
ARROW_EXPORT
Status ReadSparseTensor(io::InputStream* file, std::shared_ptr<SparseTensor>* out);

/// \brief EXPERIMENTAL: Read arrow::SparseTensor from IPC message
///
/// \param[in] message a Message containing the tensor metadata and body
/// \param[out] out the read sparse tensor
/// \return Status
ARROW_EXPORT
Status ReadSparseTensor(const Message& message, std::shared_ptr<SparseTensor>* out);

}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_READER_H
