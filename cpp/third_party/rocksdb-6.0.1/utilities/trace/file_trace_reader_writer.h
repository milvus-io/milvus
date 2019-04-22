//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/trace_reader_writer.h"

namespace rocksdb {

class RandomAccessFileReader;
class WritableFileWriter;

// FileTraceReader allows reading RocksDB traces from a file.
class FileTraceReader : public TraceReader {
 public:
  explicit FileTraceReader(std::unique_ptr<RandomAccessFileReader>&& reader);
  ~FileTraceReader();

  virtual Status Read(std::string* data) override;
  virtual Status Close() override;

 private:
  std::unique_ptr<RandomAccessFileReader> file_reader_;
  Slice result_;
  size_t offset_;
  char* const buffer_;

  static const unsigned int kBufferSize;
};

// FileTraceWriter allows writing RocksDB traces to a file.
class FileTraceWriter : public TraceWriter {
 public:
  explicit FileTraceWriter(std::unique_ptr<WritableFileWriter>&& file_writer)
      : file_writer_(std::move(file_writer)) {}
  ~FileTraceWriter();

  virtual Status Write(const Slice& data) override;
  virtual Status Close() override;
  virtual uint64_t GetFileSize() override;

 private:
  std::unique_ptr<WritableFileWriter> file_writer_;
};

}  // namespace rocksdb
