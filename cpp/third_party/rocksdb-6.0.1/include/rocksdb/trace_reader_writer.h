//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/env.h"

namespace rocksdb {

// Allow custom implementations of TraceWriter and TraceReader.
// By default, RocksDB provides a way to capture the traces to a file using the
// factory NewFileTraceWriter(). But users could also choose to export traces to
// any other system by providing custom implementations of TraceWriter and
// TraceReader.

// TraceWriter allows exporting RocksDB traces to any system, one operation at
// a time.
class TraceWriter {
 public:
  TraceWriter() {}
  virtual ~TraceWriter() {}

  virtual Status Write(const Slice& data) = 0;
  virtual Status Close() = 0;
  virtual uint64_t GetFileSize() = 0;
};

// TraceReader allows reading RocksDB traces from any system, one operation at
// a time. A RocksDB Replayer could depend on this to replay opertions.
class TraceReader {
 public:
  TraceReader() {}
  virtual ~TraceReader() {}

  virtual Status Read(std::string* data) = 0;
  virtual Status Close() = 0;
};

// Factory methods to read/write traces from/to a file.
Status NewFileTraceWriter(Env* env, const EnvOptions& env_options,
                          const std::string& trace_filename,
                          std::unique_ptr<TraceWriter>* trace_writer);
Status NewFileTraceReader(Env* env, const EnvOptions& env_options,
                          const std::string& trace_filename,
                          std::unique_ptr<TraceReader>* trace_reader);
}  // namespace rocksdb
