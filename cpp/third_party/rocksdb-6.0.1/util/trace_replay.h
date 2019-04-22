//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>

#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/trace_reader_writer.h"

namespace rocksdb {

class ColumnFamilyHandle;
class ColumnFamilyData;
class DB;
class DBImpl;
class Slice;
class WriteBatch;

extern const std::string kTraceMagic;
const unsigned int kTraceTimestampSize = 8;
const unsigned int kTraceTypeSize = 1;
const unsigned int kTracePayloadLengthSize = 4;
const unsigned int kTraceMetadataSize =
    kTraceTimestampSize + kTraceTypeSize + kTracePayloadLengthSize;

enum TraceType : char {
  kTraceBegin = 1,
  kTraceEnd = 2,
  kTraceWrite = 3,
  kTraceGet = 4,
  kTraceIteratorSeek = 5,
  kTraceIteratorSeekForPrev = 6,
  kTraceMax,
};

// TODO: This should also be made part of public interface to help users build
// custom TracerReaders and TraceWriters.
struct Trace {
  uint64_t ts;
  TraceType type;
  std::string payload;

  void reset() {
    ts = 0;
    type = kTraceMax;
    payload.clear();
  }
};

// Trace RocksDB operations using a TraceWriter.
class Tracer {
 public:
  Tracer(Env* env, const TraceOptions& trace_options,
         std::unique_ptr<TraceWriter>&& trace_writer);
  ~Tracer();

  Status Write(WriteBatch* write_batch);
  Status Get(ColumnFamilyHandle* cfname, const Slice& key);
  Status IteratorSeek(const uint32_t& cf_id, const Slice& key);
  Status IteratorSeekForPrev(const uint32_t& cf_id, const Slice& key);
  bool IsTraceFileOverMax();

  Status Close();

 private:
  Status WriteHeader();
  Status WriteFooter();
  Status WriteTrace(const Trace& trace);
  bool ShouldSkipTrace();

  Env* env_;
  TraceOptions trace_options_;
  std::unique_ptr<TraceWriter> trace_writer_;
  uint64_t trace_request_count_;
};

// Replay RocksDB operations from a trace.
class Replayer {
 public:
  Replayer(DB* db, const std::vector<ColumnFamilyHandle*>& handles,
           std::unique_ptr<TraceReader>&& reader);
  ~Replayer();

  Status Replay();

 private:
  Status ReadHeader(Trace* header);
  Status ReadFooter(Trace* footer);
  Status ReadTrace(Trace* trace);

  DBImpl* db_;
  std::unique_ptr<TraceReader> trace_reader_;
  std::unordered_map<uint32_t, ColumnFamilyHandle*> cf_map_;
};

}  // namespace rocksdb
