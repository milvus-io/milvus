//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/trace_replay.h"

#include <chrono>
#include <sstream>
#include <thread>
#include "db/db_impl.h"
#include "rocksdb/slice.h"
#include "rocksdb/write_batch.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace rocksdb {

const std::string kTraceMagic = "feedcafedeadbeef";

namespace {
void EncodeCFAndKey(std::string* dst, uint32_t cf_id, const Slice& key) {
  PutFixed32(dst, cf_id);
  PutLengthPrefixedSlice(dst, key);
}

void DecodeCFAndKey(std::string& buffer, uint32_t* cf_id, Slice* key) {
  Slice buf(buffer);
  GetFixed32(&buf, cf_id);
  GetLengthPrefixedSlice(&buf, key);
}
}  // namespace

Tracer::Tracer(Env* env, const TraceOptions& trace_options,
               std::unique_ptr<TraceWriter>&& trace_writer)
    : env_(env),
      trace_options_(trace_options),
      trace_writer_(std::move(trace_writer)),
      trace_request_count_ (0) {
  WriteHeader();
}

Tracer::~Tracer() { trace_writer_.reset(); }

Status Tracer::Write(WriteBatch* write_batch) {
  if (ShouldSkipTrace()) {
    return Status::OK();
  }
  Trace trace;
  trace.ts = env_->NowMicros();
  trace.type = kTraceWrite;
  trace.payload = write_batch->Data();
  return WriteTrace(trace);
}

Status Tracer::Get(ColumnFamilyHandle* column_family, const Slice& key) {
  if (ShouldSkipTrace()) {
    return Status::OK();
  }
  Trace trace;
  trace.ts = env_->NowMicros();
  trace.type = kTraceGet;
  EncodeCFAndKey(&trace.payload, column_family->GetID(), key);
  return WriteTrace(trace);
}

Status Tracer::IteratorSeek(const uint32_t& cf_id, const Slice& key) {
  if (ShouldSkipTrace()) {
    return Status::OK();
  }
  Trace trace;
  trace.ts = env_->NowMicros();
  trace.type = kTraceIteratorSeek;
  EncodeCFAndKey(&trace.payload, cf_id, key);
  return WriteTrace(trace);
}

Status Tracer::IteratorSeekForPrev(const uint32_t& cf_id, const Slice& key) {
  if (ShouldSkipTrace()) {
    return Status::OK();
  }
  Trace trace;
  trace.ts = env_->NowMicros();
  trace.type = kTraceIteratorSeekForPrev;
  EncodeCFAndKey(&trace.payload, cf_id, key);
  return WriteTrace(trace);
}

bool Tracer::ShouldSkipTrace() {
  if (IsTraceFileOverMax()) {
    return true;
  }
  ++trace_request_count_;
  if (trace_request_count_ < trace_options_.sampling_frequency) {
    return true;
  }
  trace_request_count_ = 0;
  return false;
}

bool Tracer::IsTraceFileOverMax() {
  uint64_t trace_file_size = trace_writer_->GetFileSize();
  return (trace_file_size > trace_options_.max_trace_file_size);
}

Status Tracer::WriteHeader() {
  std::ostringstream s;
  s << kTraceMagic << "\t"
    << "Trace Version: 0.1\t"
    << "RocksDB Version: " << kMajorVersion << "." << kMinorVersion << "\t"
    << "Format: Timestamp OpType Payload\n";
  std::string header(s.str());

  Trace trace;
  trace.ts = env_->NowMicros();
  trace.type = kTraceBegin;
  trace.payload = header;
  return WriteTrace(trace);
}

Status Tracer::WriteFooter() {
  Trace trace;
  trace.ts = env_->NowMicros();
  trace.type = kTraceEnd;
  trace.payload = "";
  return WriteTrace(trace);
}

Status Tracer::WriteTrace(const Trace& trace) {
  std::string encoded_trace;
  PutFixed64(&encoded_trace, trace.ts);
  encoded_trace.push_back(trace.type);
  PutFixed32(&encoded_trace, static_cast<uint32_t>(trace.payload.size()));
  encoded_trace.append(trace.payload);
  return trace_writer_->Write(Slice(encoded_trace));
}

Status Tracer::Close() { return WriteFooter(); }

Replayer::Replayer(DB* db, const std::vector<ColumnFamilyHandle*>& handles,
                   std::unique_ptr<TraceReader>&& reader)
    : trace_reader_(std::move(reader)) {
  assert(db != nullptr);
  db_ = static_cast<DBImpl*>(db->GetRootDB());
  for (ColumnFamilyHandle* cfh : handles) {
    cf_map_[cfh->GetID()] = cfh;
  }
}

Replayer::~Replayer() { trace_reader_.reset(); }

Status Replayer::Replay() {
  Status s;
  Trace header;
  s = ReadHeader(&header);
  if (!s.ok()) {
    return s;
  }

  std::chrono::system_clock::time_point replay_epoch =
      std::chrono::system_clock::now();
  WriteOptions woptions;
  ReadOptions roptions;
  Trace trace;
  uint64_t ops = 0;
  Iterator* single_iter = nullptr;
  while (s.ok()) {
    trace.reset();
    s = ReadTrace(&trace);
    if (!s.ok()) {
      break;
    }

    std::this_thread::sleep_until(
        replay_epoch + std::chrono::microseconds(trace.ts - header.ts));
    if (trace.type == kTraceWrite) {
      WriteBatch batch(trace.payload);
      db_->Write(woptions, &batch);
      ops++;
    } else if (trace.type == kTraceGet) {
      uint32_t cf_id = 0;
      Slice key;
      DecodeCFAndKey(trace.payload, &cf_id, &key);
      if (cf_id > 0 && cf_map_.find(cf_id) == cf_map_.end()) {
        return Status::Corruption("Invalid Column Family ID.");
      }

      std::string value;
      if (cf_id == 0) {
        db_->Get(roptions, key, &value);
      } else {
        db_->Get(roptions, cf_map_[cf_id], key, &value);
      }
      ops++;
    } else if (trace.type == kTraceIteratorSeek) {
      uint32_t cf_id = 0;
      Slice key;
      DecodeCFAndKey(trace.payload, &cf_id, &key);
      if (cf_id > 0 && cf_map_.find(cf_id) == cf_map_.end()) {
        return Status::Corruption("Invalid Column Family ID.");
      }

      if (cf_id == 0) {
        single_iter = db_->NewIterator(roptions);
      } else {
        single_iter = db_->NewIterator(roptions, cf_map_[cf_id]);
      }
      single_iter->Seek(key);
      ops++;
      delete single_iter;
    } else if (trace.type == kTraceIteratorSeekForPrev) {
      // Currently, only support to call the Seek()
      uint32_t cf_id = 0;
      Slice key;
      DecodeCFAndKey(trace.payload, &cf_id, &key);
      if (cf_id > 0 && cf_map_.find(cf_id) == cf_map_.end()) {
        return Status::Corruption("Invalid Column Family ID.");
      }

      if (cf_id == 0) {
        single_iter = db_->NewIterator(roptions);
      } else {
        single_iter = db_->NewIterator(roptions, cf_map_[cf_id]);
      }
      single_iter->SeekForPrev(key);
      ops++;
      delete single_iter;
    } else if (trace.type == kTraceEnd) {
      // Do nothing for now.
      // TODO: Add some validations later.
      break;
    }
  }

  if (s.IsIncomplete()) {
    // Reaching eof returns Incomplete status at the moment.
    // Could happen when killing a process without calling EndTrace() API.
    // TODO: Add better error handling.
    return Status::OK();
  }
  return s;
}

Status Replayer::ReadHeader(Trace* header) {
  assert(header != nullptr);
  Status s = ReadTrace(header);
  if (!s.ok()) {
    return s;
  }
  if (header->type != kTraceBegin) {
    return Status::Corruption("Corrupted trace file. Incorrect header.");
  }
  if (header->payload.substr(0, kTraceMagic.length()) != kTraceMagic) {
    return Status::Corruption("Corrupted trace file. Incorrect magic.");
  }

  return s;
}

Status Replayer::ReadFooter(Trace* footer) {
  assert(footer != nullptr);
  Status s = ReadTrace(footer);
  if (!s.ok()) {
    return s;
  }
  if (footer->type != kTraceEnd) {
    return Status::Corruption("Corrupted trace file. Incorrect footer.");
  }

  // TODO: Add more validations later
  return s;
}

Status Replayer::ReadTrace(Trace* trace) {
  assert(trace != nullptr);
  std::string encoded_trace;
  Status s = trace_reader_->Read(&encoded_trace);
  if (!s.ok()) {
    return s;
  }

  Slice enc_slice = Slice(encoded_trace);
  GetFixed64(&enc_slice, &trace->ts);
  trace->type = static_cast<TraceType>(enc_slice[0]);
  enc_slice.remove_prefix(kTraceTypeSize + kTracePayloadLengthSize);
  trace->payload = enc_slice.ToString();
  return s;
}

}  // namespace rocksdb
