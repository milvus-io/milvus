//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#ifndef ROCKSDB_LITE

#include "utilities/blob_db/blob_log_writer.h"

#include <cstdint>
#include <string>

#include "monitoring/statistics.h"
#include "rocksdb/env.h"
#include "util/coding.h"
#include "util/file_reader_writer.h"
#include "util/stop_watch.h"
#include "utilities/blob_db/blob_log_format.h"

namespace rocksdb {
namespace blob_db {

Writer::Writer(unique_ptr<WritableFileWriter>&& dest, Env* env,
               Statistics* statistics, uint64_t log_number, uint64_t bpsync,
               bool use_fs, uint64_t boffset)
    : dest_(std::move(dest)),
      env_(env),
      statistics_(statistics),
      log_number_(log_number),
      block_offset_(boffset),
      bytes_per_sync_(bpsync),
      next_sync_offset_(0),
      use_fsync_(use_fs),
      last_elem_type_(kEtNone) {}

Status Writer::Sync() {
  StopWatch sync_sw(env_, statistics_, BLOB_DB_BLOB_FILE_SYNC_MICROS);
  Status s = dest_->Sync(use_fsync_);
  RecordTick(statistics_, BLOB_DB_BLOB_FILE_SYNCED);
  return s;
}

Status Writer::WriteHeader(BlobLogHeader& header) {
  assert(block_offset_ == 0);
  assert(last_elem_type_ == kEtNone);
  std::string str;
  header.EncodeTo(&str);

  Status s = dest_->Append(Slice(str));
  if (s.ok()) {
    block_offset_ += str.size();
    s = dest_->Flush();
  }
  last_elem_type_ = kEtFileHdr;
  RecordTick(statistics_, BLOB_DB_BLOB_FILE_BYTES_WRITTEN,
             BlobLogHeader::kSize);
  return s;
}

Status Writer::AppendFooter(BlobLogFooter& footer) {
  assert(block_offset_ != 0);
  assert(last_elem_type_ == kEtFileHdr || last_elem_type_ == kEtRecord);

  std::string str;
  footer.EncodeTo(&str);

  Status s = dest_->Append(Slice(str));
  if (s.ok()) {
    block_offset_ += str.size();
    s = dest_->Close();
    dest_.reset();
  }

  last_elem_type_ = kEtFileFooter;
  RecordTick(statistics_, BLOB_DB_BLOB_FILE_BYTES_WRITTEN,
             BlobLogFooter::kSize);
  return s;
}

Status Writer::AddRecord(const Slice& key, const Slice& val,
                         uint64_t expiration, uint64_t* key_offset,
                         uint64_t* blob_offset) {
  assert(block_offset_ != 0);
  assert(last_elem_type_ == kEtFileHdr || last_elem_type_ == kEtRecord);

  std::string buf;
  ConstructBlobHeader(&buf, key, val, expiration);

  Status s = EmitPhysicalRecord(buf, key, val, key_offset, blob_offset);
  return s;
}

Status Writer::AddRecord(const Slice& key, const Slice& val,
                         uint64_t* key_offset, uint64_t* blob_offset) {
  assert(block_offset_ != 0);
  assert(last_elem_type_ == kEtFileHdr || last_elem_type_ == kEtRecord);

  std::string buf;
  ConstructBlobHeader(&buf, key, val, 0);

  Status s = EmitPhysicalRecord(buf, key, val, key_offset, blob_offset);
  return s;
}

void Writer::ConstructBlobHeader(std::string* buf, const Slice& key,
                                 const Slice& val, uint64_t expiration) {
  BlobLogRecord record;
  record.key = key;
  record.value = val;
  record.expiration = expiration;
  record.EncodeHeaderTo(buf);
}

Status Writer::EmitPhysicalRecord(const std::string& headerbuf,
                                  const Slice& key, const Slice& val,
                                  uint64_t* key_offset, uint64_t* blob_offset) {
  StopWatch write_sw(env_, statistics_, BLOB_DB_BLOB_FILE_WRITE_MICROS);
  Status s = dest_->Append(Slice(headerbuf));
  if (s.ok()) {
    s = dest_->Append(key);
  }
  if (s.ok()) {
    s = dest_->Append(val);
  }
  if (s.ok()) {
    s = dest_->Flush();
  }

  *key_offset = block_offset_ + BlobLogRecord::kHeaderSize;
  *blob_offset = *key_offset + key.size();
  block_offset_ = *blob_offset + val.size();
  last_elem_type_ = kEtRecord;
  RecordTick(statistics_, BLOB_DB_BLOB_FILE_BYTES_WRITTEN,
             BlobLogRecord::kHeaderSize + key.size() + val.size());
  return s;
}

}  // namespace blob_db
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
