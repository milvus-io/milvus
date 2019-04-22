//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE

#include "utilities/blob_db/blob_log_reader.h"

#include <algorithm>

#include "monitoring/statistics.h"
#include "util/file_reader_writer.h"
#include "util/stop_watch.h"

namespace rocksdb {
namespace blob_db {

Reader::Reader(unique_ptr<RandomAccessFileReader>&& file_reader, Env* env,
               Statistics* statistics)
    : file_(std::move(file_reader)),
      env_(env),
      statistics_(statistics),
      buffer_(),
      next_byte_(0) {}

Status Reader::ReadSlice(uint64_t size, Slice* slice, char* buf) {
  StopWatch read_sw(env_, statistics_, BLOB_DB_BLOB_FILE_READ_MICROS);
  Status s = file_->Read(next_byte_, static_cast<size_t>(size), slice, buf);
  next_byte_ += size;
  if (!s.ok()) {
    return s;
  }
  RecordTick(statistics_, BLOB_DB_BLOB_FILE_BYTES_READ, slice->size());
  if (slice->size() != size) {
    return Status::Corruption("EOF reached while reading record");
  }
  return s;
}

Status Reader::ReadHeader(BlobLogHeader* header) {
  assert(file_.get() != nullptr);
  assert(next_byte_ == 0);
  Status s = ReadSlice(BlobLogHeader::kSize, &buffer_, header_buf_);
  if (!s.ok()) {
    return s;
  }

  if (buffer_.size() != BlobLogHeader::kSize) {
    return Status::Corruption("EOF reached before file header");
  }

  return header->DecodeFrom(buffer_);
}

Status Reader::ReadRecord(BlobLogRecord* record, ReadLevel level,
                          uint64_t* blob_offset) {
  Status s = ReadSlice(BlobLogRecord::kHeaderSize, &buffer_, header_buf_);
  if (!s.ok()) {
    return s;
  }
  if (buffer_.size() != BlobLogRecord::kHeaderSize) {
    return Status::Corruption("EOF reached before record header");
  }

  s = record->DecodeHeaderFrom(buffer_);
  if (!s.ok()) {
    return s;
  }

  uint64_t kb_size = record->key_size + record->value_size;
  if (blob_offset != nullptr) {
    *blob_offset = next_byte_ + record->key_size;
  }

  switch (level) {
    case kReadHeader:
      next_byte_ += kb_size;
      break;

    case kReadHeaderKey:
      record->key_buf.reset(new char[record->key_size]);
      s = ReadSlice(record->key_size, &record->key, record->key_buf.get());
      next_byte_ += record->value_size;
      break;

    case kReadHeaderKeyBlob:
      record->key_buf.reset(new char[record->key_size]);
      s = ReadSlice(record->key_size, &record->key, record->key_buf.get());
      if (s.ok()) {
        record->value_buf.reset(new char[record->value_size]);
        s = ReadSlice(record->value_size, &record->value,
                      record->value_buf.get());
      }
      if (s.ok()) {
        s = record->CheckBlobCRC();
      }
      break;
  }
  return s;
}

}  // namespace blob_db
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
