//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"

#include <stdio.h>
#include "rocksdb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/file_reader_writer.h"
#include "util/util.h"

namespace rocksdb {
namespace log {

Reader::Reporter::~Reporter() {
}

Reader::Reader(std::shared_ptr<Logger> info_log,
               std::unique_ptr<SequentialFileReader>&& _file,
               Reporter* reporter, bool checksum, uint64_t log_num,
               bool retry_after_eof)
    : info_log_(info_log),
      file_(std::move(_file)),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      eof_(false),
      read_error_(false),
      eof_offset_(0),
      last_record_offset_(0),
      end_of_buffer_offset_(0),
      log_number_(log_num),
      recycled_(false),
      retry_after_eof_(retry_after_eof) {}

Reader::~Reader() {
  delete[] backing_store_;
}

// For kAbsoluteConsistency, on clean shutdown we don't expect any error
// in the log files.  For other modes, we can ignore only incomplete records
// in the last log file, which are presumably due to a write in progress
// during restart (or from log recycling).
//
// TODO krad: Evaluate if we need to move to a more strict mode where we
// restrict the inconsistency to only the last log
bool Reader::ReadRecord(Slice* record, std::string* scratch,
                        WALRecoveryMode wal_recovery_mode) {
  scratch->clear();
  record->clear();
  bool in_fragmented_record = false;
  // Record offset of the logical record that we're reading
  // 0 is a dummy value to make compilers happy
  uint64_t prospective_record_offset = 0;

  Slice fragment;
  while (true) {
    uint64_t physical_record_offset = end_of_buffer_offset_ - buffer_.size();
    size_t drop_size = 0;
    const unsigned int record_type = ReadPhysicalRecord(&fragment, &drop_size);
    switch (record_type) {
      case kFullType:
      case kRecyclableFullType:
        if (in_fragmented_record && !scratch->empty()) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          ReportCorruption(scratch->size(), "partial record without end(1)");
        }
        prospective_record_offset = physical_record_offset;
        scratch->clear();
        *record = fragment;
        last_record_offset_ = prospective_record_offset;
        return true;

      case kFirstType:
      case kRecyclableFirstType:
        if (in_fragmented_record && !scratch->empty()) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          ReportCorruption(scratch->size(), "partial record without end(2)");
        }
        prospective_record_offset = physical_record_offset;
        scratch->assign(fragment.data(), fragment.size());
        in_fragmented_record = true;
        break;

      case kMiddleType:
      case kRecyclableMiddleType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(1)");
        } else {
          scratch->append(fragment.data(), fragment.size());
        }
        break;

      case kLastType:
      case kRecyclableLastType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(2)");
        } else {
          scratch->append(fragment.data(), fragment.size());
          *record = Slice(*scratch);
          last_record_offset_ = prospective_record_offset;
          return true;
        }
        break;

      case kBadHeader:
        if (wal_recovery_mode == WALRecoveryMode::kAbsoluteConsistency) {
          // in clean shutdown we don't expect any error in the log files
          ReportCorruption(drop_size, "truncated header");
        }
        FALLTHROUGH_INTENDED;

      case kEof:
        if (in_fragmented_record) {
          if (wal_recovery_mode == WALRecoveryMode::kAbsoluteConsistency) {
            // in clean shutdown we don't expect any error in the log files
            ReportCorruption(scratch->size(), "error reading trailing data");
          }
          // This can be caused by the writer dying immediately after
          //  writing a physical record but before completing the next; don't
          //  treat it as a corruption, just ignore the entire logical record.
          scratch->clear();
        }
        return false;

      case kOldRecord:
        if (wal_recovery_mode != WALRecoveryMode::kSkipAnyCorruptedRecords) {
          // Treat a record from a previous instance of the log as EOF.
          if (in_fragmented_record) {
            if (wal_recovery_mode == WALRecoveryMode::kAbsoluteConsistency) {
              // in clean shutdown we don't expect any error in the log files
              ReportCorruption(scratch->size(), "error reading trailing data");
            }
            // This can be caused by the writer dying immediately after
            //  writing a physical record but before completing the next; don't
            //  treat it as a corruption, just ignore the entire logical record.
            scratch->clear();
          }
          return false;
        }
        FALLTHROUGH_INTENDED;

      case kBadRecord:
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;

      case kBadRecordLen:
      case kBadRecordChecksum:
        if (recycled_ &&
            wal_recovery_mode ==
                WALRecoveryMode::kTolerateCorruptedTailRecords) {
          scratch->clear();
          return false;
        }
        if (record_type == kBadRecordLen) {
          ReportCorruption(drop_size, "bad record length");
        } else {
          ReportCorruption(drop_size, "checksum mismatch");
        }
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;

      default: {
        char buf[40];
        snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
        ReportCorruption(
            (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
            buf);
        in_fragmented_record = false;
        scratch->clear();
        break;
      }
    }
  }
  return false;
}

uint64_t Reader::LastRecordOffset() {
  return last_record_offset_;
}

void Reader::UnmarkEOF() {
  if (read_error_) {
    return;
  }

  eof_ = false;

  // If retry_after_eof_ is true, we have to proceed to read anyway.
  if (!retry_after_eof_ && eof_offset_ == 0) {
    return;
  }

  // If the EOF was in the middle of a block (a partial block was read) we have
  // to read the rest of the block as ReadPhysicalRecord can only read full
  // blocks and expects the file position indicator to be aligned to the start
  // of a block.
  //
  //      consumed_bytes + buffer_size() + remaining == kBlockSize

  size_t consumed_bytes = eof_offset_ - buffer_.size();
  size_t remaining = kBlockSize - eof_offset_;

  // backing_store_ is used to concatenate what is left in buffer_ and
  // the remainder of the block. If buffer_ already uses backing_store_,
  // we just append the new data.
  if (buffer_.data() != backing_store_ + consumed_bytes) {
    // Buffer_ does not use backing_store_ for storage.
    // Copy what is left in buffer_ to backing_store.
    memmove(backing_store_ + consumed_bytes, buffer_.data(), buffer_.size());
  }

  Slice read_buffer;
  Status status = file_->Read(remaining, &read_buffer,
    backing_store_ + eof_offset_);

  size_t added = read_buffer.size();
  end_of_buffer_offset_ += added;

  if (!status.ok()) {
    if (added > 0) {
      ReportDrop(added, status);
    }

    read_error_ = true;
    return;
  }

  if (read_buffer.data() != backing_store_ + eof_offset_) {
    // Read did not write to backing_store_
    memmove(backing_store_ + eof_offset_, read_buffer.data(),
      read_buffer.size());
  }

  buffer_ = Slice(backing_store_ + consumed_bytes,
    eof_offset_ + added - consumed_bytes);

  if (added < remaining) {
    eof_ = true;
    eof_offset_ += added;
  } else {
    eof_offset_ = 0;
  }
}

void Reader::ReportCorruption(size_t bytes, const char* reason) {
  ReportDrop(bytes, Status::Corruption(reason));
}

void Reader::ReportDrop(size_t bytes, const Status& reason) {
  if (reporter_ != nullptr) {
    reporter_->Corruption(bytes, reason);
  }
}

bool Reader::ReadMore(size_t* drop_size, int *error) {
  if (!eof_ && !read_error_) {
    // Last read was a full read, so this is a trailer to skip
    buffer_.clear();
    Status status = file_->Read(kBlockSize, &buffer_, backing_store_);
    end_of_buffer_offset_ += buffer_.size();
    if (!status.ok()) {
      buffer_.clear();
      ReportDrop(kBlockSize, status);
      read_error_ = true;
      *error = kEof;
      return false;
    } else if (buffer_.size() < static_cast<size_t>(kBlockSize)) {
      eof_ = true;
      eof_offset_ = buffer_.size();
      TEST_SYNC_POINT("LogReader::ReadMore:FirstEOF");
    }
    return true;
  } else if (retry_after_eof_ && !read_error_) {
    UnmarkEOF();
    return !read_error_;
  } else {
    // Note that if buffer_ is non-empty, we have a truncated header at the
    //  end of the file, which can be caused by the writer crashing in the
    //  middle of writing the header. Unless explicitly requested we don't
    //  considering this an error, just report EOF.
    if (buffer_.size()) {
      *drop_size = buffer_.size();
      buffer_.clear();
      *error = kBadHeader;
      return false;
    }
    buffer_.clear();
    *error = kEof;
    return false;
  }
}

unsigned int Reader::ReadPhysicalRecord(Slice* result, size_t* drop_size) {
  while (true) {
    // We need at least the minimum header size
    if (buffer_.size() < static_cast<size_t>(kHeaderSize)) {
      // the default value of r is meaningless because ReadMore will overwrite
      // it if it returns false; in case it returns true, the return value will
      // not be used anyway
      int r = kEof;
      if (!ReadMore(drop_size, &r)) {
        return r;
      }
      continue;
    }

    // Parse the header
    const char* header = buffer_.data();
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
    const unsigned int type = header[6];
    const uint32_t length = a | (b << 8);
    int header_size = kHeaderSize;
    if (type >= kRecyclableFullType && type <= kRecyclableLastType) {
      if (end_of_buffer_offset_ - buffer_.size() == 0) {
        recycled_ = true;
      }
      header_size = kRecyclableHeaderSize;
      // We need enough for the larger header
      if (buffer_.size() < static_cast<size_t>(kRecyclableHeaderSize)) {
        int r = kEof;
        if (!ReadMore(drop_size, &r)) {
          return r;
        }
        continue;
      }
      const uint32_t log_num = DecodeFixed32(header + 7);
      if (log_num != log_number_) {
        return kOldRecord;
      }
    }
    if (header_size + length > buffer_.size()) {
      if (!retry_after_eof_) {
        *drop_size = buffer_.size();
        buffer_.clear();
        if (!eof_) {
          return kBadRecordLen;
        }
        // If the end of the file has been reached without reading |length|
        // bytes of payload, assume the writer died in the middle of writing the
        // record. Don't report a corruption unless requested.
        if (*drop_size) {
          return kBadHeader;
        }
      } else {
        int r = kEof;
        if (!ReadMore(drop_size, &r)) {
          return r;
        }
        continue;
      }
      return kEof;
    }

    if (type == kZeroType && length == 0) {
      // Skip zero length record without reporting any drops since
      // such records are produced by the mmap based writing code in
      // env_posix.cc that preallocates file regions.
      // NOTE: this should never happen in DB written by new RocksDB versions,
      // since we turn off mmap writes to manifest and log files
      buffer_.clear();
      return kBadRecord;
    }

    // Check crc
    if (checksum_) {
      uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
      uint32_t actual_crc = crc32c::Value(header + 6, length + header_size - 6);
      if (actual_crc != expected_crc) {
        // Drop the rest of the buffer since "length" itself may have
        // been corrupted and if we trust it, we could find some
        // fragment of a real log record that just happens to look
        // like a valid log record.
        *drop_size = buffer_.size();
        buffer_.clear();
        return kBadRecordChecksum;
      }
    }

    buffer_.remove_prefix(header_size + length);

    *result = Slice(header + header_size, length);
    return type;
  }
}

}  // namespace log
}  // namespace rocksdb
