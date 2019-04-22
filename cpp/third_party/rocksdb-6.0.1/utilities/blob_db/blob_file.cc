//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#ifndef ROCKSDB_LITE
#include "utilities/blob_db/blob_file.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <stdio.h>

#include <algorithm>
#include <limits>
#include <memory>

#include "db/column_family.h"
#include "db/db_impl.h"
#include "db/dbformat.h"
#include "util/filename.h"
#include "util/logging.h"
#include "utilities/blob_db/blob_db_impl.h"

namespace rocksdb {

namespace blob_db {

BlobFile::BlobFile()
    : parent_(nullptr),
      file_number_(0),
      info_log_(nullptr),
      column_family_id_(std::numeric_limits<uint32_t>::max()),
      compression_(kNoCompression),
      has_ttl_(false),
      blob_count_(0),
      file_size_(0),
      closed_(false),
      obsolete_(false),
      expiration_range_({0, 0}),
      last_access_(-1),
      last_fsync_(0),
      header_valid_(false),
      footer_valid_(false) {}

BlobFile::BlobFile(const BlobDBImpl* p, const std::string& bdir, uint64_t fn,
                   Logger* info_log)
    : parent_(p),
      path_to_dir_(bdir),
      file_number_(fn),
      info_log_(info_log),
      column_family_id_(std::numeric_limits<uint32_t>::max()),
      compression_(kNoCompression),
      has_ttl_(false),
      blob_count_(0),
      file_size_(0),
      closed_(false),
      obsolete_(false),
      expiration_range_({0, 0}),
      last_access_(-1),
      last_fsync_(0),
      header_valid_(false),
      footer_valid_(false) {}

BlobFile::~BlobFile() {
  if (obsolete_) {
    std::string pn(PathName());
    Status s = Env::Default()->DeleteFile(PathName());
    if (!s.ok()) {
      // ROCKS_LOG_INFO(db_options_.info_log,
      // "File could not be deleted %s", pn.c_str());
    }
  }
}

uint32_t BlobFile::column_family_id() const { return column_family_id_; }

std::string BlobFile::PathName() const {
  return BlobFileName(path_to_dir_, file_number_);
}

std::shared_ptr<Reader> BlobFile::OpenRandomAccessReader(
    Env* env, const DBOptions& db_options,
    const EnvOptions& env_options) const {
  constexpr size_t kReadaheadSize = 2 * 1024 * 1024;
  std::unique_ptr<RandomAccessFile> sfile;
  std::string path_name(PathName());
  Status s = env->NewRandomAccessFile(path_name, &sfile, env_options);
  if (!s.ok()) {
    // report something here.
    return nullptr;
  }
  sfile = NewReadaheadRandomAccessFile(std::move(sfile), kReadaheadSize);

  std::unique_ptr<RandomAccessFileReader> sfile_reader;
  sfile_reader.reset(new RandomAccessFileReader(std::move(sfile), path_name));

  std::shared_ptr<Reader> log_reader = std::make_shared<Reader>(
      std::move(sfile_reader), db_options.env, db_options.statistics.get());

  return log_reader;
}

std::string BlobFile::DumpState() const {
  char str[1000];
  snprintf(
      str, sizeof(str),
      "path: %s fn: %" PRIu64 " blob_count: %" PRIu64 " file_size: %" PRIu64
      " closed: %d obsolete: %d expiration_range: (%" PRIu64 ", %" PRIu64
      "), writer: %d reader: %d",
      path_to_dir_.c_str(), file_number_, blob_count_.load(), file_size_.load(),
      closed_.load(), obsolete_.load(), expiration_range_.first,
      expiration_range_.second, (!!log_writer_), (!!ra_file_reader_));
  return str;
}

void BlobFile::MarkObsolete(SequenceNumber sequence) {
  assert(Immutable());
  obsolete_sequence_ = sequence;
  obsolete_.store(true);
}

bool BlobFile::NeedsFsync(bool hard, uint64_t bytes_per_sync) const {
  assert(last_fsync_ <= file_size_);
  return (hard) ? file_size_ > last_fsync_
                : (file_size_ - last_fsync_) >= bytes_per_sync;
}

Status BlobFile::WriteFooterAndCloseLocked() {
  BlobLogFooter footer;
  footer.blob_count = blob_count_;
  if (HasTTL()) {
    footer.expiration_range = expiration_range_;
  }

  // this will close the file and reset the Writable File Pointer.
  Status s = log_writer_->AppendFooter(footer);
  if (s.ok()) {
    closed_ = true;
    file_size_ += BlobLogFooter::kSize;
  }
  // delete the sequential writer
  log_writer_.reset();
  return s;
}

Status BlobFile::ReadFooter(BlobLogFooter* bf) {
  if (file_size_ < (BlobLogHeader::kSize + BlobLogFooter::kSize)) {
    return Status::IOError("File does not have footer", PathName());
  }

  uint64_t footer_offset = file_size_ - BlobLogFooter::kSize;
  // assume that ra_file_reader_ is valid before we enter this
  assert(ra_file_reader_);

  Slice result;
  char scratch[BlobLogFooter::kSize + 10];
  Status s = ra_file_reader_->Read(footer_offset, BlobLogFooter::kSize, &result,
                                   scratch);
  if (!s.ok()) return s;
  if (result.size() != BlobLogFooter::kSize) {
    // should not happen
    return Status::IOError("EOF reached before footer");
  }

  s = bf->DecodeFrom(result);
  return s;
}

Status BlobFile::SetFromFooterLocked(const BlobLogFooter& footer) {
  // assume that file has been fully fsync'd
  last_fsync_.store(file_size_);
  blob_count_ = footer.blob_count;
  expiration_range_ = footer.expiration_range;
  closed_ = true;
  return Status::OK();
}

Status BlobFile::Fsync() {
  Status s;
  if (log_writer_.get()) {
    s = log_writer_->Sync();
    last_fsync_.store(file_size_.load());
  }
  return s;
}

void BlobFile::CloseRandomAccessLocked() {
  ra_file_reader_.reset();
  last_access_ = -1;
}

Status BlobFile::GetReader(Env* env, const EnvOptions& env_options,
                           std::shared_ptr<RandomAccessFileReader>* reader,
                           bool* fresh_open) {
  assert(reader != nullptr);
  assert(fresh_open != nullptr);
  *fresh_open = false;
  int64_t current_time = 0;
  env->GetCurrentTime(&current_time);
  last_access_.store(current_time);
  Status s;

  {
    ReadLock lockbfile_r(&mutex_);
    if (ra_file_reader_) {
      *reader = ra_file_reader_;
      return s;
    }
  }

  WriteLock lockbfile_w(&mutex_);
  // Double check.
  if (ra_file_reader_) {
    *reader = ra_file_reader_;
    return s;
  }

  std::unique_ptr<RandomAccessFile> rfile;
  s = env->NewRandomAccessFile(PathName(), &rfile, env_options);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(info_log_,
                    "Failed to open blob file for random-read: %s status: '%s'"
                    " exists: '%s'",
                    PathName().c_str(), s.ToString().c_str(),
                    env->FileExists(PathName()).ToString().c_str());
    return s;
  }

  ra_file_reader_ = std::make_shared<RandomAccessFileReader>(std::move(rfile),
                                                             PathName());
  *reader = ra_file_reader_;
  *fresh_open = true;
  return s;
}

Status BlobFile::ReadMetadata(Env* env, const EnvOptions& env_options) {
  assert(Immutable());
  // Get file size.
  uint64_t file_size = 0;
  Status s = env->GetFileSize(PathName(), &file_size);
  if (s.ok()) {
    file_size_ = file_size;
  } else {
    ROCKS_LOG_ERROR(info_log_,
                    "Failed to get size of blob file %" ROCKSDB_PRIszt
                    ", status: %s",
                    file_number_, s.ToString().c_str());
    return s;
  }
  if (file_size < BlobLogHeader::kSize) {
    ROCKS_LOG_ERROR(info_log_,
                    "Incomplete blob file blob file %" ROCKSDB_PRIszt
                    ", size: %" PRIu64,
                    file_number_, file_size);
    return Status::Corruption("Incomplete blob file header.");
  }

  // Create file reader.
  std::unique_ptr<RandomAccessFile> file;
  s = env->NewRandomAccessFile(PathName(), &file, env_options);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(info_log_,
                    "Failed to open blob file %" ROCKSDB_PRIszt ", status: %s",
                    file_number_, s.ToString().c_str());
    return s;
  }
  std::unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(std::move(file), PathName()));

  // Read file header.
  char header_buf[BlobLogHeader::kSize];
  Slice header_slice;
  s = file_reader->Read(0, BlobLogHeader::kSize, &header_slice, header_buf);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(info_log_,
                    "Failed to read header of blob file %" ROCKSDB_PRIszt
                    ", status: %s",
                    file_number_, s.ToString().c_str());
    return s;
  }
  BlobLogHeader header;
  s = header.DecodeFrom(header_slice);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(info_log_,
                    "Failed to decode header of blob file %" ROCKSDB_PRIszt
                    ", status: %s",
                    file_number_, s.ToString().c_str());
    return s;
  }
  column_family_id_ = header.column_family_id;
  compression_ = header.compression;
  has_ttl_ = header.has_ttl;
  if (has_ttl_) {
    expiration_range_ = header.expiration_range;
  }
  header_valid_ = true;

  // Read file footer.
  if (file_size_ < BlobLogHeader::kSize + BlobLogFooter::kSize) {
    // OK not to have footer.
    assert(!footer_valid_);
    return Status::OK();
  }
  char footer_buf[BlobLogFooter::kSize];
  Slice footer_slice;
  s = file_reader->Read(file_size - BlobLogFooter::kSize, BlobLogFooter::kSize,
                        &footer_slice, footer_buf);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(info_log_,
                    "Failed to read footer of blob file %" ROCKSDB_PRIszt
                    ", status: %s",
                    file_number_, s.ToString().c_str());
    return s;
  }
  BlobLogFooter footer;
  s = footer.DecodeFrom(footer_slice);
  if (!s.ok()) {
    // OK not to have footer.
    assert(!footer_valid_);
    return Status::OK();
  }
  blob_count_ = footer.blob_count;
  if (has_ttl_) {
    assert(header.expiration_range.first <= footer.expiration_range.first);
    assert(header.expiration_range.second >= footer.expiration_range.second);
    expiration_range_ = footer.expiration_range;
  }
  footer_valid_ = true;
  return Status::OK();
}

}  // namespace blob_db
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
