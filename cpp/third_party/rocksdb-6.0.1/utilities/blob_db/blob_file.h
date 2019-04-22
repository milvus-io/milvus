//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once
#ifndef ROCKSDB_LITE

#include <atomic>
#include <memory>

#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "util/file_reader_writer.h"
#include "utilities/blob_db/blob_log_format.h"
#include "utilities/blob_db/blob_log_reader.h"
#include "utilities/blob_db/blob_log_writer.h"

namespace rocksdb {
namespace blob_db {

class BlobDBImpl;

class BlobFile {
  friend class BlobDBImpl;
  friend struct BlobFileComparator;
  friend struct BlobFileComparatorTTL;

 private:
  // access to parent
  const BlobDBImpl* parent_;

  // path to blob directory
  std::string path_to_dir_;

  // the id of the file.
  // the above 2 are created during file creation and never changed
  // after that
  uint64_t file_number_;

  // Info log.
  Logger* info_log_;

  // Column family id.
  uint32_t column_family_id_;

  // Compression type of blobs in the file
  CompressionType compression_;

  // If true, the keys in this file all has TTL. Otherwise all keys don't
  // have TTL.
  bool has_ttl_;

  // number of blobs in the file
  std::atomic<uint64_t> blob_count_;

  // size of the file
  std::atomic<uint64_t> file_size_;

  BlobLogHeader header_;

  // closed_ = true implies the file is no more mutable
  // no more blobs will be appended and the footer has been written out
  std::atomic<bool> closed_;

  // has a pass of garbage collection successfully finished on this file
  // obsolete_ still needs to do iterator/snapshot checks
  std::atomic<bool> obsolete_;

  // The last sequence number by the time the file marked as obsolete.
  // Data in this file is visible to a snapshot taken before the sequence.
  SequenceNumber obsolete_sequence_;

  ExpirationRange expiration_range_;

  // Sequential/Append writer for blobs
  std::shared_ptr<Writer> log_writer_;

  // random access file reader for GET calls
  std::shared_ptr<RandomAccessFileReader> ra_file_reader_;

  // This Read-Write mutex is per file specific and protects
  // all the datastructures
  mutable port::RWMutex mutex_;

  // time when the random access reader was last created.
  std::atomic<std::int64_t> last_access_;

  // last time file was fsync'd/fdatasyncd
  std::atomic<uint64_t> last_fsync_;

  bool header_valid_;

  bool footer_valid_;

  SequenceNumber garbage_collection_finish_sequence_;

 public:
  BlobFile();

  BlobFile(const BlobDBImpl* parent, const std::string& bdir, uint64_t fnum,
           Logger* info_log);

  ~BlobFile();

  uint32_t column_family_id() const;

  void SetColumnFamilyId(uint32_t cf_id) {
    column_family_id_ = cf_id;
  }

  // Returns log file's absolute pathname.
  std::string PathName() const;

  // Primary identifier for blob file.
  // once the file is created, this never changes
  uint64_t BlobFileNumber() const { return file_number_; }

  // the following functions are atomic, and don't need
  // read lock
  uint64_t BlobCount() const {
    return blob_count_.load(std::memory_order_acquire);
  }

  std::string DumpState() const;

  // if the file is not taking any more appends.
  bool Immutable() const { return closed_.load(); }

  // Mark the file as immutable.
  // REQUIRES: write lock held, or access from single thread (on DB open).
  void MarkImmutable() { closed_ = true; }

  // if the file has gone through GC and blobs have been relocated
  bool Obsolete() const {
    assert(Immutable() || !obsolete_.load());
    return obsolete_.load();
  }

  // Mark file as obsolete by garbage collection. The file is not visible to
  // snapshots with sequence greater or equal to the given sequence.
  void MarkObsolete(SequenceNumber sequence);

  SequenceNumber GetObsoleteSequence() const {
    assert(Obsolete());
    return obsolete_sequence_;
  }

  // we will assume this is atomic
  bool NeedsFsync(bool hard, uint64_t bytes_per_sync) const;

  Status Fsync();

  uint64_t GetFileSize() const {
    return file_size_.load(std::memory_order_acquire);
  }

  // All Get functions which are not atomic, will need ReadLock on the mutex

  ExpirationRange GetExpirationRange() const { return expiration_range_; }

  void ExtendExpirationRange(uint64_t expiration) {
    expiration_range_.first = std::min(expiration_range_.first, expiration);
    expiration_range_.second = std::max(expiration_range_.second, expiration);
  }

  bool HasTTL() const { return has_ttl_; }

  void SetHasTTL(bool has_ttl) { has_ttl_ = has_ttl; }

  CompressionType compression() const { return compression_; }

  void SetCompression(CompressionType c) {
    compression_ = c;
  }

  std::shared_ptr<Writer> GetWriter() const { return log_writer_; }

  // Read blob file header and footer. Return corruption if file header is
  // malform or incomplete. If footer is malform or incomplete, set
  // footer_valid_ to false and return Status::OK.
  Status ReadMetadata(Env* env, const EnvOptions& env_options);

  Status GetReader(Env* env, const EnvOptions& env_options,
                   std::shared_ptr<RandomAccessFileReader>* reader,
                   bool* fresh_open);

 private:
  std::shared_ptr<Reader> OpenRandomAccessReader(
      Env* env, const DBOptions& db_options,
      const EnvOptions& env_options) const;

  Status ReadFooter(BlobLogFooter* footer);

  Status WriteFooterAndCloseLocked();

  void CloseRandomAccessLocked();

  // this is used, when you are reading only the footer of a
  // previously closed file
  Status SetFromFooterLocked(const BlobLogFooter& footer);

  void set_expiration_range(const ExpirationRange& expiration_range) {
    expiration_range_ = expiration_range;
  }

  // The following functions are atomic, and don't need locks
  void SetFileSize(uint64_t fs) { file_size_ = fs; }

  void SetBlobCount(uint64_t bc) { blob_count_ = bc; }
};
}  // namespace blob_db
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
