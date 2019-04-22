//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include "db/db_impl.h"
#include <vector>
#include <string>

namespace rocksdb {

class DBImplReadOnly : public DBImpl {
 public:
  DBImplReadOnly(const DBOptions& options, const std::string& dbname);
  virtual ~DBImplReadOnly();

  // Implementations of the DB interface
  using DB::Get;
  virtual Status Get(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     PinnableSlice* value) override;

  // TODO: Implement ReadOnly MultiGet?

  using DBImpl::NewIterator;
  virtual Iterator* NewIterator(const ReadOptions&,
                                ColumnFamilyHandle* column_family) override;

  virtual Status NewIterators(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_families,
      std::vector<Iterator*>* iterators) override;

  using DBImpl::Put;
  virtual Status Put(const WriteOptions& /*options*/,
                     ColumnFamilyHandle* /*column_family*/,
                     const Slice& /*key*/, const Slice& /*value*/) override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }
  using DBImpl::Merge;
  virtual Status Merge(const WriteOptions& /*options*/,
                       ColumnFamilyHandle* /*column_family*/,
                       const Slice& /*key*/, const Slice& /*value*/) override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }
  using DBImpl::Delete;
  virtual Status Delete(const WriteOptions& /*options*/,
                        ColumnFamilyHandle* /*column_family*/,
                        const Slice& /*key*/) override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }
  using DBImpl::SingleDelete;
  virtual Status SingleDelete(const WriteOptions& /*options*/,
                              ColumnFamilyHandle* /*column_family*/,
                              const Slice& /*key*/) override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }
  virtual Status Write(const WriteOptions& /*options*/,
                       WriteBatch* /*updates*/) override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }
  using DBImpl::CompactRange;
  virtual Status CompactRange(const CompactRangeOptions& /*options*/,
                              ColumnFamilyHandle* /*column_family*/,
                              const Slice* /*begin*/,
                              const Slice* /*end*/) override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }

  using DBImpl::CompactFiles;
  virtual Status CompactFiles(
      const CompactionOptions& /*compact_options*/,
      ColumnFamilyHandle* /*column_family*/,
      const std::vector<std::string>& /*input_file_names*/,
      const int /*output_level*/, const int /*output_path_id*/ = -1,
      std::vector<std::string>* const /*output_file_names*/ = nullptr,
      CompactionJobInfo* /*compaction_job_info*/ = nullptr) override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }

  virtual Status DisableFileDeletions() override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }

  virtual Status EnableFileDeletions(bool /*force*/) override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }
  virtual Status GetLiveFiles(std::vector<std::string>& ret,
                              uint64_t* manifest_file_size,
                              bool /*flush_memtable*/) override {
    return DBImpl::GetLiveFiles(ret, manifest_file_size,
                                false /* flush_memtable */);
  }

  using DBImpl::Flush;
  virtual Status Flush(const FlushOptions& /*options*/,
                       ColumnFamilyHandle* /*column_family*/) override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }

  using DBImpl::SyncWAL;
  virtual Status SyncWAL() override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }

  using DB::IngestExternalFile;
  virtual Status IngestExternalFile(
      ColumnFamilyHandle* /*column_family*/,
      const std::vector<std::string>& /*external_files*/,
      const IngestExternalFileOptions& /*ingestion_options*/) override {
    return Status::NotSupported("Not supported operation in read only mode.");
  }

 private:
  friend class DB;

  // No copying allowed
  DBImplReadOnly(const DBImplReadOnly&);
  void operator=(const DBImplReadOnly&);
};
}

#endif  // !ROCKSDB_LITE
