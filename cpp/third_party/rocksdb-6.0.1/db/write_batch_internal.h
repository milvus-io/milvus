//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <vector>
#include "db/write_thread.h"
#include "rocksdb/types.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "util/autovector.h"

namespace rocksdb {

class MemTable;
class FlushScheduler;
class ColumnFamilyData;

class ColumnFamilyMemTables {
 public:
  virtual ~ColumnFamilyMemTables() {}
  virtual bool Seek(uint32_t column_family_id) = 0;
  // returns true if the update to memtable should be ignored
  // (useful when recovering from log whose updates have already
  // been processed)
  virtual uint64_t GetLogNumber() const = 0;
  virtual MemTable* GetMemTable() const = 0;
  virtual ColumnFamilyHandle* GetColumnFamilyHandle() = 0;
  virtual ColumnFamilyData* current() { return nullptr; }
};

class ColumnFamilyMemTablesDefault : public ColumnFamilyMemTables {
 public:
  explicit ColumnFamilyMemTablesDefault(MemTable* mem)
      : ok_(false), mem_(mem) {}

  bool Seek(uint32_t column_family_id) override {
    ok_ = (column_family_id == 0);
    return ok_;
  }

  uint64_t GetLogNumber() const override { return 0; }

  MemTable* GetMemTable() const override {
    assert(ok_);
    return mem_;
  }

  ColumnFamilyHandle* GetColumnFamilyHandle() override { return nullptr; }

 private:
  bool ok_;
  MemTable* mem_;
};

// WriteBatchInternal provides static methods for manipulating a
// WriteBatch that we don't want in the public WriteBatch interface.
class WriteBatchInternal {
 public:

  // WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
  static const size_t kHeader = 12;

  // WriteBatch methods with column_family_id instead of ColumnFamilyHandle*
  static Status Put(WriteBatch* batch, uint32_t column_family_id,
                    const Slice& key, const Slice& value);

  static Status Put(WriteBatch* batch, uint32_t column_family_id,
                    const SliceParts& key, const SliceParts& value);

  static Status Delete(WriteBatch* batch, uint32_t column_family_id,
                       const SliceParts& key);

  static Status Delete(WriteBatch* batch, uint32_t column_family_id,
                       const Slice& key);

  static Status SingleDelete(WriteBatch* batch, uint32_t column_family_id,
                             const SliceParts& key);

  static Status SingleDelete(WriteBatch* batch, uint32_t column_family_id,
                             const Slice& key);

  static Status DeleteRange(WriteBatch* b, uint32_t column_family_id,
                            const Slice& begin_key, const Slice& end_key);

  static Status DeleteRange(WriteBatch* b, uint32_t column_family_id,
                            const SliceParts& begin_key,
                            const SliceParts& end_key);

  static Status Merge(WriteBatch* batch, uint32_t column_family_id,
                      const Slice& key, const Slice& value);

  static Status Merge(WriteBatch* batch, uint32_t column_family_id,
                      const SliceParts& key, const SliceParts& value);

  static Status PutBlobIndex(WriteBatch* batch, uint32_t column_family_id,
                             const Slice& key, const Slice& value);

  static Status MarkEndPrepare(WriteBatch* batch, const Slice& xid,
                               const bool write_after_commit = true,
                               const bool unprepared_batch = false);

  static Status MarkRollback(WriteBatch* batch, const Slice& xid);

  static Status MarkCommit(WriteBatch* batch, const Slice& xid);

  static Status InsertNoop(WriteBatch* batch);

  // Return the number of entries in the batch.
  static int Count(const WriteBatch* batch);

  // Set the count for the number of entries in the batch.
  static void SetCount(WriteBatch* batch, int n);

  // Return the sequence number for the start of this batch.
  static SequenceNumber Sequence(const WriteBatch* batch);

  // Store the specified number as the sequence number for the start of
  // this batch.
  static void SetSequence(WriteBatch* batch, SequenceNumber seq);

  // Returns the offset of the first entry in the batch.
  // This offset is only valid if the batch is not empty.
  static size_t GetFirstOffset(WriteBatch* batch);

  static Slice Contents(const WriteBatch* batch) {
    return Slice(batch->rep_);
  }

  static size_t ByteSize(const WriteBatch* batch) {
    return batch->rep_.size();
  }

  static Status SetContents(WriteBatch* batch, const Slice& contents);

  static Status CheckSlicePartsLength(const SliceParts& key,
                                      const SliceParts& value);

  // Inserts batches[i] into memtable, for i in 0..num_batches-1 inclusive.
  //
  // If ignore_missing_column_families == true. WriteBatch
  // referencing non-existing column family will be ignored.
  // If ignore_missing_column_families == false, processing of the
  // batches will be stopped if a reference is found to a non-existing
  // column family and InvalidArgument() will be returned.  The writes
  // in batches may be only partially applied at that point.
  //
  // If log_number is non-zero, the memtable will be updated only if
  // memtables->GetLogNumber() >= log_number.
  //
  // If flush_scheduler is non-null, it will be invoked if the memtable
  // should be flushed.
  //
  // Under concurrent use, the caller is responsible for making sure that
  // the memtables object itself is thread-local.
  static Status InsertInto(
      WriteThread::WriteGroup& write_group, SequenceNumber sequence,
      ColumnFamilyMemTables* memtables, FlushScheduler* flush_scheduler,
      bool ignore_missing_column_families = false, uint64_t log_number = 0,
      DB* db = nullptr, bool concurrent_memtable_writes = false,
      bool seq_per_batch = false, bool batch_per_txn = true);

  // Convenience form of InsertInto when you have only one batch
  // next_seq returns the seq after last sequence number used in MemTable insert
  static Status InsertInto(
      const WriteBatch* batch, ColumnFamilyMemTables* memtables,
      FlushScheduler* flush_scheduler,
      bool ignore_missing_column_families = false, uint64_t log_number = 0,
      DB* db = nullptr, bool concurrent_memtable_writes = false,
      SequenceNumber* next_seq = nullptr, bool* has_valid_writes = nullptr,
      bool seq_per_batch = false, bool batch_per_txn = true);

  static Status InsertInto(WriteThread::Writer* writer, SequenceNumber sequence,
                           ColumnFamilyMemTables* memtables,
                           FlushScheduler* flush_scheduler,
                           bool ignore_missing_column_families = false,
                           uint64_t log_number = 0, DB* db = nullptr,
                           bool concurrent_memtable_writes = false,
                           bool seq_per_batch = false, size_t batch_cnt = 0,
                           bool batch_per_txn = true);

  static Status Append(WriteBatch* dst, const WriteBatch* src,
                       const bool WAL_only = false);

  // Returns the byte size of appending a WriteBatch with ByteSize
  // leftByteSize and a WriteBatch with ByteSize rightByteSize
  static size_t AppendedByteSize(size_t leftByteSize, size_t rightByteSize);

  // This write batch includes the latest state that should be persisted. Such
  // state meant to be used only during recovery.
  static void SetAsLastestPersistentState(WriteBatch* b);
  static bool IsLatestPersistentState(const WriteBatch* b);
};

// LocalSavePoint is similar to a scope guard
class LocalSavePoint {
 public:
  explicit LocalSavePoint(WriteBatch* batch)
      : batch_(batch),
        savepoint_(batch->GetDataSize(), batch->Count(),
                   batch->content_flags_.load(std::memory_order_relaxed))
#ifndef NDEBUG
        ,
        committed_(false)
#endif
  {
  }

#ifndef NDEBUG
  ~LocalSavePoint() { assert(committed_); }
#endif
  Status commit() {
#ifndef NDEBUG
    committed_ = true;
#endif
    if (batch_->max_bytes_ && batch_->rep_.size() > batch_->max_bytes_) {
      batch_->rep_.resize(savepoint_.size);
      WriteBatchInternal::SetCount(batch_, savepoint_.count);
      batch_->content_flags_.store(savepoint_.content_flags,
                                   std::memory_order_relaxed);
      return Status::MemoryLimit();
    }
    return Status::OK();
  }

 private:
  WriteBatch* batch_;
  SavePoint savepoint_;
#ifndef NDEBUG
  bool committed_;
#endif
};

}  // namespace rocksdb
