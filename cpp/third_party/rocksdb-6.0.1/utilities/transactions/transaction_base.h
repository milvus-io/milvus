// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include <stack>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "utilities/transactions/transaction_util.h"

namespace rocksdb {

class TransactionBaseImpl : public Transaction {
 public:
  TransactionBaseImpl(DB* db, const WriteOptions& write_options);

  virtual ~TransactionBaseImpl();

  // Remove pending operations queued in this transaction.
  virtual void Clear();

  void Reinitialize(DB* db, const WriteOptions& write_options);

  // Called before executing Put, Merge, Delete, and GetForUpdate.  If TryLock
  // returns non-OK, the Put/Merge/Delete/GetForUpdate will be failed.
  // do_validate will be false if called from PutUntracked, DeleteUntracked,
  // MergeUntracked, or GetForUpdate(do_validate=false)
  virtual Status TryLock(ColumnFamilyHandle* column_family, const Slice& key,
                         bool read_only, bool exclusive,
                         const bool do_validate = true,
                         const bool assume_tracked = false) = 0;

  void SetSavePoint() override;

  Status RollbackToSavePoint() override;
  
  Status PopSavePoint() override;

  using Transaction::Get;
  Status Get(const ReadOptions& options, ColumnFamilyHandle* column_family,
             const Slice& key, std::string* value) override;

  Status Get(const ReadOptions& options, ColumnFamilyHandle* column_family,
             const Slice& key, PinnableSlice* value) override;

  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override {
    return Get(options, db_->DefaultColumnFamily(), key, value);
  }

  using Transaction::GetForUpdate;
  Status GetForUpdate(const ReadOptions& options,
                      ColumnFamilyHandle* column_family, const Slice& key,
                      std::string* value, bool exclusive,
                      const bool do_validate) override;

  Status GetForUpdate(const ReadOptions& options,
                      ColumnFamilyHandle* column_family, const Slice& key,
                      PinnableSlice* pinnable_val, bool exclusive,
                      const bool do_validate) override;

  Status GetForUpdate(const ReadOptions& options, const Slice& key,
                      std::string* value, bool exclusive,
                      const bool do_validate) override {
    return GetForUpdate(options, db_->DefaultColumnFamily(), key, value,
                        exclusive, do_validate);
  }

  std::vector<Status> MultiGet(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_family,
      const std::vector<Slice>& keys,
      std::vector<std::string>* values) override;

  std::vector<Status> MultiGet(const ReadOptions& options,
                               const std::vector<Slice>& keys,
                               std::vector<std::string>* values) override {
    return MultiGet(options, std::vector<ColumnFamilyHandle*>(
                                 keys.size(), db_->DefaultColumnFamily()),
                    keys, values);
  }

  std::vector<Status> MultiGetForUpdate(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_family,
      const std::vector<Slice>& keys,
      std::vector<std::string>* values) override;

  std::vector<Status> MultiGetForUpdate(
      const ReadOptions& options, const std::vector<Slice>& keys,
      std::vector<std::string>* values) override {
    return MultiGetForUpdate(options,
                             std::vector<ColumnFamilyHandle*>(
                                 keys.size(), db_->DefaultColumnFamily()),
                             keys, values);
  }

  Iterator* GetIterator(const ReadOptions& read_options) override;
  Iterator* GetIterator(const ReadOptions& read_options,
                        ColumnFamilyHandle* column_family) override;

  Status Put(ColumnFamilyHandle* column_family, const Slice& key,
             const Slice& value, const bool assume_tracked = false) override;
  Status Put(const Slice& key, const Slice& value) override {
    return Put(nullptr, key, value);
  }

  Status Put(ColumnFamilyHandle* column_family, const SliceParts& key,
             const SliceParts& value,
             const bool assume_tracked = false) override;
  Status Put(const SliceParts& key, const SliceParts& value) override {
    return Put(nullptr, key, value);
  }

  Status Merge(ColumnFamilyHandle* column_family, const Slice& key,
               const Slice& value, const bool assume_tracked = false) override;
  Status Merge(const Slice& key, const Slice& value) override {
    return Merge(nullptr, key, value);
  }

  Status Delete(ColumnFamilyHandle* column_family, const Slice& key,
                const bool assume_tracked = false) override;
  Status Delete(const Slice& key) override { return Delete(nullptr, key); }
  Status Delete(ColumnFamilyHandle* column_family, const SliceParts& key,
                const bool assume_tracked = false) override;
  Status Delete(const SliceParts& key) override { return Delete(nullptr, key); }

  Status SingleDelete(ColumnFamilyHandle* column_family, const Slice& key,
                      const bool assume_tracked = false) override;
  Status SingleDelete(const Slice& key) override {
    return SingleDelete(nullptr, key);
  }
  Status SingleDelete(ColumnFamilyHandle* column_family, const SliceParts& key,
                      const bool assume_tracked = false) override;
  Status SingleDelete(const SliceParts& key) override {
    return SingleDelete(nullptr, key);
  }

  Status PutUntracked(ColumnFamilyHandle* column_family, const Slice& key,
                      const Slice& value) override;
  Status PutUntracked(const Slice& key, const Slice& value) override {
    return PutUntracked(nullptr, key, value);
  }

  Status PutUntracked(ColumnFamilyHandle* column_family, const SliceParts& key,
                      const SliceParts& value) override;
  Status PutUntracked(const SliceParts& key, const SliceParts& value) override {
    return PutUntracked(nullptr, key, value);
  }

  Status MergeUntracked(ColumnFamilyHandle* column_family, const Slice& key,
                        const Slice& value) override;
  Status MergeUntracked(const Slice& key, const Slice& value) override {
    return MergeUntracked(nullptr, key, value);
  }

  Status DeleteUntracked(ColumnFamilyHandle* column_family,
                         const Slice& key) override;
  Status DeleteUntracked(const Slice& key) override {
    return DeleteUntracked(nullptr, key);
  }
  Status DeleteUntracked(ColumnFamilyHandle* column_family,
                         const SliceParts& key) override;
  Status DeleteUntracked(const SliceParts& key) override {
    return DeleteUntracked(nullptr, key);
  }

  Status SingleDeleteUntracked(ColumnFamilyHandle* column_family,
                               const Slice& key) override;
  Status SingleDeleteUntracked(const Slice& key) override {
    return SingleDeleteUntracked(nullptr, key);
  }

  void PutLogData(const Slice& blob) override;

  WriteBatchWithIndex* GetWriteBatch() override;

  virtual void SetLockTimeout(int64_t /*timeout*/) override { /* Do nothing */
  }

  const Snapshot* GetSnapshot() const override {
    return snapshot_ ? snapshot_.get() : nullptr;
  }

  virtual void SetSnapshot() override;
  void SetSnapshotOnNextOperation(
      std::shared_ptr<TransactionNotifier> notifier = nullptr) override;

  void ClearSnapshot() override {
    snapshot_.reset();
    snapshot_needed_ = false;
    snapshot_notifier_ = nullptr;
  }

  void DisableIndexing() override { indexing_enabled_ = false; }

  void EnableIndexing() override { indexing_enabled_ = true; }

  uint64_t GetElapsedTime() const override;

  uint64_t GetNumPuts() const override;

  uint64_t GetNumDeletes() const override;

  uint64_t GetNumMerges() const override;

  uint64_t GetNumKeys() const override;

  void UndoGetForUpdate(ColumnFamilyHandle* column_family,
                        const Slice& key) override;
  void UndoGetForUpdate(const Slice& key) override {
    return UndoGetForUpdate(nullptr, key);
  };

  // Get list of keys in this transaction that must not have any conflicts
  // with writes in other transactions.
  const TransactionKeyMap& GetTrackedKeys() const { return tracked_keys_; }

  WriteOptions* GetWriteOptions() override { return &write_options_; }

  void SetWriteOptions(const WriteOptions& write_options) override {
    write_options_ = write_options;
  }

  // Used for memory management for snapshot_
  void ReleaseSnapshot(const Snapshot* snapshot, DB* db);

  // iterates over the given batch and makes the appropriate inserts.
  // used for rebuilding prepared transactions after recovery.
  virtual Status RebuildFromWriteBatch(WriteBatch* src_batch) override;

  WriteBatch* GetCommitTimeWriteBatch() override;

 protected:
  // Add a key to the list of tracked keys.
  //
  // seqno is the earliest seqno this key was involved with this transaction.
  // readonly should be set to true if no data was written for this key
  void TrackKey(uint32_t cfh_id, const std::string& key, SequenceNumber seqno,
                bool readonly, bool exclusive);

  // Helper function to add a key to the given TransactionKeyMap
  static void TrackKey(TransactionKeyMap* key_map, uint32_t cfh_id,
                       const std::string& key, SequenceNumber seqno,
                       bool readonly, bool exclusive);

  // Called when UndoGetForUpdate determines that this key can be unlocked.
  virtual void UnlockGetForUpdate(ColumnFamilyHandle* column_family,
                                  const Slice& key) = 0;

  std::unique_ptr<TransactionKeyMap> GetTrackedKeysSinceSavePoint();

  // Sets a snapshot if SetSnapshotOnNextOperation() has been called.
  void SetSnapshotIfNeeded();

  DB* db_;
  DBImpl* dbimpl_;

  WriteOptions write_options_;

  const Comparator* cmp_;

  // Stores that time the txn was constructed, in microseconds.
  uint64_t start_time_;

  // Stores the current snapshot that was set by SetSnapshot or null if
  // no snapshot is currently set.
  std::shared_ptr<const Snapshot> snapshot_;

  // Count of various operations pending in this transaction
  uint64_t num_puts_ = 0;
  uint64_t num_deletes_ = 0;
  uint64_t num_merges_ = 0;

  struct SavePoint {
    std::shared_ptr<const Snapshot> snapshot_;
    bool snapshot_needed_;
    std::shared_ptr<TransactionNotifier> snapshot_notifier_;
    uint64_t num_puts_;
    uint64_t num_deletes_;
    uint64_t num_merges_;

    // Record all keys tracked since the last savepoint
    TransactionKeyMap new_keys_;

    SavePoint(std::shared_ptr<const Snapshot> snapshot, bool snapshot_needed,
              std::shared_ptr<TransactionNotifier> snapshot_notifier,
              uint64_t num_puts, uint64_t num_deletes, uint64_t num_merges)
        : snapshot_(snapshot),
          snapshot_needed_(snapshot_needed),
          snapshot_notifier_(snapshot_notifier),
          num_puts_(num_puts),
          num_deletes_(num_deletes),
          num_merges_(num_merges) {}
  };

  // Records writes pending in this transaction
  WriteBatchWithIndex write_batch_;

 private:
  friend class WritePreparedTxn;
  // Extra data to be persisted with the commit. Note this is only used when
  // prepare phase is not skipped.
  WriteBatch commit_time_batch_;

  // Stack of the Snapshot saved at each save point.  Saved snapshots may be
  // nullptr if there was no snapshot at the time SetSavePoint() was called.
  std::unique_ptr<std::stack<TransactionBaseImpl::SavePoint>> save_points_;

  // Map from column_family_id to map of keys that are involved in this
  // transaction.
  // For Pessimistic Transactions this is the list of locked keys.
  // Optimistic Transactions will wait till commit time to do conflict checking.
  TransactionKeyMap tracked_keys_;

  // If true, future Put/Merge/Deletes will be indexed in the
  // WriteBatchWithIndex.
  // If false, future Put/Merge/Deletes will be inserted directly into the
  // underlying WriteBatch and not indexed in the WriteBatchWithIndex.
  bool indexing_enabled_;

  // SetSnapshotOnNextOperation() has been called and the snapshot has not yet
  // been reset.
  bool snapshot_needed_ = false;

  // SetSnapshotOnNextOperation() has been called and the caller would like
  // a notification through the TransactionNotifier interface
  std::shared_ptr<TransactionNotifier> snapshot_notifier_ = nullptr;

  Status TryLock(ColumnFamilyHandle* column_family, const SliceParts& key,
                 bool read_only, bool exclusive, const bool do_validate = true,
                 const bool assume_tracked = false);

  WriteBatchBase* GetBatchForWrite();
  void SetSnapshotInternal(const Snapshot* snapshot);
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
