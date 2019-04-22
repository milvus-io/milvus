// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/transactions/transaction_base.h"

#include "db/db_impl.h"
#include "db/column_family.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "util/string_util.h"

namespace rocksdb {

TransactionBaseImpl::TransactionBaseImpl(DB* db,
                                         const WriteOptions& write_options)
    : db_(db),
      dbimpl_(reinterpret_cast<DBImpl*>(db)),
      write_options_(write_options),
      cmp_(GetColumnFamilyUserComparator(db->DefaultColumnFamily())),
      start_time_(db_->GetEnv()->NowMicros()),
      write_batch_(cmp_, 0, true, 0),
      indexing_enabled_(true) {
  assert(dynamic_cast<DBImpl*>(db_) != nullptr);
  log_number_ = 0;
  if (dbimpl_->allow_2pc()) {
    WriteBatchInternal::InsertNoop(write_batch_.GetWriteBatch());
  }
}

TransactionBaseImpl::~TransactionBaseImpl() {
  // Release snapshot if snapshot is set
  SetSnapshotInternal(nullptr);
}

void TransactionBaseImpl::Clear() {
  save_points_.reset(nullptr);
  write_batch_.Clear();
  commit_time_batch_.Clear();
  tracked_keys_.clear();
  num_puts_ = 0;
  num_deletes_ = 0;
  num_merges_ = 0;

  if (dbimpl_->allow_2pc()) {
    WriteBatchInternal::InsertNoop(write_batch_.GetWriteBatch());
  }
}

void TransactionBaseImpl::Reinitialize(DB* db,
                                       const WriteOptions& write_options) {
  Clear();
  ClearSnapshot();
  id_ = 0;
  db_ = db;
  name_.clear();
  log_number_ = 0;
  write_options_ = write_options;
  start_time_ = db_->GetEnv()->NowMicros();
  indexing_enabled_ = true;
  cmp_ = GetColumnFamilyUserComparator(db_->DefaultColumnFamily());
}

void TransactionBaseImpl::SetSnapshot() {
  const Snapshot* snapshot = dbimpl_->GetSnapshotForWriteConflictBoundary();
  SetSnapshotInternal(snapshot);
}

void TransactionBaseImpl::SetSnapshotInternal(const Snapshot* snapshot) {
  // Set a custom deleter for the snapshot_ SharedPtr as the snapshot needs to
  // be released, not deleted when it is no longer referenced.
  snapshot_.reset(snapshot, std::bind(&TransactionBaseImpl::ReleaseSnapshot,
                                      this, std::placeholders::_1, db_));
  snapshot_needed_ = false;
  snapshot_notifier_ = nullptr;
}

void TransactionBaseImpl::SetSnapshotOnNextOperation(
    std::shared_ptr<TransactionNotifier> notifier) {
  snapshot_needed_ = true;
  snapshot_notifier_ = notifier;
}

void TransactionBaseImpl::SetSnapshotIfNeeded() {
  if (snapshot_needed_) {
    std::shared_ptr<TransactionNotifier> notifier = snapshot_notifier_;
    SetSnapshot();
    if (notifier != nullptr) {
      notifier->SnapshotCreated(GetSnapshot());
    }
  }
}

Status TransactionBaseImpl::TryLock(ColumnFamilyHandle* column_family,
                                    const SliceParts& key, bool read_only,
                                    bool exclusive, const bool do_validate,
                                    const bool assume_tracked) {
  size_t key_size = 0;
  for (int i = 0; i < key.num_parts; ++i) {
    key_size += key.parts[i].size();
  }

  std::string str;
  str.reserve(key_size);

  for (int i = 0; i < key.num_parts; ++i) {
    str.append(key.parts[i].data(), key.parts[i].size());
  }

  return TryLock(column_family, str, read_only, exclusive, do_validate,
                 assume_tracked);
}

void TransactionBaseImpl::SetSavePoint() {
  if (save_points_ == nullptr) {
    save_points_.reset(new std::stack<TransactionBaseImpl::SavePoint>());
  }
  save_points_->emplace(snapshot_, snapshot_needed_, snapshot_notifier_,
                        num_puts_, num_deletes_, num_merges_);
  write_batch_.SetSavePoint();
}

Status TransactionBaseImpl::RollbackToSavePoint() {
  if (save_points_ != nullptr && save_points_->size() > 0) {
    // Restore saved SavePoint
    TransactionBaseImpl::SavePoint& save_point = save_points_->top();
    snapshot_ = save_point.snapshot_;
    snapshot_needed_ = save_point.snapshot_needed_;
    snapshot_notifier_ = save_point.snapshot_notifier_;
    num_puts_ = save_point.num_puts_;
    num_deletes_ = save_point.num_deletes_;
    num_merges_ = save_point.num_merges_;

    // Rollback batch
    Status s = write_batch_.RollbackToSavePoint();
    assert(s.ok());

    // Rollback any keys that were tracked since the last savepoint
    const TransactionKeyMap& key_map = save_point.new_keys_;
    for (const auto& key_map_iter : key_map) {
      uint32_t column_family_id = key_map_iter.first;
      auto& keys = key_map_iter.second;

      auto& cf_tracked_keys = tracked_keys_[column_family_id];

      for (const auto& key_iter : keys) {
        const std::string& key = key_iter.first;
        uint32_t num_reads = key_iter.second.num_reads;
        uint32_t num_writes = key_iter.second.num_writes;

        auto tracked_keys_iter = cf_tracked_keys.find(key);
        assert(tracked_keys_iter != cf_tracked_keys.end());

        // Decrement the total reads/writes of this key by the number of
        // reads/writes done since the last SavePoint.
        if (num_reads > 0) {
          assert(tracked_keys_iter->second.num_reads >= num_reads);
          tracked_keys_iter->second.num_reads -= num_reads;
        }
        if (num_writes > 0) {
          assert(tracked_keys_iter->second.num_writes >= num_writes);
          tracked_keys_iter->second.num_writes -= num_writes;
        }
        if (tracked_keys_iter->second.num_reads == 0 &&
            tracked_keys_iter->second.num_writes == 0) {
          tracked_keys_[column_family_id].erase(tracked_keys_iter);
        }
      }
    }

    save_points_->pop();

    return s;
  } else {
    assert(write_batch_.RollbackToSavePoint().IsNotFound());
    return Status::NotFound();
  }
}

Status TransactionBaseImpl::PopSavePoint() {
  if (save_points_ == nullptr ||
      save_points_->empty()) {
    // No SavePoint yet.
    assert(write_batch_.PopSavePoint().IsNotFound());
    return Status::NotFound();
  }

  assert(!save_points_->empty());
  save_points_->pop();
  return write_batch_.PopSavePoint();
}

Status TransactionBaseImpl::Get(const ReadOptions& read_options,
                                ColumnFamilyHandle* column_family,
                                const Slice& key, std::string* value) {
  assert(value != nullptr);
  PinnableSlice pinnable_val(value);
  assert(!pinnable_val.IsPinned());
  auto s = Get(read_options, column_family, key, &pinnable_val);
  if (s.ok() && pinnable_val.IsPinned()) {
    value->assign(pinnable_val.data(), pinnable_val.size());
  }  // else value is already assigned
  return s;
}

Status TransactionBaseImpl::Get(const ReadOptions& read_options,
                                ColumnFamilyHandle* column_family,
                                const Slice& key, PinnableSlice* pinnable_val) {
  return write_batch_.GetFromBatchAndDB(db_, read_options, column_family, key,
                                        pinnable_val);
}

Status TransactionBaseImpl::GetForUpdate(const ReadOptions& read_options,
                                         ColumnFamilyHandle* column_family,
                                         const Slice& key, std::string* value,
                                         bool exclusive,
                                         const bool do_validate) {
  if (!do_validate && read_options.snapshot != nullptr) {
    return Status::InvalidArgument(
        "If do_validate is false then GetForUpdate with snapshot is not "
        "defined.");
  }
  Status s =
      TryLock(column_family, key, true /* read_only */, exclusive, do_validate);

  if (s.ok() && value != nullptr) {
    assert(value != nullptr);
    PinnableSlice pinnable_val(value);
    assert(!pinnable_val.IsPinned());
    s = Get(read_options, column_family, key, &pinnable_val);
    if (s.ok() && pinnable_val.IsPinned()) {
      value->assign(pinnable_val.data(), pinnable_val.size());
    }  // else value is already assigned
  }
  return s;
}

Status TransactionBaseImpl::GetForUpdate(const ReadOptions& read_options,
                                         ColumnFamilyHandle* column_family,
                                         const Slice& key,
                                         PinnableSlice* pinnable_val,
                                         bool exclusive,
                                         const bool do_validate) {
  if (!do_validate && read_options.snapshot != nullptr) {
    return Status::InvalidArgument(
        "If do_validate is false then GetForUpdate with snapshot is not "
        "defined.");
  }
  Status s =
      TryLock(column_family, key, true /* read_only */, exclusive, do_validate);

  if (s.ok() && pinnable_val != nullptr) {
    s = Get(read_options, column_family, key, pinnable_val);
  }
  return s;
}

std::vector<Status> TransactionBaseImpl::MultiGet(
    const ReadOptions& read_options,
    const std::vector<ColumnFamilyHandle*>& column_family,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  size_t num_keys = keys.size();
  values->resize(num_keys);

  std::vector<Status> stat_list(num_keys);
  for (size_t i = 0; i < num_keys; ++i) {
    std::string* value = values ? &(*values)[i] : nullptr;
    stat_list[i] = Get(read_options, column_family[i], keys[i], value);
  }

  return stat_list;
}

std::vector<Status> TransactionBaseImpl::MultiGetForUpdate(
    const ReadOptions& read_options,
    const std::vector<ColumnFamilyHandle*>& column_family,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  // Regardless of whether the MultiGet succeeded, track these keys.
  size_t num_keys = keys.size();
  values->resize(num_keys);

  // Lock all keys
  for (size_t i = 0; i < num_keys; ++i) {
    Status s = TryLock(column_family[i], keys[i], true /* read_only */,
                       true /* exclusive */);
    if (!s.ok()) {
      // Fail entire multiget if we cannot lock all keys
      return std::vector<Status>(num_keys, s);
    }
  }

  // TODO(agiardullo): optimize multiget?
  std::vector<Status> stat_list(num_keys);
  for (size_t i = 0; i < num_keys; ++i) {
    std::string* value = values ? &(*values)[i] : nullptr;
    stat_list[i] = Get(read_options, column_family[i], keys[i], value);
  }

  return stat_list;
}

Iterator* TransactionBaseImpl::GetIterator(const ReadOptions& read_options) {
  Iterator* db_iter = db_->NewIterator(read_options);
  assert(db_iter);

  return write_batch_.NewIteratorWithBase(db_iter);
}

Iterator* TransactionBaseImpl::GetIterator(const ReadOptions& read_options,
                                           ColumnFamilyHandle* column_family) {
  Iterator* db_iter = db_->NewIterator(read_options, column_family);
  assert(db_iter);

  return write_batch_.NewIteratorWithBase(column_family, db_iter);
}

Status TransactionBaseImpl::Put(ColumnFamilyHandle* column_family,
                                const Slice& key, const Slice& value,
                                const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  Status s = TryLock(column_family, key, false /* read_only */,
                     true /* exclusive */, do_validate, assume_tracked);

  if (s.ok()) {
    s = GetBatchForWrite()->Put(column_family, key, value);
    if (s.ok()) {
      num_puts_++;
    }
  }

  return s;
}

Status TransactionBaseImpl::Put(ColumnFamilyHandle* column_family,
                                const SliceParts& key, const SliceParts& value,
                                const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  Status s = TryLock(column_family, key, false /* read_only */,
                     true /* exclusive */, do_validate, assume_tracked);

  if (s.ok()) {
    s = GetBatchForWrite()->Put(column_family, key, value);
    if (s.ok()) {
      num_puts_++;
    }
  }

  return s;
}

Status TransactionBaseImpl::Merge(ColumnFamilyHandle* column_family,
                                  const Slice& key, const Slice& value,
                                  const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  Status s = TryLock(column_family, key, false /* read_only */,
                     true /* exclusive */, do_validate, assume_tracked);

  if (s.ok()) {
    s = GetBatchForWrite()->Merge(column_family, key, value);
    if (s.ok()) {
      num_merges_++;
    }
  }

  return s;
}

Status TransactionBaseImpl::Delete(ColumnFamilyHandle* column_family,
                                   const Slice& key,
                                   const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  Status s = TryLock(column_family, key, false /* read_only */,
                     true /* exclusive */, do_validate, assume_tracked);

  if (s.ok()) {
    s = GetBatchForWrite()->Delete(column_family, key);
    if (s.ok()) {
      num_deletes_++;
    }
  }

  return s;
}

Status TransactionBaseImpl::Delete(ColumnFamilyHandle* column_family,
                                   const SliceParts& key,
                                   const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  Status s = TryLock(column_family, key, false /* read_only */,
                     true /* exclusive */, do_validate, assume_tracked);

  if (s.ok()) {
    s = GetBatchForWrite()->Delete(column_family, key);
    if (s.ok()) {
      num_deletes_++;
    }
  }

  return s;
}

Status TransactionBaseImpl::SingleDelete(ColumnFamilyHandle* column_family,
                                         const Slice& key,
                                         const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  Status s = TryLock(column_family, key, false /* read_only */,
                     true /* exclusive */, do_validate, assume_tracked);

  if (s.ok()) {
    s = GetBatchForWrite()->SingleDelete(column_family, key);
    if (s.ok()) {
      num_deletes_++;
    }
  }

  return s;
}

Status TransactionBaseImpl::SingleDelete(ColumnFamilyHandle* column_family,
                                         const SliceParts& key,
                                         const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  Status s = TryLock(column_family, key, false /* read_only */,
                     true /* exclusive */, do_validate, assume_tracked);

  if (s.ok()) {
    s = GetBatchForWrite()->SingleDelete(column_family, key);
    if (s.ok()) {
      num_deletes_++;
    }
  }

  return s;
}

Status TransactionBaseImpl::PutUntracked(ColumnFamilyHandle* column_family,
                                         const Slice& key, const Slice& value) {
  Status s = TryLock(column_family, key, false /* read_only */,
                     true /* exclusive */, false /* do_validate */);

  if (s.ok()) {
    s = GetBatchForWrite()->Put(column_family, key, value);
    if (s.ok()) {
      num_puts_++;
    }
  }

  return s;
}

Status TransactionBaseImpl::PutUntracked(ColumnFamilyHandle* column_family,
                                         const SliceParts& key,
                                         const SliceParts& value) {
  Status s = TryLock(column_family, key, false /* read_only */,
                     true /* exclusive */, false /* do_validate */);

  if (s.ok()) {
    s = GetBatchForWrite()->Put(column_family, key, value);
    if (s.ok()) {
      num_puts_++;
    }
  }

  return s;
}

Status TransactionBaseImpl::MergeUntracked(ColumnFamilyHandle* column_family,
                                           const Slice& key,
                                           const Slice& value) {
  Status s = TryLock(column_family, key, false /* read_only */,
                     true /* exclusive */, false /* do_validate */);

  if (s.ok()) {
    s = GetBatchForWrite()->Merge(column_family, key, value);
    if (s.ok()) {
      num_merges_++;
    }
  }

  return s;
}

Status TransactionBaseImpl::DeleteUntracked(ColumnFamilyHandle* column_family,
                                            const Slice& key) {
  Status s = TryLock(column_family, key, false /* read_only */,
                     true /* exclusive */, false /* do_validate */);

  if (s.ok()) {
    s = GetBatchForWrite()->Delete(column_family, key);
    if (s.ok()) {
      num_deletes_++;
    }
  }

  return s;
}

Status TransactionBaseImpl::DeleteUntracked(ColumnFamilyHandle* column_family,
                                            const SliceParts& key) {
  Status s = TryLock(column_family, key, false /* read_only */,
                     true /* exclusive */, false /* do_validate */);

  if (s.ok()) {
    s = GetBatchForWrite()->Delete(column_family, key);
    if (s.ok()) {
      num_deletes_++;
    }
  }

  return s;
}

Status TransactionBaseImpl::SingleDeleteUntracked(
    ColumnFamilyHandle* column_family, const Slice& key) {
  Status s = TryLock(column_family, key, false /* read_only */,
                     true /* exclusive */, false /* do_validate */);

  if (s.ok()) {
    s = GetBatchForWrite()->SingleDelete(column_family, key);
    if (s.ok()) {
      num_deletes_++;
    }
  }

  return s;
}

void TransactionBaseImpl::PutLogData(const Slice& blob) {
  write_batch_.PutLogData(blob);
}

WriteBatchWithIndex* TransactionBaseImpl::GetWriteBatch() {
  return &write_batch_;
}

uint64_t TransactionBaseImpl::GetElapsedTime() const {
  return (db_->GetEnv()->NowMicros() - start_time_) / 1000;
}

uint64_t TransactionBaseImpl::GetNumPuts() const { return num_puts_; }

uint64_t TransactionBaseImpl::GetNumDeletes() const { return num_deletes_; }

uint64_t TransactionBaseImpl::GetNumMerges() const { return num_merges_; }

uint64_t TransactionBaseImpl::GetNumKeys() const {
  uint64_t count = 0;

  // sum up locked keys in all column families
  for (const auto& key_map_iter : tracked_keys_) {
    const auto& keys = key_map_iter.second;
    count += keys.size();
  }

  return count;
}

void TransactionBaseImpl::TrackKey(uint32_t cfh_id, const std::string& key,
                                   SequenceNumber seq, bool read_only,
                                   bool exclusive) {
  // Update map of all tracked keys for this transaction
  TrackKey(&tracked_keys_, cfh_id, key, seq, read_only, exclusive);

  if (save_points_ != nullptr && !save_points_->empty()) {
    // Update map of tracked keys in this SavePoint
    TrackKey(&save_points_->top().new_keys_, cfh_id, key, seq, read_only,
             exclusive);
  }
}

// Add a key to the given TransactionKeyMap
// seq for pessimistic transactions is the sequence number from which we know
// there has not been a concurrent update to the key.
void TransactionBaseImpl::TrackKey(TransactionKeyMap* key_map, uint32_t cfh_id,
                                   const std::string& key, SequenceNumber seq,
                                   bool read_only, bool exclusive) {
  auto& cf_key_map = (*key_map)[cfh_id];
  auto iter = cf_key_map.find(key);
  if (iter == cf_key_map.end()) {
    auto result = cf_key_map.insert({key, TransactionKeyMapInfo(seq)});
    iter = result.first;
  } else if (seq < iter->second.seq) {
    // Now tracking this key with an earlier sequence number
    iter->second.seq = seq;
  }
  // else we do not update the seq. The smaller the tracked seq, the stronger it
  // the guarantee since it implies from the seq onward there has not been a
  // concurrent update to the key. So we update the seq if it implies stronger
  // guarantees, i.e., if it is smaller than the existing trakced seq.

  if (read_only) {
    iter->second.num_reads++;
  } else {
    iter->second.num_writes++;
  }
  iter->second.exclusive |= exclusive;
}

std::unique_ptr<TransactionKeyMap>
TransactionBaseImpl::GetTrackedKeysSinceSavePoint() {
  if (save_points_ != nullptr && !save_points_->empty()) {
    // Examine the number of reads/writes performed on all keys written
    // since the last SavePoint and compare to the total number of reads/writes
    // for each key.
    TransactionKeyMap* result = new TransactionKeyMap();
    for (const auto& key_map_iter : save_points_->top().new_keys_) {
      uint32_t column_family_id = key_map_iter.first;
      auto& keys = key_map_iter.second;

      auto& cf_tracked_keys = tracked_keys_[column_family_id];

      for (const auto& key_iter : keys) {
        const std::string& key = key_iter.first;
        uint32_t num_reads = key_iter.second.num_reads;
        uint32_t num_writes = key_iter.second.num_writes;

        auto total_key_info = cf_tracked_keys.find(key);
        assert(total_key_info != cf_tracked_keys.end());
        assert(total_key_info->second.num_reads >= num_reads);
        assert(total_key_info->second.num_writes >= num_writes);

        if (total_key_info->second.num_reads == num_reads &&
            total_key_info->second.num_writes == num_writes) {
          // All the reads/writes to this key were done in the last savepoint.
          bool read_only = (num_writes == 0);
          TrackKey(result, column_family_id, key, key_iter.second.seq,
                   read_only, key_iter.second.exclusive);
        }
      }
    }
    return std::unique_ptr<TransactionKeyMap>(result);
  }

  // No SavePoint
  return nullptr;
}

// Gets the write batch that should be used for Put/Merge/Deletes.
//
// Returns either a WriteBatch or WriteBatchWithIndex depending on whether
// DisableIndexing() has been called.
WriteBatchBase* TransactionBaseImpl::GetBatchForWrite() {
  if (indexing_enabled_) {
    // Use WriteBatchWithIndex
    return &write_batch_;
  } else {
    // Don't use WriteBatchWithIndex. Return base WriteBatch.
    return write_batch_.GetWriteBatch();
  }
}

void TransactionBaseImpl::ReleaseSnapshot(const Snapshot* snapshot, DB* db) {
  if (snapshot != nullptr) {
    ROCKS_LOG_DETAILS(dbimpl_->immutable_db_options().info_log,
                      "ReleaseSnapshot %" PRIu64 " Set",
                      snapshot->GetSequenceNumber());
    db->ReleaseSnapshot(snapshot);
  }
}

void TransactionBaseImpl::UndoGetForUpdate(ColumnFamilyHandle* column_family,
                                           const Slice& key) {
  uint32_t column_family_id = GetColumnFamilyID(column_family);
  auto& cf_tracked_keys = tracked_keys_[column_family_id];
  std::string key_str = key.ToString();
  bool can_decrement = false;
  bool can_unlock __attribute__((__unused__)) = false;

  if (save_points_ != nullptr && !save_points_->empty()) {
    // Check if this key was fetched ForUpdate in this SavePoint
    auto& cf_savepoint_keys = save_points_->top().new_keys_[column_family_id];

    auto savepoint_iter = cf_savepoint_keys.find(key_str);
    if (savepoint_iter != cf_savepoint_keys.end()) {
      if (savepoint_iter->second.num_reads > 0) {
        savepoint_iter->second.num_reads--;
        can_decrement = true;

        if (savepoint_iter->second.num_reads == 0 &&
            savepoint_iter->second.num_writes == 0) {
          // No other GetForUpdates or write on this key in this SavePoint
          cf_savepoint_keys.erase(savepoint_iter);
          can_unlock = true;
        }
      }
    }
  } else {
    // No SavePoint set
    can_decrement = true;
    can_unlock = true;
  }

  // We can only decrement the read count for this key if we were able to
  // decrement the read count in the current SavePoint, OR if there is no
  // SavePoint set.
  if (can_decrement) {
    auto key_iter = cf_tracked_keys.find(key_str);

    if (key_iter != cf_tracked_keys.end()) {
      if (key_iter->second.num_reads > 0) {
        key_iter->second.num_reads--;

        if (key_iter->second.num_reads == 0 &&
            key_iter->second.num_writes == 0) {
          // No other GetForUpdates or writes on this key
          assert(can_unlock);
          cf_tracked_keys.erase(key_iter);
          UnlockGetForUpdate(column_family, key);
        }
      }
    }
  }
}

Status TransactionBaseImpl::RebuildFromWriteBatch(WriteBatch* src_batch) {
  struct IndexedWriteBatchBuilder : public WriteBatch::Handler {
    Transaction* txn_;
    DBImpl* db_;
    IndexedWriteBatchBuilder(Transaction* txn, DBImpl* db)
        : txn_(txn), db_(db) {
      assert(dynamic_cast<TransactionBaseImpl*>(txn_) != nullptr);
    }

    Status PutCF(uint32_t cf, const Slice& key, const Slice& val) override {
      return txn_->Put(db_->GetColumnFamilyHandle(cf), key, val);
    }

    Status DeleteCF(uint32_t cf, const Slice& key) override {
      return txn_->Delete(db_->GetColumnFamilyHandle(cf), key);
    }

    Status SingleDeleteCF(uint32_t cf, const Slice& key) override {
      return txn_->SingleDelete(db_->GetColumnFamilyHandle(cf), key);
    }

    Status MergeCF(uint32_t cf, const Slice& key, const Slice& val) override {
      return txn_->Merge(db_->GetColumnFamilyHandle(cf), key, val);
    }

    // this is used for reconstructing prepared transactions upon
    // recovery. there should not be any meta markers in the batches
    // we are processing.
    Status MarkBeginPrepare(bool) override { return Status::InvalidArgument(); }

    Status MarkEndPrepare(const Slice&) override {
      return Status::InvalidArgument();
    }

    Status MarkCommit(const Slice&) override {
      return Status::InvalidArgument();
    }

    Status MarkRollback(const Slice&) override {
      return Status::InvalidArgument();
    }
  };

  IndexedWriteBatchBuilder copycat(this, dbimpl_);
  return src_batch->Iterate(&copycat);
}

WriteBatch* TransactionBaseImpl::GetCommitTimeWriteBatch() {
  return &commit_time_batch_;
}
}  // namespace rocksdb

#endif  // ROCKSDB_LITE
