//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "utilities/transactions/write_unprepared_txn_db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "util/cast_util.h"

namespace rocksdb {

// Instead of reconstructing a Transaction object, and calling rollback on it,
// we can be more efficient with RollbackRecoveredTransaction by skipping
// unnecessary steps (eg. updating CommitMap, reconstructing keyset)
Status WriteUnpreparedTxnDB::RollbackRecoveredTransaction(
    const DBImpl::RecoveredTransaction* rtxn) {
  // TODO(lth): Reduce duplicate code with WritePrepared rollback logic.
  assert(rtxn->unprepared_);
  auto cf_map_shared_ptr = WritePreparedTxnDB::GetCFHandleMap();
  auto cf_comp_map_shared_ptr = WritePreparedTxnDB::GetCFComparatorMap();
  WriteOptions w_options;
  // If we crash during recovery, we can just recalculate and rewrite the
  // rollback batch.
  w_options.disableWAL = true;

  class InvalidSnapshotReadCallback : public ReadCallback {
   public:
    InvalidSnapshotReadCallback(WritePreparedTxnDB* db, SequenceNumber snapshot,
                                SequenceNumber min_uncommitted)
        : db_(db), snapshot_(snapshot), min_uncommitted_(min_uncommitted) {}

    // Will be called to see if the seq number visible; if not it moves on to
    // the next seq number.
    inline bool IsVisible(SequenceNumber seq) override {
      // Becomes true if it cannot tell by comparing seq with snapshot seq since
      // the snapshot_ is not a real snapshot.
      bool released = false;
      auto ret = db_->IsInSnapshot(seq, snapshot_, min_uncommitted_, &released);
      assert(!released || ret);
      return ret;
    }

   private:
    WritePreparedTxnDB* db_;
    SequenceNumber snapshot_;
    SequenceNumber min_uncommitted_;
  };

  // Iterate starting with largest sequence number.
  for (auto it = rtxn->batches_.rbegin(); it != rtxn->batches_.rend(); it++) {
    auto last_visible_txn = it->first - 1;
    const auto& batch = it->second.batch_;
    WriteBatch rollback_batch;

    struct RollbackWriteBatchBuilder : public WriteBatch::Handler {
      DBImpl* db_;
      ReadOptions roptions;
      InvalidSnapshotReadCallback callback;
      WriteBatch* rollback_batch_;
      std::map<uint32_t, const Comparator*>& comparators_;
      std::map<uint32_t, ColumnFamilyHandle*>& handles_;
      using CFKeys = std::set<Slice, SetComparator>;
      std::map<uint32_t, CFKeys> keys_;
      bool rollback_merge_operands_;
      RollbackWriteBatchBuilder(
          DBImpl* db, WritePreparedTxnDB* wpt_db, SequenceNumber snap_seq,
          WriteBatch* dst_batch,
          std::map<uint32_t, const Comparator*>& comparators,
          std::map<uint32_t, ColumnFamilyHandle*>& handles,
          bool rollback_merge_operands)
          : db_(db),
            callback(wpt_db, snap_seq,
                     0),  // 0 disables min_uncommitted optimization
            rollback_batch_(dst_batch),
            comparators_(comparators),
            handles_(handles),
            rollback_merge_operands_(rollback_merge_operands) {}

      Status Rollback(uint32_t cf, const Slice& key) {
        Status s;
        CFKeys& cf_keys = keys_[cf];
        if (cf_keys.size() == 0) {  // just inserted
          auto cmp = comparators_[cf];
          keys_[cf] = CFKeys(SetComparator(cmp));
        }
        auto res = cf_keys.insert(key);
        if (res.second ==
            false) {  // second is false if a element already existed.
          return s;
        }

        PinnableSlice pinnable_val;
        bool not_used;
        auto cf_handle = handles_[cf];
        s = db_->GetImpl(roptions, cf_handle, key, &pinnable_val, &not_used,
                         &callback);
        assert(s.ok() || s.IsNotFound());
        if (s.ok()) {
          s = rollback_batch_->Put(cf_handle, key, pinnable_val);
          assert(s.ok());
        } else if (s.IsNotFound()) {
          // There has been no readable value before txn. By adding a delete we
          // make sure that there will be none afterwards either.
          s = rollback_batch_->Delete(cf_handle, key);
          assert(s.ok());
        } else {
          // Unexpected status. Return it to the user.
        }
        return s;
      }

      Status PutCF(uint32_t cf, const Slice& key,
                   const Slice& /*val*/) override {
        return Rollback(cf, key);
      }

      Status DeleteCF(uint32_t cf, const Slice& key) override {
        return Rollback(cf, key);
      }

      Status SingleDeleteCF(uint32_t cf, const Slice& key) override {
        return Rollback(cf, key);
      }

      Status MergeCF(uint32_t cf, const Slice& key,
                     const Slice& /*val*/) override {
        if (rollback_merge_operands_) {
          return Rollback(cf, key);
        } else {
          return Status::OK();
        }
      }

      // Recovered batches do not contain 2PC markers.
      Status MarkNoop(bool) override { return Status::InvalidArgument(); }
      Status MarkBeginPrepare(bool) override {
        return Status::InvalidArgument();
      }
      Status MarkEndPrepare(const Slice&) override {
        return Status::InvalidArgument();
      }
      Status MarkCommit(const Slice&) override {
        return Status::InvalidArgument();
      }
      Status MarkRollback(const Slice&) override {
        return Status::InvalidArgument();
      }
    } rollback_handler(db_impl_, this, last_visible_txn, &rollback_batch,
                       *cf_comp_map_shared_ptr.get(), *cf_map_shared_ptr.get(),
                       txn_db_options_.rollback_merge_operands);

    auto s = batch->Iterate(&rollback_handler);
    if (!s.ok()) {
      return s;
    }

    // The Rollback marker will be used as a batch separator
    WriteBatchInternal::MarkRollback(&rollback_batch, rtxn->name_);

    const uint64_t kNoLogRef = 0;
    const bool kDisableMemtable = true;
    const size_t kOneBatch = 1;
    uint64_t seq_used = kMaxSequenceNumber;
    s = db_impl_->WriteImpl(w_options, &rollback_batch, nullptr, nullptr,
                            kNoLogRef, !kDisableMemtable, &seq_used, kOneBatch);
    if (!s.ok()) {
      return s;
    }

    // If two_write_queues, we must manually release the sequence number to
    // readers.
    if (db_impl_->immutable_db_options().two_write_queues) {
      db_impl_->SetLastPublishedSequence(seq_used);
    }
  }

  return Status::OK();
}

Status WriteUnpreparedTxnDB::Initialize(
    const std::vector<size_t>& compaction_enabled_cf_indices,
    const std::vector<ColumnFamilyHandle*>& handles) {
  // TODO(lth): Reduce code duplication in this function.
  auto dbimpl = reinterpret_cast<DBImpl*>(GetRootDB());
  assert(dbimpl != nullptr);

  db_impl_->SetSnapshotChecker(new WritePreparedSnapshotChecker(this));
  // A callback to commit a single sub-batch
  class CommitSubBatchPreReleaseCallback : public PreReleaseCallback {
   public:
    explicit CommitSubBatchPreReleaseCallback(WritePreparedTxnDB* db)
        : db_(db) {}
    Status Callback(SequenceNumber commit_seq, bool is_mem_disabled) override {
#ifdef NDEBUG
      (void)is_mem_disabled;
#endif
      assert(!is_mem_disabled);
      db_->AddCommitted(commit_seq, commit_seq);
      return Status::OK();
    }

   private:
    WritePreparedTxnDB* db_;
  };
  db_impl_->SetRecoverableStatePreReleaseCallback(
      new CommitSubBatchPreReleaseCallback(this));

  // PessimisticTransactionDB::Initialize
  for (auto cf_ptr : handles) {
    AddColumnFamily(cf_ptr);
  }
  // Verify cf options
  for (auto handle : handles) {
    ColumnFamilyDescriptor cfd;
    Status s = handle->GetDescriptor(&cfd);
    if (!s.ok()) {
      return s;
    }
    s = VerifyCFOptions(cfd.options);
    if (!s.ok()) {
      return s;
    }
  }

  // Re-enable compaction for the column families that initially had
  // compaction enabled.
  std::vector<ColumnFamilyHandle*> compaction_enabled_cf_handles;
  compaction_enabled_cf_handles.reserve(compaction_enabled_cf_indices.size());
  for (auto index : compaction_enabled_cf_indices) {
    compaction_enabled_cf_handles.push_back(handles[index]);
  }

  Status s = EnableAutoCompaction(compaction_enabled_cf_handles);
  if (!s.ok()) {
    return s;
  }

  // create 'real' transactions from recovered shell transactions
  auto rtxns = dbimpl->recovered_transactions();
  for (auto rtxn : rtxns) {
    auto recovered_trx = rtxn.second;
    assert(recovered_trx);
    assert(recovered_trx->batches_.size() >= 1);
    assert(recovered_trx->name_.length());

    // We can only rollback transactions after AdvanceMaxEvictedSeq is called,
    // but AddPrepared must occur before AdvanceMaxEvictedSeq, which is why
    // two iterations is required.
    if (recovered_trx->unprepared_) {
      continue;
    }

    WriteOptions w_options;
    w_options.sync = true;
    TransactionOptions t_options;

    auto first_log_number = recovered_trx->batches_.begin()->second.log_number_;
    auto first_seq = recovered_trx->batches_.begin()->first;
    auto last_prepare_batch_cnt =
        recovered_trx->batches_.begin()->second.batch_cnt_;

    Transaction* real_trx = BeginTransaction(w_options, t_options, nullptr);
    assert(real_trx);
    auto wupt =
        static_cast_with_check<WriteUnpreparedTxn, Transaction>(real_trx);

    real_trx->SetLogNumber(first_log_number);
    real_trx->SetId(first_seq);
    s = real_trx->SetName(recovered_trx->name_);
    if (!s.ok()) {
      break;
    }
    wupt->prepare_batch_cnt_ = last_prepare_batch_cnt;

    for (auto batch : recovered_trx->batches_) {
      const auto& seq = batch.first;
      const auto& batch_info = batch.second;
      auto cnt = batch_info.batch_cnt_ ? batch_info.batch_cnt_ : 1;
      assert(batch_info.log_number_);

      for (size_t i = 0; i < cnt; i++) {
        AddPrepared(seq + i);
      }
      assert(wupt->unprep_seqs_.count(seq) == 0);
      wupt->unprep_seqs_[seq] = cnt;
      KeySetBuilder keyset_handler(wupt,
                                   txn_db_options_.rollback_merge_operands);
      s = batch_info.batch_->Iterate(&keyset_handler);
      assert(s.ok());
      if (!s.ok()) {
        break;
      }
    }

    wupt->write_batch_.Clear();
    WriteBatchInternal::InsertNoop(wupt->write_batch_.GetWriteBatch());

    real_trx->SetState(Transaction::PREPARED);
    if (!s.ok()) {
      break;
    }
  }

  SequenceNumber prev_max = max_evicted_seq_;
  SequenceNumber last_seq = db_impl_->GetLatestSequenceNumber();
  AdvanceMaxEvictedSeq(prev_max, last_seq);

  // Rollback unprepared transactions.
  for (auto rtxn : rtxns) {
    auto recovered_trx = rtxn.second;
    if (recovered_trx->unprepared_) {
      s = RollbackRecoveredTransaction(recovered_trx);
      if (!s.ok()) {
        return s;
      }
      continue;
    }
  }

  if (s.ok()) {
    dbimpl->DeleteAllRecoveredTransactions();
  }

  return s;
}

Transaction* WriteUnpreparedTxnDB::BeginTransaction(
    const WriteOptions& write_options, const TransactionOptions& txn_options,
    Transaction* old_txn) {
  if (old_txn != nullptr) {
    ReinitializeTransaction(old_txn, write_options, txn_options);
    return old_txn;
  } else {
    return new WriteUnpreparedTxn(this, write_options, txn_options);
  }
}

// Struct to hold ownership of snapshot and read callback for iterator cleanup.
struct WriteUnpreparedTxnDB::IteratorState {
  IteratorState(WritePreparedTxnDB* txn_db, SequenceNumber sequence,
                std::shared_ptr<ManagedSnapshot> s,
                SequenceNumber min_uncommitted, WriteUnpreparedTxn* txn)
      : callback(txn_db, sequence, min_uncommitted, txn), snapshot(s) {}

  WriteUnpreparedTxnReadCallback callback;
  std::shared_ptr<ManagedSnapshot> snapshot;
};

namespace {
static void CleanupWriteUnpreparedTxnDBIterator(void* arg1, void* /*arg2*/) {
  delete reinterpret_cast<WriteUnpreparedTxnDB::IteratorState*>(arg1);
}
}  // anonymous namespace

Iterator* WriteUnpreparedTxnDB::NewIterator(const ReadOptions& options,
                                            ColumnFamilyHandle* column_family,
                                            WriteUnpreparedTxn* txn) {
  // TODO(lth): Refactor so that this logic is shared with WritePrepared.
  constexpr bool ALLOW_BLOB = true;
  constexpr bool ALLOW_REFRESH = true;
  std::shared_ptr<ManagedSnapshot> own_snapshot = nullptr;
  SequenceNumber snapshot_seq;
  SequenceNumber min_uncommitted = 0;
  if (options.snapshot != nullptr) {
    snapshot_seq = options.snapshot->GetSequenceNumber();
    min_uncommitted =
        static_cast_with_check<const SnapshotImpl, const Snapshot>(
            options.snapshot)
            ->min_uncommitted_;
  } else {
    auto* snapshot = GetSnapshot();
    // We take a snapshot to make sure that the related data in the commit map
    // are not deleted.
    snapshot_seq = snapshot->GetSequenceNumber();
    min_uncommitted =
        static_cast_with_check<const SnapshotImpl, const Snapshot>(snapshot)
            ->min_uncommitted_;
    own_snapshot = std::make_shared<ManagedSnapshot>(db_impl_, snapshot);
  }
  assert(snapshot_seq != kMaxSequenceNumber);
  auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family)->cfd();
  auto* state =
      new IteratorState(this, snapshot_seq, own_snapshot, min_uncommitted, txn);
  auto* db_iter =
      db_impl_->NewIteratorImpl(options, cfd, snapshot_seq, &state->callback,
                                !ALLOW_BLOB, !ALLOW_REFRESH);
  db_iter->RegisterCleanup(CleanupWriteUnpreparedTxnDBIterator, state, nullptr);
  return db_iter;
}

Status KeySetBuilder::PutCF(uint32_t cf, const Slice& key,
                            const Slice& /*val*/) {
  txn_->UpdateWriteKeySet(cf, key);
  return Status::OK();
}

Status KeySetBuilder::DeleteCF(uint32_t cf, const Slice& key) {
  txn_->UpdateWriteKeySet(cf, key);
  return Status::OK();
}

Status KeySetBuilder::SingleDeleteCF(uint32_t cf, const Slice& key) {
  txn_->UpdateWriteKeySet(cf, key);
  return Status::OK();
}

Status KeySetBuilder::MergeCF(uint32_t cf, const Slice& key,
                              const Slice& /*val*/) {
  if (rollback_merge_operands_) {
    txn_->UpdateWriteKeySet(cf, key);
  }
  return Status::OK();
}

}  //  namespace rocksdb
#endif  // ROCKSDB_LITE
