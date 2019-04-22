//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "utilities/transactions/write_prepared_txn_db.h"

#include <inttypes.h>
#include <algorithm>
#include <string>
#include <unordered_set>
#include <vector>

#include "db/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction_db.h"
#include "util/cast_util.h"
#include "util/mutexlock.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "utilities/transactions/pessimistic_transaction.h"
#include "utilities/transactions/transaction_db_mutex_impl.h"

namespace rocksdb {

Status WritePreparedTxnDB::Initialize(
    const std::vector<size_t>& compaction_enabled_cf_indices,
    const std::vector<ColumnFamilyHandle*>& handles) {
  auto dbimpl = reinterpret_cast<DBImpl*>(GetRootDB());
  assert(dbimpl != nullptr);
  auto rtxns = dbimpl->recovered_transactions();
  for (auto rtxn : rtxns) {
    // There should only one batch for WritePrepared policy.
    assert(rtxn.second->batches_.size() == 1);
    const auto& seq = rtxn.second->batches_.begin()->first;
    const auto& batch_info = rtxn.second->batches_.begin()->second;
    auto cnt = batch_info.batch_cnt_ ? batch_info.batch_cnt_ : 1;
    for (size_t i = 0; i < cnt; i++) {
      AddPrepared(seq + i);
    }
  }
  SequenceNumber prev_max = max_evicted_seq_;
  SequenceNumber last_seq = db_impl_->GetLatestSequenceNumber();
  AdvanceMaxEvictedSeq(prev_max, last_seq);
  // Create a gap between max and the next snapshot. This simplifies the logic
  // in IsInSnapshot by not having to consider the special case of max ==
  // snapshot after recovery. This is tested in IsInSnapshotEmptyMapTest.
  if (last_seq) {
    db_impl_->versions_->SetLastAllocatedSequence(last_seq + 1);
    db_impl_->versions_->SetLastSequence(last_seq + 1);
    db_impl_->versions_->SetLastPublishedSequence(last_seq + 1);
  }

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

  auto s = PessimisticTransactionDB::Initialize(compaction_enabled_cf_indices,
                                                handles);
  return s;
}

Status WritePreparedTxnDB::VerifyCFOptions(
    const ColumnFamilyOptions& cf_options) {
  Status s = PessimisticTransactionDB::VerifyCFOptions(cf_options);
  if (!s.ok()) {
    return s;
  }
  if (!cf_options.memtable_factory->CanHandleDuplicatedKey()) {
    return Status::InvalidArgument(
        "memtable_factory->CanHandleDuplicatedKey() cannot be false with "
        "WritePrpeared transactions");
  }
  return Status::OK();
}

Transaction* WritePreparedTxnDB::BeginTransaction(
    const WriteOptions& write_options, const TransactionOptions& txn_options,
    Transaction* old_txn) {
  if (old_txn != nullptr) {
    ReinitializeTransaction(old_txn, write_options, txn_options);
    return old_txn;
  } else {
    return new WritePreparedTxn(this, write_options, txn_options);
  }
}

Status WritePreparedTxnDB::Write(
    const WriteOptions& opts,
    const TransactionDBWriteOptimizations& optimizations, WriteBatch* updates) {
  if (optimizations.skip_concurrency_control) {
    // Skip locking the rows
    const size_t UNKNOWN_BATCH_CNT = 0;
    const size_t ONE_BATCH_CNT = 1;
    const size_t batch_cnt = optimizations.skip_duplicate_key_check
                                 ? ONE_BATCH_CNT
                                 : UNKNOWN_BATCH_CNT;
    WritePreparedTxn* NO_TXN = nullptr;
    return WriteInternal(opts, updates, batch_cnt, NO_TXN);
  } else {
    // TODO(myabandeh): Make use of skip_duplicate_key_check hint
    // Fall back to unoptimized version
    return PessimisticTransactionDB::Write(opts, updates);
  }
}

Status WritePreparedTxnDB::WriteInternal(const WriteOptions& write_options_orig,
                                         WriteBatch* batch, size_t batch_cnt,
                                         WritePreparedTxn* txn) {
  ROCKS_LOG_DETAILS(db_impl_->immutable_db_options().info_log,
                    "CommitBatchInternal");
  if (batch->Count() == 0) {
    // Otherwise our 1 seq per batch logic will break since there is no seq
    // increased for this batch.
    return Status::OK();
  }
  if (batch_cnt == 0) {  // not provided, then compute it
    // TODO(myabandeh): add an option to allow user skipping this cost
    SubBatchCounter counter(*GetCFComparatorMap());
    auto s = batch->Iterate(&counter);
    assert(s.ok());
    batch_cnt = counter.BatchCount();
    WPRecordTick(TXN_DUPLICATE_KEY_OVERHEAD);
    ROCKS_LOG_DETAILS(info_log_, "Duplicate key overhead: %" PRIu64 " batches",
                      static_cast<uint64_t>(batch_cnt));
  }
  assert(batch_cnt);

  bool do_one_write = !db_impl_->immutable_db_options().two_write_queues;
  WriteOptions write_options(write_options_orig);
  bool sync = write_options.sync;
  if (!do_one_write) {
    // No need to sync on the first write
    write_options.sync = false;
  }
  // In the absence of Prepare markers, use Noop as a batch separator
  WriteBatchInternal::InsertNoop(batch);
  const bool DISABLE_MEMTABLE = true;
  const uint64_t no_log_ref = 0;
  uint64_t seq_used = kMaxSequenceNumber;
  const size_t ZERO_PREPARES = 0;
  // Since this is not 2pc, there is no need for AddPrepared but having it in
  // the PreReleaseCallback enables an optimization. Refer to
  // SmallestUnCommittedSeq for more details.
  AddPreparedCallback add_prepared_callback(
      this, batch_cnt, db_impl_->immutable_db_options().two_write_queues);
  WritePreparedCommitEntryPreReleaseCallback update_commit_map(
      this, db_impl_, kMaxSequenceNumber, ZERO_PREPARES, batch_cnt);
  PreReleaseCallback* pre_release_callback;
  if (do_one_write) {
    pre_release_callback = &update_commit_map;
  } else {
    pre_release_callback = &add_prepared_callback;
  }
  auto s = db_impl_->WriteImpl(write_options, batch, nullptr, nullptr,
                               no_log_ref, !DISABLE_MEMTABLE, &seq_used,
                               batch_cnt, pre_release_callback);
  assert(!s.ok() || seq_used != kMaxSequenceNumber);
  uint64_t prepare_seq = seq_used;
  if (txn != nullptr) {
    txn->SetId(prepare_seq);
  }
  if (!s.ok()) {
    return s;
  }
  if (do_one_write) {
    return s;
  }  // else do the 2nd write for commit
  // Set the original value of sync
  write_options.sync = sync;
  ROCKS_LOG_DETAILS(db_impl_->immutable_db_options().info_log,
                    "CommitBatchInternal 2nd write prepare_seq: %" PRIu64,
                    prepare_seq);
  // Commit the batch by writing an empty batch to the 2nd queue that will
  // release the commit sequence number to readers.
  const size_t ZERO_COMMITS = 0;
  WritePreparedCommitEntryPreReleaseCallback update_commit_map_with_prepare(
      this, db_impl_, prepare_seq, batch_cnt, ZERO_COMMITS);
  WriteBatch empty_batch;
  empty_batch.PutLogData(Slice());
  const size_t ONE_BATCH = 1;
  // In the absence of Prepare markers, use Noop as a batch separator
  WriteBatchInternal::InsertNoop(&empty_batch);
  s = db_impl_->WriteImpl(write_options, &empty_batch, nullptr, nullptr,
                          no_log_ref, DISABLE_MEMTABLE, &seq_used, ONE_BATCH,
                          &update_commit_map_with_prepare);
  assert(!s.ok() || seq_used != kMaxSequenceNumber);
  // Note RemovePrepared should be called after WriteImpl that publishsed the
  // seq. Otherwise SmallestUnCommittedSeq optimization breaks.
  RemovePrepared(prepare_seq, batch_cnt);
  return s;
}

Status WritePreparedTxnDB::Get(const ReadOptions& options,
                               ColumnFamilyHandle* column_family,
                               const Slice& key, PinnableSlice* value) {
  // We are fine with the latest committed value. This could be done by
  // specifying the snapshot as kMaxSequenceNumber.
  SequenceNumber seq = kMaxSequenceNumber;
  SequenceNumber min_uncommitted = 0;
  if (options.snapshot != nullptr) {
    seq = options.snapshot->GetSequenceNumber();
    min_uncommitted = static_cast_with_check<const SnapshotImpl, const Snapshot>(
                        options.snapshot)
                        ->min_uncommitted_;
  } else {
    min_uncommitted = SmallestUnCommittedSeq();
  }
  WritePreparedTxnReadCallback callback(this, seq, min_uncommitted);
  bool* dont_care = nullptr;
  // Note: no need to specify a snapshot for read options as no specific
  // snapshot is requested by the user.
  return db_impl_->GetImpl(options, column_family, key, value, dont_care,
                           &callback);
}

void WritePreparedTxnDB::UpdateCFComparatorMap(
    const std::vector<ColumnFamilyHandle*>& handles) {
  auto cf_map = new std::map<uint32_t, const Comparator*>();
  auto handle_map = new std::map<uint32_t, ColumnFamilyHandle*>();
  for (auto h : handles) {
    auto id = h->GetID();
    const Comparator* comparator = h->GetComparator();
    (*cf_map)[id] = comparator;
    if (id != 0) {
      (*handle_map)[id] = h;
    } else {
      // The pointer to the default cf handle in the handles will be deleted.
      // Use the pointer maintained by the db instead.
      (*handle_map)[id] = DefaultColumnFamily();
    }
  }
  cf_map_.reset(cf_map);
  handle_map_.reset(handle_map);
}

void WritePreparedTxnDB::UpdateCFComparatorMap(ColumnFamilyHandle* h) {
  auto old_cf_map_ptr = cf_map_.get();
  assert(old_cf_map_ptr);
  auto cf_map = new std::map<uint32_t, const Comparator*>(*old_cf_map_ptr);
  auto old_handle_map_ptr = handle_map_.get();
  assert(old_handle_map_ptr);
  auto handle_map =
      new std::map<uint32_t, ColumnFamilyHandle*>(*old_handle_map_ptr);
  auto id = h->GetID();
  const Comparator* comparator = h->GetComparator();
  (*cf_map)[id] = comparator;
  (*handle_map)[id] = h;
  cf_map_.reset(cf_map);
  handle_map_.reset(handle_map);
}


std::vector<Status> WritePreparedTxnDB::MultiGet(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_family,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  assert(values);
  size_t num_keys = keys.size();
  values->resize(num_keys);

  std::vector<Status> stat_list(num_keys);
  for (size_t i = 0; i < num_keys; ++i) {
    std::string* value = values ? &(*values)[i] : nullptr;
    stat_list[i] = this->Get(options, column_family[i], keys[i], value);
  }
  return stat_list;
}

// Struct to hold ownership of snapshot and read callback for iterator cleanup.
struct WritePreparedTxnDB::IteratorState {
  IteratorState(WritePreparedTxnDB* txn_db, SequenceNumber sequence,
                std::shared_ptr<ManagedSnapshot> s,
                SequenceNumber min_uncommitted)
      : callback(txn_db, sequence, min_uncommitted), snapshot(s) {}

  WritePreparedTxnReadCallback callback;
  std::shared_ptr<ManagedSnapshot> snapshot;
};

namespace {
static void CleanupWritePreparedTxnDBIterator(void* arg1, void* /*arg2*/) {
  delete reinterpret_cast<WritePreparedTxnDB::IteratorState*>(arg1);
}
}  // anonymous namespace

Iterator* WritePreparedTxnDB::NewIterator(const ReadOptions& options,
                                          ColumnFamilyHandle* column_family) {
  constexpr bool ALLOW_BLOB = true;
  constexpr bool ALLOW_REFRESH = true;
  std::shared_ptr<ManagedSnapshot> own_snapshot = nullptr;
  SequenceNumber snapshot_seq = kMaxSequenceNumber;
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
      new IteratorState(this, snapshot_seq, own_snapshot, min_uncommitted);
  auto* db_iter =
      db_impl_->NewIteratorImpl(options, cfd, snapshot_seq, &state->callback,
                                !ALLOW_BLOB, !ALLOW_REFRESH);
  db_iter->RegisterCleanup(CleanupWritePreparedTxnDBIterator, state, nullptr);
  return db_iter;
}

Status WritePreparedTxnDB::NewIterators(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    std::vector<Iterator*>* iterators) {
  constexpr bool ALLOW_BLOB = true;
  constexpr bool ALLOW_REFRESH = true;
  std::shared_ptr<ManagedSnapshot> own_snapshot = nullptr;
  SequenceNumber snapshot_seq = kMaxSequenceNumber;
  SequenceNumber min_uncommitted = 0;
  if (options.snapshot != nullptr) {
    snapshot_seq = options.snapshot->GetSequenceNumber();
    min_uncommitted = static_cast_with_check<const SnapshotImpl, const Snapshot>(
                        options.snapshot)
                        ->min_uncommitted_;
  } else {
    auto* snapshot = GetSnapshot();
    // We take a snapshot to make sure that the related data in the commit map
    // are not deleted.
    snapshot_seq = snapshot->GetSequenceNumber();
    own_snapshot = std::make_shared<ManagedSnapshot>(db_impl_, snapshot);
    min_uncommitted =
        static_cast_with_check<const SnapshotImpl, const Snapshot>(snapshot)
            ->min_uncommitted_;
  }
  iterators->clear();
  iterators->reserve(column_families.size());
  for (auto* column_family : column_families) {
    auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family)->cfd();
    auto* state =
        new IteratorState(this, snapshot_seq, own_snapshot, min_uncommitted);
    auto* db_iter =
        db_impl_->NewIteratorImpl(options, cfd, snapshot_seq, &state->callback,
                                  !ALLOW_BLOB, !ALLOW_REFRESH);
    db_iter->RegisterCleanup(CleanupWritePreparedTxnDBIterator, state, nullptr);
    iterators->push_back(db_iter);
  }
  return Status::OK();
}

void WritePreparedTxnDB::Init(const TransactionDBOptions& /* unused */) {
  // Adcance max_evicted_seq_ no more than 100 times before the cache wraps
  // around.
  INC_STEP_FOR_MAX_EVICTED =
      std::max(COMMIT_CACHE_SIZE / 100, static_cast<size_t>(1));
  snapshot_cache_ = std::unique_ptr<std::atomic<SequenceNumber>[]>(
      new std::atomic<SequenceNumber>[SNAPSHOT_CACHE_SIZE] {});
  commit_cache_ = std::unique_ptr<std::atomic<CommitEntry64b>[]>(
      new std::atomic<CommitEntry64b>[COMMIT_CACHE_SIZE] {});
}

void WritePreparedTxnDB::AddPrepared(uint64_t seq) {
  ROCKS_LOG_DETAILS(info_log_, "Txn %" PRIu64 " Prepareing with max %" PRIu64,
                    seq, max_evicted_seq_.load());
  WriteLock wl(&prepared_mutex_);
  if (UNLIKELY(seq <= max_evicted_seq_)) {
    // This should not happen in normal case
    ROCKS_LOG_ERROR(
        info_log_,
        "Added prepare_seq is not larger than max_evicted_seq_: %" PRIu64
        " <= %" PRIu64,
        seq, max_evicted_seq_.load());
    delayed_prepared_.insert(seq);
    delayed_prepared_empty_.store(false, std::memory_order_release);
  } else {
    prepared_txns_.push(seq);
  }
}

void WritePreparedTxnDB::AddCommitted(uint64_t prepare_seq, uint64_t commit_seq,
                                      uint8_t loop_cnt) {
  ROCKS_LOG_DETAILS(info_log_, "Txn %" PRIu64 " Committing with %" PRIu64,
                    prepare_seq, commit_seq);
  TEST_SYNC_POINT("WritePreparedTxnDB::AddCommitted:start");
  TEST_SYNC_POINT("WritePreparedTxnDB::AddCommitted:start:pause");
  auto indexed_seq = prepare_seq % COMMIT_CACHE_SIZE;
  CommitEntry64b evicted_64b;
  CommitEntry evicted;
  bool to_be_evicted = GetCommitEntry(indexed_seq, &evicted_64b, &evicted);
  if (LIKELY(to_be_evicted)) {
    assert(evicted.prep_seq != prepare_seq);
    auto prev_max = max_evicted_seq_.load(std::memory_order_acquire);
    ROCKS_LOG_DETAILS(info_log_,
                      "Evicting %" PRIu64 ",%" PRIu64 " with max %" PRIu64,
                      evicted.prep_seq, evicted.commit_seq, prev_max);
    if (prev_max < evicted.commit_seq) {
      auto last = db_impl_->GetLastPublishedSequence();  // could be 0
      SequenceNumber max_evicted_seq;
      if (LIKELY(evicted.commit_seq < last)) {
        assert(last > 0);
        // Inc max in larger steps to avoid frequent updates
        max_evicted_seq =
            std::min(evicted.commit_seq + INC_STEP_FOR_MAX_EVICTED, last - 1);
      } else {
        // legit when a commit entry in a write batch overwrite the previous one
        max_evicted_seq = evicted.commit_seq;
      }
      ROCKS_LOG_DETAILS(info_log_,
                        "%lu Evicting %" PRIu64 ",%" PRIu64 " with max %" PRIu64
                        " => %lu",
                        prepare_seq, evicted.prep_seq, evicted.commit_seq,
                        prev_max, max_evicted_seq);
      AdvanceMaxEvictedSeq(prev_max, max_evicted_seq);
    }
    // After each eviction from commit cache, check if the commit entry should
    // be kept around because it overlaps with a live snapshot.
    CheckAgainstSnapshots(evicted);
    if (UNLIKELY(!delayed_prepared_empty_.load(std::memory_order_acquire))) {
      WriteLock wl(&prepared_mutex_);
      for (auto dp : delayed_prepared_) {
        if (dp == evicted.prep_seq) {
          // This is a rare case that txn is committed but prepared_txns_ is not
          // cleaned up yet. Refer to delayed_prepared_commits_ definition for
          // why it should be kept updated.
          delayed_prepared_commits_[evicted.prep_seq] = evicted.commit_seq;
          ROCKS_LOG_DEBUG(info_log_,
                          "delayed_prepared_commits_[%" PRIu64 "]=%" PRIu64,
                          evicted.prep_seq, evicted.commit_seq);
          break;
        }
      }
    }
  }
  bool succ =
      ExchangeCommitEntry(indexed_seq, evicted_64b, {prepare_seq, commit_seq});
  if (UNLIKELY(!succ)) {
    ROCKS_LOG_ERROR(info_log_,
                    "ExchangeCommitEntry failed on [%" PRIu64 "] %" PRIu64
                    ",%" PRIu64 " retrying...",
                    indexed_seq, prepare_seq, commit_seq);
    // A very rare event, in which the commit entry is updated before we do.
    // Here we apply a very simple solution of retrying.
    if (loop_cnt > 100) {
      throw std::runtime_error("Infinite loop in AddCommitted!");
    }
    AddCommitted(prepare_seq, commit_seq, ++loop_cnt);
    return;
  }
  TEST_SYNC_POINT("WritePreparedTxnDB::AddCommitted:end");
  TEST_SYNC_POINT("WritePreparedTxnDB::AddCommitted:end:pause");
}

void WritePreparedTxnDB::RemovePrepared(const uint64_t prepare_seq,
                                        const size_t batch_cnt) {
  TEST_SYNC_POINT_CALLBACK(
      "RemovePrepared:Start",
      const_cast<void*>(reinterpret_cast<const void*>(&prepare_seq)));
  TEST_SYNC_POINT("WritePreparedTxnDB::RemovePrepared:pause");
  TEST_SYNC_POINT("WritePreparedTxnDB::RemovePrepared:resume");
  ROCKS_LOG_DETAILS(info_log_,
                    "RemovePrepared %" PRIu64 " cnt: %" ROCKSDB_PRIszt,
                    prepare_seq, batch_cnt);
  WriteLock wl(&prepared_mutex_);
  for (size_t i = 0; i < batch_cnt; i++) {
    prepared_txns_.erase(prepare_seq + i);
    bool was_empty = delayed_prepared_.empty();
    if (!was_empty) {
      delayed_prepared_.erase(prepare_seq + i);
      auto it = delayed_prepared_commits_.find(prepare_seq + i);
      if (it != delayed_prepared_commits_.end()) {
        ROCKS_LOG_DETAILS(info_log_, "delayed_prepared_commits_.erase %" PRIu64,
                          prepare_seq + i);
        delayed_prepared_commits_.erase(it);
      }
      bool is_empty = delayed_prepared_.empty();
      if (was_empty != is_empty) {
        delayed_prepared_empty_.store(is_empty, std::memory_order_release);
      }
    }
  }
}

bool WritePreparedTxnDB::GetCommitEntry(const uint64_t indexed_seq,
                                        CommitEntry64b* entry_64b,
                                        CommitEntry* entry) const {
  *entry_64b = commit_cache_[static_cast<size_t>(indexed_seq)].load(std::memory_order_acquire);
  bool valid = entry_64b->Parse(indexed_seq, entry, FORMAT);
  return valid;
}

bool WritePreparedTxnDB::AddCommitEntry(const uint64_t indexed_seq,
                                        const CommitEntry& new_entry,
                                        CommitEntry* evicted_entry) {
  CommitEntry64b new_entry_64b(new_entry, FORMAT);
  CommitEntry64b evicted_entry_64b = commit_cache_[static_cast<size_t>(indexed_seq)].exchange(
      new_entry_64b, std::memory_order_acq_rel);
  bool valid = evicted_entry_64b.Parse(indexed_seq, evicted_entry, FORMAT);
  return valid;
}

bool WritePreparedTxnDB::ExchangeCommitEntry(const uint64_t indexed_seq,
                                             CommitEntry64b& expected_entry_64b,
                                             const CommitEntry& new_entry) {
  auto& atomic_entry = commit_cache_[static_cast<size_t>(indexed_seq)];
  CommitEntry64b new_entry_64b(new_entry, FORMAT);
  bool succ = atomic_entry.compare_exchange_strong(
      expected_entry_64b, new_entry_64b, std::memory_order_acq_rel,
      std::memory_order_acquire);
  return succ;
}

void WritePreparedTxnDB::AdvanceMaxEvictedSeq(const SequenceNumber& prev_max,
                                              const SequenceNumber& new_max) {
  ROCKS_LOG_DETAILS(info_log_,
                    "AdvanceMaxEvictedSeq overhead %" PRIu64 " => %" PRIu64,
                    prev_max, new_max);
  // Declare the intention before getting snapshot from the DB. This helps a
  // concurrent GetSnapshot to wait to catch up with future_max_evicted_seq_ if
  // it has not already. Otherwise the new snapshot is when we ask DB for
  // snapshots smaller than future max.
  auto updated_future_max = prev_max;
  while (updated_future_max < new_max &&
         !future_max_evicted_seq_.compare_exchange_weak(
             updated_future_max, new_max, std::memory_order_acq_rel,
             std::memory_order_relaxed)) {
  };
  // When max_evicted_seq_ advances, move older entries from prepared_txns_
  // to delayed_prepared_. This guarantees that if a seq is lower than max,
  // then it is not in prepared_txns_ ans save an expensive, synchronized
  // lookup from a shared set. delayed_prepared_ is expected to be empty in
  // normal cases.
  {
    WriteLock wl(&prepared_mutex_);
    ROCKS_LOG_DETAILS(
        info_log_,
        "AdvanceMaxEvictedSeq prepared_txns_.empty() %d top: %" PRIu64,
        prepared_txns_.empty(),
        prepared_txns_.empty() ? 0 : prepared_txns_.top());
    while (!prepared_txns_.empty() && prepared_txns_.top() <= new_max) {
      auto to_be_popped = prepared_txns_.top();
      delayed_prepared_.insert(to_be_popped);
      ROCKS_LOG_WARN(info_log_,
                     "prepared_mutex_ overhead %" PRIu64 " (prep=%" PRIu64
                     " new_max=%" PRIu64 " oldmax=%" PRIu64,
                     static_cast<uint64_t>(delayed_prepared_.size()),
                     to_be_popped, new_max, prev_max);
      prepared_txns_.pop();
      delayed_prepared_empty_.store(false, std::memory_order_release);
    }
  }

  // With each change to max_evicted_seq_ fetch the live snapshots behind it.
  // We use max as the version of snapshots to identify how fresh are the
  // snapshot list. This works because the snapshots are between 0 and
  // max, so the larger the max, the more complete they are.
  SequenceNumber new_snapshots_version = new_max;
  std::vector<SequenceNumber> snapshots;
  bool update_snapshots = false;
  if (new_snapshots_version > snapshots_version_) {
    // This is to avoid updating the snapshots_ if it already updated
    // with a more recent vesion by a concrrent thread
    update_snapshots = true;
    // We only care about snapshots lower then max
    snapshots = GetSnapshotListFromDB(new_max);
  }
  if (update_snapshots) {
    UpdateSnapshots(snapshots, new_snapshots_version);
    if (!snapshots.empty()) {
      WriteLock wl(&old_commit_map_mutex_);
      for (auto snap : snapshots) {
        // This allows IsInSnapshot to tell apart the reads from in valid
        // snapshots from the reads from committed values in valid snapshots.
        old_commit_map_[snap];
      }
      old_commit_map_empty_.store(false, std::memory_order_release);
    }
  }
  auto updated_prev_max = prev_max;
  while (updated_prev_max < new_max &&
         !max_evicted_seq_.compare_exchange_weak(updated_prev_max, new_max,
                                                 std::memory_order_acq_rel,
                                                 std::memory_order_relaxed)) {
  };
}

const Snapshot* WritePreparedTxnDB::GetSnapshot() {
  const bool kForWWConflictCheck = true;
  return GetSnapshotInternal(!kForWWConflictCheck);
}

SnapshotImpl* WritePreparedTxnDB::GetSnapshotInternal(
    bool for_ww_conflict_check) {
  // Note: for this optimization setting the last sequence number and obtaining
  // the smallest uncommitted seq should be done atomically. However to avoid
  // the mutex overhead, we call SmallestUnCommittedSeq BEFORE taking the
  // snapshot. Since we always updated the list of unprepared seq (via
  // AddPrepared) AFTER the last sequence is updated, this guarantees that the
  // smallest uncommitted seq that we pair with the snapshot is smaller or equal
  // the value that would be obtained otherwise atomically. That is ok since
  // this optimization works as long as min_uncommitted is less than or equal
  // than the smallest uncommitted seq when the snapshot was taken.
  auto min_uncommitted = WritePreparedTxnDB::SmallestUnCommittedSeq();
  SnapshotImpl* snap_impl = db_impl_->GetSnapshotImpl(for_ww_conflict_check);
  assert(snap_impl);
  SequenceNumber snap_seq = snap_impl->GetSequenceNumber();
  // Note: Check against future_max_evicted_seq_ (in contrast with
  // max_evicted_seq_) in case there is a concurrent AdvanceMaxEvictedSeq.
  if (UNLIKELY(snap_seq != 0 && snap_seq <= future_max_evicted_seq_)) {
    // There is a very rare case in which the commit entry evicts another commit
    // entry that is not published yet thus advancing max evicted seq beyond the
    // last published seq. This case is not likely in real-world setup so we
    // handle it with a few retries.
    size_t retry = 0;
    SequenceNumber max;
    while ((max = future_max_evicted_seq_.load()) != 0 &&
           snap_impl->GetSequenceNumber() <= max && retry < 100) {
      ROCKS_LOG_WARN(info_log_,
                     "GetSnapshot snap: %" PRIu64 " max: %" PRIu64
                     " retry %" ROCKSDB_PRIszt,
                     snap_impl->GetSequenceNumber(), max, retry);
      ReleaseSnapshot(snap_impl);
      // Wait for last visible seq to catch up with max, and also go beyond it
      // by one.
      AdvanceSeqByOne();
      snap_impl = db_impl_->GetSnapshotImpl(for_ww_conflict_check);
      assert(snap_impl);
      retry++;
    }
    assert(snap_impl->GetSequenceNumber() > max);
    if (snap_impl->GetSequenceNumber() <= max) {
      throw std::runtime_error(
          "Snapshot seq " + ToString(snap_impl->GetSequenceNumber()) +
          " after " + ToString(retry) +
          " retries is still less than futre_max_evicted_seq_" + ToString(max));
    }
  }
  EnhanceSnapshot(snap_impl, min_uncommitted);
  ROCKS_LOG_DETAILS(
      db_impl_->immutable_db_options().info_log,
      "GetSnapshot %" PRIu64 " ww:%" PRIi32 " min_uncommitted: %" PRIu64,
      snap_impl->GetSequenceNumber(), for_ww_conflict_check, min_uncommitted);
  return snap_impl;
}

void WritePreparedTxnDB::AdvanceSeqByOne() {
  // Inserting an empty value will i) let the max evicted entry to be
  // published, i.e., max == last_published, increase the last published to
  // be one beyond max, i.e., max < last_published.
  WriteOptions woptions;
  TransactionOptions txn_options;
  Transaction* txn0 = BeginTransaction(woptions, txn_options, nullptr);
  std::hash<std::thread::id> hasher;
  char name[64];
  snprintf(name, 64, "txn%" ROCKSDB_PRIszt, hasher(std::this_thread::get_id()));
  assert(strlen(name) < 64 - 1);
  Status s = txn0->SetName(name);
  assert(s.ok());
  if (s.ok()) {
    // Without prepare it would simply skip the commit
    s = txn0->Prepare();
  }
  assert(s.ok());
  if (s.ok()) {
    s = txn0->Commit();
  }
  assert(s.ok());
  delete txn0;
}

const std::vector<SequenceNumber> WritePreparedTxnDB::GetSnapshotListFromDB(
    SequenceNumber max) {
  ROCKS_LOG_DETAILS(info_log_, "GetSnapshotListFromDB with max %" PRIu64, max);
  InstrumentedMutexLock dblock(db_impl_->mutex());
  db_impl_->mutex()->AssertHeld();
  return db_impl_->snapshots().GetAll(nullptr, max);
}

void WritePreparedTxnDB::ReleaseSnapshotInternal(
    const SequenceNumber snap_seq) {
  // TODO(myabandeh): relax should enough since the synchronizatin is already
  // done by snapshots_mutex_ under which this function is called.
  if (snap_seq <= max_evicted_seq_.load(std::memory_order_acquire)) {
    // Then this is a rare case that transaction did not finish before max
    // advances. It is expected for a few read-only backup snapshots. For such
    // snapshots we might have kept around a couple of entries in the
    // old_commit_map_. Check and do garbage collection if that is the case.
    bool need_gc = false;
    {
      WPRecordTick(TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD);
      ROCKS_LOG_WARN(info_log_, "old_commit_map_mutex_ overhead for %" PRIu64,
                     snap_seq);
      ReadLock rl(&old_commit_map_mutex_);
      auto prep_set_entry = old_commit_map_.find(snap_seq);
      need_gc = prep_set_entry != old_commit_map_.end();
    }
    if (need_gc) {
      WPRecordTick(TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD);
      ROCKS_LOG_WARN(info_log_, "old_commit_map_mutex_ overhead for %" PRIu64,
                     snap_seq);
      WriteLock wl(&old_commit_map_mutex_);
      old_commit_map_.erase(snap_seq);
      old_commit_map_empty_.store(old_commit_map_.empty(),
                                  std::memory_order_release);
    }
  }
}

void WritePreparedTxnDB::CleanupReleasedSnapshots(
    const std::vector<SequenceNumber>& new_snapshots,
    const std::vector<SequenceNumber>& old_snapshots) {
  auto newi = new_snapshots.begin();
  auto oldi = old_snapshots.begin();
  for (; newi != new_snapshots.end() && oldi != old_snapshots.end();) {
    assert(*newi >= *oldi);  // cannot have new snapshots with lower seq
    if (*newi == *oldi) {    // still not released
      auto value = *newi;
      while (newi != new_snapshots.end() && *newi == value) {
        newi++;
      }
      while (oldi != old_snapshots.end() && *oldi == value) {
        oldi++;
      }
    } else {
      assert(*newi > *oldi);  // *oldi is released
      ReleaseSnapshotInternal(*oldi);
      oldi++;
    }
  }
  // Everything remained in old_snapshots is released and must be cleaned up
  for (; oldi != old_snapshots.end(); oldi++) {
    ReleaseSnapshotInternal(*oldi);
  }
}

void WritePreparedTxnDB::UpdateSnapshots(
    const std::vector<SequenceNumber>& snapshots,
    const SequenceNumber& version) {
  ROCKS_LOG_DETAILS(info_log_, "UpdateSnapshots with version %" PRIu64,
                    version);
  TEST_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:p:start");
  TEST_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:s:start");
#ifndef NDEBUG
  size_t sync_i = 0;
#endif
  ROCKS_LOG_DETAILS(info_log_, "snapshots_mutex_ overhead");
  WriteLock wl(&snapshots_mutex_);
  snapshots_version_ = version;
  // We update the list concurrently with the readers.
  // Both new and old lists are sorted and the new list is subset of the
  // previous list plus some new items. Thus if a snapshot repeats in
  // both new and old lists, it will appear upper in the new list. So if
  // we simply insert the new snapshots in order, if an overwritten item
  // is still valid in the new list is either written to the same place in
  // the array or it is written in a higher palce before it gets
  // overwritten by another item. This guarantess a reader that reads the
  // list bottom-up will eventaully see a snapshot that repeats in the
  // update, either before it gets overwritten by the writer or
  // afterwards.
  size_t i = 0;
  auto it = snapshots.begin();
  for (; it != snapshots.end() && i < SNAPSHOT_CACHE_SIZE; it++, i++) {
    snapshot_cache_[i].store(*it, std::memory_order_release);
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:p:", ++sync_i);
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:s:", sync_i);
  }
#ifndef NDEBUG
  // Release the remaining sync points since they are useless given that the
  // reader would also use lock to access snapshots
  for (++sync_i; sync_i <= 10; ++sync_i) {
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:p:", sync_i);
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:s:", sync_i);
  }
#endif
  snapshots_.clear();
  for (; it != snapshots.end(); it++) {
    // Insert them to a vector that is less efficient to access
    // concurrently
    snapshots_.push_back(*it);
  }
  // Update the size at the end. Otherwise a parallel reader might read
  // items that are not set yet.
  snapshots_total_.store(snapshots.size(), std::memory_order_release);

  // Note: this must be done after the snapshots data structures are updated
  // with the new list of snapshots.
  CleanupReleasedSnapshots(snapshots, snapshots_all_);
  snapshots_all_ = snapshots;

  TEST_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:p:end");
  TEST_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:s:end");
}

void WritePreparedTxnDB::CheckAgainstSnapshots(const CommitEntry& evicted) {
  TEST_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:p:start");
  TEST_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:s:start");
#ifndef NDEBUG
  size_t sync_i = 0;
#endif
  // First check the snapshot cache that is efficient for concurrent access
  auto cnt = snapshots_total_.load(std::memory_order_acquire);
  // The list might get updated concurrently as we are reading from it. The
  // reader should be able to read all the snapshots that are still valid
  // after the update. Since the survived snapshots are written in a higher
  // place before gets overwritten the reader that reads bottom-up will
  // eventully see it.
  const bool next_is_larger = true;
  // We will set to true if the border line snapshot suggests that.
  bool search_larger_list = false;
  size_t ip1 = std::min(cnt, SNAPSHOT_CACHE_SIZE);
  for (; 0 < ip1; ip1--) {
    SequenceNumber snapshot_seq =
        snapshot_cache_[ip1 - 1].load(std::memory_order_acquire);
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:p:",
                        ++sync_i);
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:s:", sync_i);
    if (ip1 == SNAPSHOT_CACHE_SIZE) {  // border line snapshot
      // snapshot_seq < commit_seq => larger_snapshot_seq <= commit_seq
      // then later also continue the search to larger snapshots
      search_larger_list = snapshot_seq < evicted.commit_seq;
    }
    if (!MaybeUpdateOldCommitMap(evicted.prep_seq, evicted.commit_seq,
                                 snapshot_seq, !next_is_larger)) {
      break;
    }
  }
#ifndef NDEBUG
  // Release the remaining sync points before accquiring the lock
  for (++sync_i; sync_i <= 10; ++sync_i) {
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:p:", sync_i);
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:s:", sync_i);
  }
#endif
  TEST_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:p:end");
  TEST_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:s:end");
  if (UNLIKELY(SNAPSHOT_CACHE_SIZE < cnt && search_larger_list)) {
    // Then access the less efficient list of snapshots_
    WPRecordTick(TXN_SNAPSHOT_MUTEX_OVERHEAD);
    ROCKS_LOG_WARN(info_log_,
                   "snapshots_mutex_ overhead for <%" PRIu64 ",%" PRIu64
                   "> with %" ROCKSDB_PRIszt " snapshots",
                   evicted.prep_seq, evicted.commit_seq, cnt);
    ReadLock rl(&snapshots_mutex_);
    // Items could have moved from the snapshots_ to snapshot_cache_ before
    // accquiring the lock. To make sure that we do not miss a valid snapshot,
    // read snapshot_cache_ again while holding the lock.
    for (size_t i = 0; i < SNAPSHOT_CACHE_SIZE; i++) {
      SequenceNumber snapshot_seq =
          snapshot_cache_[i].load(std::memory_order_acquire);
      if (!MaybeUpdateOldCommitMap(evicted.prep_seq, evicted.commit_seq,
                                   snapshot_seq, next_is_larger)) {
        break;
      }
    }
    for (auto snapshot_seq_2 : snapshots_) {
      if (!MaybeUpdateOldCommitMap(evicted.prep_seq, evicted.commit_seq,
                                   snapshot_seq_2, next_is_larger)) {
        break;
      }
    }
  }
}

bool WritePreparedTxnDB::MaybeUpdateOldCommitMap(
    const uint64_t& prep_seq, const uint64_t& commit_seq,
    const uint64_t& snapshot_seq, const bool next_is_larger = true) {
  // If we do not store an entry in old_commit_map_ we assume it is committed in
  // all snapshots. If commit_seq <= snapshot_seq, it is considered already in
  // the snapshot so we need not to keep the entry around for this snapshot.
  if (commit_seq <= snapshot_seq) {
    // continue the search if the next snapshot could be smaller than commit_seq
    return !next_is_larger;
  }
  // then snapshot_seq < commit_seq
  if (prep_seq <= snapshot_seq) {  // overlapping range
    WPRecordTick(TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD);
    ROCKS_LOG_WARN(info_log_,
                   "old_commit_map_mutex_ overhead for %" PRIu64
                   " commit entry: <%" PRIu64 ",%" PRIu64 ">",
                   snapshot_seq, prep_seq, commit_seq);
    WriteLock wl(&old_commit_map_mutex_);
    old_commit_map_empty_.store(false, std::memory_order_release);
    auto& vec = old_commit_map_[snapshot_seq];
    vec.insert(std::upper_bound(vec.begin(), vec.end(), prep_seq), prep_seq);
    // We need to store it once for each overlapping snapshot. Returning true to
    // continue the search if there is more overlapping snapshot.
    return true;
  }
  // continue the search if the next snapshot could be larger than prep_seq
  return next_is_larger;
}

WritePreparedTxnDB::~WritePreparedTxnDB() {
  // At this point there could be running compaction/flush holding a
  // SnapshotChecker, which holds a pointer back to WritePreparedTxnDB.
  // Make sure those jobs finished before destructing WritePreparedTxnDB.
  db_impl_->CancelAllBackgroundWork(true /*wait*/);
}

void SubBatchCounter::InitWithComp(const uint32_t cf) {
  auto cmp = comparators_[cf];
  keys_[cf] = CFKeys(SetComparator(cmp));
}

void SubBatchCounter::AddKey(const uint32_t cf, const Slice& key) {
  CFKeys& cf_keys = keys_[cf];
  if (cf_keys.size() == 0) {  // just inserted
    InitWithComp(cf);
  }
  auto it = cf_keys.insert(key);
  if (it.second == false) {  // second is false if a element already existed.
    batches_++;
    keys_.clear();
    InitWithComp(cf);
    keys_[cf].insert(key);
  }
}

}  //  namespace rocksdb
#endif  // ROCKSDB_LITE
