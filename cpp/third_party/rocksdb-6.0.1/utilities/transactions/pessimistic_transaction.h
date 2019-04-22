// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include <algorithm>
#include <atomic>
#include <mutex>
#include <stack>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/write_callback.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "util/autovector.h"
#include "utilities/transactions/transaction_base.h"
#include "utilities/transactions/transaction_util.h"

namespace rocksdb {

class PessimisticTransactionDB;

// A transaction under pessimistic concurrency control. This class implements
// the locking API and interfaces with the lock manager as well as the
// pessimistic transactional db.
class PessimisticTransaction : public TransactionBaseImpl {
 public:
  PessimisticTransaction(TransactionDB* db, const WriteOptions& write_options,
                         const TransactionOptions& txn_options,
                         const bool init = true);

  virtual ~PessimisticTransaction();

  void Reinitialize(TransactionDB* txn_db, const WriteOptions& write_options,
                    const TransactionOptions& txn_options);

  Status Prepare() override;

  Status Commit() override;

  // It is basically Commit without going through Prepare phase. The write batch
  // is also directly provided instead of expecting txn to gradually batch the
  // transactions writes to an internal write batch.
  Status CommitBatch(WriteBatch* batch);

  Status Rollback() override;

  Status RollbackToSavePoint() override;

  Status SetName(const TransactionName& name) override;

  // Generate a new unique transaction identifier
  static TransactionID GenTxnID();

  TransactionID GetID() const override { return txn_id_; }

  std::vector<TransactionID> GetWaitingTxns(uint32_t* column_family_id,
                                            std::string* key) const override {
    std::lock_guard<std::mutex> lock(wait_mutex_);
    std::vector<TransactionID> ids(waiting_txn_ids_.size());
    if (key) *key = waiting_key_ ? *waiting_key_ : "";
    if (column_family_id) *column_family_id = waiting_cf_id_;
    std::copy(waiting_txn_ids_.begin(), waiting_txn_ids_.end(), ids.begin());
    return ids;
  }

  void SetWaitingTxn(autovector<TransactionID> ids, uint32_t column_family_id,
                     const std::string* key) {
    std::lock_guard<std::mutex> lock(wait_mutex_);
    waiting_txn_ids_ = ids;
    waiting_cf_id_ = column_family_id;
    waiting_key_ = key;
  }

  void ClearWaitingTxn() {
    std::lock_guard<std::mutex> lock(wait_mutex_);
    waiting_txn_ids_.clear();
    waiting_cf_id_ = 0;
    waiting_key_ = nullptr;
  }

  // Returns the time (in microseconds according to Env->GetMicros())
  // that this transaction will be expired.  Returns 0 if this transaction does
  // not expire.
  uint64_t GetExpirationTime() const { return expiration_time_; }

  // returns true if this transaction has an expiration_time and has expired.
  bool IsExpired() const;

  // Returns the number of microseconds a transaction can wait on acquiring a
  // lock or -1 if there is no timeout.
  int64_t GetLockTimeout() const { return lock_timeout_; }
  void SetLockTimeout(int64_t timeout) override {
    lock_timeout_ = timeout * 1000;
  }

  // Returns true if locks were stolen successfully, false otherwise.
  bool TryStealingLocks();

  bool IsDeadlockDetect() const override { return deadlock_detect_; }

  int64_t GetDeadlockDetectDepth() const { return deadlock_detect_depth_; }

 protected:
  // Refer to
  // TransactionOptions::use_only_the_last_commit_time_batch_for_recovery
  bool use_only_the_last_commit_time_batch_for_recovery_ = false;

  virtual Status PrepareInternal() = 0;

  virtual Status CommitWithoutPrepareInternal() = 0;

  // batch_cnt if non-zero is the number of sub-batches. A sub-batch is a batch
  // with no duplicate keys. If zero, then the number of sub-batches is unknown.
  virtual Status CommitBatchInternal(WriteBatch* batch,
                                     size_t batch_cnt = 0) = 0;

  virtual Status CommitInternal() = 0;

  virtual Status RollbackInternal() = 0;

  virtual void Initialize(const TransactionOptions& txn_options);

  Status LockBatch(WriteBatch* batch, TransactionKeyMap* keys_to_unlock);

  Status TryLock(ColumnFamilyHandle* column_family, const Slice& key,
                 bool read_only, bool exclusive, const bool do_validate = true,
                 const bool assume_tracked = false) override;

  void Clear() override;

  PessimisticTransactionDB* txn_db_impl_;
  DBImpl* db_impl_;

  // If non-zero, this transaction should not be committed after this time (in
  // microseconds according to Env->NowMicros())
  uint64_t expiration_time_;

 private:
  friend class TransactionTest_ValidateSnapshotTest_Test;
  // Used to create unique ids for transactions.
  static std::atomic<TransactionID> txn_id_counter_;

  // Unique ID for this transaction
  TransactionID txn_id_;

  // IDs for the transactions that are blocking the current transaction.
  //
  // empty if current transaction is not waiting.
  autovector<TransactionID> waiting_txn_ids_;

  // The following two represents the (cf, key) that a transaction is waiting
  // on.
  //
  // If waiting_key_ is not null, then the pointer should always point to
  // a valid string object. The reason is that it is only non-null when the
  // transaction is blocked in the TransactionLockMgr::AcquireWithTimeout
  // function. At that point, the key string object is one of the function
  // parameters.
  uint32_t waiting_cf_id_;
  const std::string* waiting_key_;

  // Mutex protecting waiting_txn_ids_, waiting_cf_id_ and waiting_key_.
  mutable std::mutex wait_mutex_;

  // Timeout in microseconds when locking a key or -1 if there is no timeout.
  int64_t lock_timeout_;

  // Whether to perform deadlock detection or not.
  bool deadlock_detect_;

  // Whether to perform deadlock detection or not.
  int64_t deadlock_detect_depth_;

  // Refer to TransactionOptions::skip_concurrency_control
  bool skip_concurrency_control_;

  virtual Status ValidateSnapshot(ColumnFamilyHandle* column_family,
                                  const Slice& key,
                                  SequenceNumber* tracked_at_seq);

  void UnlockGetForUpdate(ColumnFamilyHandle* column_family,
                          const Slice& key) override;

  // No copying allowed
  PessimisticTransaction(const PessimisticTransaction&);
  void operator=(const PessimisticTransaction&);
};

class WriteCommittedTxn : public PessimisticTransaction {
 public:
  WriteCommittedTxn(TransactionDB* db, const WriteOptions& write_options,
                    const TransactionOptions& txn_options);

  virtual ~WriteCommittedTxn() {}

 private:
  Status PrepareInternal() override;

  Status CommitWithoutPrepareInternal() override;

  Status CommitBatchInternal(WriteBatch* batch, size_t batch_cnt) override;

  Status CommitInternal() override;

  Status RollbackInternal() override;

  // No copying allowed
  WriteCommittedTxn(const WriteCommittedTxn&);
  void operator=(const WriteCommittedTxn&);
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
