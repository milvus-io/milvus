//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#include <string>
#include <utility>
#include <vector>

#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/stackable_db.h"
#include "rocksdb/utilities/transaction.h"

// Database with Transaction support.
//
// See transaction.h and examples/transaction_example.cc

namespace rocksdb {

class TransactionDBMutexFactory;

enum TxnDBWritePolicy {
  WRITE_COMMITTED = 0,  // write only the committed data
  // TODO(myabandeh): Not implemented yet
  WRITE_PREPARED,  // write data after the prepare phase of 2pc
  // TODO(myabandeh): Not implemented yet
  WRITE_UNPREPARED  // write data before the prepare phase of 2pc
};

const uint32_t kInitialMaxDeadlocks = 5;

struct TransactionDBOptions {
  // Specifies the maximum number of keys that can be locked at the same time
  // per column family.
  // If the number of locked keys is greater than max_num_locks, transaction
  // writes (or GetForUpdate) will return an error.
  // If this value is not positive, no limit will be enforced.
  int64_t max_num_locks = -1;

  // Stores the number of latest deadlocks to track
  uint32_t max_num_deadlocks = kInitialMaxDeadlocks;

  // Increasing this value will increase the concurrency by dividing the lock
  // table (per column family) into more sub-tables, each with their own
  // separate
  // mutex.
  size_t num_stripes = 16;

  // If positive, specifies the default wait timeout in milliseconds when
  // a transaction attempts to lock a key if not specified by
  // TransactionOptions::lock_timeout.
  //
  // If 0, no waiting is done if a lock cannot instantly be acquired.
  // If negative, there is no timeout.  Not using a timeout is not recommended
  // as it can lead to deadlocks.  Currently, there is no deadlock-detection to
  // recover
  // from a deadlock.
  int64_t transaction_lock_timeout = 1000;  // 1 second

  // If positive, specifies the wait timeout in milliseconds when writing a key
  // OUTSIDE of a transaction (ie by calling DB::Put(),Merge(),Delete(),Write()
  // directly).
  // If 0, no waiting is done if a lock cannot instantly be acquired.
  // If negative, there is no timeout and will block indefinitely when acquiring
  // a lock.
  //
  // Not using a timeout can lead to deadlocks.  Currently, there
  // is no deadlock-detection to recover from a deadlock.  While DB writes
  // cannot deadlock with other DB writes, they can deadlock with a transaction.
  // A negative timeout should only be used if all transactions have a small
  // expiration set.
  int64_t default_lock_timeout = 1000;  // 1 second

  // If set, the TransactionDB will use this implementation of a mutex and
  // condition variable for all transaction locking instead of the default
  // mutex/condvar implementation.
  std::shared_ptr<TransactionDBMutexFactory> custom_mutex_factory;

  // The policy for when to write the data into the DB. The default policy is to
  // write only the committed data (WRITE_COMMITTED). The data could be written
  // before the commit phase. The DB then needs to provide the mechanisms to
  // tell apart committed from uncommitted data.
  TxnDBWritePolicy write_policy = TxnDBWritePolicy::WRITE_COMMITTED;

  // TODO(myabandeh): remove this option
  // Note: this is a temporary option as a hot fix in rollback of writeprepared
  // txns in myrocks. MyRocks uses merge operands for autoinc column id without
  // however obtaining locks. This breaks the assumption behind the rollback
  // logic in myrocks. This hack of simply not rolling back merge operands works
  // for the special way that myrocks uses this operands.
  bool rollback_merge_operands = false;

 private:
  // 128 entries
  size_t wp_snapshot_cache_bits = static_cast<size_t>(7);
  // 8m entry, 64MB size
  size_t wp_commit_cache_bits = static_cast<size_t>(23);

  friend class WritePreparedTxnDB;
  friend class WritePreparedTransactionTestBase;
  friend class MySQLStyleTransactionTest;
};

struct TransactionOptions {
  // Setting set_snapshot=true is the same as calling
  // Transaction::SetSnapshot().
  bool set_snapshot = false;

  // Setting to true means that before acquiring locks, this transaction will
  // check if doing so will cause a deadlock. If so, it will return with
  // Status::Busy.  The user should retry their transaction.
  bool deadlock_detect = false;

  // If set, it states that the CommitTimeWriteBatch represents the latest state
  // of the application, has only one sub-batch, i.e., no duplicate keys,  and
  // meant to be used later during recovery. It enables an optimization to
  // postpone updating the memtable with CommitTimeWriteBatch to only
  // SwitchMemtable or recovery.
  bool use_only_the_last_commit_time_batch_for_recovery = false;

  // TODO(agiardullo): TransactionDB does not yet support comparators that allow
  // two non-equal keys to be equivalent.  Ie, cmp->Compare(a,b) should only
  // return 0 if
  // a.compare(b) returns 0.


  // If positive, specifies the wait timeout in milliseconds when
  // a transaction attempts to lock a key.
  //
  // If 0, no waiting is done if a lock cannot instantly be acquired.
  // If negative, TransactionDBOptions::transaction_lock_timeout will be used.
  int64_t lock_timeout = -1;

  // Expiration duration in milliseconds.  If non-negative, transactions that
  // last longer than this many milliseconds will fail to commit.  If not set,
  // a forgotten transaction that is never committed, rolled back, or deleted
  // will never relinquish any locks it holds.  This could prevent keys from
  // being written by other writers.
  int64_t expiration = -1;

  // The number of traversals to make during deadlock detection.
  int64_t deadlock_detect_depth = 50;

  // The maximum number of bytes used for the write batch. 0 means no limit.
  size_t max_write_batch_size = 0;

  // Skip Concurrency Control. This could be as an optimization if the
  // application knows that the transaction would not have any conflict with
  // concurrent transactions. It could also be used during recovery if (i)
  // application guarantees no conflict between prepared transactions in the WAL
  // (ii) application guarantees that recovered transactions will be rolled
  // back/commit before new transactions start.
  // Default: false
  bool skip_concurrency_control = false;
};

// The per-write optimizations that do not involve transactions. TransactionDB
// implementation might or might not make use of the specified optimizations.
struct TransactionDBWriteOptimizations {
  // If it is true it means that the application guarantees that the
  // key-set in the write batch do not conflict with any concurrent transaction
  // and hence the concurrency control mechanism could be skipped for this
  // write.
  bool skip_concurrency_control = false;
  // If true, the application guarantees that there is no duplicate <column
  // family, key> in the write batch and any employed mechanism to handle
  // duplicate keys could be skipped.
  bool skip_duplicate_key_check = false;
};

struct KeyLockInfo {
  std::string key;
  std::vector<TransactionID> ids;
  bool exclusive;
};

struct DeadlockInfo {
  TransactionID m_txn_id;
  uint32_t m_cf_id;
  bool m_exclusive;
  std::string m_waiting_key;
};

struct DeadlockPath {
  std::vector<DeadlockInfo> path;
  bool limit_exceeded;
  int64_t deadlock_time;

  explicit DeadlockPath(std::vector<DeadlockInfo> path_entry,
                        const int64_t& dl_time)
      : path(path_entry), limit_exceeded(false), deadlock_time(dl_time) {}

  // empty path, limit exceeded constructor and default constructor
  explicit DeadlockPath(const int64_t& dl_time = 0, bool limit = false)
      : path(0), limit_exceeded(limit), deadlock_time(dl_time) {}

  bool empty() { return path.empty() && !limit_exceeded; }
};

class TransactionDB : public StackableDB {
 public:
  // Optimized version of ::Write that receives more optimization request such
  // as skip_concurrency_control.
  using StackableDB::Write;
  virtual Status Write(const WriteOptions& opts,
                       const TransactionDBWriteOptimizations&,
                       WriteBatch* updates) {
    // The default implementation ignores TransactionDBWriteOptimizations and
    // falls back to the un-optimized version of ::Write
    return Write(opts, updates);
  }
  // Open a TransactionDB similar to DB::Open().
  // Internally call PrepareWrap() and WrapDB()
  // If the return status is not ok, then dbptr is set to nullptr.
  static Status Open(const Options& options,
                     const TransactionDBOptions& txn_db_options,
                     const std::string& dbname, TransactionDB** dbptr);

  static Status Open(const DBOptions& db_options,
                     const TransactionDBOptions& txn_db_options,
                     const std::string& dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     std::vector<ColumnFamilyHandle*>* handles,
                     TransactionDB** dbptr);
  // Note: PrepareWrap() may change parameters, make copies before the
  // invocation if needed.
  static void PrepareWrap(DBOptions* db_options,
                          std::vector<ColumnFamilyDescriptor>* column_families,
                          std::vector<size_t>* compaction_enabled_cf_indices);
  // If the return status is not ok, then dbptr will bet set to nullptr. The
  // input db parameter might or might not be deleted as a result of the
  // failure. If it is properly deleted it will be set to nullptr. If the return
  // status is ok, the ownership of db is transferred to dbptr.
  static Status WrapDB(DB* db, const TransactionDBOptions& txn_db_options,
                       const std::vector<size_t>& compaction_enabled_cf_indices,
                       const std::vector<ColumnFamilyHandle*>& handles,
                       TransactionDB** dbptr);
  // If the return status is not ok, then dbptr will bet set to nullptr. The
  // input db parameter might or might not be deleted as a result of the
  // failure. If it is properly deleted it will be set to nullptr. If the return
  // status is ok, the ownership of db is transferred to dbptr.
  static Status WrapStackableDB(
      StackableDB* db, const TransactionDBOptions& txn_db_options,
      const std::vector<size_t>& compaction_enabled_cf_indices,
      const std::vector<ColumnFamilyHandle*>& handles, TransactionDB** dbptr);
  // Since the destructor in StackableDB is virtual, this destructor is virtual
  // too. The root db will be deleted by the base's destructor.
  ~TransactionDB() override {}

  // Starts a new Transaction.
  //
  // Caller is responsible for deleting the returned transaction when no
  // longer needed.
  //
  // If old_txn is not null, BeginTransaction will reuse this Transaction
  // handle instead of allocating a new one.  This is an optimization to avoid
  // extra allocations when repeatedly creating transactions.
  virtual Transaction* BeginTransaction(
      const WriteOptions& write_options,
      const TransactionOptions& txn_options = TransactionOptions(),
      Transaction* old_txn = nullptr) = 0;

  virtual Transaction* GetTransactionByName(const TransactionName& name) = 0;
  virtual void GetAllPreparedTransactions(std::vector<Transaction*>* trans) = 0;

  // Returns set of all locks held.
  //
  // The mapping is column family id -> KeyLockInfo
  virtual std::unordered_multimap<uint32_t, KeyLockInfo>
  GetLockStatusData() = 0;
  virtual std::vector<DeadlockPath> GetDeadlockInfoBuffer() = 0;
  virtual void SetDeadlockInfoBufferSize(uint32_t target_size) = 0;

 protected:
  // To Create an TransactionDB, call Open()
  // The ownership of db is transferred to the base StackableDB
  explicit TransactionDB(DB* db) : StackableDB(db) {}

 private:
  // No copying allowed
  TransactionDB(const TransactionDB&);
  void operator=(const TransactionDB&);
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
