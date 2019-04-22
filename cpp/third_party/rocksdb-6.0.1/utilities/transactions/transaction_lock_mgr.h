//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#include <chrono>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "monitoring/instrumented_mutex.h"
#include "rocksdb/utilities/transaction.h"
#include "util/autovector.h"
#include "util/hash_map.h"
#include "util/thread_local.h"
#include "utilities/transactions/pessimistic_transaction.h"

namespace rocksdb {

class ColumnFamilyHandle;
struct LockInfo;
struct LockMap;
struct LockMapStripe;

struct DeadlockInfoBuffer {
 private:
  std::vector<DeadlockPath> paths_buffer_;
  uint32_t buffer_idx_;
  std::mutex paths_buffer_mutex_;
  std::vector<DeadlockPath> Normalize();

 public:
  explicit DeadlockInfoBuffer(uint32_t n_latest_dlocks)
      : paths_buffer_(n_latest_dlocks), buffer_idx_(0) {}
  void AddNewPath(DeadlockPath path);
  void Resize(uint32_t target_size);
  std::vector<DeadlockPath> PrepareBuffer();
};

struct TrackedTrxInfo {
  autovector<TransactionID> m_neighbors;
  uint32_t m_cf_id;
  std::string m_waiting_key;
  bool m_exclusive;
};

class Slice;
class PessimisticTransactionDB;

class TransactionLockMgr {
 public:
  TransactionLockMgr(TransactionDB* txn_db, size_t default_num_stripes,
                     int64_t max_num_locks, uint32_t max_num_deadlocks,
                     std::shared_ptr<TransactionDBMutexFactory> factory);

  ~TransactionLockMgr();

  // Creates a new LockMap for this column family.  Caller should guarantee
  // that this column family does not already exist.
  void AddColumnFamily(uint32_t column_family_id);

  // Deletes the LockMap for this column family.  Caller should guarantee that
  // this column family is no longer in use.
  void RemoveColumnFamily(uint32_t column_family_id);

  // Attempt to lock key.  If OK status is returned, the caller is responsible
  // for calling UnLock() on this key.
  Status TryLock(PessimisticTransaction* txn, uint32_t column_family_id,
                 const std::string& key, Env* env, bool exclusive);

  // Unlock a key locked by TryLock().  txn must be the same Transaction that
  // locked this key.
  void UnLock(const PessimisticTransaction* txn, const TransactionKeyMap* keys,
              Env* env);
  void UnLock(PessimisticTransaction* txn, uint32_t column_family_id,
              const std::string& key, Env* env);

  using LockStatusData = std::unordered_multimap<uint32_t, KeyLockInfo>;
  LockStatusData GetLockStatusData();
  std::vector<DeadlockPath> GetDeadlockInfoBuffer();
  void Resize(uint32_t);

 private:
  PessimisticTransactionDB* txn_db_impl_;

  // Default number of lock map stripes per column family
  const size_t default_num_stripes_;

  // Limit on number of keys locked per column family
  const int64_t max_num_locks_;

  // The following lock order must be satisfied in order to avoid deadlocking
  // ourselves.
  //   - lock_map_mutex_
  //   - stripe mutexes in ascending cf id, ascending stripe order
  //   - wait_txn_map_mutex_
  //
  // Must be held when accessing/modifying lock_maps_.
  InstrumentedMutex lock_map_mutex_;

  // Map of ColumnFamilyId to locked key info
  using LockMaps = std::unordered_map<uint32_t, std::shared_ptr<LockMap>>;
  LockMaps lock_maps_;

  // Thread-local cache of entries in lock_maps_.  This is an optimization
  // to avoid acquiring a mutex in order to look up a LockMap
  std::unique_ptr<ThreadLocalPtr> lock_maps_cache_;

  // Must be held when modifying wait_txn_map_ and rev_wait_txn_map_.
  std::mutex wait_txn_map_mutex_;

  // Maps from waitee -> number of waiters.
  HashMap<TransactionID, int> rev_wait_txn_map_;
  // Maps from waiter -> waitee.
  HashMap<TransactionID, TrackedTrxInfo> wait_txn_map_;
  DeadlockInfoBuffer dlock_buffer_;

  // Used to allocate mutexes/condvars to use when locking keys
  std::shared_ptr<TransactionDBMutexFactory> mutex_factory_;

  bool IsLockExpired(TransactionID txn_id, const LockInfo& lock_info, Env* env,
                     uint64_t* wait_time);

  std::shared_ptr<LockMap> GetLockMap(uint32_t column_family_id);

  Status AcquireWithTimeout(PessimisticTransaction* txn, LockMap* lock_map,
                            LockMapStripe* stripe, uint32_t column_family_id,
                            const std::string& key, Env* env, int64_t timeout,
                            const LockInfo& lock_info);

  Status AcquireLocked(LockMap* lock_map, LockMapStripe* stripe,
                       const std::string& key, Env* env,
                       const LockInfo& lock_info, uint64_t* wait_time,
                       autovector<TransactionID>* txn_ids);

  void UnLockKey(const PessimisticTransaction* txn, const std::string& key,
                 LockMapStripe* stripe, LockMap* lock_map, Env* env);

  bool IncrementWaiters(const PessimisticTransaction* txn,
                        const autovector<TransactionID>& wait_ids,
                        const std::string& key, const uint32_t& cf_id,
                        const bool& exclusive, Env* const env);
  void DecrementWaiters(const PessimisticTransaction* txn,
                        const autovector<TransactionID>& wait_ids);
  void DecrementWaitersImpl(const PessimisticTransaction* txn,
                            const autovector<TransactionID>& wait_ids);

  // No copying allowed
  TransactionLockMgr(const TransactionLockMgr&);
  void operator=(const TransactionLockMgr&);
};

}  //  namespace rocksdb
#endif  // ROCKSDB_LITE
