//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "utilities/transactions/pessimistic_transaction_db.h"

#include <inttypes.h>
#include <string>
#include <unordered_set>
#include <vector>

#include "db/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction_db.h"
#include "util/cast_util.h"
#include "util/mutexlock.h"
#include "util/sync_point.h"
#include "utilities/transactions/pessimistic_transaction.h"
#include "utilities/transactions/transaction_db_mutex_impl.h"
#include "utilities/transactions/write_prepared_txn_db.h"
#include "utilities/transactions/write_unprepared_txn_db.h"

namespace rocksdb {

PessimisticTransactionDB::PessimisticTransactionDB(
    DB* db, const TransactionDBOptions& txn_db_options)
    : TransactionDB(db),
      db_impl_(static_cast_with_check<DBImpl, DB>(db)),
      txn_db_options_(txn_db_options),
      lock_mgr_(this, txn_db_options_.num_stripes, txn_db_options.max_num_locks,
                txn_db_options_.max_num_deadlocks,
                txn_db_options_.custom_mutex_factory
                    ? txn_db_options_.custom_mutex_factory
                    : std::shared_ptr<TransactionDBMutexFactory>(
                          new TransactionDBMutexFactoryImpl())) {
  assert(db_impl_ != nullptr);
  info_log_ = db_impl_->GetDBOptions().info_log;
}

// Support initiliazing PessimisticTransactionDB from a stackable db
//
//    PessimisticTransactionDB
//     ^        ^
//     |        |
//     |        +
//     |   StackableDB
//     |   ^
//     |   |
//     +   +
//     DBImpl
//       ^
//       |(inherit)
//       +
//       DB
//
PessimisticTransactionDB::PessimisticTransactionDB(
    StackableDB* db, const TransactionDBOptions& txn_db_options)
    : TransactionDB(db),
      db_impl_(static_cast_with_check<DBImpl, DB>(db->GetRootDB())),
      txn_db_options_(txn_db_options),
      lock_mgr_(this, txn_db_options_.num_stripes, txn_db_options.max_num_locks,
                txn_db_options_.max_num_deadlocks,
                txn_db_options_.custom_mutex_factory
                    ? txn_db_options_.custom_mutex_factory
                    : std::shared_ptr<TransactionDBMutexFactory>(
                          new TransactionDBMutexFactoryImpl())) {
  assert(db_impl_ != nullptr);
}

PessimisticTransactionDB::~PessimisticTransactionDB() {
  while (!transactions_.empty()) {
    delete transactions_.begin()->second;
    // TODO(myabandeh): this seems to be an unsafe approach as it is not quite
    // clear whether delete would also remove the entry from transactions_.
  }
}

Status PessimisticTransactionDB::VerifyCFOptions(const ColumnFamilyOptions&) {
  return Status::OK();
}

Status PessimisticTransactionDB::Initialize(
    const std::vector<size_t>& compaction_enabled_cf_indices,
    const std::vector<ColumnFamilyHandle*>& handles) {
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

  // create 'real' transactions from recovered shell transactions
  auto dbimpl = reinterpret_cast<DBImpl*>(GetRootDB());
  assert(dbimpl != nullptr);
  auto rtrxs = dbimpl->recovered_transactions();

  for (auto it = rtrxs.begin(); it != rtrxs.end(); it++) {
    auto recovered_trx = it->second;
    assert(recovered_trx);
    assert(recovered_trx->batches_.size() == 1);
    const auto& seq = recovered_trx->batches_.begin()->first;
    const auto& batch_info = recovered_trx->batches_.begin()->second;
    assert(batch_info.log_number_);
    assert(recovered_trx->name_.length());

    WriteOptions w_options;
    w_options.sync = true;
    TransactionOptions t_options;
    // This would help avoiding deadlock for keys that although exist in the WAL
    // did not go through concurrency control. This includes the merge that
    // MyRocks uses for auto-inc columns. It is safe to do so, since (i) if
    // there is a conflict between the keys of two transactions that must be
    // avoided, it is already avoided by the application, MyRocks, before the
    // restart (ii) application, MyRocks, guarntees to rollback/commit the
    // recovered transactions before new transactions start.
    t_options.skip_concurrency_control = true;

    Transaction* real_trx = BeginTransaction(w_options, t_options, nullptr);
    assert(real_trx);
    real_trx->SetLogNumber(batch_info.log_number_);
    assert(seq != kMaxSequenceNumber);
    if (GetTxnDBOptions().write_policy != WRITE_COMMITTED) {
      real_trx->SetId(seq);
    }

    s = real_trx->SetName(recovered_trx->name_);
    if (!s.ok()) {
      break;
    }

    s = real_trx->RebuildFromWriteBatch(batch_info.batch_);
    // WriteCommitted set this to to disable this check that is specific to
    // WritePrepared txns
    assert(batch_info.batch_cnt_ == 0 ||
           real_trx->GetWriteBatch()->SubBatchCnt() == batch_info.batch_cnt_);
    real_trx->SetState(Transaction::PREPARED);
    if (!s.ok()) {
      break;
    }
  }
  if (s.ok()) {
    dbimpl->DeleteAllRecoveredTransactions();
  }
  return s;
}

Transaction* WriteCommittedTxnDB::BeginTransaction(
    const WriteOptions& write_options, const TransactionOptions& txn_options,
    Transaction* old_txn) {
  if (old_txn != nullptr) {
    ReinitializeTransaction(old_txn, write_options, txn_options);
    return old_txn;
  } else {
    return new WriteCommittedTxn(this, write_options, txn_options);
  }
}

TransactionDBOptions PessimisticTransactionDB::ValidateTxnDBOptions(
    const TransactionDBOptions& txn_db_options) {
  TransactionDBOptions validated = txn_db_options;

  if (txn_db_options.num_stripes == 0) {
    validated.num_stripes = 1;
  }

  return validated;
}

Status TransactionDB::Open(const Options& options,
                           const TransactionDBOptions& txn_db_options,
                           const std::string& dbname, TransactionDB** dbptr) {
  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;
  Status s = TransactionDB::Open(db_options, txn_db_options, dbname,
                                 column_families, &handles, dbptr);
  if (s.ok()) {
    assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    delete handles[0];
  }

  return s;
}

Status TransactionDB::Open(
    const DBOptions& db_options, const TransactionDBOptions& txn_db_options,
    const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, TransactionDB** dbptr) {
  Status s;
  DB* db = nullptr;

  ROCKS_LOG_WARN(db_options.info_log, "Transaction write_policy is %" PRId32,
                 static_cast<int>(txn_db_options.write_policy));
  std::vector<ColumnFamilyDescriptor> column_families_copy = column_families;
  std::vector<size_t> compaction_enabled_cf_indices;
  DBOptions db_options_2pc = db_options;
  PrepareWrap(&db_options_2pc, &column_families_copy,
              &compaction_enabled_cf_indices);
  const bool use_seq_per_batch =
      txn_db_options.write_policy == WRITE_PREPARED ||
      txn_db_options.write_policy == WRITE_UNPREPARED;
  const bool use_batch_per_txn =
      txn_db_options.write_policy == WRITE_COMMITTED ||
      txn_db_options.write_policy == WRITE_PREPARED;
  s = DBImpl::Open(db_options_2pc, dbname, column_families_copy, handles, &db,
                   use_seq_per_batch, use_batch_per_txn);
  if (s.ok()) {
    s = WrapDB(db, txn_db_options, compaction_enabled_cf_indices, *handles,
               dbptr);
  }
  if (!s.ok()) {
    // just in case it was not deleted (and not set to nullptr).
    delete db;
  }
  return s;
}

void TransactionDB::PrepareWrap(
    DBOptions* db_options, std::vector<ColumnFamilyDescriptor>* column_families,
    std::vector<size_t>* compaction_enabled_cf_indices) {
  compaction_enabled_cf_indices->clear();

  // Enable MemTable History if not already enabled
  for (size_t i = 0; i < column_families->size(); i++) {
    ColumnFamilyOptions* cf_options = &(*column_families)[i].options;

    if (cf_options->max_write_buffer_number_to_maintain == 0) {
      // Setting to -1 will set the History size to max_write_buffer_number.
      cf_options->max_write_buffer_number_to_maintain = -1;
    }
    if (!cf_options->disable_auto_compactions) {
      // Disable compactions momentarily to prevent race with DB::Open
      cf_options->disable_auto_compactions = true;
      compaction_enabled_cf_indices->push_back(i);
    }
  }
  db_options->allow_2pc = true;
}

Status TransactionDB::WrapDB(
    // make sure this db is already opened with memtable history enabled,
    // auto compaction distabled and 2 phase commit enabled
    DB* db, const TransactionDBOptions& txn_db_options,
    const std::vector<size_t>& compaction_enabled_cf_indices,
    const std::vector<ColumnFamilyHandle*>& handles, TransactionDB** dbptr) {
  assert(db != nullptr);
  assert(dbptr != nullptr);
  *dbptr = nullptr;
  std::unique_ptr<PessimisticTransactionDB> txn_db;
  switch (txn_db_options.write_policy) {
    case WRITE_UNPREPARED:
      txn_db.reset(new WriteUnpreparedTxnDB(
          db, PessimisticTransactionDB::ValidateTxnDBOptions(txn_db_options)));
      break;
    case WRITE_PREPARED:
      txn_db.reset(new WritePreparedTxnDB(
          db, PessimisticTransactionDB::ValidateTxnDBOptions(txn_db_options)));
      break;
    case WRITE_COMMITTED:
    default:
      txn_db.reset(new WriteCommittedTxnDB(
          db, PessimisticTransactionDB::ValidateTxnDBOptions(txn_db_options)));
  }
  txn_db->UpdateCFComparatorMap(handles);
  Status s = txn_db->Initialize(compaction_enabled_cf_indices, handles);
  // In case of a failure at this point, db is deleted via the txn_db destructor
  // and set to nullptr.
  if (s.ok()) {
    *dbptr = txn_db.release();
  }
  return s;
}

Status TransactionDB::WrapStackableDB(
    // make sure this stackable_db is already opened with memtable history
    // enabled, auto compaction distabled and 2 phase commit enabled
    StackableDB* db, const TransactionDBOptions& txn_db_options,
    const std::vector<size_t>& compaction_enabled_cf_indices,
    const std::vector<ColumnFamilyHandle*>& handles, TransactionDB** dbptr) {
  assert(db != nullptr);
  assert(dbptr != nullptr);
  *dbptr = nullptr;
  std::unique_ptr<PessimisticTransactionDB> txn_db;

  switch (txn_db_options.write_policy) {
    case WRITE_UNPREPARED:
      txn_db.reset(new WriteUnpreparedTxnDB(
          db, PessimisticTransactionDB::ValidateTxnDBOptions(txn_db_options)));
      break;
    case WRITE_PREPARED:
      txn_db.reset(new WritePreparedTxnDB(
          db, PessimisticTransactionDB::ValidateTxnDBOptions(txn_db_options)));
      break;
    case WRITE_COMMITTED:
    default:
      txn_db.reset(new WriteCommittedTxnDB(
          db, PessimisticTransactionDB::ValidateTxnDBOptions(txn_db_options)));
  }
  txn_db->UpdateCFComparatorMap(handles);
  Status s = txn_db->Initialize(compaction_enabled_cf_indices, handles);
  // In case of a failure at this point, db is deleted via the txn_db destructor
  // and set to nullptr.
  if (s.ok()) {
    *dbptr = txn_db.release();
  }
  return s;
}

// Let TransactionLockMgr know that this column family exists so it can
// allocate a LockMap for it.
void PessimisticTransactionDB::AddColumnFamily(
    const ColumnFamilyHandle* handle) {
  lock_mgr_.AddColumnFamily(handle->GetID());
}

Status PessimisticTransactionDB::CreateColumnFamily(
    const ColumnFamilyOptions& options, const std::string& column_family_name,
    ColumnFamilyHandle** handle) {
  InstrumentedMutexLock l(&column_family_mutex_);
  Status s = VerifyCFOptions(options);
  if (!s.ok()) {
    return s;
  }

  s = db_->CreateColumnFamily(options, column_family_name, handle);
  if (s.ok()) {
    lock_mgr_.AddColumnFamily((*handle)->GetID());
    UpdateCFComparatorMap(*handle);
  }

  return s;
}

// Let TransactionLockMgr know that it can deallocate the LockMap for this
// column family.
Status PessimisticTransactionDB::DropColumnFamily(
    ColumnFamilyHandle* column_family) {
  InstrumentedMutexLock l(&column_family_mutex_);

  Status s = db_->DropColumnFamily(column_family);
  if (s.ok()) {
    lock_mgr_.RemoveColumnFamily(column_family->GetID());
  }

  return s;
}

Status PessimisticTransactionDB::TryLock(PessimisticTransaction* txn,
                                         uint32_t cfh_id,
                                         const std::string& key,
                                         bool exclusive) {
  return lock_mgr_.TryLock(txn, cfh_id, key, GetEnv(), exclusive);
}

void PessimisticTransactionDB::UnLock(PessimisticTransaction* txn,
                                      const TransactionKeyMap* keys) {
  lock_mgr_.UnLock(txn, keys, GetEnv());
}

void PessimisticTransactionDB::UnLock(PessimisticTransaction* txn,
                                      uint32_t cfh_id, const std::string& key) {
  lock_mgr_.UnLock(txn, cfh_id, key, GetEnv());
}

// Used when wrapping DB write operations in a transaction
Transaction* PessimisticTransactionDB::BeginInternalTransaction(
    const WriteOptions& options) {
  TransactionOptions txn_options;
  Transaction* txn = BeginTransaction(options, txn_options, nullptr);

  // Use default timeout for non-transactional writes
  txn->SetLockTimeout(txn_db_options_.default_lock_timeout);
  return txn;
}

// All user Put, Merge, Delete, and Write requests must be intercepted to make
// sure that they lock all keys that they are writing to avoid causing conflicts
// with any concurrent transactions. The easiest way to do this is to wrap all
// write operations in a transaction.
//
// Put(), Merge(), and Delete() only lock a single key per call.  Write() will
// sort its keys before locking them.  This guarantees that TransactionDB write
// methods cannot deadlock with each other (but still could deadlock with a
// Transaction).
Status PessimisticTransactionDB::Put(const WriteOptions& options,
                                     ColumnFamilyHandle* column_family,
                                     const Slice& key, const Slice& val) {
  Status s;

  Transaction* txn = BeginInternalTransaction(options);
  txn->DisableIndexing();

  // Since the client didn't create a transaction, they don't care about
  // conflict checking for this write.  So we just need to do PutUntracked().
  s = txn->PutUntracked(column_family, key, val);

  if (s.ok()) {
    s = txn->Commit();
  }

  delete txn;

  return s;
}

Status PessimisticTransactionDB::Delete(const WriteOptions& wopts,
                                        ColumnFamilyHandle* column_family,
                                        const Slice& key) {
  Status s;

  Transaction* txn = BeginInternalTransaction(wopts);
  txn->DisableIndexing();

  // Since the client didn't create a transaction, they don't care about
  // conflict checking for this write.  So we just need to do
  // DeleteUntracked().
  s = txn->DeleteUntracked(column_family, key);

  if (s.ok()) {
    s = txn->Commit();
  }

  delete txn;

  return s;
}

Status PessimisticTransactionDB::SingleDelete(const WriteOptions& wopts,
                                              ColumnFamilyHandle* column_family,
                                              const Slice& key) {
  Status s;

  Transaction* txn = BeginInternalTransaction(wopts);
  txn->DisableIndexing();

  // Since the client didn't create a transaction, they don't care about
  // conflict checking for this write.  So we just need to do
  // SingleDeleteUntracked().
  s = txn->SingleDeleteUntracked(column_family, key);

  if (s.ok()) {
    s = txn->Commit();
  }

  delete txn;

  return s;
}

Status PessimisticTransactionDB::Merge(const WriteOptions& options,
                                       ColumnFamilyHandle* column_family,
                                       const Slice& key, const Slice& value) {
  Status s;

  Transaction* txn = BeginInternalTransaction(options);
  txn->DisableIndexing();

  // Since the client didn't create a transaction, they don't care about
  // conflict checking for this write.  So we just need to do
  // MergeUntracked().
  s = txn->MergeUntracked(column_family, key, value);

  if (s.ok()) {
    s = txn->Commit();
  }

  delete txn;

  return s;
}

Status PessimisticTransactionDB::Write(const WriteOptions& opts,
                                       WriteBatch* updates) {
  // Need to lock all keys in this batch to prevent write conflicts with
  // concurrent transactions.
  Transaction* txn = BeginInternalTransaction(opts);
  txn->DisableIndexing();

  auto txn_impl =
      static_cast_with_check<PessimisticTransaction, Transaction>(txn);

  // Since commitBatch sorts the keys before locking, concurrent Write()
  // operations will not cause a deadlock.
  // In order to avoid a deadlock with a concurrent Transaction, Transactions
  // should use a lock timeout.
  Status s = txn_impl->CommitBatch(updates);

  delete txn;

  return s;
}

Status WriteCommittedTxnDB::Write(
    const WriteOptions& opts,
    const TransactionDBWriteOptimizations& optimizations, WriteBatch* updates) {
  if (optimizations.skip_concurrency_control) {
    return db_impl_->Write(opts, updates);
  } else {
    return Write(opts, updates);
  }
}

void PessimisticTransactionDB::InsertExpirableTransaction(
    TransactionID tx_id, PessimisticTransaction* tx) {
  assert(tx->GetExpirationTime() > 0);
  std::lock_guard<std::mutex> lock(map_mutex_);
  expirable_transactions_map_.insert({tx_id, tx});
}

void PessimisticTransactionDB::RemoveExpirableTransaction(TransactionID tx_id) {
  std::lock_guard<std::mutex> lock(map_mutex_);
  expirable_transactions_map_.erase(tx_id);
}

bool PessimisticTransactionDB::TryStealingExpiredTransactionLocks(
    TransactionID tx_id) {
  std::lock_guard<std::mutex> lock(map_mutex_);

  auto tx_it = expirable_transactions_map_.find(tx_id);
  if (tx_it == expirable_transactions_map_.end()) {
    return true;
  }
  PessimisticTransaction& tx = *(tx_it->second);
  return tx.TryStealingLocks();
}

void PessimisticTransactionDB::ReinitializeTransaction(
    Transaction* txn, const WriteOptions& write_options,
    const TransactionOptions& txn_options) {
  auto txn_impl =
      static_cast_with_check<PessimisticTransaction, Transaction>(txn);

  txn_impl->Reinitialize(this, write_options, txn_options);
}

Transaction* PessimisticTransactionDB::GetTransactionByName(
    const TransactionName& name) {
  std::lock_guard<std::mutex> lock(name_map_mutex_);
  auto it = transactions_.find(name);
  if (it == transactions_.end()) {
    return nullptr;
  } else {
    return it->second;
  }
}

void PessimisticTransactionDB::GetAllPreparedTransactions(
    std::vector<Transaction*>* transv) {
  assert(transv);
  transv->clear();
  std::lock_guard<std::mutex> lock(name_map_mutex_);
  for (auto it = transactions_.begin(); it != transactions_.end(); it++) {
    if (it->second->GetState() == Transaction::PREPARED) {
      transv->push_back(it->second);
    }
  }
}

TransactionLockMgr::LockStatusData
PessimisticTransactionDB::GetLockStatusData() {
  return lock_mgr_.GetLockStatusData();
}

std::vector<DeadlockPath> PessimisticTransactionDB::GetDeadlockInfoBuffer() {
  return lock_mgr_.GetDeadlockInfoBuffer();
}

void PessimisticTransactionDB::SetDeadlockInfoBufferSize(uint32_t target_size) {
  lock_mgr_.Resize(target_size);
}

void PessimisticTransactionDB::RegisterTransaction(Transaction* txn) {
  assert(txn);
  assert(txn->GetName().length() > 0);
  assert(GetTransactionByName(txn->GetName()) == nullptr);
  assert(txn->GetState() == Transaction::STARTED);
  std::lock_guard<std::mutex> lock(name_map_mutex_);
  transactions_[txn->GetName()] = txn;
}

void PessimisticTransactionDB::UnregisterTransaction(Transaction* txn) {
  assert(txn);
  std::lock_guard<std::mutex> lock(name_map_mutex_);
  auto it = transactions_.find(txn->GetName());
  assert(it != transactions_.end());
  transactions_.erase(it);
}

}  //  namespace rocksdb
#endif  // ROCKSDB_LITE
