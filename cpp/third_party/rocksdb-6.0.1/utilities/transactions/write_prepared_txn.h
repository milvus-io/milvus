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
#include "utilities/transactions/pessimistic_transaction.h"
#include "utilities/transactions/pessimistic_transaction_db.h"
#include "utilities/transactions/transaction_base.h"
#include "utilities/transactions/transaction_util.h"

namespace rocksdb {

class WritePreparedTxnDB;

// This impl could write to DB also uncommitted data and then later tell apart
// committed data from uncommitted data. Uncommitted data could be after the
// Prepare phase in 2PC (WritePreparedTxn) or before that
// (WriteUnpreparedTxnImpl).
class WritePreparedTxn : public PessimisticTransaction {
 public:
  WritePreparedTxn(WritePreparedTxnDB* db, const WriteOptions& write_options,
                   const TransactionOptions& txn_options);

  virtual ~WritePreparedTxn() {}

  // To make WAL commit markers visible, the snapshot will be based on the last
  // seq in the WAL that is also published, LastPublishedSequence, as opposed to
  // the last seq in the memtable.
  using Transaction::Get;
  virtual Status Get(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     PinnableSlice* value) override;

  // To make WAL commit markers visible, the snapshot will be based on the last
  // seq in the WAL that is also published, LastPublishedSequence, as opposed to
  // the last seq in the memtable.
  using Transaction::GetIterator;
  virtual Iterator* GetIterator(const ReadOptions& options) override;
  virtual Iterator* GetIterator(const ReadOptions& options,
                                ColumnFamilyHandle* column_family) override;

  virtual void SetSnapshot() override;

 protected:
  void Initialize(const TransactionOptions& txn_options) override;
  // Override the protected SetId to make it visible to the friend class
  // WritePreparedTxnDB
  inline void SetId(uint64_t id) override { Transaction::SetId(id); }

 private:
  friend class WritePreparedTransactionTest_BasicRecoveryTest_Test;
  friend class WritePreparedTxnDB;
  friend class WriteUnpreparedTxnDB;
  friend class WriteUnpreparedTxn;

  Status PrepareInternal() override;

  Status CommitWithoutPrepareInternal() override;

  Status CommitBatchInternal(WriteBatch* batch, size_t batch_cnt) override;

  // Since the data is already written to memtables at the Prepare phase, the
  // commit entails writing only a commit marker in the WAL. The sequence number
  // of the commit marker is then the commit timestamp of the transaction. To
  // make WAL commit markers visible, the snapshot will be based on the last seq
  // in the WAL that is also published, LastPublishedSequence, as opposed to the
  // last seq in the memtable.
  Status CommitInternal() override;

  Status RollbackInternal() override;

  virtual Status ValidateSnapshot(ColumnFamilyHandle* column_family,
                                  const Slice& key,
                                  SequenceNumber* tracked_at_seq) override;

  virtual Status RebuildFromWriteBatch(WriteBatch* src_batch) override;

  // No copying allowed
  WritePreparedTxn(const WritePreparedTxn&);
  void operator=(const WritePreparedTxn&);

  WritePreparedTxnDB* wpt_db_;
  // Number of sub-batches in prepare
  size_t prepare_batch_cnt_ = 0;
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
