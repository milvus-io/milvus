// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include <set>

#include "utilities/transactions/write_prepared_txn.h"
#include "utilities/transactions/write_unprepared_txn_db.h"

namespace rocksdb {

class WriteUnpreparedTxnDB;
class WriteUnpreparedTxn;

class WriteUnpreparedTxnReadCallback : public ReadCallback {
 public:
  WriteUnpreparedTxnReadCallback(WritePreparedTxnDB* db,
                                 SequenceNumber snapshot,
                                 SequenceNumber min_uncommitted,
                                 WriteUnpreparedTxn* txn)
      : db_(db),
        snapshot_(snapshot),
        min_uncommitted_(min_uncommitted),
        txn_(txn) {}

  virtual bool IsVisible(SequenceNumber seq) override;
  virtual SequenceNumber MaxUnpreparedSequenceNumber() override;

 private:
  WritePreparedTxnDB* db_;
  SequenceNumber snapshot_;
  SequenceNumber min_uncommitted_;
  WriteUnpreparedTxn* txn_;
};

class WriteUnpreparedTxn : public WritePreparedTxn {
 public:
  WriteUnpreparedTxn(WriteUnpreparedTxnDB* db,
                     const WriteOptions& write_options,
                     const TransactionOptions& txn_options);

  virtual ~WriteUnpreparedTxn();

  using TransactionBaseImpl::Put;
  virtual Status Put(ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& value,
                     const bool assume_tracked = false) override;
  virtual Status Put(ColumnFamilyHandle* column_family, const SliceParts& key,
                     const SliceParts& value,
                     const bool assume_tracked = false) override;

  using TransactionBaseImpl::Merge;
  virtual Status Merge(ColumnFamilyHandle* column_family, const Slice& key,
                       const Slice& value,
                       const bool assume_tracked = false) override;

  using TransactionBaseImpl::Delete;
  virtual Status Delete(ColumnFamilyHandle* column_family, const Slice& key,
                        const bool assume_tracked = false) override;
  virtual Status Delete(ColumnFamilyHandle* column_family,
                        const SliceParts& key,
                        const bool assume_tracked = false) override;

  using TransactionBaseImpl::SingleDelete;
  virtual Status SingleDelete(ColumnFamilyHandle* column_family,
                              const Slice& key,
                              const bool assume_tracked = false) override;
  virtual Status SingleDelete(ColumnFamilyHandle* column_family,
                              const SliceParts& key,
                              const bool assume_tracked = false) override;

  virtual Status RebuildFromWriteBatch(WriteBatch*) override {
    // This function was only useful for recovering prepared transactions, but
    // is unused for write prepared because a transaction may consist of
    // multiple write batches.
    //
    // If there are use cases outside of recovery that can make use of this,
    // then support could be added.
    return Status::NotSupported("Not supported for WriteUnprepared");
  }

  const std::map<SequenceNumber, size_t>& GetUnpreparedSequenceNumbers();

  void UpdateWriteKeySet(uint32_t cfid, const Slice& key);

 protected:
  void Initialize(const TransactionOptions& txn_options) override;

  Status PrepareInternal() override;

  Status CommitWithoutPrepareInternal() override;
  Status CommitInternal() override;

  Status RollbackInternal() override;

  // Get and GetIterator needs to be overridden so that a ReadCallback to
  // handle read-your-own-write is used.
  using Transaction::Get;
  virtual Status Get(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     PinnableSlice* value) override;

  using Transaction::GetIterator;
  virtual Iterator* GetIterator(const ReadOptions& options) override;
  virtual Iterator* GetIterator(const ReadOptions& options,
                                ColumnFamilyHandle* column_family) override;

 private:
  friend class WriteUnpreparedTransactionTest_ReadYourOwnWrite_Test;
  friend class WriteUnpreparedTransactionTest_RecoveryTest_Test;
  friend class WriteUnpreparedTransactionTest_UnpreparedBatch_Test;
  friend class WriteUnpreparedTxnDB;

  Status MaybeFlushWriteBatchToDB();
  Status FlushWriteBatchToDB(bool prepared);

  // For write unprepared, we check on every writebatch append to see if
  // max_write_batch_size_ has been exceeded, and then call
  // FlushWriteBatchToDB if so. This logic is encapsulated in
  // MaybeFlushWriteBatchToDB.
  size_t max_write_batch_size_;
  WriteUnpreparedTxnDB* wupt_db_;

  // Ordered list of unprep_seq sequence numbers that we have already written
  // to DB.
  //
  // This maps unprep_seq => prepare_batch_cnt for each unprepared batch
  // written by this transaction.
  //
  // Note that this contains both prepared and unprepared batches, since they
  // are treated similarily in prepare heap/commit map, so it simplifies the
  // commit callbacks.
  std::map<SequenceNumber, size_t> unprep_seqs_;

  // Set of keys that have written to that have already been written to DB
  // (ie. not in write_batch_).
  //
  std::map<uint32_t, std::vector<std::string>> write_set_keys_;
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
