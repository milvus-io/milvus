//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "utilities/transactions/write_prepared_txn_db.h"
#include "utilities/transactions/write_unprepared_txn.h"

namespace rocksdb {

class WriteUnpreparedTxn;

class WriteUnpreparedTxnDB : public WritePreparedTxnDB {
 public:
  using WritePreparedTxnDB::WritePreparedTxnDB;

  Status Initialize(const std::vector<size_t>& compaction_enabled_cf_indices,
                    const std::vector<ColumnFamilyHandle*>& handles) override;

  Transaction* BeginTransaction(const WriteOptions& write_options,
                                const TransactionOptions& txn_options,
                                Transaction* old_txn) override;

  // Struct to hold ownership of snapshot and read callback for cleanup.
  struct IteratorState;

  using WritePreparedTxnDB::NewIterator;
  Iterator* NewIterator(const ReadOptions& options,
                        ColumnFamilyHandle* column_family,
                        WriteUnpreparedTxn* txn);

 private:
  Status RollbackRecoveredTransaction(const DBImpl::RecoveredTransaction* rtxn);
};

class WriteUnpreparedCommitEntryPreReleaseCallback : public PreReleaseCallback {
  // TODO(lth): Reduce code duplication with
  // WritePreparedCommitEntryPreReleaseCallback
 public:
  // includes_data indicates that the commit also writes non-empty
  // CommitTimeWriteBatch to memtable, which needs to be committed separately.
  WriteUnpreparedCommitEntryPreReleaseCallback(
      WritePreparedTxnDB* db, DBImpl* db_impl,
      const std::map<SequenceNumber, size_t>& unprep_seqs,
      size_t data_batch_cnt = 0, bool publish_seq = true)
      : db_(db),
        db_impl_(db_impl),
        unprep_seqs_(unprep_seqs),
        data_batch_cnt_(data_batch_cnt),
        includes_data_(data_batch_cnt_ > 0),
        publish_seq_(publish_seq) {
    assert(unprep_seqs.size() > 0);
  }

  virtual Status Callback(SequenceNumber commit_seq, bool is_mem_disabled
                          __attribute__((__unused__))) override {
    const uint64_t last_commit_seq = LIKELY(data_batch_cnt_ <= 1)
                                         ? commit_seq
                                         : commit_seq + data_batch_cnt_ - 1;
    // Recall that unprep_seqs maps (un)prepared_seq => prepare_batch_cnt.
    for (const auto& s : unprep_seqs_) {
      for (size_t i = 0; i < s.second; i++) {
        db_->AddCommitted(s.first + i, last_commit_seq);
      }
    }

    if (includes_data_) {
      assert(data_batch_cnt_);
      // Commit the data that is accompanied with the commit request
      for (size_t i = 0; i < data_batch_cnt_; i++) {
        // For commit seq of each batch use the commit seq of the last batch.
        // This would make debugging easier by having all the batches having
        // the same sequence number.
        db_->AddCommitted(commit_seq + i, last_commit_seq);
      }
    }
    if (db_impl_->immutable_db_options().two_write_queues && publish_seq_) {
      assert(is_mem_disabled);  // implies the 2nd queue
      // Publish the sequence number. We can do that here assuming the callback
      // is invoked only from one write queue, which would guarantee that the
      // publish sequence numbers will be in order, i.e., once a seq is
      // published all the seq prior to that are also publishable.
      db_impl_->SetLastPublishedSequence(last_commit_seq);
    }
    // else SequenceNumber that is updated as part of the write already does the
    // publishing
    return Status::OK();
  }

 private:
  WritePreparedTxnDB* db_;
  DBImpl* db_impl_;
  const std::map<SequenceNumber, size_t>& unprep_seqs_;
  size_t data_batch_cnt_;
  // Either because it is commit without prepare or it has a
  // CommitTimeWriteBatch
  bool includes_data_;
  // Should the callback also publishes the commit seq number
  bool publish_seq_;
};

class WriteUnpreparedRollbackPreReleaseCallback : public PreReleaseCallback {
  // TODO(lth): Reduce code duplication with
  // WritePreparedCommitEntryPreReleaseCallback
 public:
  WriteUnpreparedRollbackPreReleaseCallback(
      WritePreparedTxnDB* db, DBImpl* db_impl,
      const std::map<SequenceNumber, size_t>& unprep_seqs,
      SequenceNumber rollback_seq)
      : db_(db),
        db_impl_(db_impl),
        unprep_seqs_(unprep_seqs),
        rollback_seq_(rollback_seq) {
    assert(unprep_seqs.size() > 0);
    assert(db_impl_->immutable_db_options().two_write_queues);
  }

  virtual Status Callback(SequenceNumber commit_seq, bool is_mem_disabled
                          __attribute__((__unused__))) override {
    assert(is_mem_disabled);  // implies the 2nd queue
    const uint64_t last_commit_seq = commit_seq;
    db_->AddCommitted(rollback_seq_, last_commit_seq);
    // Recall that unprep_seqs maps (un)prepared_seq => prepare_batch_cnt.
    for (const auto& s : unprep_seqs_) {
      for (size_t i = 0; i < s.second; i++) {
        db_->AddCommitted(s.first + i, last_commit_seq);
      }
    }
    db_impl_->SetLastPublishedSequence(last_commit_seq);
    return Status::OK();
  }

 private:
  WritePreparedTxnDB* db_;
  DBImpl* db_impl_;
  const std::map<SequenceNumber, size_t>& unprep_seqs_;
  SequenceNumber rollback_seq_;
};

struct KeySetBuilder : public WriteBatch::Handler {
  WriteUnpreparedTxn* txn_;
  bool rollback_merge_operands_;

  KeySetBuilder(WriteUnpreparedTxn* txn, bool rollback_merge_operands)
      : txn_(txn), rollback_merge_operands_(rollback_merge_operands) {}

  Status PutCF(uint32_t cf, const Slice& key, const Slice& val) override;

  Status DeleteCF(uint32_t cf, const Slice& key) override;

  Status SingleDeleteCF(uint32_t cf, const Slice& key) override;

  Status MergeCF(uint32_t cf, const Slice& key, const Slice& val) override;

  // Recovered batches do not contain 2PC markers.
  Status MarkNoop(bool) override { return Status::InvalidArgument(); }
  Status MarkBeginPrepare(bool) override { return Status::InvalidArgument(); }
  Status MarkEndPrepare(const Slice&) override {
    return Status::InvalidArgument();
  }
  Status MarkCommit(const Slice&) override { return Status::InvalidArgument(); }
  Status MarkRollback(const Slice&) override {
    return Status::InvalidArgument();
  }
};

}  //  namespace rocksdb
#endif  // ROCKSDB_LITE
