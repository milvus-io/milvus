//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "utilities/transactions/transaction_test.h"
#include "utilities/transactions/write_unprepared_txn.h"
#include "utilities/transactions/write_unprepared_txn_db.h"

namespace rocksdb {

class WriteUnpreparedTransactionTestBase : public TransactionTestBase {
 public:
  WriteUnpreparedTransactionTestBase(bool use_stackable_db,
                                     bool two_write_queue,
                                     TxnDBWritePolicy write_policy)
      : TransactionTestBase(use_stackable_db, two_write_queue, write_policy){}
};

class WriteUnpreparedTransactionTest
    : public WriteUnpreparedTransactionTestBase,
      virtual public ::testing::WithParamInterface<
          std::tuple<bool, bool, TxnDBWritePolicy>> {
 public:
  WriteUnpreparedTransactionTest()
      : WriteUnpreparedTransactionTestBase(std::get<0>(GetParam()),
                                           std::get<1>(GetParam()),
                                           std::get<2>(GetParam())){}
};

INSTANTIATE_TEST_CASE_P(
    WriteUnpreparedTransactionTest, WriteUnpreparedTransactionTest,
    ::testing::Values(std::make_tuple(false, false, WRITE_UNPREPARED),
                      std::make_tuple(false, true, WRITE_UNPREPARED)));

TEST_P(WriteUnpreparedTransactionTest, ReadYourOwnWrite) {
  auto verify_state = [](Iterator* iter, const std::string& key,
                         const std::string& value) {
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(key, iter->key().ToString());
    ASSERT_EQ(value, iter->value().ToString());
  };

  options.disable_auto_compactions = true;
  ReOpen();

  // The following tests checks whether reading your own write for
  // a transaction works for write unprepared, when there are uncommitted
  // values written into DB.
  //
  // Although the values written by DB::Put are technically committed, we add
  // their seq num to unprep_seqs_ to pretend that they were written into DB
  // as part of an unprepared batch, and then check if they are visible to the
  // transaction.
  auto snapshot0 = db->GetSnapshot();
  ASSERT_OK(db->Put(WriteOptions(), "a", "v1"));
  ASSERT_OK(db->Put(WriteOptions(), "b", "v2"));
  auto snapshot2 = db->GetSnapshot();
  ASSERT_OK(db->Put(WriteOptions(), "a", "v3"));
  ASSERT_OK(db->Put(WriteOptions(), "b", "v4"));
  auto snapshot4 = db->GetSnapshot();
  ASSERT_OK(db->Put(WriteOptions(), "a", "v5"));
  ASSERT_OK(db->Put(WriteOptions(), "b", "v6"));
  auto snapshot6 = db->GetSnapshot();
  ASSERT_OK(db->Put(WriteOptions(), "a", "v7"));
  ASSERT_OK(db->Put(WriteOptions(), "b", "v8"));
  auto snapshot8 = db->GetSnapshot();

  TransactionOptions txn_options;
  WriteOptions write_options;
  Transaction* txn = db->BeginTransaction(write_options, txn_options);
  WriteUnpreparedTxn* wup_txn = dynamic_cast<WriteUnpreparedTxn*>(txn);

  ReadOptions roptions;
  roptions.snapshot = snapshot0;

  auto iter = txn->GetIterator(roptions);

  // Test Get().
  std::string value;
  wup_txn->unprep_seqs_[snapshot2->GetSequenceNumber() + 1] =
      snapshot4->GetSequenceNumber() - snapshot2->GetSequenceNumber();

  ASSERT_OK(txn->Get(roptions, Slice("a"), &value));
  ASSERT_EQ(value, "v3");

  ASSERT_OK(txn->Get(roptions, Slice("b"), &value));
  ASSERT_EQ(value, "v4");

  wup_txn->unprep_seqs_[snapshot6->GetSequenceNumber() + 1] =
      snapshot8->GetSequenceNumber() - snapshot6->GetSequenceNumber();

  ASSERT_OK(txn->Get(roptions, Slice("a"), &value));
  ASSERT_EQ(value, "v7");

  ASSERT_OK(txn->Get(roptions, Slice("b"), &value));
  ASSERT_EQ(value, "v8");

  wup_txn->unprep_seqs_.clear();

  // Test Next().
  wup_txn->unprep_seqs_[snapshot2->GetSequenceNumber() + 1] =
      snapshot4->GetSequenceNumber() - snapshot2->GetSequenceNumber();

  iter->Seek("a");
  verify_state(iter, "a", "v3");

  iter->Next();
  verify_state(iter, "b", "v4");

  iter->SeekToFirst();
  verify_state(iter, "a", "v3");

  iter->Next();
  verify_state(iter, "b", "v4");

  wup_txn->unprep_seqs_[snapshot6->GetSequenceNumber() + 1] =
      snapshot8->GetSequenceNumber() - snapshot6->GetSequenceNumber();

  iter->Seek("a");
  verify_state(iter, "a", "v7");

  iter->Next();
  verify_state(iter, "b", "v8");

  iter->SeekToFirst();
  verify_state(iter, "a", "v7");

  iter->Next();
  verify_state(iter, "b", "v8");

  wup_txn->unprep_seqs_.clear();

  // Test Prev(). For Prev(), we need to adjust the snapshot to match what is
  // possible in WriteUnpreparedTxn.
  //
  // Because of row locks and ValidateSnapshot, there cannot be any committed
  // entries after snapshot, but before the first prepared key.
  delete iter;
  roptions.snapshot = snapshot2;
  iter = txn->GetIterator(roptions);
  wup_txn->unprep_seqs_[snapshot2->GetSequenceNumber() + 1] =
      snapshot4->GetSequenceNumber() - snapshot2->GetSequenceNumber();

  iter->SeekForPrev("b");
  verify_state(iter, "b", "v4");

  iter->Prev();
  verify_state(iter, "a", "v3");

  iter->SeekToLast();
  verify_state(iter, "b", "v4");

  iter->Prev();
  verify_state(iter, "a", "v3");

  delete iter;
  roptions.snapshot = snapshot6;
  iter = txn->GetIterator(roptions);
  wup_txn->unprep_seqs_[snapshot6->GetSequenceNumber() + 1] =
      snapshot8->GetSequenceNumber() - snapshot6->GetSequenceNumber();

  iter->SeekForPrev("b");
  verify_state(iter, "b", "v8");

  iter->Prev();
  verify_state(iter, "a", "v7");

  iter->SeekToLast();
  verify_state(iter, "b", "v8");

  iter->Prev();
  verify_state(iter, "a", "v7");

  // Since the unprep_seqs_ data were faked for testing, we do not want the
  // destructor for the transaction to be rolling back data that did not
  // exist.
  wup_txn->unprep_seqs_.clear();

  db->ReleaseSnapshot(snapshot0);
  db->ReleaseSnapshot(snapshot2);
  db->ReleaseSnapshot(snapshot4);
  db->ReleaseSnapshot(snapshot6);
  db->ReleaseSnapshot(snapshot8);
  delete iter;
  delete txn;
}

// This tests how write unprepared behaves during recovery when the DB crashes
// after a transaction has either been unprepared or prepared, and tests if
// the changes are correctly applied for prepared transactions if we decide to
// rollback/commit.
TEST_P(WriteUnpreparedTransactionTest, RecoveryTest) {
  WriteOptions write_options;
  write_options.disableWAL = false;
  TransactionOptions txn_options;
  std::vector<Transaction*> prepared_trans;
  WriteUnpreparedTxnDB* wup_db;
  options.disable_auto_compactions = true;

  enum Action { UNPREPARED, ROLLBACK, COMMIT };

  // batch_size of 1 causes writes to DB for every marker.
  for (size_t batch_size : {1, 1000000}) {
    txn_options.max_write_batch_size = batch_size;
    for (bool empty : {true, false}) {
      for (Action a : {UNPREPARED, ROLLBACK, COMMIT}) {
        for (int num_batches = 1; num_batches < 10; num_batches++) {
          // Reset database.
          prepared_trans.clear();
          ReOpen();
          wup_db = dynamic_cast<WriteUnpreparedTxnDB*>(db);
          if (!empty) {
            for (int i = 0; i < num_batches; i++) {
              ASSERT_OK(db->Put(WriteOptions(), "k" + ToString(i),
                                "before value" + ToString(i)));
            }
          }

          // Write num_batches unprepared batches.
          Transaction* txn = db->BeginTransaction(write_options, txn_options);
          WriteUnpreparedTxn* wup_txn = dynamic_cast<WriteUnpreparedTxn*>(txn);
          txn->SetName("xid");
          for (int i = 0; i < num_batches; i++) {
            ASSERT_OK(txn->Put("k" + ToString(i), "value" + ToString(i)));
            if (txn_options.max_write_batch_size == 1) {
              ASSERT_EQ(wup_txn->GetUnpreparedSequenceNumbers().size(), i + 1);
            } else {
              ASSERT_EQ(wup_txn->GetUnpreparedSequenceNumbers().size(), 0);
            }
          }
          if (a == UNPREPARED) {
            // This is done to prevent the destructor from rolling back the
            // transaction for us, since we want to pretend we crashed and
            // test that recovery does the rollback.
            wup_txn->unprep_seqs_.clear();
          } else {
            txn->Prepare();
          }
          delete txn;

          // Crash and run recovery code paths.
          wup_db->db_impl_->FlushWAL(true);
          wup_db->TEST_Crash();
          ReOpenNoDelete();
          assert(db != nullptr);

          db->GetAllPreparedTransactions(&prepared_trans);
          ASSERT_EQ(prepared_trans.size(), a == UNPREPARED ? 0 : 1);
          if (a == ROLLBACK) {
            ASSERT_OK(prepared_trans[0]->Rollback());
            delete prepared_trans[0];
          } else if (a == COMMIT) {
            ASSERT_OK(prepared_trans[0]->Commit());
            delete prepared_trans[0];
          }

          Iterator* iter = db->NewIterator(ReadOptions());
          iter->SeekToFirst();
          // Check that DB has before values.
          if (!empty || a == COMMIT) {
            for (int i = 0; i < num_batches; i++) {
              ASSERT_TRUE(iter->Valid());
              ASSERT_EQ(iter->key().ToString(), "k" + ToString(i));
              if (a == COMMIT) {
                ASSERT_EQ(iter->value().ToString(), "value" + ToString(i));
              } else {
                ASSERT_EQ(iter->value().ToString(),
                          "before value" + ToString(i));
              }
              iter->Next();
            }
          }
          ASSERT_FALSE(iter->Valid());
          delete iter;
        }
      }
    }
  }
}

// Basic test to see that unprepared batch gets written to DB when batch size
// is exceeded. It also does some basic checks to see if commit/rollback works
// as expected for write unprepared.
TEST_P(WriteUnpreparedTransactionTest, UnpreparedBatch) {
  WriteOptions write_options;
  TransactionOptions txn_options;
  const int kNumKeys = 10;

  // batch_size of 1 causes writes to DB for every marker.
  for (size_t batch_size : {1, 1000000}) {
    txn_options.max_write_batch_size = batch_size;
    for (bool prepare : {false, true}) {
      for (bool commit : {false, true}) {
        ReOpen();
        Transaction* txn = db->BeginTransaction(write_options, txn_options);
        WriteUnpreparedTxn* wup_txn = dynamic_cast<WriteUnpreparedTxn*>(txn);
        txn->SetName("xid");

        for (int i = 0; i < kNumKeys; i++) {
          txn->Put("k" + ToString(i), "v" + ToString(i));
          if (txn_options.max_write_batch_size == 1) {
            ASSERT_EQ(wup_txn->GetUnpreparedSequenceNumbers().size(), i + 1);
          } else {
            ASSERT_EQ(wup_txn->GetUnpreparedSequenceNumbers().size(), 0);
          }
        }

        if (prepare) {
          ASSERT_OK(txn->Prepare());
        }

        Iterator* iter = db->NewIterator(ReadOptions());
        iter->SeekToFirst();
        assert(!iter->Valid());
        ASSERT_FALSE(iter->Valid());
        delete iter;

        if (commit) {
          ASSERT_OK(txn->Commit());
        } else {
          ASSERT_OK(txn->Rollback());
        }
        delete txn;

        iter = db->NewIterator(ReadOptions());
        iter->SeekToFirst();

        for (int i = 0; i < (commit ? kNumKeys : 0); i++) {
          ASSERT_TRUE(iter->Valid());
          ASSERT_EQ(iter->key().ToString(), "k" + ToString(i));
          ASSERT_EQ(iter->value().ToString(), "v" + ToString(i));
          iter->Next();
        }
        ASSERT_FALSE(iter->Valid());
        delete iter;
      }
    }
  }
}

// Test whether logs containing unprepared/prepared batches are kept even
// after memtable finishes flushing, and whether they are removed when
// transaction commits/aborts.
//
// TODO(lth): Merge with TransactionTest/TwoPhaseLogRollingTest tests.
TEST_P(WriteUnpreparedTransactionTest, MarkLogWithPrepSection) {
  WriteOptions write_options;
  TransactionOptions txn_options;
  // batch_size of 1 causes writes to DB for every marker.
  txn_options.max_write_batch_size = 1;
  const int kNumKeys = 10;

  WriteOptions wopts;
  wopts.sync = true;

  for (bool prepare : {false, true}) {
    for (bool commit : {false, true}) {
      ReOpen();
      auto wup_db = dynamic_cast<WriteUnpreparedTxnDB*>(db);
      auto db_impl = wup_db->db_impl_;

      Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
      ASSERT_OK(txn1->SetName("xid1"));

      Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
      ASSERT_OK(txn2->SetName("xid2"));

      // Spread this transaction across multiple log files.
      for (int i = 0; i < kNumKeys; i++) {
        ASSERT_OK(txn1->Put("k1" + ToString(i), "v" + ToString(i)));
        if (i >= kNumKeys / 2) {
          ASSERT_OK(txn2->Put("k2" + ToString(i), "v" + ToString(i)));
        }

        if (i > 0) {
          db_impl->TEST_SwitchWAL();
        }
      }

      ASSERT_GT(txn1->GetLogNumber(), 0);
      ASSERT_GT(txn2->GetLogNumber(), 0);

      ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(),
                txn1->GetLogNumber());
      ASSERT_GT(db_impl->TEST_LogfileNumber(), txn1->GetLogNumber());

      if (prepare) {
        ASSERT_OK(txn1->Prepare());
        ASSERT_OK(txn2->Prepare());
      }

      ASSERT_GE(db_impl->TEST_LogfileNumber(), txn1->GetLogNumber());
      ASSERT_GE(db_impl->TEST_LogfileNumber(), txn2->GetLogNumber());

      ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(),
                txn1->GetLogNumber());
      if (commit) {
        ASSERT_OK(txn1->Commit());
      } else {
        ASSERT_OK(txn1->Rollback());
      }

      ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(),
                txn2->GetLogNumber());

      if (commit) {
        ASSERT_OK(txn2->Commit());
      } else {
        ASSERT_OK(txn2->Rollback());
      }

      ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(), 0);

      delete txn1;
      delete txn2;
    }
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr,
          "SKIPPED as Transactions are not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // ROCKSDB_LITE
