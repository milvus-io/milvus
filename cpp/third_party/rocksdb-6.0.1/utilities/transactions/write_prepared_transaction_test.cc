//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "utilities/transactions/transaction_test.h"

#include <inttypes.h>
#include <algorithm>
#include <atomic>
#include <functional>
#include <string>
#include <thread>

#include "db/db_impl.h"
#include "db/dbformat.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/debug.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "table/mock_table.h"
#include "util/fault_injection_test_env.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "util/transaction_test_util.h"
#include "utilities/merge_operators.h"
#include "utilities/merge_operators/string_append/stringappend.h"
#include "utilities/transactions/pessimistic_transaction_db.h"
#include "utilities/transactions/write_prepared_txn_db.h"

#include "port/port.h"

using std::string;

namespace rocksdb {

using CommitEntry = WritePreparedTxnDB::CommitEntry;
using CommitEntry64b = WritePreparedTxnDB::CommitEntry64b;
using CommitEntry64bFormat = WritePreparedTxnDB::CommitEntry64bFormat;

TEST(PreparedHeap, BasicsTest) {
  WritePreparedTxnDB::PreparedHeap heap;
  heap.push(14l);
  // Test with one element
  ASSERT_EQ(14l, heap.top());
  heap.push(24l);
  heap.push(34l);
  // Test that old min is still on top
  ASSERT_EQ(14l, heap.top());
  heap.push(13l);
  // Test that the new min will be on top
  ASSERT_EQ(13l, heap.top());
  // Test that it is persistent
  ASSERT_EQ(13l, heap.top());
  heap.push(44l);
  heap.push(54l);
  heap.push(64l);
  heap.push(74l);
  heap.push(84l);
  // Test that old min is still on top
  ASSERT_EQ(13l, heap.top());
  heap.erase(24l);
  // Test that old min is still on top
  ASSERT_EQ(13l, heap.top());
  heap.erase(14l);
  // Test that old min is still on top
  ASSERT_EQ(13l, heap.top());
  heap.erase(13l);
  // Test that the new comes to the top after multiple erase
  ASSERT_EQ(34l, heap.top());
  heap.erase(34l);
  // Test that the new comes to the top after single erase
  ASSERT_EQ(44l, heap.top());
  heap.erase(54l);
  ASSERT_EQ(44l, heap.top());
  heap.pop();  // pop 44l
  // Test that the erased items are ignored after pop
  ASSERT_EQ(64l, heap.top());
  heap.erase(44l);
  // Test that erasing an already popped item would work
  ASSERT_EQ(64l, heap.top());
  heap.erase(84l);
  ASSERT_EQ(64l, heap.top());
  heap.push(85l);
  heap.push(86l);
  heap.push(87l);
  heap.push(88l);
  heap.push(89l);
  heap.erase(87l);
  heap.erase(85l);
  heap.erase(89l);
  heap.erase(86l);
  heap.erase(88l);
  // Test top remains the same after a random order of many erases
  ASSERT_EQ(64l, heap.top());
  heap.pop();
  // Test that pop works with a series of random pending erases
  ASSERT_EQ(74l, heap.top());
  ASSERT_FALSE(heap.empty());
  heap.pop();
  // Test that empty works
  ASSERT_TRUE(heap.empty());
}

// This is a scenario reconstructed from a buggy trace. Test that the bug does
// not resurface again.
TEST(PreparedHeap, EmptyAtTheEnd) {
  WritePreparedTxnDB::PreparedHeap heap;
  heap.push(40l);
  ASSERT_EQ(40l, heap.top());
  // Although not a recommended scenario, we must be resilient against erase
  // without a prior push.
  heap.erase(50l);
  ASSERT_EQ(40l, heap.top());
  heap.push(60l);
  ASSERT_EQ(40l, heap.top());

  heap.erase(60l);
  ASSERT_EQ(40l, heap.top());
  heap.erase(40l);
  ASSERT_TRUE(heap.empty());

  heap.push(40l);
  ASSERT_EQ(40l, heap.top());
  heap.erase(50l);
  ASSERT_EQ(40l, heap.top());
  heap.push(60l);
  ASSERT_EQ(40l, heap.top());

  heap.erase(40l);
  // Test that the erase has not emptied the heap (we had a bug doing that)
  ASSERT_FALSE(heap.empty());
  ASSERT_EQ(60l, heap.top());
  heap.erase(60l);
  ASSERT_TRUE(heap.empty());
}

// Generate random order of PreparedHeap access and test that the heap will be
// successfully emptied at the end.
TEST(PreparedHeap, Concurrent) {
  const size_t t_cnt = 10;
  rocksdb::port::Thread t[t_cnt];
  Random rnd(1103);
  WritePreparedTxnDB::PreparedHeap heap;
  port::RWMutex prepared_mutex;

  for (size_t n = 0; n < 100; n++) {
    for (size_t i = 0; i < t_cnt; i++) {
      // This is not recommended usage but we should be resilient against it.
      bool skip_push = rnd.OneIn(5);
      t[i] = rocksdb::port::Thread([&heap, &prepared_mutex, skip_push, i]() {
        auto seq = i;
        std::this_thread::yield();
        if (!skip_push) {
          WriteLock wl(&prepared_mutex);
          heap.push(seq);
        }
        std::this_thread::yield();
        {
          WriteLock wl(&prepared_mutex);
          heap.erase(seq);
        }
      });
    }
    for (size_t i = 0; i < t_cnt; i++) {
      t[i].join();
    }
    ASSERT_TRUE(heap.empty());
  }
}

// Test that WriteBatchWithIndex correctly counts the number of sub-batches
TEST(WriteBatchWithIndex, SubBatchCnt) {
  ColumnFamilyOptions cf_options;
  std::string cf_name = "two";
  DB* db;
  Options options;
  options.create_if_missing = true;
  const std::string dbname = test::PerThreadDBPath("transaction_testdb");
  DestroyDB(dbname, options);
  ASSERT_OK(DB::Open(options, dbname, &db));
  ColumnFamilyHandle* cf_handle = nullptr;
  ASSERT_OK(db->CreateColumnFamily(cf_options, cf_name, &cf_handle));
  WriteOptions write_options;
  size_t batch_cnt = 1;
  size_t save_points = 0;
  std::vector<size_t> batch_cnt_at;
  WriteBatchWithIndex batch(db->DefaultColumnFamily()->GetComparator(), 0, true,
                            0);
  ASSERT_EQ(batch_cnt, batch.SubBatchCnt());
  batch_cnt_at.push_back(batch_cnt);
  batch.SetSavePoint();
  save_points++;
  batch.Put(Slice("key"), Slice("value"));
  ASSERT_EQ(batch_cnt, batch.SubBatchCnt());
  batch_cnt_at.push_back(batch_cnt);
  batch.SetSavePoint();
  save_points++;
  batch.Put(Slice("key2"), Slice("value2"));
  ASSERT_EQ(batch_cnt, batch.SubBatchCnt());
  // duplicate the keys
  batch_cnt_at.push_back(batch_cnt);
  batch.SetSavePoint();
  save_points++;
  batch.Put(Slice("key"), Slice("value3"));
  batch_cnt++;
  ASSERT_EQ(batch_cnt, batch.SubBatchCnt());
  // duplicate the 2nd key. It should not be counted duplicate since a
  // sub-patch is cut after the last duplicate.
  batch_cnt_at.push_back(batch_cnt);
  batch.SetSavePoint();
  save_points++;
  batch.Put(Slice("key2"), Slice("value4"));
  ASSERT_EQ(batch_cnt, batch.SubBatchCnt());
  // duplicate the keys but in a different cf. It should not be counted as
  // duplicate keys
  batch_cnt_at.push_back(batch_cnt);
  batch.SetSavePoint();
  save_points++;
  batch.Put(cf_handle, Slice("key"), Slice("value5"));
  ASSERT_EQ(batch_cnt, batch.SubBatchCnt());

  // Test that the number of sub-batches matches what we count with
  // SubBatchCounter
  std::map<uint32_t, const Comparator*> comparators;
  comparators[0] = db->DefaultColumnFamily()->GetComparator();
  comparators[cf_handle->GetID()] = cf_handle->GetComparator();
  SubBatchCounter counter(comparators);
  ASSERT_OK(batch.GetWriteBatch()->Iterate(&counter));
  ASSERT_EQ(batch_cnt, counter.BatchCount());

  // Test that RollbackToSavePoint will properly resets the number of
  // sub-batches
  for (size_t i = save_points; i > 0; i--) {
    batch.RollbackToSavePoint();
    ASSERT_EQ(batch_cnt_at[i - 1], batch.SubBatchCnt());
  }

  // Test the count is right with random batches
  {
    const size_t TOTAL_KEYS = 20;  // 20 ~= 10 to cause a few randoms
    Random rnd(1131);
    std::string keys[TOTAL_KEYS];
    for (size_t k = 0; k < TOTAL_KEYS; k++) {
      int len = static_cast<int>(rnd.Uniform(50));
      keys[k] = test::RandomKey(&rnd, len);
    }
    for (size_t i = 0; i < 1000; i++) {  // 1000 random batches
      WriteBatchWithIndex rndbatch(db->DefaultColumnFamily()->GetComparator(),
                                   0, true, 0);
      for (size_t k = 0; k < 10; k++) {  // 10 key per batch
        size_t ki = static_cast<size_t>(rnd.Uniform(TOTAL_KEYS));
        Slice key = Slice(keys[ki]);
        std::string buffer;
        Slice value = Slice(test::RandomString(&rnd, 16, &buffer));
        rndbatch.Put(key, value);
      }
      SubBatchCounter batch_counter(comparators);
      ASSERT_OK(rndbatch.GetWriteBatch()->Iterate(&batch_counter));
      ASSERT_EQ(rndbatch.SubBatchCnt(), batch_counter.BatchCount());
    }
  }

  delete cf_handle;
  delete db;
}

TEST(CommitEntry64b, BasicTest) {
  const size_t INDEX_BITS = static_cast<size_t>(21);
  const size_t INDEX_SIZE = static_cast<size_t>(1ull << INDEX_BITS);
  const CommitEntry64bFormat FORMAT(static_cast<size_t>(INDEX_BITS));

  // zero-initialized CommitEntry64b should indicate an empty entry
  CommitEntry64b empty_entry64b;
  uint64_t empty_index = 11ul;
  CommitEntry empty_entry;
  bool ok = empty_entry64b.Parse(empty_index, &empty_entry, FORMAT);
  ASSERT_FALSE(ok);

  // the zero entry is reserved for un-initialized entries
  const size_t MAX_COMMIT = (1 << FORMAT.COMMIT_BITS) - 1 - 1;
  // Samples over the numbers that are covered by that many index bits
  std::array<uint64_t, 4> is = {{0, 1, INDEX_SIZE / 2 + 1, INDEX_SIZE - 1}};
  // Samples over the numbers that are covered by that many commit bits
  std::array<uint64_t, 4> ds = {{0, 1, MAX_COMMIT / 2 + 1, MAX_COMMIT}};
  // Iterate over prepare numbers that have i) cover all bits of a sequence
  // number, and ii) include some bits that fall into the range of index or
  // commit bits
  for (uint64_t base = 1; base < kMaxSequenceNumber; base *= 2) {
    for (uint64_t i : is) {
      for (uint64_t d : ds) {
        uint64_t p = base + i + d;
        for (uint64_t c : {p, p + d / 2, p + d}) {
          uint64_t index = p % INDEX_SIZE;
          CommitEntry before(p, c), after;
          CommitEntry64b entry64b(before, FORMAT);
          ok = entry64b.Parse(index, &after, FORMAT);
          ASSERT_TRUE(ok);
          if (!(before == after)) {
            printf("base %" PRIu64 " i %" PRIu64 " d %" PRIu64 " p %" PRIu64
                   " c %" PRIu64 " index %" PRIu64 "\n",
                   base, i, d, p, c, index);
          }
          ASSERT_EQ(before, after);
        }
      }
    }
  }
}

class WritePreparedTxnDBMock : public WritePreparedTxnDB {
 public:
  WritePreparedTxnDBMock(DBImpl* db_impl, TransactionDBOptions& opt)
      : WritePreparedTxnDB(db_impl, opt) {}
  void SetDBSnapshots(const std::vector<SequenceNumber>& snapshots) {
    snapshots_ = snapshots;
  }
  void TakeSnapshot(SequenceNumber seq) { snapshots_.push_back(seq); }

 protected:
  const std::vector<SequenceNumber> GetSnapshotListFromDB(
      SequenceNumber /* unused */) override {
    return snapshots_;
  }

 private:
  std::vector<SequenceNumber> snapshots_;
};

class WritePreparedTransactionTestBase : public TransactionTestBase {
 public:
  WritePreparedTransactionTestBase(bool use_stackable_db, bool two_write_queue,
                                   TxnDBWritePolicy write_policy)
      : TransactionTestBase(use_stackable_db, two_write_queue, write_policy){};

 protected:
  void UpdateTransactionDBOptions(size_t snapshot_cache_bits,
                                  size_t commit_cache_bits) {
    txn_db_options.wp_snapshot_cache_bits = snapshot_cache_bits;
    txn_db_options.wp_commit_cache_bits = commit_cache_bits;
  }
  void UpdateTransactionDBOptions(size_t snapshot_cache_bits) {
    txn_db_options.wp_snapshot_cache_bits = snapshot_cache_bits;
  }
  // If expect_update is set, check if it actually updated old_commit_map_. If
  // it did not and yet suggested not to check the next snapshot, do the
  // opposite to check if it was not a bad suggestion.
  void MaybeUpdateOldCommitMapTestWithNext(uint64_t prepare, uint64_t commit,
                                           uint64_t snapshot,
                                           uint64_t next_snapshot,
                                           bool expect_update) {
    WritePreparedTxnDB* wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
    // reset old_commit_map_empty_ so that its value indicate whether
    // old_commit_map_ was updated
    wp_db->old_commit_map_empty_ = true;
    bool check_next = wp_db->MaybeUpdateOldCommitMap(prepare, commit, snapshot,
                                                     snapshot < next_snapshot);
    if (expect_update == wp_db->old_commit_map_empty_) {
      printf("prepare: %" PRIu64 " commit: %" PRIu64 " snapshot: %" PRIu64
             " next: %" PRIu64 "\n",
             prepare, commit, snapshot, next_snapshot);
    }
    EXPECT_EQ(!expect_update, wp_db->old_commit_map_empty_);
    if (!check_next && wp_db->old_commit_map_empty_) {
      // do the opposite to make sure it was not a bad suggestion
      const bool dont_care_bool = true;
      wp_db->MaybeUpdateOldCommitMap(prepare, commit, next_snapshot,
                                     dont_care_bool);
      if (!wp_db->old_commit_map_empty_) {
        printf("prepare: %" PRIu64 " commit: %" PRIu64 " snapshot: %" PRIu64
               " next: %" PRIu64 "\n",
               prepare, commit, snapshot, next_snapshot);
      }
      EXPECT_TRUE(wp_db->old_commit_map_empty_);
    }
  }

  // Test that a CheckAgainstSnapshots thread reading old_snapshots will not
  // miss a snapshot because of a concurrent update by UpdateSnapshots that is
  // writing new_snapshots. Both threads are broken at two points. The sync
  // points to enforce them are specified by a1, a2, b1, and b2. CommitEntry
  // entry is expected to be vital for one of the snapshots that is common
  // between the old and new list of snapshots.
  void SnapshotConcurrentAccessTestInternal(
      WritePreparedTxnDB* wp_db,
      const std::vector<SequenceNumber>& old_snapshots,
      const std::vector<SequenceNumber>& new_snapshots, CommitEntry& entry,
      SequenceNumber& version, size_t a1, size_t a2, size_t b1, size_t b2) {
    // First reset the snapshot list
    const std::vector<SequenceNumber> empty_snapshots;
    wp_db->old_commit_map_empty_ = true;
    wp_db->UpdateSnapshots(empty_snapshots, ++version);
    // Then initialize it with the old_snapshots
    wp_db->UpdateSnapshots(old_snapshots, ++version);

    // Starting from the first thread, cut each thread at two points
    rocksdb::SyncPoint::GetInstance()->LoadDependency({
        {"WritePreparedTxnDB::CheckAgainstSnapshots:p:" + std::to_string(a1),
         "WritePreparedTxnDB::UpdateSnapshots:s:start"},
        {"WritePreparedTxnDB::UpdateSnapshots:p:" + std::to_string(b1),
         "WritePreparedTxnDB::CheckAgainstSnapshots:s:" + std::to_string(a1)},
        {"WritePreparedTxnDB::CheckAgainstSnapshots:p:" + std::to_string(a2),
         "WritePreparedTxnDB::UpdateSnapshots:s:" + std::to_string(b1)},
        {"WritePreparedTxnDB::UpdateSnapshots:p:" + std::to_string(b2),
         "WritePreparedTxnDB::CheckAgainstSnapshots:s:" + std::to_string(a2)},
        {"WritePreparedTxnDB::CheckAgainstSnapshots:p:end",
         "WritePreparedTxnDB::UpdateSnapshots:s:" + std::to_string(b2)},
    });
    rocksdb::SyncPoint::GetInstance()->EnableProcessing();
    {
      ASSERT_TRUE(wp_db->old_commit_map_empty_);
      rocksdb::port::Thread t1(
          [&]() { wp_db->UpdateSnapshots(new_snapshots, version); });
      rocksdb::port::Thread t2([&]() { wp_db->CheckAgainstSnapshots(entry); });
      t1.join();
      t2.join();
      ASSERT_FALSE(wp_db->old_commit_map_empty_);
    }
    rocksdb::SyncPoint::GetInstance()->DisableProcessing();

    wp_db->old_commit_map_empty_ = true;
    wp_db->UpdateSnapshots(empty_snapshots, ++version);
    wp_db->UpdateSnapshots(old_snapshots, ++version);
    // Starting from the second thread, cut each thread at two points
    rocksdb::SyncPoint::GetInstance()->LoadDependency({
        {"WritePreparedTxnDB::UpdateSnapshots:p:" + std::to_string(a1),
         "WritePreparedTxnDB::CheckAgainstSnapshots:s:start"},
        {"WritePreparedTxnDB::CheckAgainstSnapshots:p:" + std::to_string(b1),
         "WritePreparedTxnDB::UpdateSnapshots:s:" + std::to_string(a1)},
        {"WritePreparedTxnDB::UpdateSnapshots:p:" + std::to_string(a2),
         "WritePreparedTxnDB::CheckAgainstSnapshots:s:" + std::to_string(b1)},
        {"WritePreparedTxnDB::CheckAgainstSnapshots:p:" + std::to_string(b2),
         "WritePreparedTxnDB::UpdateSnapshots:s:" + std::to_string(a2)},
        {"WritePreparedTxnDB::UpdateSnapshots:p:end",
         "WritePreparedTxnDB::CheckAgainstSnapshots:s:" + std::to_string(b2)},
    });
    rocksdb::SyncPoint::GetInstance()->EnableProcessing();
    {
      ASSERT_TRUE(wp_db->old_commit_map_empty_);
      rocksdb::port::Thread t1(
          [&]() { wp_db->UpdateSnapshots(new_snapshots, version); });
      rocksdb::port::Thread t2([&]() { wp_db->CheckAgainstSnapshots(entry); });
      t1.join();
      t2.join();
      ASSERT_FALSE(wp_db->old_commit_map_empty_);
    }
    rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  }

  // Verify value of keys.
  void VerifyKeys(const std::unordered_map<std::string, std::string>& data,
                  const Snapshot* snapshot = nullptr) {
    std::string value;
    ReadOptions read_options;
    read_options.snapshot = snapshot;
    for (auto& kv : data) {
      auto s = db->Get(read_options, kv.first, &value);
      ASSERT_TRUE(s.ok() || s.IsNotFound());
      if (s.ok()) {
        if (kv.second != value) {
          printf("key = %s\n", kv.first.c_str());
        }
        ASSERT_EQ(kv.second, value);
      } else {
        ASSERT_EQ(kv.second, "NOT_FOUND");
      }

      // Try with MultiGet API too
      std::vector<std::string> values;
      auto s_vec = db->MultiGet(read_options, {db->DefaultColumnFamily()},
                                {kv.first}, &values);
      ASSERT_EQ(1, values.size());
      ASSERT_EQ(1, s_vec.size());
      s = s_vec[0];
      ASSERT_TRUE(s.ok() || s.IsNotFound());
      if (s.ok()) {
        ASSERT_TRUE(kv.second == values[0]);
      } else {
        ASSERT_EQ(kv.second, "NOT_FOUND");
      }
    }
  }

  // Verify all versions of keys.
  void VerifyInternalKeys(const std::vector<KeyVersion>& expected_versions) {
    std::vector<KeyVersion> versions;
    const size_t kMaxKeys = 100000;
    ASSERT_OK(GetAllKeyVersions(db, expected_versions.front().user_key,
                                expected_versions.back().user_key, kMaxKeys,
                                &versions));
    ASSERT_EQ(expected_versions.size(), versions.size());
    for (size_t i = 0; i < versions.size(); i++) {
      ASSERT_EQ(expected_versions[i].user_key, versions[i].user_key);
      ASSERT_EQ(expected_versions[i].sequence, versions[i].sequence);
      ASSERT_EQ(expected_versions[i].type, versions[i].type);
      if (versions[i].type != kTypeDeletion &&
          versions[i].type != kTypeSingleDeletion) {
        ASSERT_EQ(expected_versions[i].value, versions[i].value);
      }
      // Range delete not supported.
      assert(expected_versions[i].type != kTypeRangeDeletion);
    }
  }
};

class WritePreparedTransactionTest
    : public WritePreparedTransactionTestBase,
      virtual public ::testing::WithParamInterface<
          std::tuple<bool, bool, TxnDBWritePolicy>> {
 public:
  WritePreparedTransactionTest()
      : WritePreparedTransactionTestBase(std::get<0>(GetParam()),
                                         std::get<1>(GetParam()),
                                         std::get<2>(GetParam())){};
};

#ifndef ROCKSDB_VALGRIND_RUN
class SnapshotConcurrentAccessTest
    : public WritePreparedTransactionTestBase,
      virtual public ::testing::WithParamInterface<
          std::tuple<bool, bool, TxnDBWritePolicy, size_t, size_t>> {
 public:
  SnapshotConcurrentAccessTest()
      : WritePreparedTransactionTestBase(std::get<0>(GetParam()),
                                         std::get<1>(GetParam()),
                                         std::get<2>(GetParam())),
        split_id_(std::get<3>(GetParam())),
        split_cnt_(std::get<4>(GetParam())){};

 protected:
  // A test is split into split_cnt_ tests, each identified with split_id_ where
  // 0 <= split_id_ < split_cnt_
  size_t split_id_;
  size_t split_cnt_;
};
#endif  // ROCKSDB_VALGRIND_RUN

class SeqAdvanceConcurrentTest
    : public WritePreparedTransactionTestBase,
      virtual public ::testing::WithParamInterface<
          std::tuple<bool, bool, TxnDBWritePolicy, size_t, size_t>> {
 public:
  SeqAdvanceConcurrentTest()
      : WritePreparedTransactionTestBase(std::get<0>(GetParam()),
                                         std::get<1>(GetParam()),
                                         std::get<2>(GetParam())),
        split_id_(std::get<3>(GetParam())),
        split_cnt_(std::get<4>(GetParam())){};

 protected:
  // A test is split into split_cnt_ tests, each identified with split_id_ where
  // 0 <= split_id_ < split_cnt_
  size_t split_id_;
  size_t split_cnt_;
};

INSTANTIATE_TEST_CASE_P(
    WritePreparedTransactionTest, WritePreparedTransactionTest,
    ::testing::Values(std::make_tuple(false, false, WRITE_PREPARED),
                      std::make_tuple(false, true, WRITE_PREPARED)));

#ifndef ROCKSDB_VALGRIND_RUN
INSTANTIATE_TEST_CASE_P(
    TwoWriteQueues, SnapshotConcurrentAccessTest,
    ::testing::Values(std::make_tuple(false, true, WRITE_PREPARED, 0, 20),
                      std::make_tuple(false, true, WRITE_PREPARED, 1, 20),
                      std::make_tuple(false, true, WRITE_PREPARED, 2, 20),
                      std::make_tuple(false, true, WRITE_PREPARED, 3, 20),
                      std::make_tuple(false, true, WRITE_PREPARED, 4, 20),
                      std::make_tuple(false, true, WRITE_PREPARED, 5, 20),
                      std::make_tuple(false, true, WRITE_PREPARED, 6, 20),
                      std::make_tuple(false, true, WRITE_PREPARED, 7, 20),
                      std::make_tuple(false, true, WRITE_PREPARED, 8, 20),
                      std::make_tuple(false, true, WRITE_PREPARED, 9, 20),
                      std::make_tuple(false, true, WRITE_PREPARED, 10, 20),
                      std::make_tuple(false, true, WRITE_PREPARED, 11, 20),
                      std::make_tuple(false, true, WRITE_PREPARED, 12, 20),
                      std::make_tuple(false, true, WRITE_PREPARED, 13, 20),
                      std::make_tuple(false, true, WRITE_PREPARED, 14, 20),
                      std::make_tuple(false, true, WRITE_PREPARED, 15, 20),
                      std::make_tuple(false, true, WRITE_PREPARED, 16, 20),
                      std::make_tuple(false, true, WRITE_PREPARED, 17, 20),
                      std::make_tuple(false, true, WRITE_PREPARED, 18, 20),
                      std::make_tuple(false, true, WRITE_PREPARED, 19, 20)));

INSTANTIATE_TEST_CASE_P(
    OneWriteQueue, SnapshotConcurrentAccessTest,
    ::testing::Values(std::make_tuple(false, false, WRITE_PREPARED, 0, 20),
                      std::make_tuple(false, false, WRITE_PREPARED, 1, 20),
                      std::make_tuple(false, false, WRITE_PREPARED, 2, 20),
                      std::make_tuple(false, false, WRITE_PREPARED, 3, 20),
                      std::make_tuple(false, false, WRITE_PREPARED, 4, 20),
                      std::make_tuple(false, false, WRITE_PREPARED, 5, 20),
                      std::make_tuple(false, false, WRITE_PREPARED, 6, 20),
                      std::make_tuple(false, false, WRITE_PREPARED, 7, 20),
                      std::make_tuple(false, false, WRITE_PREPARED, 8, 20),
                      std::make_tuple(false, false, WRITE_PREPARED, 9, 20),
                      std::make_tuple(false, false, WRITE_PREPARED, 10, 20),
                      std::make_tuple(false, false, WRITE_PREPARED, 11, 20),
                      std::make_tuple(false, false, WRITE_PREPARED, 12, 20),
                      std::make_tuple(false, false, WRITE_PREPARED, 13, 20),
                      std::make_tuple(false, false, WRITE_PREPARED, 14, 20),
                      std::make_tuple(false, false, WRITE_PREPARED, 15, 20),
                      std::make_tuple(false, false, WRITE_PREPARED, 16, 20),
                      std::make_tuple(false, false, WRITE_PREPARED, 17, 20),
                      std::make_tuple(false, false, WRITE_PREPARED, 18, 20),
                      std::make_tuple(false, false, WRITE_PREPARED, 19, 20)));

INSTANTIATE_TEST_CASE_P(
    TwoWriteQueues, SeqAdvanceConcurrentTest,
    ::testing::Values(std::make_tuple(false, true, WRITE_PREPARED, 0, 10),
                      std::make_tuple(false, true, WRITE_PREPARED, 1, 10),
                      std::make_tuple(false, true, WRITE_PREPARED, 2, 10),
                      std::make_tuple(false, true, WRITE_PREPARED, 3, 10),
                      std::make_tuple(false, true, WRITE_PREPARED, 4, 10),
                      std::make_tuple(false, true, WRITE_PREPARED, 5, 10),
                      std::make_tuple(false, true, WRITE_PREPARED, 6, 10),
                      std::make_tuple(false, true, WRITE_PREPARED, 7, 10),
                      std::make_tuple(false, true, WRITE_PREPARED, 8, 10),
                      std::make_tuple(false, true, WRITE_PREPARED, 9, 10)));

INSTANTIATE_TEST_CASE_P(
    OneWriteQueue, SeqAdvanceConcurrentTest,
    ::testing::Values(std::make_tuple(false, false, WRITE_PREPARED, 0, 10),
                      std::make_tuple(false, false, WRITE_PREPARED, 1, 10),
                      std::make_tuple(false, false, WRITE_PREPARED, 2, 10),
                      std::make_tuple(false, false, WRITE_PREPARED, 3, 10),
                      std::make_tuple(false, false, WRITE_PREPARED, 4, 10),
                      std::make_tuple(false, false, WRITE_PREPARED, 5, 10),
                      std::make_tuple(false, false, WRITE_PREPARED, 6, 10),
                      std::make_tuple(false, false, WRITE_PREPARED, 7, 10),
                      std::make_tuple(false, false, WRITE_PREPARED, 8, 10),
                      std::make_tuple(false, false, WRITE_PREPARED, 9, 10)));
#endif  // ROCKSDB_VALGRIND_RUN

TEST_P(WritePreparedTransactionTest, CommitMapTest) {
  WritePreparedTxnDB* wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
  assert(wp_db);
  assert(wp_db->db_impl_);
  size_t size = wp_db->COMMIT_CACHE_SIZE;
  CommitEntry c = {5, 12}, e;
  bool evicted = wp_db->AddCommitEntry(c.prep_seq % size, c, &e);
  ASSERT_FALSE(evicted);

  // Should be able to read the same value
  CommitEntry64b dont_care;
  bool found = wp_db->GetCommitEntry(c.prep_seq % size, &dont_care, &e);
  ASSERT_TRUE(found);
  ASSERT_EQ(c, e);
  // Should be able to distinguish between overlapping entries
  found = wp_db->GetCommitEntry((c.prep_seq + size) % size, &dont_care, &e);
  ASSERT_TRUE(found);
  ASSERT_NE(c.prep_seq + size, e.prep_seq);
  // Should be able to detect non-existent entry
  found = wp_db->GetCommitEntry((c.prep_seq + 1) % size, &dont_care, &e);
  ASSERT_FALSE(found);

  // Reject an invalid exchange
  CommitEntry e2 = {c.prep_seq + size, c.commit_seq + size};
  CommitEntry64b e2_64b(e2, wp_db->FORMAT);
  bool exchanged = wp_db->ExchangeCommitEntry(e2.prep_seq % size, e2_64b, e);
  ASSERT_FALSE(exchanged);
  // check whether it did actually reject that
  found = wp_db->GetCommitEntry(e2.prep_seq % size, &dont_care, &e);
  ASSERT_TRUE(found);
  ASSERT_EQ(c, e);

  // Accept a valid exchange
  CommitEntry64b c_64b(c, wp_db->FORMAT);
  CommitEntry e3 = {c.prep_seq + size, c.commit_seq + size + 1};
  exchanged = wp_db->ExchangeCommitEntry(c.prep_seq % size, c_64b, e3);
  ASSERT_TRUE(exchanged);
  // check whether it did actually accepted that
  found = wp_db->GetCommitEntry(c.prep_seq % size, &dont_care, &e);
  ASSERT_TRUE(found);
  ASSERT_EQ(e3, e);

  // Rewrite an entry
  CommitEntry e4 = {e3.prep_seq + size, e3.commit_seq + size + 1};
  evicted = wp_db->AddCommitEntry(e4.prep_seq % size, e4, &e);
  ASSERT_TRUE(evicted);
  ASSERT_EQ(e3, e);
  found = wp_db->GetCommitEntry(e4.prep_seq % size, &dont_care, &e);
  ASSERT_TRUE(found);
  ASSERT_EQ(e4, e);
}

TEST_P(WritePreparedTransactionTest, MaybeUpdateOldCommitMap) {
  // If prepare <= snapshot < commit we should keep the entry around since its
  // nonexistence could be interpreted as committed in the snapshot while it is
  // not true. We keep such entries around by adding them to the
  // old_commit_map_.
  uint64_t p /*prepare*/, c /*commit*/, s /*snapshot*/, ns /*next_snapshot*/;
  p = 10l, c = 15l, s = 20l, ns = 21l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, false);
  // If we do not expect the old commit map to be updated, try also with a next
  // snapshot that is expected to update the old commit map. This would test
  // that MaybeUpdateOldCommitMap would not prevent us from checking the next
  // snapshot that must be checked.
  p = 10l, c = 15l, s = 20l, ns = 11l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, false);

  p = 10l, c = 20l, s = 20l, ns = 19l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, false);
  p = 10l, c = 20l, s = 20l, ns = 21l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, false);

  p = 20l, c = 20l, s = 20l, ns = 21l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, false);
  p = 20l, c = 20l, s = 20l, ns = 19l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, false);

  p = 10l, c = 25l, s = 20l, ns = 21l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, true);

  p = 20l, c = 25l, s = 20l, ns = 21l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, true);

  p = 21l, c = 25l, s = 20l, ns = 22l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, false);
  p = 21l, c = 25l, s = 20l, ns = 19l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, false);
}

// Reproduce the bug with two snapshots with the same seuqence number and test
// that the release of the first snapshot will not affect the reads by the other
// snapshot
TEST_P(WritePreparedTransactionTest, DoubleSnapshot) {
  TransactionOptions txn_options;
  Status s;

  // Insert initial value
  ASSERT_OK(db->Put(WriteOptions(), "key", "value1"));

  WritePreparedTxnDB* wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
  Transaction* txn =
      wp_db->BeginTransaction(WriteOptions(), txn_options, nullptr);
  ASSERT_OK(txn->SetName("txn"));
  ASSERT_OK(txn->Put("key", "value2"));
  ASSERT_OK(txn->Prepare());
  // Three snapshots with the same seq number
  const Snapshot* snapshot0 = wp_db->GetSnapshot();
  const Snapshot* snapshot1 = wp_db->GetSnapshot();
  const Snapshot* snapshot2 = wp_db->GetSnapshot();
  ASSERT_OK(txn->Commit());
  SequenceNumber cache_size = wp_db->COMMIT_CACHE_SIZE;
  SequenceNumber overlap_seq = txn->GetId() + cache_size;
  delete txn;

  // 4th snapshot with a larger seq
  const Snapshot* snapshot3 = wp_db->GetSnapshot();
  // Cause an eviction to advance max evicted seq number
  // This also fetches the 4 snapshots from db since their seq is lower than the
  // new max
  wp_db->AddCommitted(overlap_seq, overlap_seq);

  ReadOptions ropt;
  // It should see the value before commit
  ropt.snapshot = snapshot2;
  PinnableSlice pinnable_val;
  s = wp_db->Get(ropt, wp_db->DefaultColumnFamily(), "key", &pinnable_val);
  ASSERT_OK(s);
  ASSERT_TRUE(pinnable_val == "value1");
  pinnable_val.Reset();

  wp_db->ReleaseSnapshot(snapshot1);

  // It should still see the value before commit
  s = wp_db->Get(ropt, wp_db->DefaultColumnFamily(), "key", &pinnable_val);
  ASSERT_OK(s);
  ASSERT_TRUE(pinnable_val == "value1");
  pinnable_val.Reset();

  // Cause an eviction to advance max evicted seq number and trigger updating
  // the snapshot list
  overlap_seq += cache_size;
  wp_db->AddCommitted(overlap_seq, overlap_seq);

  // It should still see the value before commit
  s = wp_db->Get(ropt, wp_db->DefaultColumnFamily(), "key", &pinnable_val);
  ASSERT_OK(s);
  ASSERT_TRUE(pinnable_val == "value1");
  pinnable_val.Reset();

  wp_db->ReleaseSnapshot(snapshot0);
  wp_db->ReleaseSnapshot(snapshot2);
  wp_db->ReleaseSnapshot(snapshot3);
}

size_t UniqueCnt(std::vector<SequenceNumber> vec) {
  std::set<SequenceNumber> aset;
  for (auto i : vec) {
    aset.insert(i);
  }
  return aset.size();
}
// Test that the entries in old_commit_map_ get garbage collected properly
TEST_P(WritePreparedTransactionTest, OldCommitMapGC) {
  const size_t snapshot_cache_bits = 0;
  const size_t commit_cache_bits = 0;
  DBImpl* mock_db = new DBImpl(options, dbname);
  UpdateTransactionDBOptions(snapshot_cache_bits, commit_cache_bits);
  std::unique_ptr<WritePreparedTxnDBMock> wp_db(
      new WritePreparedTxnDBMock(mock_db, txn_db_options));

  SequenceNumber seq = 0;
  // Take the first snapshot that overlaps with two txn
  auto prep_seq = ++seq;
  wp_db->AddPrepared(prep_seq);
  auto prep_seq2 = ++seq;
  wp_db->AddPrepared(prep_seq2);
  auto snap_seq1 = seq;
  wp_db->TakeSnapshot(snap_seq1);
  auto commit_seq = ++seq;
  wp_db->AddCommitted(prep_seq, commit_seq);
  wp_db->RemovePrepared(prep_seq);
  auto commit_seq2 = ++seq;
  wp_db->AddCommitted(prep_seq2, commit_seq2);
  wp_db->RemovePrepared(prep_seq2);
  // Take the 2nd and 3rd snapshot that overlap with the same txn
  prep_seq = ++seq;
  wp_db->AddPrepared(prep_seq);
  auto snap_seq2 = seq;
  wp_db->TakeSnapshot(snap_seq2);
  seq++;
  auto snap_seq3 = seq;
  wp_db->TakeSnapshot(snap_seq3);
  seq++;
  commit_seq = ++seq;
  wp_db->AddCommitted(prep_seq, commit_seq);
  wp_db->RemovePrepared(prep_seq);
  // Make sure max_evicted_seq_ will be larger than 2nd snapshot by evicting the
  // only item in the commit_cache_ via another commit.
  prep_seq = ++seq;
  wp_db->AddPrepared(prep_seq);
  commit_seq = ++seq;
  wp_db->AddCommitted(prep_seq, commit_seq);
  wp_db->RemovePrepared(prep_seq);

  // Verify that the evicted commit entries for all snapshots are in the
  // old_commit_map_
  {
    ASSERT_FALSE(wp_db->old_commit_map_empty_.load());
    ReadLock rl(&wp_db->old_commit_map_mutex_);
    ASSERT_EQ(3, wp_db->old_commit_map_.size());
    ASSERT_EQ(2, UniqueCnt(wp_db->old_commit_map_[snap_seq1]));
    ASSERT_EQ(1, UniqueCnt(wp_db->old_commit_map_[snap_seq2]));
    ASSERT_EQ(1, UniqueCnt(wp_db->old_commit_map_[snap_seq3]));
  }

  // Verify that the 2nd snapshot is cleaned up after the release
  wp_db->ReleaseSnapshotInternal(snap_seq2);
  {
    ASSERT_FALSE(wp_db->old_commit_map_empty_.load());
    ReadLock rl(&wp_db->old_commit_map_mutex_);
    ASSERT_EQ(2, wp_db->old_commit_map_.size());
    ASSERT_EQ(2, UniqueCnt(wp_db->old_commit_map_[snap_seq1]));
    ASSERT_EQ(1, UniqueCnt(wp_db->old_commit_map_[snap_seq3]));
  }

  // Verify that the 1st snapshot is cleaned up after the release
  wp_db->ReleaseSnapshotInternal(snap_seq1);
  {
    ASSERT_FALSE(wp_db->old_commit_map_empty_.load());
    ReadLock rl(&wp_db->old_commit_map_mutex_);
    ASSERT_EQ(1, wp_db->old_commit_map_.size());
    ASSERT_EQ(1, UniqueCnt(wp_db->old_commit_map_[snap_seq3]));
  }

  // Verify that the 3rd snapshot is cleaned up after the release
  wp_db->ReleaseSnapshotInternal(snap_seq3);
  {
    ASSERT_TRUE(wp_db->old_commit_map_empty_.load());
    ReadLock rl(&wp_db->old_commit_map_mutex_);
    ASSERT_EQ(0, wp_db->old_commit_map_.size());
  }
}

TEST_P(WritePreparedTransactionTest, CheckAgainstSnapshotsTest) {
  std::vector<SequenceNumber> snapshots = {100l, 200l, 300l, 400l, 500l,
                                           600l, 700l, 800l, 900l};
  const size_t snapshot_cache_bits = 2;
  const uint64_t cache_size = 1ul << snapshot_cache_bits;
  // Safety check to express the intended size in the test. Can be adjusted if
  // the snapshots lists changed.
  assert((1ul << snapshot_cache_bits) * 2 + 1 == snapshots.size());
  DBImpl* mock_db = new DBImpl(options, dbname);
  UpdateTransactionDBOptions(snapshot_cache_bits);
  std::unique_ptr<WritePreparedTxnDBMock> wp_db(
      new WritePreparedTxnDBMock(mock_db, txn_db_options));
  SequenceNumber version = 1000l;
  ASSERT_EQ(0, wp_db->snapshots_total_);
  wp_db->UpdateSnapshots(snapshots, version);
  ASSERT_EQ(snapshots.size(), wp_db->snapshots_total_);
  // seq numbers are chosen so that we have two of them between each two
  // snapshots. If the diff of two consecutive seq is more than 5, there is a
  // snapshot between them.
  std::vector<SequenceNumber> seqs = {50l,  55l,  150l, 155l, 250l, 255l, 350l,
                                      355l, 450l, 455l, 550l, 555l, 650l, 655l,
                                      750l, 755l, 850l, 855l, 950l, 955l};
  assert(seqs.size() > 1);
  for (size_t i = 0; i < seqs.size() - 1; i++) {
    wp_db->old_commit_map_empty_ = true;  // reset
    CommitEntry commit_entry = {seqs[i], seqs[i + 1]};
    wp_db->CheckAgainstSnapshots(commit_entry);
    // Expect update if there is snapshot in between the prepare and commit
    bool expect_update = commit_entry.commit_seq - commit_entry.prep_seq > 5 &&
                         commit_entry.commit_seq >= snapshots.front() &&
                         commit_entry.prep_seq <= snapshots.back();
    ASSERT_EQ(expect_update, !wp_db->old_commit_map_empty_);
  }

  // Test that search will include multiple snapshot from snapshot cache
  {
    // exclude first and last item in the cache
    CommitEntry commit_entry = {snapshots.front() + 1,
                                snapshots[cache_size - 1] - 1};
    wp_db->old_commit_map_empty_ = true;  // reset
    wp_db->old_commit_map_.clear();
    wp_db->CheckAgainstSnapshots(commit_entry);
    ASSERT_EQ(wp_db->old_commit_map_.size(), cache_size - 2);
  }

  // Test that search will include multiple snapshot from old snapshots
  {
    // include two in the middle
    CommitEntry commit_entry = {snapshots[cache_size] + 1,
                                snapshots[cache_size + 2] + 1};
    wp_db->old_commit_map_empty_ = true;  // reset
    wp_db->old_commit_map_.clear();
    wp_db->CheckAgainstSnapshots(commit_entry);
    ASSERT_EQ(wp_db->old_commit_map_.size(), 2);
  }

  // Test that search will include both snapshot cache and old snapshots
  // Case 1: includes all in snapshot cache
  {
    CommitEntry commit_entry = {snapshots.front() - 1, snapshots.back() + 1};
    wp_db->old_commit_map_empty_ = true;  // reset
    wp_db->old_commit_map_.clear();
    wp_db->CheckAgainstSnapshots(commit_entry);
    ASSERT_EQ(wp_db->old_commit_map_.size(), snapshots.size());
  }

  // Case 2: includes all snapshot caches except the smallest
  {
    CommitEntry commit_entry = {snapshots.front() + 1, snapshots.back() + 1};
    wp_db->old_commit_map_empty_ = true;  // reset
    wp_db->old_commit_map_.clear();
    wp_db->CheckAgainstSnapshots(commit_entry);
    ASSERT_EQ(wp_db->old_commit_map_.size(), snapshots.size() - 1);
  }

  // Case 3: includes only the largest of snapshot cache
  {
    CommitEntry commit_entry = {snapshots[cache_size - 1] - 1,
                                snapshots.back() + 1};
    wp_db->old_commit_map_empty_ = true;  // reset
    wp_db->old_commit_map_.clear();
    wp_db->CheckAgainstSnapshots(commit_entry);
    ASSERT_EQ(wp_db->old_commit_map_.size(), snapshots.size() - cache_size + 1);
  }
}

// This test is too slow for travis
#ifndef TRAVIS
#ifndef ROCKSDB_VALGRIND_RUN
// Test that CheckAgainstSnapshots will not miss a live snapshot if it is run in
// parallel with UpdateSnapshots.
TEST_P(SnapshotConcurrentAccessTest, SnapshotConcurrentAccessTest) {
  // We have a sync point in the method under test after checking each snapshot.
  // If you increase the max number of snapshots in this test, more sync points
  // in the methods must also be added.
  const std::vector<SequenceNumber> snapshots = {10l, 20l, 30l, 40l, 50l,
                                                 60l, 70l, 80l, 90l, 100l};
  const size_t snapshot_cache_bits = 2;
  // Safety check to express the intended size in the test. Can be adjusted if
  // the snapshots lists changed.
  assert((1ul << snapshot_cache_bits) * 2 + 2 == snapshots.size());
  SequenceNumber version = 1000l;
  // Choose the cache size so that the new snapshot list could replace all the
  // existing items in the cache and also have some overflow.
  DBImpl* mock_db = new DBImpl(options, dbname);
  UpdateTransactionDBOptions(snapshot_cache_bits);
  std::unique_ptr<WritePreparedTxnDBMock> wp_db(
      new WritePreparedTxnDBMock(mock_db, txn_db_options));
  const size_t extra = 2;
  size_t loop_id = 0;
  // Add up to extra items that do not fit into the cache
  for (size_t old_size = 1; old_size <= wp_db->SNAPSHOT_CACHE_SIZE + extra;
       old_size++) {
    const std::vector<SequenceNumber> old_snapshots(
        snapshots.begin(), snapshots.begin() + old_size);

    // Each member of old snapshot might or might not appear in the new list. We
    // create a common_snapshots for each combination.
    size_t new_comb_cnt = size_t(1) << old_size;
    for (size_t new_comb = 0; new_comb < new_comb_cnt; new_comb++, loop_id++) {
      if (loop_id % split_cnt_ != split_id_) continue;
      printf(".");  // To signal progress
      fflush(stdout);
      std::vector<SequenceNumber> common_snapshots;
      for (size_t i = 0; i < old_snapshots.size(); i++) {
        if (IsInCombination(i, new_comb)) {
          common_snapshots.push_back(old_snapshots[i]);
        }
      }
      // And add some new snapshots to the common list
      for (size_t added_snapshots = 0;
           added_snapshots <= snapshots.size() - old_snapshots.size();
           added_snapshots++) {
        std::vector<SequenceNumber> new_snapshots = common_snapshots;
        for (size_t i = 0; i < added_snapshots; i++) {
          new_snapshots.push_back(snapshots[old_snapshots.size() + i]);
        }
        for (auto it = common_snapshots.begin(); it != common_snapshots.end();
             it++) {
          auto snapshot = *it;
          // Create a commit entry that is around the snapshot and thus should
          // be not be discarded
          CommitEntry entry = {static_cast<uint64_t>(snapshot - 1),
                               snapshot + 1};
          // The critical part is when iterating the snapshot cache. Afterwards,
          // we are operating under the lock
          size_t a_range =
              std::min(old_snapshots.size(), wp_db->SNAPSHOT_CACHE_SIZE) + 1;
          size_t b_range =
              std::min(new_snapshots.size(), wp_db->SNAPSHOT_CACHE_SIZE) + 1;
          // Break each thread at two points
          for (size_t a1 = 1; a1 <= a_range; a1++) {
            for (size_t a2 = a1 + 1; a2 <= a_range; a2++) {
              for (size_t b1 = 1; b1 <= b_range; b1++) {
                for (size_t b2 = b1 + 1; b2 <= b_range; b2++) {
                  SnapshotConcurrentAccessTestInternal(
                      wp_db.get(), old_snapshots, new_snapshots, entry, version,
                      a1, a2, b1, b2);
                }
              }
            }
          }
        }
      }
    }
  }
  printf("\n");
}
#endif  // ROCKSDB_VALGRIND_RUN
#endif  // TRAVIS

// This test clarifies the contract of AdvanceMaxEvictedSeq method
TEST_P(WritePreparedTransactionTest, AdvanceMaxEvictedSeqBasicTest) {
  DBImpl* mock_db = new DBImpl(options, dbname);
  std::unique_ptr<WritePreparedTxnDBMock> wp_db(
      new WritePreparedTxnDBMock(mock_db, txn_db_options));

  // 1. Set the initial values for max, prepared, and snapshots
  SequenceNumber zero_max = 0l;
  // Set the initial list of prepared txns
  const std::vector<SequenceNumber> initial_prepared = {10,  30,  50, 100,
                                                        150, 200, 250};
  for (auto p : initial_prepared) {
    wp_db->AddPrepared(p);
  }
  // This updates the max value and also set old prepared
  SequenceNumber init_max = 100;
  wp_db->AdvanceMaxEvictedSeq(zero_max, init_max);
  const std::vector<SequenceNumber> initial_snapshots = {20, 40};
  wp_db->SetDBSnapshots(initial_snapshots);
  // This will update the internal cache of snapshots from the DB
  wp_db->UpdateSnapshots(initial_snapshots, init_max);

  // 2. Invoke AdvanceMaxEvictedSeq
  const std::vector<SequenceNumber> latest_snapshots = {20, 110, 220, 300};
  wp_db->SetDBSnapshots(latest_snapshots);
  SequenceNumber new_max = 200;
  wp_db->AdvanceMaxEvictedSeq(init_max, new_max);

  // 3. Verify that the state matches with AdvanceMaxEvictedSeq contract
  // a. max should be updated to new_max
  ASSERT_EQ(wp_db->max_evicted_seq_, new_max);
  // b. delayed prepared should contain every txn <= max and prepared should
  // only contain txns > max
  auto it = initial_prepared.begin();
  for (; it != initial_prepared.end() && *it <= new_max; it++) {
    ASSERT_EQ(1, wp_db->delayed_prepared_.erase(*it));
  }
  ASSERT_TRUE(wp_db->delayed_prepared_.empty());
  for (; it != initial_prepared.end() && !wp_db->prepared_txns_.empty();
       it++, wp_db->prepared_txns_.pop()) {
    ASSERT_EQ(*it, wp_db->prepared_txns_.top());
  }
  ASSERT_TRUE(it == initial_prepared.end());
  ASSERT_TRUE(wp_db->prepared_txns_.empty());
  // c. snapshots should contain everything below new_max
  auto sit = latest_snapshots.begin();
  for (size_t i = 0; sit != latest_snapshots.end() && *sit <= new_max &&
                     i < wp_db->snapshots_total_;
       sit++, i++) {
    ASSERT_TRUE(i < wp_db->snapshots_total_);
    // This test is in small scale and the list of snapshots are assumed to be
    // within the cache size limit. This is just a safety check to double check
    // that assumption.
    ASSERT_TRUE(i < wp_db->SNAPSHOT_CACHE_SIZE);
    ASSERT_EQ(*sit, wp_db->snapshot_cache_[i]);
  }
}

// A new snapshot should always be always larger than max_evicted_seq_
// Otherwise the snapshot does not go through AdvanceMaxEvictedSeq
TEST_P(WritePreparedTransactionTest, NewSnapshotLargerThanMax) {
  WriteOptions woptions;
  TransactionOptions txn_options;
  WritePreparedTxnDB* wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
  Transaction* txn0 = db->BeginTransaction(woptions, txn_options);
  ASSERT_OK(txn0->Put(Slice("key"), Slice("value")));
  ASSERT_OK(txn0->Commit());
  const SequenceNumber seq = txn0->GetId();  // is also prepare seq
  delete txn0;
  std::vector<Transaction*> txns;
  // Inc seq without committing anything
  for (int i = 0; i < 10; i++) {
    Transaction* txn = db->BeginTransaction(woptions, txn_options);
    ASSERT_OK(txn->SetName("xid" + std::to_string(i)));
    ASSERT_OK(txn->Put(Slice("key" + std::to_string(i)), Slice("value")));
    ASSERT_OK(txn->Prepare());
    txns.push_back(txn);
  }

  // The new commit is seq + 10
  ASSERT_OK(db->Put(woptions, "key", "value"));
  auto snap = wp_db->GetSnapshot();
  const SequenceNumber last_seq = snap->GetSequenceNumber();
  wp_db->ReleaseSnapshot(snap);
  ASSERT_LT(seq, last_seq);
  // Otherwise our test is not effective
  ASSERT_LT(last_seq - seq, wp_db->INC_STEP_FOR_MAX_EVICTED);

  // Evict seq out of commit cache
  const SequenceNumber overwrite_seq = seq + wp_db->COMMIT_CACHE_SIZE;
  // Check that the next write could make max go beyond last
  auto last_max = wp_db->max_evicted_seq_.load();
  wp_db->AddCommitted(overwrite_seq, overwrite_seq);
  // Check that eviction has advanced the max
  ASSERT_LT(last_max, wp_db->max_evicted_seq_.load());
  // Check that the new max has not advanced the last seq
  ASSERT_LT(wp_db->max_evicted_seq_.load(), last_seq);
  for (auto txn : txns) {
    txn->Rollback();
    delete txn;
  }
}

// A new snapshot should always be always larger than max_evicted_seq_
// In very rare cases max could be below last published seq. Test that
// taking snapshot will wait for max to catch up.
TEST_P(WritePreparedTransactionTest, MaxCatchupWithNewSnapshot) {
  const size_t snapshot_cache_bits = 7;  // same as default
  const size_t commit_cache_bits = 0;    // only 1 entry => frequent eviction
  UpdateTransactionDBOptions(snapshot_cache_bits, commit_cache_bits);
  ReOpen();
  WriteOptions woptions;
  WritePreparedTxnDB* wp_db = dynamic_cast<WritePreparedTxnDB*>(db);

  const int writes = 50;
  const int batch_cnt = 4;
  rocksdb::port::Thread t1([&]() {
    for (int i = 0; i < writes; i++) {
      WriteBatch batch;
      // For duplicate keys cause 4 commit entries, each evicting an entry that
      // is not published yet, thus causing max ecited seq go higher than last
      // published.
      for (int b = 0; b < batch_cnt; b++) {
        batch.Put("foo", "foo");
      }
      db->Write(woptions, &batch);
    }
  });

  rocksdb::port::Thread t2([&]() {
    while (wp_db->max_evicted_seq_ == 0) {  // wait for insert thread
      std::this_thread::yield();
    }
    for (int i = 0; i < 10; i++) {
      auto snap = db->GetSnapshot();
      if (snap->GetSequenceNumber() != 0) {
        ASSERT_LT(wp_db->max_evicted_seq_, snap->GetSequenceNumber());
      }  // seq 0 is ok to be less than max since nothing is visible to it
      db->ReleaseSnapshot(snap);
    }
  });

  t1.join();
  t2.join();

  // Make sure that the test has worked and seq number has advanced as we
  // thought
  auto snap = db->GetSnapshot();
  ASSERT_GT(snap->GetSequenceNumber(), batch_cnt * writes - 1);
  db->ReleaseSnapshot(snap);
}

// Check that old_commit_map_ cleanup works correctly if the snapshot equals
// max_evicted_seq_.
TEST_P(WritePreparedTransactionTest, CleanupSnapshotEqualToMax) {
  const size_t snapshot_cache_bits = 7;  // same as default
  const size_t commit_cache_bits = 0;    // only 1 entry => frequent eviction
  UpdateTransactionDBOptions(snapshot_cache_bits, commit_cache_bits);
  ReOpen();
  WriteOptions woptions;
  WritePreparedTxnDB* wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
  // Insert something to increase seq
  ASSERT_OK(db->Put(woptions, "key", "value"));
  auto snap = db->GetSnapshot();
  auto snap_seq = snap->GetSequenceNumber();
  // Another insert should trigger eviction + load snapshot from db
  ASSERT_OK(db->Put(woptions, "key", "value"));
  // This is the scenario that we check agaisnt
  ASSERT_EQ(snap_seq, wp_db->max_evicted_seq_);
  // old_commit_map_ now has some data that needs gc
  ASSERT_EQ(1, wp_db->snapshots_total_);
  ASSERT_EQ(1, wp_db->old_commit_map_.size());

  db->ReleaseSnapshot(snap);

  // Another insert should trigger eviction + load snapshot from db
  ASSERT_OK(db->Put(woptions, "key", "value"));

  // the snapshot and related metadata must be properly garbage collected
  ASSERT_EQ(0, wp_db->snapshots_total_);
  ASSERT_TRUE(wp_db->snapshots_all_.empty());
  ASSERT_EQ(0, wp_db->old_commit_map_.size());
}

TEST_P(WritePreparedTransactionTest, AdvanceSeqByOne) {
  auto snap = db->GetSnapshot();
  auto seq1 = snap->GetSequenceNumber();
  db->ReleaseSnapshot(snap);

  WritePreparedTxnDB* wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
  wp_db->AdvanceSeqByOne();

  snap = db->GetSnapshot();
  auto seq2 = snap->GetSequenceNumber();
  db->ReleaseSnapshot(snap);

  ASSERT_LT(seq1, seq2);
}

// Test that the txn Initilize calls the overridden functions
TEST_P(WritePreparedTransactionTest, TxnInitialize) {
  TransactionOptions txn_options;
  WriteOptions write_options;
  Transaction* txn0 = db->BeginTransaction(write_options, txn_options);
  ASSERT_OK(txn0->SetName("xid"));
  ASSERT_OK(txn0->Put(Slice("key"), Slice("value1")));
  ASSERT_OK(txn0->Prepare());

  // SetSnapshot is overridden to update min_uncommitted_
  txn_options.set_snapshot = true;
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
  auto snap = txn1->GetSnapshot();
  auto snap_impl = reinterpret_cast<const SnapshotImpl*>(snap);
  // If ::Initialize calls the overriden SetSnapshot, min_uncommitted_ must be
  // udpated
  ASSERT_GT(snap_impl->min_uncommitted_, 0);

  txn0->Rollback();
  txn1->Rollback();
  delete txn0;
  delete txn1;
}

// This tests that transactions with duplicate keys perform correctly after max
// is advancing their prepared sequence numbers. This will not be the case if
// for example the txn does not add the prepared seq for the second sub-batch to
// the PrepareHeap structure.
TEST_P(WritePreparedTransactionTest, AdvanceMaxEvictedSeqWithDuplicatesTest) {
  WriteOptions write_options;
  TransactionOptions txn_options;
  Transaction* txn0 = db->BeginTransaction(write_options, txn_options);
  ASSERT_OK(txn0->SetName("xid"));
  ASSERT_OK(txn0->Put(Slice("key"), Slice("value1")));
  ASSERT_OK(txn0->Put(Slice("key"), Slice("value2")));
  ASSERT_OK(txn0->Prepare());

  WritePreparedTxnDB* wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
  // Ensure that all the prepared sequence numbers will be removed from the
  // PrepareHeap.
  SequenceNumber new_max = wp_db->COMMIT_CACHE_SIZE;
  wp_db->AdvanceMaxEvictedSeq(0, new_max);

  ReadOptions ropt;
  PinnableSlice pinnable_val;
  auto s = db->Get(ropt, db->DefaultColumnFamily(), "key", &pinnable_val);
  ASSERT_TRUE(s.IsNotFound());
  delete txn0;

  wp_db->db_impl_->FlushWAL(true);
  wp_db->TEST_Crash();
  ReOpenNoDelete();
  assert(db != nullptr);
  wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
  wp_db->AdvanceMaxEvictedSeq(0, new_max);
  s = db->Get(ropt, db->DefaultColumnFamily(), "key", &pinnable_val);
  ASSERT_TRUE(s.IsNotFound());

  txn0 = db->GetTransactionByName("xid");
  ASSERT_OK(txn0->Rollback());
  delete txn0;
}

TEST_P(SeqAdvanceConcurrentTest, SeqAdvanceConcurrentTest) {
  // Given the sequential run of txns, with this timeout we should never see a
  // deadlock nor a timeout unless we have a key conflict, which should be
  // almost infeasible.
  txn_db_options.transaction_lock_timeout = 1000;
  txn_db_options.default_lock_timeout = 1000;
  ReOpen();
  FlushOptions fopt;

  // Number of different txn types we use in this test
  const size_t type_cnt = 5;
  // The size of the first write group
  // TODO(myabandeh): This should be increase for pre-release tests
  const size_t first_group_size = 2;
  // Total number of txns we run in each test
  // TODO(myabandeh): This should be increase for pre-release tests
  const size_t txn_cnt = first_group_size + 1;

  size_t base[txn_cnt + 1] = {
      1,
  };
  for (size_t bi = 1; bi <= txn_cnt; bi++) {
    base[bi] = base[bi - 1] * type_cnt;
  }
  const size_t max_n = static_cast<size_t>(std::pow(type_cnt, txn_cnt));
  printf("Number of cases being tested is %" ROCKSDB_PRIszt "\n", max_n);
  for (size_t n = 0; n < max_n; n++, ReOpen()) {
    if (n % split_cnt_ != split_id_) continue;
    if (n % 1000 == 0) {
      printf("Tested %" ROCKSDB_PRIszt " cases so far\n", n);
    }
    DBImpl* db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
    auto seq = db_impl->TEST_GetLastVisibleSequence();
    exp_seq = seq;
    // This is increased before writing the batch for commit
    commit_writes = 0;
    // This is increased before txn starts linking if it expects to do a commit
    // eventually
    expected_commits = 0;
    std::vector<port::Thread> threads;

    linked = 0;
    std::atomic<bool> batch_formed(false);
    rocksdb::SyncPoint::GetInstance()->SetCallBack(
        "WriteThread::EnterAsBatchGroupLeader:End",
        [&](void* /*arg*/) { batch_formed = true; });
    rocksdb::SyncPoint::GetInstance()->SetCallBack(
        "WriteThread::JoinBatchGroup:Wait", [&](void* /*arg*/) {
          linked++;
          if (linked == 1) {
            // Wait until the others are linked too.
            while (linked < first_group_size) {
            }
          } else if (linked == 1 + first_group_size) {
            // Make the 2nd batch of the rest of writes plus any followup
            // commits from the first batch
            while (linked < txn_cnt + commit_writes) {
            }
          }
          // Then we will have one or more batches consisting of follow-up
          // commits from the 2nd batch. There is a bit of non-determinism here
          // but it should be tolerable.
        });

    rocksdb::SyncPoint::GetInstance()->EnableProcessing();
    for (size_t bi = 0; bi < txn_cnt; bi++) {
      // get the bi-th digit in number system based on type_cnt
      size_t d = (n % base[bi + 1]) / base[bi];
      switch (d) {
        case 0:
          threads.emplace_back(txn_t0, bi);
          break;
        case 1:
          threads.emplace_back(txn_t1, bi);
          break;
        case 2:
          threads.emplace_back(txn_t2, bi);
          break;
        case 3:
          threads.emplace_back(txn_t3, bi);
          break;
        case 4:
          threads.emplace_back(txn_t3, bi);
          break;
        default:
          assert(false);
      }
      // wait to be linked
      while (linked.load() <= bi) {
      }
      // after a queue of size first_group_size
      if (bi + 1 == first_group_size) {
        while (!batch_formed) {
        }
        // to make it more deterministic, wait until the commits are linked
        while (linked.load() <= bi + expected_commits) {
        }
      }
    }
    for (auto& t : threads) {
      t.join();
    }
    if (options.two_write_queues) {
      // In this case none of the above scheduling tricks to deterministically
      // form merged batches works because the writes go to separate queues.
      // This would result in different write groups in each run of the test. We
      // still keep the test since although non-deterministic and hard to debug,
      // it is still useful to have.
      // TODO(myabandeh): Add a deterministic unit test for two_write_queues
    }

    // Check if memtable inserts advanced seq number as expected
    seq = db_impl->TEST_GetLastVisibleSequence();
    ASSERT_EQ(exp_seq, seq);

    rocksdb::SyncPoint::GetInstance()->DisableProcessing();
    rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();

    // Check if recovery preserves the last sequence number
    db_impl->FlushWAL(true);
    ReOpenNoDelete();
    assert(db != nullptr);
    db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
    seq = db_impl->TEST_GetLastVisibleSequence();
    ASSERT_LE(exp_seq, seq);

    // Check if flush preserves the last sequence number
    db_impl->Flush(fopt);
    seq = db_impl->GetLatestSequenceNumber();
    ASSERT_LE(exp_seq, seq);

    // Check if recovery after flush preserves the last sequence number
    db_impl->FlushWAL(true);
    ReOpenNoDelete();
    assert(db != nullptr);
    db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
    seq = db_impl->GetLatestSequenceNumber();
    ASSERT_LE(exp_seq, seq);
  }
}

// Run a couple of different txns among them some uncommitted. Restart the db at
// a couple points to check whether the list of uncommitted txns are recovered
// properly.
TEST_P(WritePreparedTransactionTest, BasicRecoveryTest) {
  options.disable_auto_compactions = true;
  ReOpen();
  WritePreparedTxnDB* wp_db = dynamic_cast<WritePreparedTxnDB*>(db);

  txn_t0(0);

  TransactionOptions txn_options;
  WriteOptions write_options;
  size_t index = 1000;
  Transaction* txn0 = db->BeginTransaction(write_options, txn_options);
  auto istr0 = std::to_string(index);
  auto s = txn0->SetName("xid" + istr0);
  ASSERT_OK(s);
  s = txn0->Put(Slice("foo0" + istr0), Slice("bar0" + istr0));
  ASSERT_OK(s);
  s = txn0->Prepare();
  auto prep_seq_0 = txn0->GetId();

  txn_t1(0);

  index++;
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
  auto istr1 = std::to_string(index);
  s = txn1->SetName("xid" + istr1);
  ASSERT_OK(s);
  s = txn1->Put(Slice("foo1" + istr1), Slice("bar"));
  ASSERT_OK(s);
  s = txn1->Prepare();
  auto prep_seq_1 = txn1->GetId();

  txn_t2(0);

  ReadOptions ropt;
  PinnableSlice pinnable_val;
  // Check the value is not committed before restart
  s = db->Get(ropt, db->DefaultColumnFamily(), "foo0" + istr0, &pinnable_val);
  ASSERT_TRUE(s.IsNotFound());
  pinnable_val.Reset();

  delete txn0;
  delete txn1;
  wp_db->db_impl_->FlushWAL(true);
  wp_db->TEST_Crash();
  ReOpenNoDelete();
  assert(db != nullptr);
  wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
  // After recovery, all the uncommitted txns (0 and 1) should be inserted into
  // delayed_prepared_
  ASSERT_TRUE(wp_db->prepared_txns_.empty());
  ASSERT_FALSE(wp_db->delayed_prepared_empty_);
  ASSERT_LE(prep_seq_0, wp_db->max_evicted_seq_);
  ASSERT_LE(prep_seq_1, wp_db->max_evicted_seq_);
  {
    ReadLock rl(&wp_db->prepared_mutex_);
    ASSERT_EQ(2, wp_db->delayed_prepared_.size());
    ASSERT_TRUE(wp_db->delayed_prepared_.find(prep_seq_0) !=
                wp_db->delayed_prepared_.end());
    ASSERT_TRUE(wp_db->delayed_prepared_.find(prep_seq_1) !=
                wp_db->delayed_prepared_.end());
  }

  // Check the value is still not committed after restart
  s = db->Get(ropt, db->DefaultColumnFamily(), "foo0" + istr0, &pinnable_val);
  ASSERT_TRUE(s.IsNotFound());
  pinnable_val.Reset();

  txn_t3(0);

  // Test that a recovered txns will be properly marked committed for the next
  // recovery
  txn1 = db->GetTransactionByName("xid" + istr1);
  ASSERT_NE(txn1, nullptr);
  txn1->Commit();
  delete txn1;

  index++;
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  auto istr2 = std::to_string(index);
  s = txn2->SetName("xid" + istr2);
  ASSERT_OK(s);
  s = txn2->Put(Slice("foo2" + istr2), Slice("bar"));
  ASSERT_OK(s);
  s = txn2->Prepare();
  auto prep_seq_2 = txn2->GetId();

  delete txn2;
  wp_db->db_impl_->FlushWAL(true);
  wp_db->TEST_Crash();
  ReOpenNoDelete();
  assert(db != nullptr);
  wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
  ASSERT_TRUE(wp_db->prepared_txns_.empty());
  ASSERT_FALSE(wp_db->delayed_prepared_empty_);

  // 0 and 2 are prepared and 1 is committed
  {
    ReadLock rl(&wp_db->prepared_mutex_);
    ASSERT_EQ(2, wp_db->delayed_prepared_.size());
    const auto& end = wp_db->delayed_prepared_.end();
    ASSERT_NE(wp_db->delayed_prepared_.find(prep_seq_0), end);
    ASSERT_EQ(wp_db->delayed_prepared_.find(prep_seq_1), end);
    ASSERT_NE(wp_db->delayed_prepared_.find(prep_seq_2), end);
  }
  ASSERT_LE(prep_seq_0, wp_db->max_evicted_seq_);
  ASSERT_LE(prep_seq_2, wp_db->max_evicted_seq_);

  // Commit all the remaining txns
  txn0 = db->GetTransactionByName("xid" + istr0);
  ASSERT_NE(txn0, nullptr);
  txn0->Commit();
  txn2 = db->GetTransactionByName("xid" + istr2);
  ASSERT_NE(txn2, nullptr);
  txn2->Commit();

  // Check the value is committed after commit
  s = db->Get(ropt, db->DefaultColumnFamily(), "foo0" + istr0, &pinnable_val);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(pinnable_val == ("bar0" + istr0));
  pinnable_val.Reset();

  delete txn0;
  delete txn2;
  wp_db->db_impl_->FlushWAL(true);
  ReOpenNoDelete();
  assert(db != nullptr);
  wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
  ASSERT_TRUE(wp_db->prepared_txns_.empty());
  ASSERT_TRUE(wp_db->delayed_prepared_empty_);

  // Check the value is still committed after recovery
  s = db->Get(ropt, db->DefaultColumnFamily(), "foo0" + istr0, &pinnable_val);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(pinnable_val == ("bar0" + istr0));
  pinnable_val.Reset();
}

// After recovery the commit map is empty while the max is set. The code would
// go through a different path which requires a separate test. Test that the
// committed data before the restart is visible to all snapshots.
TEST_P(WritePreparedTransactionTest, IsInSnapshotEmptyMapTest) {
  for (bool end_with_prepare : {false, true}) {
    ReOpen();
    WriteOptions woptions;
    ASSERT_OK(db->Put(woptions, "key", "value"));
    ASSERT_OK(db->Put(woptions, "key", "value"));
    ASSERT_OK(db->Put(woptions, "key", "value"));
    SequenceNumber prepare_seq = kMaxSequenceNumber;
    if (end_with_prepare) {
      TransactionOptions txn_options;
      Transaction* txn = db->BeginTransaction(woptions, txn_options);
      ASSERT_OK(txn->SetName("xid0"));
      ASSERT_OK(txn->Prepare());
      prepare_seq = txn->GetId();
      delete txn;
    }
    dynamic_cast<WritePreparedTxnDB*>(db)->TEST_Crash();
    auto db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
    db_impl->FlushWAL(true);
    ReOpenNoDelete();
    WritePreparedTxnDB* wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
    assert(wp_db != nullptr);
    ASSERT_GT(wp_db->max_evicted_seq_, 0);  // max after recovery
    // Take a snapshot right after recovery
    const Snapshot* snap = db->GetSnapshot();
    auto snap_seq = snap->GetSequenceNumber();
    ASSERT_GT(snap_seq, 0);

    for (SequenceNumber seq = 0;
         seq <= wp_db->max_evicted_seq_ && seq != prepare_seq; seq++) {
      ASSERT_TRUE(wp_db->IsInSnapshot(seq, snap_seq));
    }
    if (end_with_prepare) {
      ASSERT_FALSE(wp_db->IsInSnapshot(prepare_seq, snap_seq));
    }
    // trivial check
    ASSERT_FALSE(wp_db->IsInSnapshot(snap_seq + 1, snap_seq));

    db->ReleaseSnapshot(snap);

    ASSERT_OK(db->Put(woptions, "key", "value"));
    // Take a snapshot after some writes
    snap = db->GetSnapshot();
    snap_seq = snap->GetSequenceNumber();
    for (SequenceNumber seq = 0;
         seq <= wp_db->max_evicted_seq_ && seq != prepare_seq; seq++) {
      ASSERT_TRUE(wp_db->IsInSnapshot(seq, snap_seq));
    }
    if (end_with_prepare) {
      ASSERT_FALSE(wp_db->IsInSnapshot(prepare_seq, snap_seq));
    }
    // trivial check
    ASSERT_FALSE(wp_db->IsInSnapshot(snap_seq + 1, snap_seq));

    db->ReleaseSnapshot(snap);
  }
}

// Shows the contract of IsInSnapshot when called on invalid/released snapshots
TEST_P(WritePreparedTransactionTest, IsInSnapshotReleased) {
  WritePreparedTxnDB* wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
  WriteOptions woptions;
  ASSERT_OK(db->Put(woptions, "key", "value"));
  // snap seq = 1
  const Snapshot* snap1 = db->GetSnapshot();
  ASSERT_OK(db->Put(woptions, "key", "value"));
  ASSERT_OK(db->Put(woptions, "key", "value"));
  // snap seq = 3
  const Snapshot* snap2 = db->GetSnapshot();
  const SequenceNumber seq = 1;
  // Evict seq out of commit cache
  size_t overwrite_seq = wp_db->COMMIT_CACHE_SIZE + seq;
  wp_db->AddCommitted(overwrite_seq, overwrite_seq);
  SequenceNumber snap_seq;
  uint64_t min_uncommitted = 0;
  bool released;

  released = false;
  snap_seq = snap1->GetSequenceNumber();
  ASSERT_LE(seq, snap_seq);
  // Valid snapshot lower than max
  ASSERT_LE(snap_seq, wp_db->max_evicted_seq_);
  ASSERT_TRUE(wp_db->IsInSnapshot(seq, snap_seq, min_uncommitted, &released));
  ASSERT_FALSE(released);

  released = false;
  snap_seq = snap1->GetSequenceNumber();
  // Invaid snapshot lower than max
  ASSERT_LE(snap_seq + 1, wp_db->max_evicted_seq_);
  ASSERT_TRUE(
      wp_db->IsInSnapshot(seq, snap_seq + 1, min_uncommitted, &released));
  ASSERT_TRUE(released);

  db->ReleaseSnapshot(snap1);

  released = false;
  // Released snapshot lower than max
  ASSERT_TRUE(wp_db->IsInSnapshot(seq, snap_seq, min_uncommitted, &released));
  // The release does not take affect until the next max advance
  ASSERT_FALSE(released);

  released = false;
  // Invaid snapshot lower than max
  ASSERT_TRUE(
      wp_db->IsInSnapshot(seq, snap_seq + 1, min_uncommitted, &released));
  ASSERT_TRUE(released);

  // This make the snapshot release to reflect in txn db structures
  wp_db->AdvanceMaxEvictedSeq(wp_db->max_evicted_seq_,
                              wp_db->max_evicted_seq_ + 1);

  released = false;
  // Released snapshot lower than max
  ASSERT_TRUE(wp_db->IsInSnapshot(seq, snap_seq, min_uncommitted, &released));
  ASSERT_TRUE(released);

  released = false;
  // Invaid snapshot lower than max
  ASSERT_TRUE(
      wp_db->IsInSnapshot(seq, snap_seq + 1, min_uncommitted, &released));
  ASSERT_TRUE(released);

  snap_seq = snap2->GetSequenceNumber();

  released = false;
  // Unreleased snapshot lower than max
  ASSERT_TRUE(wp_db->IsInSnapshot(seq, snap_seq, min_uncommitted, &released));
  ASSERT_FALSE(released);

  db->ReleaseSnapshot(snap2);
}

// Test WritePreparedTxnDB's IsInSnapshot against different ordering of
// snapshot, max_committed_seq_, prepared, and commit entries.
TEST_P(WritePreparedTransactionTest, IsInSnapshotTest) {
  WriteOptions wo;
  // Use small commit cache to trigger lots of eviction and fast advance of
  // max_evicted_seq_
  const size_t commit_cache_bits = 3;
  // Same for snapshot cache size
  const size_t snapshot_cache_bits = 2;

  // Take some preliminary snapshots first. This is to stress the data structure
  // that holds the old snapshots as it will be designed to be efficient when
  // only a few snapshots are below the max_evicted_seq_.
  for (int max_snapshots = 1; max_snapshots < 20; max_snapshots++) {
    // Leave some gap between the preliminary snapshots and the final snapshot
    // that we check. This should test for also different overlapping scenarios
    // between the last snapshot and the commits.
    for (int max_gap = 1; max_gap < 10; max_gap++) {
      // Since we do not actually write to db, we mock the seq as it would be
      // increased by the db. The only exception is that we need db seq to
      // advance for our snapshots. for which we apply a dummy put each time we
      // increase our mock of seq.
      uint64_t seq = 0;
      // At each step we prepare a txn and then we commit it in the next txn.
      // This emulates the consecutive transactions that write to the same key
      uint64_t cur_txn = 0;
      // Number of snapshots taken so far
      int num_snapshots = 0;
      // Number of gaps applied so far
      int gap_cnt = 0;
      // The final snapshot that we will inspect
      uint64_t snapshot = 0;
      bool found_committed = false;
      // To stress the data structure that maintain prepared txns, at each cycle
      // we add a new prepare txn. These do not mean to be committed for
      // snapshot inspection.
      std::set<uint64_t> prepared;
      // We keep the list of txns committed before we take the last snapshot.
      // These should be the only seq numbers that will be found in the snapshot
      std::set<uint64_t> committed_before;
      // The set of commit seq numbers to be excluded from IsInSnapshot queries
      std::set<uint64_t> commit_seqs;
      DBImpl* mock_db = new DBImpl(options, dbname);
      UpdateTransactionDBOptions(snapshot_cache_bits, commit_cache_bits);
      std::unique_ptr<WritePreparedTxnDBMock> wp_db(
          new WritePreparedTxnDBMock(mock_db, txn_db_options));
      // We continue until max advances a bit beyond the snapshot.
      while (!snapshot || wp_db->max_evicted_seq_ < snapshot + 100) {
        // do prepare for a transaction
        seq++;
        wp_db->AddPrepared(seq);
        prepared.insert(seq);

        // If cur_txn is not started, do prepare for it.
        if (!cur_txn) {
          seq++;
          cur_txn = seq;
          wp_db->AddPrepared(cur_txn);
        } else {                                     // else commit it
          seq++;
          wp_db->AddCommitted(cur_txn, seq);
          wp_db->RemovePrepared(cur_txn);
          commit_seqs.insert(seq);
          if (!snapshot) {
            committed_before.insert(cur_txn);
          }
          cur_txn = 0;
        }

        if (num_snapshots < max_snapshots - 1) {
          // Take preliminary snapshots
          wp_db->TakeSnapshot(seq);
          num_snapshots++;
        } else if (gap_cnt < max_gap) {
          // Wait for some gap before taking the final snapshot
          gap_cnt++;
        } else if (!snapshot) {
          // Take the final snapshot if it is not already taken
          snapshot = seq;
          wp_db->TakeSnapshot(snapshot);
          num_snapshots++;
        }

        // If the snapshot is taken, verify seq numbers visible to it. We redo
        // it at each cycle to test that the system is still sound when
        // max_evicted_seq_ advances.
        if (snapshot) {
          for (uint64_t s = 1;
               s <= seq && commit_seqs.find(s) == commit_seqs.end(); s++) {
            bool was_committed =
                (committed_before.find(s) != committed_before.end());
            bool is_in_snapshot = wp_db->IsInSnapshot(s, snapshot);
            if (was_committed != is_in_snapshot) {
              printf("max_snapshots %d max_gap %d seq %" PRIu64 " max %" PRIu64
                     " snapshot %" PRIu64
                     " gap_cnt %d num_snapshots %d s %" PRIu64 "\n",
                     max_snapshots, max_gap, seq,
                     wp_db->max_evicted_seq_.load(), snapshot, gap_cnt,
                     num_snapshots, s);
            }
            ASSERT_EQ(was_committed, is_in_snapshot);
            found_committed = found_committed || is_in_snapshot;
          }
        }
      }
      // Safety check to make sure the test actually ran
      ASSERT_TRUE(found_committed);
      // As an extra check, check if prepared set will be properly empty after
      // they are committed.
      if (cur_txn) {
        wp_db->AddCommitted(cur_txn, seq);
        wp_db->RemovePrepared(cur_txn);
      }
      for (auto p : prepared) {
        wp_db->AddCommitted(p, seq);
        wp_db->RemovePrepared(p);
      }
      ASSERT_TRUE(wp_db->delayed_prepared_.empty());
      ASSERT_TRUE(wp_db->prepared_txns_.empty());
    }
  }
}

void ASSERT_SAME(ReadOptions roptions, TransactionDB* db, Status exp_s,
                 PinnableSlice& exp_v, Slice key) {
  Status s;
  PinnableSlice v;
  s = db->Get(roptions, db->DefaultColumnFamily(), key, &v);
  ASSERT_TRUE(exp_s == s);
  ASSERT_TRUE(s.ok() || s.IsNotFound());
  if (s.ok()) {
    ASSERT_TRUE(exp_v == v);
  }

  // Try with MultiGet API too
  std::vector<std::string> values;
  auto s_vec =
      db->MultiGet(roptions, {db->DefaultColumnFamily()}, {key}, &values);
  ASSERT_EQ(1, values.size());
  ASSERT_EQ(1, s_vec.size());
  s = s_vec[0];
  ASSERT_TRUE(exp_s == s);
  ASSERT_TRUE(s.ok() || s.IsNotFound());
  if (s.ok()) {
    ASSERT_TRUE(exp_v == values[0]);
  }
}

void ASSERT_SAME(TransactionDB* db, Status exp_s, PinnableSlice& exp_v,
                 Slice key) {
  ASSERT_SAME(ReadOptions(), db, exp_s, exp_v, key);
}

TEST_P(WritePreparedTransactionTest, RollbackTest) {
  ReadOptions roptions;
  WriteOptions woptions;
  TransactionOptions txn_options;
  const size_t num_keys = 4;
  const size_t num_values = 5;
  for (size_t ikey = 1; ikey <= num_keys; ikey++) {
    for (size_t ivalue = 0; ivalue < num_values; ivalue++) {
      for (bool crash : {false, true}) {
        ReOpen();
        WritePreparedTxnDB* wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
        std::string key_str = "key" + ToString(ikey);
        switch (ivalue) {
          case 0:
            break;
          case 1:
            ASSERT_OK(db->Put(woptions, key_str, "initvalue1"));
            break;
          case 2:
            ASSERT_OK(db->Merge(woptions, key_str, "initvalue2"));
            break;
          case 3:
            ASSERT_OK(db->Delete(woptions, key_str));
            break;
          case 4:
            ASSERT_OK(db->SingleDelete(woptions, key_str));
            break;
          default:
            assert(0);
        }

        PinnableSlice v1;
        auto s1 =
            db->Get(roptions, db->DefaultColumnFamily(), Slice("key1"), &v1);
        PinnableSlice v2;
        auto s2 =
            db->Get(roptions, db->DefaultColumnFamily(), Slice("key2"), &v2);
        PinnableSlice v3;
        auto s3 =
            db->Get(roptions, db->DefaultColumnFamily(), Slice("key3"), &v3);
        PinnableSlice v4;
        auto s4 =
            db->Get(roptions, db->DefaultColumnFamily(), Slice("key4"), &v4);
        Transaction* txn = db->BeginTransaction(woptions, txn_options);
        auto s = txn->SetName("xid0");
        ASSERT_OK(s);
        s = txn->Put(Slice("key1"), Slice("value1"));
        ASSERT_OK(s);
        s = txn->Merge(Slice("key2"), Slice("value2"));
        ASSERT_OK(s);
        s = txn->Delete(Slice("key3"));
        ASSERT_OK(s);
        s = txn->SingleDelete(Slice("key4"));
        ASSERT_OK(s);
        s = txn->Prepare();
        ASSERT_OK(s);

        {
          ReadLock rl(&wp_db->prepared_mutex_);
          ASSERT_FALSE(wp_db->prepared_txns_.empty());
          ASSERT_EQ(txn->GetId(), wp_db->prepared_txns_.top());
        }

        ASSERT_SAME(db, s1, v1, "key1");
        ASSERT_SAME(db, s2, v2, "key2");
        ASSERT_SAME(db, s3, v3, "key3");
        ASSERT_SAME(db, s4, v4, "key4");

        if (crash) {
          delete txn;
          auto db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
          db_impl->FlushWAL(true);
          dynamic_cast<WritePreparedTxnDB*>(db)->TEST_Crash();
          ReOpenNoDelete();
          assert(db != nullptr);
          wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
          txn = db->GetTransactionByName("xid0");
          ASSERT_FALSE(wp_db->delayed_prepared_empty_);
          ReadLock rl(&wp_db->prepared_mutex_);
          ASSERT_TRUE(wp_db->prepared_txns_.empty());
          ASSERT_FALSE(wp_db->delayed_prepared_.empty());
          ASSERT_TRUE(wp_db->delayed_prepared_.find(txn->GetId()) !=
                      wp_db->delayed_prepared_.end());
        }

        ASSERT_SAME(db, s1, v1, "key1");
        ASSERT_SAME(db, s2, v2, "key2");
        ASSERT_SAME(db, s3, v3, "key3");
        ASSERT_SAME(db, s4, v4, "key4");

        s = txn->Rollback();
        ASSERT_OK(s);

        {
          ASSERT_TRUE(wp_db->delayed_prepared_empty_);
          ReadLock rl(&wp_db->prepared_mutex_);
          ASSERT_TRUE(wp_db->prepared_txns_.empty());
          ASSERT_TRUE(wp_db->delayed_prepared_.empty());
        }

        ASSERT_SAME(db, s1, v1, "key1");
        ASSERT_SAME(db, s2, v2, "key2");
        ASSERT_SAME(db, s3, v3, "key3");
        ASSERT_SAME(db, s4, v4, "key4");
        delete txn;
      }
    }
  }
}

TEST_P(WritePreparedTransactionTest, DisableGCDuringRecoveryTest) {
  // Use large buffer to avoid memtable flush after 1024 insertions
  options.write_buffer_size = 1024 * 1024;
  ReOpen();
  std::vector<KeyVersion> versions;
  uint64_t seq = 0;
  for (uint64_t i = 1; i <= 1024; i++) {
    std::string v = "bar" + ToString(i);
    ASSERT_OK(db->Put(WriteOptions(), "foo", v));
    VerifyKeys({{"foo", v}});
    seq++;  // one for the key/value
    KeyVersion kv = {"foo", v, seq, kTypeValue};
    if (options.two_write_queues) {
      seq++;  // one for the commit
    }
    versions.emplace_back(kv);
  }
  std::reverse(std::begin(versions), std::end(versions));
  VerifyInternalKeys(versions);
  DBImpl* db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
  db_impl->FlushWAL(true);
  // Use small buffer to ensure memtable flush during recovery
  options.write_buffer_size = 1024;
  ReOpenNoDelete();
  VerifyInternalKeys(versions);
}

TEST_P(WritePreparedTransactionTest, SequenceNumberZeroTest) {
  ASSERT_OK(db->Put(WriteOptions(), "foo", "bar"));
  VerifyKeys({{"foo", "bar"}});
  const Snapshot* snapshot = db->GetSnapshot();
  ASSERT_OK(db->Flush(FlushOptions()));
  // Dummy keys to avoid compaction trivially move files and get around actual
  // compaction logic.
  ASSERT_OK(db->Put(WriteOptions(), "a", "dummy"));
  ASSERT_OK(db->Put(WriteOptions(), "z", "dummy"));
  ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  // Compaction will output keys with sequence number 0, if it is visible to
  // earliest snapshot. Make sure IsInSnapshot() report sequence number 0 is
  // visible to any snapshot.
  VerifyKeys({{"foo", "bar"}});
  VerifyKeys({{"foo", "bar"}}, snapshot);
  VerifyInternalKeys({{"foo", "bar", 0, kTypeValue}});
  db->ReleaseSnapshot(snapshot);
}

// Compaction should not remove a key if it is not committed, and should
// proceed with older versions of the key as-if the new version doesn't exist.
TEST_P(WritePreparedTransactionTest, CompactionShouldKeepUncommittedKeys) {
  options.disable_auto_compactions = true;
  ReOpen();
  DBImpl* db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
  // Snapshots to avoid keys get evicted.
  std::vector<const Snapshot*> snapshots;
  // Keep track of expected sequence number.
  SequenceNumber expected_seq = 0;

  auto add_key = [&](std::function<Status()> func) {
    ASSERT_OK(func());
    expected_seq++;
    if (options.two_write_queues) {
      expected_seq++;  // 1 for commit
    }
    ASSERT_EQ(expected_seq, db_impl->TEST_GetLastVisibleSequence());
    snapshots.push_back(db->GetSnapshot());
  };

  // Each key here represent a standalone test case.
  add_key([&]() { return db->Put(WriteOptions(), "key1", "value1_1"); });
  add_key([&]() { return db->Put(WriteOptions(), "key2", "value2_1"); });
  add_key([&]() { return db->Put(WriteOptions(), "key3", "value3_1"); });
  add_key([&]() { return db->Put(WriteOptions(), "key4", "value4_1"); });
  add_key([&]() { return db->Merge(WriteOptions(), "key5", "value5_1"); });
  add_key([&]() { return db->Merge(WriteOptions(), "key5", "value5_2"); });
  add_key([&]() { return db->Put(WriteOptions(), "key6", "value6_1"); });
  add_key([&]() { return db->Put(WriteOptions(), "key7", "value7_1"); });
  ASSERT_OK(db->Flush(FlushOptions()));
  add_key([&]() { return db->Delete(WriteOptions(), "key6"); });
  add_key([&]() { return db->SingleDelete(WriteOptions(), "key7"); });

  auto* transaction = db->BeginTransaction(WriteOptions());
  ASSERT_OK(transaction->SetName("txn"));
  ASSERT_OK(transaction->Put("key1", "value1_2"));
  ASSERT_OK(transaction->Delete("key2"));
  ASSERT_OK(transaction->SingleDelete("key3"));
  ASSERT_OK(transaction->Merge("key4", "value4_2"));
  ASSERT_OK(transaction->Merge("key5", "value5_3"));
  ASSERT_OK(transaction->Put("key6", "value6_2"));
  ASSERT_OK(transaction->Put("key7", "value7_2"));
  // Prepare but not commit.
  ASSERT_OK(transaction->Prepare());
  ASSERT_EQ(++expected_seq, db->GetLatestSequenceNumber());
  ASSERT_OK(db->Flush(FlushOptions()));
  for (auto* s : snapshots) {
    db->ReleaseSnapshot(s);
  }
  // Dummy keys to avoid compaction trivially move files and get around actual
  // compaction logic.
  ASSERT_OK(db->Put(WriteOptions(), "a", "dummy"));
  ASSERT_OK(db->Put(WriteOptions(), "z", "dummy"));
  ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  VerifyKeys({
      {"key1", "value1_1"},
      {"key2", "value2_1"},
      {"key3", "value3_1"},
      {"key4", "value4_1"},
      {"key5", "value5_1,value5_2"},
      {"key6", "NOT_FOUND"},
      {"key7", "NOT_FOUND"},
  });
  VerifyInternalKeys({
      {"key1", "value1_2", expected_seq, kTypeValue},
      {"key1", "value1_1", 0, kTypeValue},
      {"key2", "", expected_seq, kTypeDeletion},
      {"key2", "value2_1", 0, kTypeValue},
      {"key3", "", expected_seq, kTypeSingleDeletion},
      {"key3", "value3_1", 0, kTypeValue},
      {"key4", "value4_2", expected_seq, kTypeMerge},
      {"key4", "value4_1", 0, kTypeValue},
      {"key5", "value5_3", expected_seq, kTypeMerge},
      {"key5", "value5_1,value5_2", 0, kTypeValue},
      {"key6", "value6_2", expected_seq, kTypeValue},
      {"key7", "value7_2", expected_seq, kTypeValue},
  });
  ASSERT_OK(transaction->Commit());
  VerifyKeys({
      {"key1", "value1_2"},
      {"key2", "NOT_FOUND"},
      {"key3", "NOT_FOUND"},
      {"key4", "value4_1,value4_2"},
      {"key5", "value5_1,value5_2,value5_3"},
      {"key6", "value6_2"},
      {"key7", "value7_2"},
  });
  delete transaction;
}

// Compaction should keep keys visible to a snapshot based on commit sequence,
// not just prepare sequence.
TEST_P(WritePreparedTransactionTest, CompactionShouldKeepSnapshotVisibleKeys) {
  options.disable_auto_compactions = true;
  ReOpen();
  // Keep track of expected sequence number.
  SequenceNumber expected_seq = 0;
  auto* txn1 = db->BeginTransaction(WriteOptions());
  ASSERT_OK(txn1->SetName("txn1"));
  ASSERT_OK(txn1->Put("key1", "value1_1"));
  ASSERT_OK(txn1->Prepare());
  ASSERT_EQ(++expected_seq, db->GetLatestSequenceNumber());
  ASSERT_OK(txn1->Commit());
  DBImpl* db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
  ASSERT_EQ(++expected_seq, db_impl->TEST_GetLastVisibleSequence());
  delete txn1;
  // Take a snapshots to avoid keys get evicted before compaction.
  const Snapshot* snapshot1 = db->GetSnapshot();
  auto* txn2 = db->BeginTransaction(WriteOptions());
  ASSERT_OK(txn2->SetName("txn2"));
  ASSERT_OK(txn2->Put("key2", "value2_1"));
  ASSERT_OK(txn2->Prepare());
  ASSERT_EQ(++expected_seq, db->GetLatestSequenceNumber());
  // txn1 commit before snapshot2 and it is visible to snapshot2.
  // txn2 commit after snapshot2 and it is not visible.
  const Snapshot* snapshot2 = db->GetSnapshot();
  ASSERT_OK(txn2->Commit());
  ASSERT_EQ(++expected_seq, db_impl->TEST_GetLastVisibleSequence());
  delete txn2;
  // Take a snapshots to avoid keys get evicted before compaction.
  const Snapshot* snapshot3 = db->GetSnapshot();
  ASSERT_OK(db->Put(WriteOptions(), "key1", "value1_2"));
  expected_seq++;  // 1 for write
  SequenceNumber seq1 = expected_seq;
  if (options.two_write_queues) {
    expected_seq++;  // 1 for commit
  }
  ASSERT_EQ(expected_seq, db_impl->TEST_GetLastVisibleSequence());
  ASSERT_OK(db->Put(WriteOptions(), "key2", "value2_2"));
  expected_seq++;  // 1 for write
  SequenceNumber seq2 = expected_seq;
  if (options.two_write_queues) {
    expected_seq++;  // 1 for commit
  }
  ASSERT_EQ(expected_seq, db_impl->TEST_GetLastVisibleSequence());
  ASSERT_OK(db->Flush(FlushOptions()));
  db->ReleaseSnapshot(snapshot1);
  db->ReleaseSnapshot(snapshot3);
  // Dummy keys to avoid compaction trivially move files and get around actual
  // compaction logic.
  ASSERT_OK(db->Put(WriteOptions(), "a", "dummy"));
  ASSERT_OK(db->Put(WriteOptions(), "z", "dummy"));
  ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  VerifyKeys({{"key1", "value1_2"}, {"key2", "value2_2"}});
  VerifyKeys({{"key1", "value1_1"}, {"key2", "NOT_FOUND"}}, snapshot2);
  VerifyInternalKeys({
      {"key1", "value1_2", seq1, kTypeValue},
      // "value1_1" is visible to snapshot2. Also keys at bottom level visible
      // to earliest snapshot will output with seq = 0.
      {"key1", "value1_1", 0, kTypeValue},
      {"key2", "value2_2", seq2, kTypeValue},
  });
  db->ReleaseSnapshot(snapshot2);
}

TEST_P(WritePreparedTransactionTest, SmallestUncommittedOptimization) {
  const size_t snapshot_cache_bits = 7;  // same as default
  const size_t commit_cache_bits = 0;    // disable commit cache
  for (bool has_recent_prepare : {true, false}) {
    UpdateTransactionDBOptions(snapshot_cache_bits, commit_cache_bits);
    ReOpen();

    ASSERT_OK(db->Put(WriteOptions(), "key1", "value1"));
    auto* transaction =
        db->BeginTransaction(WriteOptions(), TransactionOptions(), nullptr);
    ASSERT_OK(transaction->SetName("txn"));
    ASSERT_OK(transaction->Delete("key1"));
    ASSERT_OK(transaction->Prepare());
    // snapshot1 should get min_uncommitted from prepared_txns_ heap.
    auto snapshot1 = db->GetSnapshot();
    ASSERT_EQ(transaction->GetId(),
              ((SnapshotImpl*)snapshot1)->min_uncommitted_);
    // Add a commit to advance max_evicted_seq and move the prepared transaction
    // into delayed_prepared_ set.
    ASSERT_OK(db->Put(WriteOptions(), "key2", "value2"));
    Transaction* txn2 = nullptr;
    if (has_recent_prepare) {
      txn2 =
          db->BeginTransaction(WriteOptions(), TransactionOptions(), nullptr);
      ASSERT_OK(txn2->SetName("txn2"));
      ASSERT_OK(txn2->Put("key3", "value3"));
      ASSERT_OK(txn2->Prepare());
    }
    // snapshot2 should get min_uncommitted from delayed_prepared_ set.
    auto snapshot2 = db->GetSnapshot();
    ASSERT_EQ(transaction->GetId(),
              ((SnapshotImpl*)snapshot1)->min_uncommitted_);
    ASSERT_OK(transaction->Commit());
    delete transaction;
    if (has_recent_prepare) {
      ASSERT_OK(txn2->Commit());
      delete txn2;
    }
    VerifyKeys({{"key1", "NOT_FOUND"}});
    VerifyKeys({{"key1", "value1"}}, snapshot1);
    VerifyKeys({{"key1", "value1"}}, snapshot2);
    db->ReleaseSnapshot(snapshot1);
    db->ReleaseSnapshot(snapshot2);
  }
}

// Insert two values, v1 and v2, for a key. Between prepare and commit of v2
// take two snapshots, s1 and s2. Release s1 during compaction.
// Test to make sure compaction doesn't get confused and think s1 can see both
// values, and thus compact out the older value by mistake.
TEST_P(WritePreparedTransactionTest, ReleaseSnapshotDuringCompaction) {
  const size_t snapshot_cache_bits = 7;  // same as default
  const size_t commit_cache_bits = 0;    // minimum commit cache
  UpdateTransactionDBOptions(snapshot_cache_bits, commit_cache_bits);
  ReOpen();

  ASSERT_OK(db->Put(WriteOptions(), "key1", "value1_1"));
  auto* transaction =
      db->BeginTransaction(WriteOptions(), TransactionOptions(), nullptr);
  ASSERT_OK(transaction->SetName("txn"));
  ASSERT_OK(transaction->Put("key1", "value1_2"));
  ASSERT_OK(transaction->Prepare());
  auto snapshot1 = db->GetSnapshot();
  // Increment sequence number.
  ASSERT_OK(db->Put(WriteOptions(), "key2", "value2"));
  auto snapshot2 = db->GetSnapshot();
  ASSERT_OK(transaction->Commit());
  delete transaction;
  VerifyKeys({{"key1", "value1_2"}});
  VerifyKeys({{"key1", "value1_1"}}, snapshot1);
  VerifyKeys({{"key1", "value1_1"}}, snapshot2);
  // Add a flush to avoid compaction to fallback to trivial move.

  auto callback = [&](void*) {
    // Release snapshot1 after CompactionIterator init.
    // CompactionIterator need to figure out the earliest snapshot
    // that can see key1:value1_2 is kMaxSequenceNumber, not
    // snapshot1 or snapshot2.
    db->ReleaseSnapshot(snapshot1);
    // Add some keys to advance max_evicted_seq.
    ASSERT_OK(db->Put(WriteOptions(), "key3", "value3"));
    ASSERT_OK(db->Put(WriteOptions(), "key4", "value4"));
  };
  SyncPoint::GetInstance()->SetCallBack("CompactionIterator:AfterInit",
                                        callback);
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(db->Flush(FlushOptions()));
  VerifyKeys({{"key1", "value1_2"}});
  VerifyKeys({{"key1", "value1_1"}}, snapshot2);
  db->ReleaseSnapshot(snapshot2);
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

// Insert two values, v1 and v2, for a key. Take two snapshots, s1 and s2,
// after committing v2. Release s1 during compaction, right after compaction
// processes v2 and before processes v1. Test to make sure compaction doesn't
// get confused and believe v1 and v2 are visible to different snapshot
// (v1 by s2, v2 by s1) and refuse to compact out v1.
TEST_P(WritePreparedTransactionTest, ReleaseSnapshotDuringCompaction2) {
  const size_t snapshot_cache_bits = 7;  // same as default
  const size_t commit_cache_bits = 0;    // minimum commit cache
  UpdateTransactionDBOptions(snapshot_cache_bits, commit_cache_bits);
  ReOpen();

  ASSERT_OK(db->Put(WriteOptions(), "key1", "value1"));
  ASSERT_OK(db->Put(WriteOptions(), "key1", "value2"));
  SequenceNumber v2_seq = db->GetLatestSequenceNumber();
  auto* s1 = db->GetSnapshot();
  // Advance sequence number.
  ASSERT_OK(db->Put(WriteOptions(), "key2", "dummy"));
  auto* s2 = db->GetSnapshot();

  int count_value = 0;
  auto callback = [&](void* arg) {
    auto* ikey = reinterpret_cast<ParsedInternalKey*>(arg);
    if (ikey->user_key == "key1") {
      count_value++;
      if (count_value == 2) {
        // Processing v1.
        db->ReleaseSnapshot(s1);
        // Add some keys to advance max_evicted_seq and update
        // old_commit_map.
        ASSERT_OK(db->Put(WriteOptions(), "key3", "dummy"));
        ASSERT_OK(db->Put(WriteOptions(), "key4", "dummy"));
      }
    }
  };
  SyncPoint::GetInstance()->SetCallBack("CompactionIterator:ProcessKV",
                                        callback);
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(db->Flush(FlushOptions()));
  // value1 should be compact out.
  VerifyInternalKeys({{"key1", "value2", v2_seq, kTypeValue}});

  // cleanup
  db->ReleaseSnapshot(s2);
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

// Insert two values, v1 and v2, for a key. Insert another dummy key
// so to evict the commit cache for v2, while v1 is still in commit cache.
// Take two snapshots, s1 and s2. Release s1 during compaction.
// Since commit cache for v2 is evicted, and old_commit_map don't have
// s1 (it is released),
// TODO(myabandeh): how can we be sure that the v2's commit info is evicted
// (and not v1's)? Instead of putting a dummy, we can directly call
// AddCommitted(v2_seq + cache_size, ...) to evict v2's entry from commit cache.
TEST_P(WritePreparedTransactionTest, ReleaseSnapshotDuringCompaction3) {
  const size_t snapshot_cache_bits = 7;  // same as default
  const size_t commit_cache_bits = 1;    // commit cache size = 2
  UpdateTransactionDBOptions(snapshot_cache_bits, commit_cache_bits);
  ReOpen();

  // Add a dummy key to evict v2 commit cache, but keep v1 commit cache.
  // It also advance max_evicted_seq and can trigger old_commit_map cleanup.
  auto add_dummy = [&]() {
    auto* txn_dummy =
        db->BeginTransaction(WriteOptions(), TransactionOptions(), nullptr);
    ASSERT_OK(txn_dummy->SetName("txn_dummy"));
    ASSERT_OK(txn_dummy->Put("dummy", "dummy"));
    ASSERT_OK(txn_dummy->Prepare());
    ASSERT_OK(txn_dummy->Commit());
    delete txn_dummy;
  };

  ASSERT_OK(db->Put(WriteOptions(), "key1", "value1"));
  auto* txn =
      db->BeginTransaction(WriteOptions(), TransactionOptions(), nullptr);
  ASSERT_OK(txn->SetName("txn"));
  ASSERT_OK(txn->Put("key1", "value2"));
  ASSERT_OK(txn->Prepare());
  // TODO(myabandeh): replace it with GetId()?
  auto v2_seq = db->GetLatestSequenceNumber();
  ASSERT_OK(txn->Commit());
  delete txn;
  auto* s1 = db->GetSnapshot();
  // Dummy key to advance sequence number.
  add_dummy();
  auto* s2 = db->GetSnapshot();

  auto callback = [&](void*) {
    db->ReleaseSnapshot(s1);
    // Add some dummy entries to trigger s1 being cleanup from old_commit_map.
    add_dummy();
    add_dummy();
  };
  SyncPoint::GetInstance()->SetCallBack("CompactionIterator:AfterInit",
                                        callback);
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(db->Flush(FlushOptions()));
  // value1 should be compact out.
  VerifyInternalKeys({{"key1", "value2", v2_seq, kTypeValue}});

  db->ReleaseSnapshot(s2);
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(WritePreparedTransactionTest, ReleaseEarliestSnapshotDuringCompaction) {
  const size_t snapshot_cache_bits = 7;  // same as default
  const size_t commit_cache_bits = 0;    // minimum commit cache
  UpdateTransactionDBOptions(snapshot_cache_bits, commit_cache_bits);
  ReOpen();

  ASSERT_OK(db->Put(WriteOptions(), "key1", "value1"));
  auto* transaction =
      db->BeginTransaction(WriteOptions(), TransactionOptions(), nullptr);
  ASSERT_OK(transaction->SetName("txn"));
  ASSERT_OK(transaction->Delete("key1"));
  ASSERT_OK(transaction->Prepare());
  SequenceNumber del_seq = db->GetLatestSequenceNumber();
  auto snapshot1 = db->GetSnapshot();
  // Increment sequence number.
  ASSERT_OK(db->Put(WriteOptions(), "key2", "value2"));
  auto snapshot2 = db->GetSnapshot();
  ASSERT_OK(transaction->Commit());
  delete transaction;
  VerifyKeys({{"key1", "NOT_FOUND"}});
  VerifyKeys({{"key1", "value1"}}, snapshot1);
  VerifyKeys({{"key1", "value1"}}, snapshot2);
  ASSERT_OK(db->Flush(FlushOptions()));

  auto callback = [&](void* compaction) {
    // Release snapshot1 after CompactionIterator init.
    // CompactionIterator need to double check and find out snapshot2 is now
    // the earliest existing snapshot.
    if (compaction != nullptr) {
      db->ReleaseSnapshot(snapshot1);
      // Add some keys to advance max_evicted_seq.
      ASSERT_OK(db->Put(WriteOptions(), "key3", "value3"));
      ASSERT_OK(db->Put(WriteOptions(), "key4", "value4"));
    }
  };
  SyncPoint::GetInstance()->SetCallBack("CompactionIterator:AfterInit",
                                        callback);
  SyncPoint::GetInstance()->EnableProcessing();

  // Dummy keys to avoid compaction trivially move files and get around actual
  // compaction logic.
  ASSERT_OK(db->Put(WriteOptions(), "a", "dummy"));
  ASSERT_OK(db->Put(WriteOptions(), "z", "dummy"));
  ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  // Only verify for key1. Both the put and delete for the key should be kept.
  // Since the delete tombstone is not visible to snapshot2, we need to keep
  // at least one version of the key, for write-conflict check.
  VerifyInternalKeys({{"key1", "", del_seq, kTypeDeletion},
                      {"key1", "value1", 0, kTypeValue}});
  db->ReleaseSnapshot(snapshot2);
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

// A more complex test to verify compaction/flush should keep keys visible
// to snapshots.
TEST_P(WritePreparedTransactionTest,
       CompactionShouldKeepSnapshotVisibleKeysRandomized) {
  constexpr size_t kNumTransactions = 10;
  constexpr size_t kNumIterations = 1000;

  std::vector<Transaction*> transactions(kNumTransactions, nullptr);
  std::vector<size_t> versions(kNumTransactions, 0);
  std::unordered_map<std::string, std::string> current_data;
  std::vector<const Snapshot*> snapshots;
  std::vector<std::unordered_map<std::string, std::string>> snapshot_data;

  Random rnd(1103);
  options.disable_auto_compactions = true;
  ReOpen();

  for (size_t i = 0; i < kNumTransactions; i++) {
    std::string key = "key" + ToString(i);
    std::string value = "value0";
    ASSERT_OK(db->Put(WriteOptions(), key, value));
    current_data[key] = value;
  }
  VerifyKeys(current_data);

  for (size_t iter = 0; iter < kNumIterations; iter++) {
    auto r = rnd.Next() % (kNumTransactions + 1);
    if (r < kNumTransactions) {
      std::string key = "key" + ToString(r);
      if (transactions[r] == nullptr) {
        std::string value = "value" + ToString(versions[r] + 1);
        auto* txn = db->BeginTransaction(WriteOptions());
        ASSERT_OK(txn->SetName("txn" + ToString(r)));
        ASSERT_OK(txn->Put(key, value));
        ASSERT_OK(txn->Prepare());
        transactions[r] = txn;
      } else {
        std::string value = "value" + ToString(++versions[r]);
        ASSERT_OK(transactions[r]->Commit());
        delete transactions[r];
        transactions[r] = nullptr;
        current_data[key] = value;
      }
    } else {
      auto* snapshot = db->GetSnapshot();
      VerifyKeys(current_data, snapshot);
      snapshots.push_back(snapshot);
      snapshot_data.push_back(current_data);
    }
    VerifyKeys(current_data);
  }
  // Take a last snapshot to test compaction with uncommitted prepared
  // transaction.
  snapshots.push_back(db->GetSnapshot());
  snapshot_data.push_back(current_data);

  assert(snapshots.size() == snapshot_data.size());
  for (size_t i = 0; i < snapshots.size(); i++) {
    VerifyKeys(snapshot_data[i], snapshots[i]);
  }
  ASSERT_OK(db->Flush(FlushOptions()));
  for (size_t i = 0; i < snapshots.size(); i++) {
    VerifyKeys(snapshot_data[i], snapshots[i]);
  }
  // Dummy keys to avoid compaction trivially move files and get around actual
  // compaction logic.
  ASSERT_OK(db->Put(WriteOptions(), "a", "dummy"));
  ASSERT_OK(db->Put(WriteOptions(), "z", "dummy"));
  ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  for (size_t i = 0; i < snapshots.size(); i++) {
    VerifyKeys(snapshot_data[i], snapshots[i]);
  }
  // cleanup
  for (size_t i = 0; i < kNumTransactions; i++) {
    if (transactions[i] == nullptr) {
      continue;
    }
    ASSERT_OK(transactions[i]->Commit());
    delete transactions[i];
  }
  for (size_t i = 0; i < snapshots.size(); i++) {
    db->ReleaseSnapshot(snapshots[i]);
  }
}

// Compaction should not apply the optimization to output key with sequence
// number equal to 0 if the key is not visible to earliest snapshot, based on
// commit sequence number.
TEST_P(WritePreparedTransactionTest,
       CompactionShouldKeepSequenceForUncommittedKeys) {
  options.disable_auto_compactions = true;
  ReOpen();
  // Keep track of expected sequence number.
  SequenceNumber expected_seq = 0;
  auto* transaction = db->BeginTransaction(WriteOptions());
  ASSERT_OK(transaction->SetName("txn"));
  ASSERT_OK(transaction->Put("key1", "value1"));
  ASSERT_OK(transaction->Prepare());
  ASSERT_EQ(++expected_seq, db->GetLatestSequenceNumber());
  SequenceNumber seq1 = expected_seq;
  ASSERT_OK(db->Put(WriteOptions(), "key2", "value2"));
  DBImpl* db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
  expected_seq++;  // one for data
  if (options.two_write_queues) {
    expected_seq++;  // one for commit
  }
  ASSERT_EQ(expected_seq, db_impl->TEST_GetLastVisibleSequence());
  ASSERT_OK(db->Flush(FlushOptions()));
  // Dummy keys to avoid compaction trivially move files and get around actual
  // compaction logic.
  ASSERT_OK(db->Put(WriteOptions(), "a", "dummy"));
  ASSERT_OK(db->Put(WriteOptions(), "z", "dummy"));
  ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  VerifyKeys({
      {"key1", "NOT_FOUND"},
      {"key2", "value2"},
  });
  VerifyInternalKeys({
      // "key1" has not been committed. It keeps its sequence number.
      {"key1", "value1", seq1, kTypeValue},
      // "key2" is committed and output with seq = 0.
      {"key2", "value2", 0, kTypeValue},
  });
  ASSERT_OK(transaction->Commit());
  VerifyKeys({
      {"key1", "value1"},
      {"key2", "value2"},
  });
  delete transaction;
}

TEST_P(WritePreparedTransactionTest, CommitAndSnapshotDuringCompaction) {
  options.disable_auto_compactions = true;
  ReOpen();

  const Snapshot* snapshot = nullptr;
  ASSERT_OK(db->Put(WriteOptions(), "key1", "value1"));
  auto* txn = db->BeginTransaction(WriteOptions());
  ASSERT_OK(txn->SetName("txn"));
  ASSERT_OK(txn->Put("key1", "value2"));
  ASSERT_OK(txn->Prepare());

  auto callback = [&](void*) {
    // Snapshot is taken after compaction start. It should be taken into
    // consideration for whether to compact out value1.
    snapshot = db->GetSnapshot();
    ASSERT_OK(txn->Commit());
    delete txn;
  };
  SyncPoint::GetInstance()->SetCallBack("CompactionIterator:AfterInit",
                                        callback);
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db->Flush(FlushOptions()));
  ASSERT_NE(nullptr, snapshot);
  VerifyKeys({{"key1", "value2"}});
  VerifyKeys({{"key1", "value1"}}, snapshot);
  db->ReleaseSnapshot(snapshot);
}

TEST_P(WritePreparedTransactionTest, Iterate) {
  auto verify_state = [](Iterator* iter, const std::string& key,
                         const std::string& value) {
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(key, iter->key().ToString());
    ASSERT_EQ(value, iter->value().ToString());
  };

  auto verify_iter = [&](const std::string& expected_val) {
    // Get iterator from a concurrent transaction and make sure it has the
    // same view as an iterator from the DB.
    auto* txn = db->BeginTransaction(WriteOptions());

    for (int i = 0; i < 2; i++) {
      Iterator* iter = (i == 0)
          ? db->NewIterator(ReadOptions())
          : txn->GetIterator(ReadOptions());
      // Seek
      iter->Seek("foo");
      verify_state(iter, "foo", expected_val);
      // Next
      iter->Seek("a");
      verify_state(iter, "a", "va");
      iter->Next();
      verify_state(iter, "foo", expected_val);
      // SeekForPrev
      iter->SeekForPrev("y");
      verify_state(iter, "foo", expected_val);
      // Prev
      iter->SeekForPrev("z");
      verify_state(iter, "z", "vz");
      iter->Prev();
      verify_state(iter, "foo", expected_val);
      delete iter;
    }
    delete txn;
  };

  ASSERT_OK(db->Put(WriteOptions(), "foo", "v1"));
  auto* transaction = db->BeginTransaction(WriteOptions());
  ASSERT_OK(transaction->SetName("txn"));
  ASSERT_OK(transaction->Put("foo", "v2"));
  ASSERT_OK(transaction->Prepare());
  VerifyKeys({{"foo", "v1"}});
  // dummy keys
  ASSERT_OK(db->Put(WriteOptions(), "a", "va"));
  ASSERT_OK(db->Put(WriteOptions(), "z", "vz"));
  verify_iter("v1");
  ASSERT_OK(transaction->Commit());
  VerifyKeys({{"foo", "v2"}});
  verify_iter("v2");
  delete transaction;
}

TEST_P(WritePreparedTransactionTest, IteratorRefreshNotSupported) {
  Iterator* iter = db->NewIterator(ReadOptions());
  ASSERT_TRUE(iter->Refresh().IsNotSupported());
  delete iter;
}

// Committing an delayed prepared has two non-atomic steps: update commit cache,
// remove seq from delayed_prepared_. The read in IsInSnapshot also involves two
// non-atomic steps of checking these two data structures. This test breaks each
// in the middle to ensure correctness in spite of non-atomic execution.
// Note: This test is limitted to the case where snapshot is larger than the
// max_evicted_seq_.
TEST_P(WritePreparedTransactionTest, NonAtomicCommitOfDelayedPrepared) {
  const size_t snapshot_cache_bits = 7;  // same as default
  const size_t commit_cache_bits = 3;    // 8 entries
  for (auto split_read : {true, false}) {
    std::vector<bool> split_options = {false};
    if (split_read) {
      // Also test for break before mutex
      split_options.push_back(true);
    }
    for (auto split_before_mutex : split_options) {
      UpdateTransactionDBOptions(snapshot_cache_bits, commit_cache_bits);
      ReOpen();
      WritePreparedTxnDB* wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
      DBImpl* db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
      // Fill up the commit cache
      std::string init_value("value1");
      for (int i = 0; i < 10; i++) {
        db->Put(WriteOptions(), Slice("key1"), Slice(init_value));
      }
      // Prepare a transaction but do not commit it
      Transaction* txn =
          db->BeginTransaction(WriteOptions(), TransactionOptions());
      ASSERT_OK(txn->SetName("xid"));
      ASSERT_OK(txn->Put(Slice("key1"), Slice("value2")));
      ASSERT_OK(txn->Prepare());
      // Commit a bunch of entries to advance max evicted seq and make the
      // prepared a delayed prepared
      for (int i = 0; i < 10; i++) {
        db->Put(WriteOptions(), Slice("key3"), Slice("value3"));
      }
      // The snapshot should not see the delayed prepared entry
      auto snap = db->GetSnapshot();

      if (split_read) {
        if (split_before_mutex) {
          // split before acquiring prepare_mutex_
          rocksdb::SyncPoint::GetInstance()->LoadDependency(
              {{"WritePreparedTxnDB::IsInSnapshot:prepared_mutex_:pause",
                "AtomicCommitOfDelayedPrepared:Commit:before"},
               {"AtomicCommitOfDelayedPrepared:Commit:after",
                "WritePreparedTxnDB::IsInSnapshot:prepared_mutex_:resume"}});
        } else {
          // split right after reading from the commit cache
          rocksdb::SyncPoint::GetInstance()->LoadDependency(
              {{"WritePreparedTxnDB::IsInSnapshot:GetCommitEntry:pause",
                "AtomicCommitOfDelayedPrepared:Commit:before"},
               {"AtomicCommitOfDelayedPrepared:Commit:after",
                "WritePreparedTxnDB::IsInSnapshot:GetCommitEntry:resume"}});
        }
      } else {  // split commit
        // split right before removing from delayed_prepared_
        rocksdb::SyncPoint::GetInstance()->LoadDependency(
            {{"WritePreparedTxnDB::RemovePrepared:pause",
              "AtomicCommitOfDelayedPrepared:Read:before"},
             {"AtomicCommitOfDelayedPrepared:Read:after",
              "WritePreparedTxnDB::RemovePrepared:resume"}});
      }
      SyncPoint::GetInstance()->EnableProcessing();

      rocksdb::port::Thread commit_thread([&]() {
        TEST_SYNC_POINT("AtomicCommitOfDelayedPrepared:Commit:before");
        ASSERT_OK(txn->Commit());
        if (split_before_mutex) {
          // Do bunch of inserts to evict the commit entry from the cache. This
          // would prevent the 2nd look into commit cache under prepare_mutex_
          // to see the commit entry.
          auto seq = db_impl->TEST_GetLastVisibleSequence();
          size_t tries = 0;
          while (wp_db->max_evicted_seq_ < seq && tries < 50) {
            db->Put(WriteOptions(), Slice("key3"), Slice("value3"));
            tries++;
          };
          ASSERT_LT(tries, 50);
        }
        TEST_SYNC_POINT("AtomicCommitOfDelayedPrepared:Commit:after");
        delete txn;
      });

      rocksdb::port::Thread read_thread([&]() {
        TEST_SYNC_POINT("AtomicCommitOfDelayedPrepared:Read:before");
        ReadOptions roptions;
        roptions.snapshot = snap;
        PinnableSlice value;
        auto s = db->Get(roptions, db->DefaultColumnFamily(), "key1", &value);
        ASSERT_OK(s);
        // It should not see the commit of delayed prepared
        ASSERT_TRUE(value == init_value);
        TEST_SYNC_POINT("AtomicCommitOfDelayedPrepared:Read:after");
        db->ReleaseSnapshot(snap);
      });

      read_thread.join();
      commit_thread.join();
      rocksdb::SyncPoint::GetInstance()->DisableProcessing();
      rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
    }  // for split_before_mutex
  }    // for split_read
}

// When max evicted seq advances a prepared seq, it involves two updates: i)
// adding prepared seq to delayed_prepared_, ii) updating max_evicted_seq_.
// ::IsInSnapshot also reads these two values in a non-atomic way. This test
// ensures correctness if the update occurs after ::IsInSnapshot reads
// delayed_prepared_empty_ and before it reads max_evicted_seq_.
// Note: this test focuses on read snapshot larger than max_evicted_seq_.
TEST_P(WritePreparedTransactionTest, NonAtomicUpdateOfDelayedPrepared) {
  const size_t snapshot_cache_bits = 7;  // same as default
  const size_t commit_cache_bits = 3;    // 8 entries
  UpdateTransactionDBOptions(snapshot_cache_bits, commit_cache_bits);
  ReOpen();
  WritePreparedTxnDB* wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
  // Fill up the commit cache
  std::string init_value("value1");
  for (int i = 0; i < 10; i++) {
    db->Put(WriteOptions(), Slice("key1"), Slice(init_value));
  }
  // Prepare a transaction but do not commit it
  Transaction* txn = db->BeginTransaction(WriteOptions(), TransactionOptions());
  ASSERT_OK(txn->SetName("xid"));
  ASSERT_OK(txn->Put(Slice("key1"), Slice("value2")));
  ASSERT_OK(txn->Prepare());
  // Create a gap between prepare seq and snapshot seq
  db->Put(WriteOptions(), Slice("key3"), Slice("value3"));
  db->Put(WriteOptions(), Slice("key3"), Slice("value3"));
  // The snapshot should not see the delayed prepared entry
  auto snap = db->GetSnapshot();
  ASSERT_LT(txn->GetId(), snap->GetSequenceNumber());

  // split right after reading delayed_prepared_empty_
  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"WritePreparedTxnDB::IsInSnapshot:delayed_prepared_empty_:pause",
        "AtomicUpdateOfDelayedPrepared:before"},
       {"AtomicUpdateOfDelayedPrepared:after",
        "WritePreparedTxnDB::IsInSnapshot:delayed_prepared_empty_:resume"}});
  SyncPoint::GetInstance()->EnableProcessing();

  rocksdb::port::Thread commit_thread([&]() {
    TEST_SYNC_POINT("AtomicUpdateOfDelayedPrepared:before");
    // Commit a bunch of entries to advance max evicted seq and make the
    // prepared a delayed prepared
    size_t tries = 0;
    while (wp_db->max_evicted_seq_ < txn->GetId() && tries < 50) {
      db->Put(WriteOptions(), Slice("key3"), Slice("value3"));
      tries++;
    };
    ASSERT_LT(tries, 50);
    // This is the case on which the test focuses
    ASSERT_LT(wp_db->max_evicted_seq_, snap->GetSequenceNumber());
    TEST_SYNC_POINT("AtomicUpdateOfDelayedPrepared:after");
  });

  rocksdb::port::Thread read_thread([&]() {
    ReadOptions roptions;
    roptions.snapshot = snap;
    PinnableSlice value;
    auto s = db->Get(roptions, db->DefaultColumnFamily(), "key1", &value);
    ASSERT_OK(s);
    // It should not see the uncommitted value of delayed prepared
    ASSERT_TRUE(value == init_value);
    db->ReleaseSnapshot(snap);
  });

  read_thread.join();
  commit_thread.join();
  ASSERT_OK(txn->Commit());
  delete txn;
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
}

// Eviction from commit cache and update of max evicted seq are two non-atomic
// steps. Similarly the read of max_evicted_seq_ in ::IsInSnapshot and reading
// from commit cache are two non-atomic steps. This tests if the update occurs
// after reading max_evicted_seq_ and before reading the commit cache.
// Note: the test focuses on snapshot larger than max_evicted_seq_
TEST_P(WritePreparedTransactionTest, NonAtomicUpdateOfMaxEvictedSeq) {
  const size_t snapshot_cache_bits = 7;  // same as default
  const size_t commit_cache_bits = 3;    // 8 entries
  UpdateTransactionDBOptions(snapshot_cache_bits, commit_cache_bits);
  ReOpen();
  WritePreparedTxnDB* wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
  // Fill up the commit cache
  std::string init_value("value1");
  std::string last_value("value_final");
  for (int i = 0; i < 10; i++) {
    db->Put(WriteOptions(), Slice("key1"), Slice(init_value));
  }
  // Do an uncommitted write to prevent min_uncommitted optimization
  Transaction* txn1 =
      db->BeginTransaction(WriteOptions(), TransactionOptions());
  ASSERT_OK(txn1->SetName("xid1"));
  ASSERT_OK(txn1->Put(Slice("key0"), last_value));
  ASSERT_OK(txn1->Prepare());
  // Do a write with prepare to get the prepare seq
  Transaction* txn = db->BeginTransaction(WriteOptions(), TransactionOptions());
  ASSERT_OK(txn->SetName("xid"));
  ASSERT_OK(txn->Put(Slice("key1"), last_value));
  ASSERT_OK(txn->Prepare());
  ASSERT_OK(txn->Commit());
  // Create a gap between commit entry and snapshot seq
  db->Put(WriteOptions(), Slice("key3"), Slice("value3"));
  db->Put(WriteOptions(), Slice("key3"), Slice("value3"));
  // The snapshot should see the last commit
  auto snap = db->GetSnapshot();
  ASSERT_LE(txn->GetId(), snap->GetSequenceNumber());

  // split right after reading max_evicted_seq_
  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"WritePreparedTxnDB::IsInSnapshot:max_evicted_seq_:pause",
        "NonAtomicUpdateOfMaxEvictedSeq:before"},
       {"NonAtomicUpdateOfMaxEvictedSeq:after",
        "WritePreparedTxnDB::IsInSnapshot:max_evicted_seq_:resume"}});
  SyncPoint::GetInstance()->EnableProcessing();

  rocksdb::port::Thread commit_thread([&]() {
    TEST_SYNC_POINT("NonAtomicUpdateOfMaxEvictedSeq:before");
    // Commit a bunch of entries to advance max evicted seq beyond txn->GetId()
    size_t tries = 0;
    while (wp_db->max_evicted_seq_ < txn->GetId() && tries < 50) {
      db->Put(WriteOptions(), Slice("key3"), Slice("value3"));
      tries++;
    };
    ASSERT_LT(tries, 50);
    // This is the case on which the test focuses
    ASSERT_LT(wp_db->max_evicted_seq_, snap->GetSequenceNumber());
    TEST_SYNC_POINT("NonAtomicUpdateOfMaxEvictedSeq:after");
  });

  rocksdb::port::Thread read_thread([&]() {
    ReadOptions roptions;
    roptions.snapshot = snap;
    PinnableSlice value;
    auto s = db->Get(roptions, db->DefaultColumnFamily(), "key1", &value);
    ASSERT_OK(s);
    // It should see the committed value of the evicted entry
    ASSERT_TRUE(value == last_value);
    db->ReleaseSnapshot(snap);
  });

  read_thread.join();
  commit_thread.join();
  delete txn;
  txn1->Commit();
  delete txn1;
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
}

// When an old prepared entry gets committed, there is a gap between the time
// that it is published and when it is cleaned up from old_prepared_. This test
// stresses such cases.
TEST_P(WritePreparedTransactionTest, CommitOfDelayedPrepared) {
  const size_t snapshot_cache_bits = 7;  // same as default
  for (const size_t commit_cache_bits : {0, 2, 3}) {
    for (const size_t sub_batch_cnt : {1, 2, 3}) {
      UpdateTransactionDBOptions(snapshot_cache_bits, commit_cache_bits);
      ReOpen();
      std::atomic<const Snapshot*> snap = {nullptr};
      std::atomic<SequenceNumber> exp_prepare = {0};
      // Value is synchronized via snap
      PinnableSlice value;
      // Take a snapshot after publish and before RemovePrepared:Start
      auto callback = [&](void* param) {
        SequenceNumber prep_seq = *((SequenceNumber*)param);
        if (prep_seq == exp_prepare.load()) {  // only for write_thread
          ASSERT_EQ(nullptr, snap.load());
          snap.store(db->GetSnapshot());
          ReadOptions roptions;
          roptions.snapshot = snap.load();
          auto s = db->Get(roptions, db->DefaultColumnFamily(), "key", &value);
          ASSERT_OK(s);
        }
      };
      SyncPoint::GetInstance()->SetCallBack("RemovePrepared:Start", callback);
      SyncPoint::GetInstance()->EnableProcessing();
      // Thread to cause frequent evictions
      rocksdb::port::Thread eviction_thread([&]() {
        // Too many txns might cause commit_seq - prepare_seq in another thread
        // to go beyond DELTA_UPPERBOUND
        for (int i = 0; i < 25 * (1 << commit_cache_bits); i++) {
          db->Put(WriteOptions(), Slice("key1"), Slice("value1"));
        }
      });
      rocksdb::port::Thread write_thread([&]() {
        for (int i = 0; i < 25 * (1 << commit_cache_bits); i++) {
          Transaction* txn =
              db->BeginTransaction(WriteOptions(), TransactionOptions());
          ASSERT_OK(txn->SetName("xid"));
          std::string val_str = "value" + ToString(i);
          for (size_t b = 0; b < sub_batch_cnt; b++) {
            ASSERT_OK(txn->Put(Slice("key"), val_str));
          }
          ASSERT_OK(txn->Prepare());
          // Let an eviction to kick in
          std::this_thread::yield();

          exp_prepare.store(txn->GetId());
          ASSERT_OK(txn->Commit());
          delete txn;

          // Read with the snapshot taken before delayed_prepared_ cleanup
          ReadOptions roptions;
          roptions.snapshot = snap.load();
          ASSERT_NE(nullptr, roptions.snapshot);
          PinnableSlice value2;
          auto s = db->Get(roptions, db->DefaultColumnFamily(), "key", &value2);
          ASSERT_OK(s);
          // It should see its own write
          ASSERT_TRUE(val_str == value2);
          // The value read by snapshot should not change
          ASSERT_STREQ(value2.ToString().c_str(), value.ToString().c_str());

          db->ReleaseSnapshot(roptions.snapshot);
          snap.store(nullptr);
        }
      });
      write_thread.join();
      eviction_thread.join();
    }
    rocksdb::SyncPoint::GetInstance()->DisableProcessing();
    rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
  }
}

// Test that updating the commit map will not affect the existing snapshots
TEST_P(WritePreparedTransactionTest, AtomicCommit) {
  for (bool skip_prepare : {true, false}) {
    rocksdb::SyncPoint::GetInstance()->LoadDependency({
        {"WritePreparedTxnDB::AddCommitted:start",
         "AtomicCommit::GetSnapshot:start"},
        {"AtomicCommit::Get:end",
         "WritePreparedTxnDB::AddCommitted:start:pause"},
        {"WritePreparedTxnDB::AddCommitted:end", "AtomicCommit::Get2:start"},
        {"AtomicCommit::Get2:end",
         "WritePreparedTxnDB::AddCommitted:end:pause:"},
    });
    rocksdb::SyncPoint::GetInstance()->EnableProcessing();
    rocksdb::port::Thread write_thread([&]() {
      if (skip_prepare) {
        db->Put(WriteOptions(), Slice("key"), Slice("value"));
      } else {
        Transaction* txn =
            db->BeginTransaction(WriteOptions(), TransactionOptions());
        ASSERT_OK(txn->SetName("xid"));
        ASSERT_OK(txn->Put(Slice("key"), Slice("value")));
        ASSERT_OK(txn->Prepare());
        ASSERT_OK(txn->Commit());
        delete txn;
      }
    });
    rocksdb::port::Thread read_thread([&]() {
      ReadOptions roptions;
      TEST_SYNC_POINT("AtomicCommit::GetSnapshot:start");
      roptions.snapshot = db->GetSnapshot();
      PinnableSlice val;
      auto s = db->Get(roptions, db->DefaultColumnFamily(), "key", &val);
      TEST_SYNC_POINT("AtomicCommit::Get:end");
      TEST_SYNC_POINT("AtomicCommit::Get2:start");
      ASSERT_SAME(roptions, db, s, val, "key");
      TEST_SYNC_POINT("AtomicCommit::Get2:end");
      db->ReleaseSnapshot(roptions.snapshot);
    });
    read_thread.join();
    write_thread.join();
    rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  }
}

// Test that we can change write policy from WriteCommitted to WritePrepared
// after a clean shutdown (which would empty the WAL)
TEST_P(WritePreparedTransactionTest, WP_WC_DBBackwardCompatibility) {
  bool empty_wal = true;
  CrossCompatibilityTest(WRITE_COMMITTED, WRITE_PREPARED, empty_wal);
}

// Test that we fail fast if WAL is not emptied between changing the write
// policy from WriteCommitted to WritePrepared
TEST_P(WritePreparedTransactionTest, WP_WC_WALBackwardIncompatibility) {
  bool empty_wal = true;
  CrossCompatibilityTest(WRITE_COMMITTED, WRITE_PREPARED, !empty_wal);
}

// Test that we can change write policy from WritePrepare back to WriteCommitted
// after a clean shutdown (which would empty the WAL)
TEST_P(WritePreparedTransactionTest, WC_WP_ForwardCompatibility) {
  bool empty_wal = true;
  CrossCompatibilityTest(WRITE_PREPARED, WRITE_COMMITTED, empty_wal);
}

// Test that we fail fast if WAL is not emptied between changing the write
// policy from WriteCommitted to WritePrepared
TEST_P(WritePreparedTransactionTest, WC_WP_WALForwardIncompatibility) {
  bool empty_wal = true;
  CrossCompatibilityTest(WRITE_PREPARED, WRITE_COMMITTED, !empty_wal);
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
