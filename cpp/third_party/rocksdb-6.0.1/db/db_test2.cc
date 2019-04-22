//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <atomic>
#include <cstdlib>
#include <functional>

#include "db/db_test_util.h"
#include "db/read_callback.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/persistent_cache.h"
#include "rocksdb/wal_filter.h"

namespace rocksdb {

class DBTest2 : public DBTestBase {
 public:
  DBTest2() : DBTestBase("/db_test2") {}
};

class PrefixFullBloomWithReverseComparator
    : public DBTestBase,
      public ::testing::WithParamInterface<bool> {
 public:
  PrefixFullBloomWithReverseComparator()
      : DBTestBase("/prefix_bloom_reverse") {}
  void SetUp() override { if_cache_filter_ = GetParam(); }
  bool if_cache_filter_;
};

TEST_P(PrefixFullBloomWithReverseComparator,
       PrefixFullBloomWithReverseComparator) {
  Options options = last_options_;
  options.comparator = ReverseBytewiseComparator();
  options.prefix_extractor.reset(NewCappedPrefixTransform(3));
  options.statistics = rocksdb::CreateDBStatistics();
  BlockBasedTableOptions bbto;
  if (if_cache_filter_) {
    bbto.no_block_cache = false;
    bbto.cache_index_and_filter_blocks = true;
    bbto.block_cache = NewLRUCache(1);
  }
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);

  ASSERT_OK(dbfull()->Put(WriteOptions(), "bar123", "foo"));
  ASSERT_OK(dbfull()->Put(WriteOptions(), "bar234", "foo2"));
  ASSERT_OK(dbfull()->Put(WriteOptions(), "foo123", "foo3"));

  dbfull()->Flush(FlushOptions());

  if (bbto.block_cache) {
    bbto.block_cache->EraseUnRefEntries();
  }

  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  iter->Seek("bar345");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("bar234", iter->key().ToString());
  ASSERT_EQ("foo2", iter->value().ToString());
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("bar123", iter->key().ToString());
  ASSERT_EQ("foo", iter->value().ToString());

  iter->Seek("foo234");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("foo123", iter->key().ToString());
  ASSERT_EQ("foo3", iter->value().ToString());

  iter->Seek("bar");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(!iter->Valid());
}

INSTANTIATE_TEST_CASE_P(PrefixFullBloomWithReverseComparator,
                        PrefixFullBloomWithReverseComparator, testing::Bool());

TEST_F(DBTest2, IteratorPropertyVersionNumber) {
  Put("", "");
  Iterator* iter1 = db_->NewIterator(ReadOptions());
  std::string prop_value;
  ASSERT_OK(
      iter1->GetProperty("rocksdb.iterator.super-version-number", &prop_value));
  uint64_t version_number1 =
      static_cast<uint64_t>(std::atoi(prop_value.c_str()));

  Put("", "");
  Flush();

  Iterator* iter2 = db_->NewIterator(ReadOptions());
  ASSERT_OK(
      iter2->GetProperty("rocksdb.iterator.super-version-number", &prop_value));
  uint64_t version_number2 =
      static_cast<uint64_t>(std::atoi(prop_value.c_str()));

  ASSERT_GT(version_number2, version_number1);

  Put("", "");

  Iterator* iter3 = db_->NewIterator(ReadOptions());
  ASSERT_OK(
      iter3->GetProperty("rocksdb.iterator.super-version-number", &prop_value));
  uint64_t version_number3 =
      static_cast<uint64_t>(std::atoi(prop_value.c_str()));

  ASSERT_EQ(version_number2, version_number3);

  iter1->SeekToFirst();
  ASSERT_OK(
      iter1->GetProperty("rocksdb.iterator.super-version-number", &prop_value));
  uint64_t version_number1_new =
      static_cast<uint64_t>(std::atoi(prop_value.c_str()));
  ASSERT_EQ(version_number1, version_number1_new);

  delete iter1;
  delete iter2;
  delete iter3;
}

TEST_F(DBTest2, CacheIndexAndFilterWithDBRestart) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.cache_index_and_filter_blocks = true;
  table_options.filter_policy.reset(NewBloomFilterPolicy(20));
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  CreateAndReopenWithCF({"pikachu"}, options);

  Put(1, "a", "begin");
  Put(1, "z", "end");
  ASSERT_OK(Flush(1));
  TryReopenWithColumnFamilies({"default", "pikachu"}, options);

  std::string value;
  value = Get(1, "a");
}

TEST_F(DBTest2, MaxSuccessiveMergesChangeWithDBRecovery) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  options.max_successive_merges = 3;
  options.merge_operator = MergeOperators::CreatePutOperator();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  Put("poi", "Finch");
  db_->Merge(WriteOptions(), "poi", "Reese");
  db_->Merge(WriteOptions(), "poi", "Shaw");
  db_->Merge(WriteOptions(), "poi", "Root");
  options.max_successive_merges = 2;
  Reopen(options);
}

#ifndef ROCKSDB_LITE
class DBTestSharedWriteBufferAcrossCFs
    : public DBTestBase,
      public testing::WithParamInterface<std::tuple<bool, bool>> {
 public:
  DBTestSharedWriteBufferAcrossCFs()
      : DBTestBase("/db_test_shared_write_buffer") {}
  void SetUp() override {
    use_old_interface_ = std::get<0>(GetParam());
    cost_cache_ = std::get<1>(GetParam());
  }
  bool use_old_interface_;
  bool cost_cache_;
};

TEST_P(DBTestSharedWriteBufferAcrossCFs, SharedWriteBufferAcrossCFs) {
  Options options = CurrentOptions();
  options.arena_block_size = 4096;

  // Avoid undeterministic value by malloc_usable_size();
  // Force arena block size to 1
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "Arena::Arena:0", [&](void* arg) {
        size_t* block_size = static_cast<size_t*>(arg);
        *block_size = 1;
      });

  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "Arena::AllocateNewBlock:0", [&](void* arg) {
        std::pair<size_t*, size_t*>* pair =
            static_cast<std::pair<size_t*, size_t*>*>(arg);
        *std::get<0>(*pair) = *std::get<1>(*pair);
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  // The total soft write buffer size is about 105000
  std::shared_ptr<Cache> cache = NewLRUCache(4 * 1024 * 1024, 2);
  ASSERT_LT(cache->GetUsage(), 1024 * 1024);

  if (use_old_interface_) {
    options.db_write_buffer_size = 120000;  // this is the real limit
  } else if (!cost_cache_) {
    options.write_buffer_manager.reset(new WriteBufferManager(114285));
  } else {
    options.write_buffer_manager.reset(new WriteBufferManager(114285, cache));
  }
  options.write_buffer_size = 500000;  // this is never hit
  CreateAndReopenWithCF({"pikachu", "dobrynia", "nikitich"}, options);

  WriteOptions wo;
  wo.disableWAL = true;

  std::function<void()> wait_flush = [&]() {
    dbfull()->TEST_WaitForFlushMemTable(handles_[0]);
    dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
    dbfull()->TEST_WaitForFlushMemTable(handles_[2]);
    dbfull()->TEST_WaitForFlushMemTable(handles_[3]);
  };

  // Create some data and flush "default" and "nikitich" so that they
  // are newer CFs created.
  ASSERT_OK(Put(3, Key(1), DummyString(1), wo));
  Flush(3);
  ASSERT_OK(Put(3, Key(1), DummyString(1), wo));
  ASSERT_OK(Put(0, Key(1), DummyString(1), wo));
  Flush(0);
  ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default"),
            static_cast<uint64_t>(1));
  ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "nikitich"),
            static_cast<uint64_t>(1));

  ASSERT_OK(Put(3, Key(1), DummyString(30000), wo));
  if (cost_cache_) {
    ASSERT_GE(cache->GetUsage(), 1024 * 1024);
    ASSERT_LE(cache->GetUsage(), 2 * 1024 * 1024);
  }
  wait_flush();
  ASSERT_OK(Put(0, Key(1), DummyString(60000), wo));
  if (cost_cache_) {
    ASSERT_GE(cache->GetUsage(), 1024 * 1024);
    ASSERT_LE(cache->GetUsage(), 2 * 1024 * 1024);
  }
  wait_flush();
  ASSERT_OK(Put(2, Key(1), DummyString(1), wo));
  // No flush should trigger
  wait_flush();
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "pikachu"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "dobrynia"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "nikitich"),
              static_cast<uint64_t>(1));
  }

  // Trigger a flush. Flushing "nikitich".
  ASSERT_OK(Put(3, Key(2), DummyString(30000), wo));
  wait_flush();
  ASSERT_OK(Put(0, Key(1), DummyString(1), wo));
  wait_flush();
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "pikachu"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "dobrynia"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "nikitich"),
              static_cast<uint64_t>(2));
  }

  // Without hitting the threshold, no flush should trigger.
  ASSERT_OK(Put(2, Key(1), DummyString(30000), wo));
  wait_flush();
  ASSERT_OK(Put(2, Key(1), DummyString(1), wo));
  wait_flush();
  ASSERT_OK(Put(2, Key(1), DummyString(1), wo));
  wait_flush();
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "pikachu"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "dobrynia"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "nikitich"),
              static_cast<uint64_t>(2));
  }

  // Hit the write buffer limit again. "default"
  // will have been flushed.
  ASSERT_OK(Put(2, Key(2), DummyString(10000), wo));
  wait_flush();
  ASSERT_OK(Put(3, Key(1), DummyString(1), wo));
  wait_flush();
  ASSERT_OK(Put(0, Key(1), DummyString(1), wo));
  wait_flush();
  ASSERT_OK(Put(0, Key(1), DummyString(1), wo));
  wait_flush();
  ASSERT_OK(Put(0, Key(1), DummyString(1), wo));
  wait_flush();
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default"),
              static_cast<uint64_t>(2));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "pikachu"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "dobrynia"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "nikitich"),
              static_cast<uint64_t>(2));
  }

  // Trigger another flush. This time "dobrynia". "pikachu" should not
  // be flushed, althrough it was never flushed.
  ASSERT_OK(Put(1, Key(1), DummyString(1), wo));
  wait_flush();
  ASSERT_OK(Put(2, Key(1), DummyString(80000), wo));
  wait_flush();
  ASSERT_OK(Put(1, Key(1), DummyString(1), wo));
  wait_flush();
  ASSERT_OK(Put(2, Key(1), DummyString(1), wo));
  wait_flush();

  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default"),
              static_cast<uint64_t>(2));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "pikachu"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "dobrynia"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "nikitich"),
              static_cast<uint64_t>(2));
  }
  if (cost_cache_) {
    ASSERT_GE(cache->GetUsage(), 1024 * 1024);
    Close();
    options.write_buffer_manager.reset();
    last_options_.write_buffer_manager.reset();
    ASSERT_LT(cache->GetUsage(), 1024 * 1024);
  }
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

INSTANTIATE_TEST_CASE_P(DBTestSharedWriteBufferAcrossCFs,
                        DBTestSharedWriteBufferAcrossCFs,
                        ::testing::Values(std::make_tuple(true, false),
                                          std::make_tuple(false, false),
                                          std::make_tuple(false, true)));

TEST_F(DBTest2, SharedWriteBufferLimitAcrossDB) {
  std::string dbname2 = test::PerThreadDBPath("db_shared_wb_db2");
  Options options = CurrentOptions();
  options.arena_block_size = 4096;
  // Avoid undeterministic value by malloc_usable_size();
  // Force arena block size to 1
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "Arena::Arena:0", [&](void* arg) {
        size_t* block_size = static_cast<size_t*>(arg);
        *block_size = 1;
      });

  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "Arena::AllocateNewBlock:0", [&](void* arg) {
        std::pair<size_t*, size_t*>* pair =
            static_cast<std::pair<size_t*, size_t*>*>(arg);
        *std::get<0>(*pair) = *std::get<1>(*pair);
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  options.write_buffer_size = 500000;  // this is never hit
  // Use a write buffer total size so that the soft limit is about
  // 105000.
  options.write_buffer_manager.reset(new WriteBufferManager(120000));
  CreateAndReopenWithCF({"cf1", "cf2"}, options);

  ASSERT_OK(DestroyDB(dbname2, options));
  DB* db2 = nullptr;
  ASSERT_OK(DB::Open(options, dbname2, &db2));

  WriteOptions wo;
  wo.disableWAL = true;

  std::function<void()> wait_flush = [&]() {
    dbfull()->TEST_WaitForFlushMemTable(handles_[0]);
    dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
    dbfull()->TEST_WaitForFlushMemTable(handles_[2]);
    static_cast<DBImpl*>(db2)->TEST_WaitForFlushMemTable();
  };

  // Trigger a flush on cf2
  ASSERT_OK(Put(2, Key(1), DummyString(70000), wo));
  wait_flush();
  ASSERT_OK(Put(0, Key(1), DummyString(20000), wo));
  wait_flush();

  // Insert to DB2
  ASSERT_OK(db2->Put(wo, Key(2), DummyString(20000)));
  wait_flush();

  ASSERT_OK(Put(2, Key(1), DummyString(1), wo));
  wait_flush();
  static_cast<DBImpl*>(db2)->TEST_WaitForFlushMemTable();
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default") +
                  GetNumberOfSstFilesForColumnFamily(db_, "cf1") +
                  GetNumberOfSstFilesForColumnFamily(db_, "cf2"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db2, "default"),
              static_cast<uint64_t>(0));
  }

  // Triggering to flush another CF in DB1
  ASSERT_OK(db2->Put(wo, Key(2), DummyString(70000)));
  wait_flush();
  ASSERT_OK(Put(2, Key(1), DummyString(1), wo));
  wait_flush();
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "cf1"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "cf2"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db2, "default"),
              static_cast<uint64_t>(0));
  }

  // Triggering flush in DB2.
  ASSERT_OK(db2->Put(wo, Key(3), DummyString(40000)));
  wait_flush();
  ASSERT_OK(db2->Put(wo, Key(1), DummyString(1)));
  wait_flush();
  static_cast<DBImpl*>(db2)->TEST_WaitForFlushMemTable();
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "cf1"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "cf2"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db2, "default"),
              static_cast<uint64_t>(1));
  }

  delete db2;
  ASSERT_OK(DestroyDB(dbname2, options));

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest2, TestWriteBufferNoLimitWithCache) {
  Options options = CurrentOptions();
  options.arena_block_size = 4096;
  std::shared_ptr<Cache> cache =
      NewLRUCache(LRUCacheOptions(10000000, 1, false, 0.0));
  options.write_buffer_size = 50000;  // this is never hit
  // Use a write buffer total size so that the soft limit is about
  // 105000.
  options.write_buffer_manager.reset(new WriteBufferManager(0, cache));
  Reopen(options);

  ASSERT_OK(Put("foo", "bar"));
  // One dummy entry is 1MB.
  ASSERT_GT(cache->GetUsage(), 500000);
}

namespace {
  void ValidateKeyExistence(DB* db, const std::vector<Slice>& keys_must_exist,
    const std::vector<Slice>& keys_must_not_exist) {
    // Ensure that expected keys exist
    std::vector<std::string> values;
    if (keys_must_exist.size() > 0) {
      std::vector<Status> status_list =
        db->MultiGet(ReadOptions(), keys_must_exist, &values);
      for (size_t i = 0; i < keys_must_exist.size(); i++) {
        ASSERT_OK(status_list[i]);
      }
    }

    // Ensure that given keys don't exist
    if (keys_must_not_exist.size() > 0) {
      std::vector<Status> status_list =
        db->MultiGet(ReadOptions(), keys_must_not_exist, &values);
      for (size_t i = 0; i < keys_must_not_exist.size(); i++) {
        ASSERT_TRUE(status_list[i].IsNotFound());
      }
    }
  }

}  // namespace

TEST_F(DBTest2, WalFilterTest) {
  class TestWalFilter : public WalFilter {
  private:
    // Processing option that is requested to be applied at the given index
    WalFilter::WalProcessingOption wal_processing_option_;
    // Index at which to apply wal_processing_option_
    // At other indexes default wal_processing_option::kContinueProcessing is
    // returned.
    size_t apply_option_at_record_index_;
    // Current record index, incremented with each record encountered.
    size_t current_record_index_;

  public:
    TestWalFilter(WalFilter::WalProcessingOption wal_processing_option,
      size_t apply_option_for_record_index)
      : wal_processing_option_(wal_processing_option),
      apply_option_at_record_index_(apply_option_for_record_index),
      current_record_index_(0) {}

    WalProcessingOption LogRecord(const WriteBatch& /*batch*/,
                                  WriteBatch* /*new_batch*/,
                                  bool* /*batch_changed*/) const override {
      WalFilter::WalProcessingOption option_to_return;

      if (current_record_index_ == apply_option_at_record_index_) {
        option_to_return = wal_processing_option_;
      }
      else {
        option_to_return = WalProcessingOption::kContinueProcessing;
      }

      // Filter is passed as a const object for RocksDB to not modify the
      // object, however we modify it for our own purpose here and hence
      // cast the constness away.
      (const_cast<TestWalFilter*>(this)->current_record_index_)++;

      return option_to_return;
    }

    const char* Name() const override { return "TestWalFilter"; }
  };

  // Create 3 batches with two keys each
  std::vector<std::vector<std::string>> batch_keys(3);

  batch_keys[0].push_back("key1");
  batch_keys[0].push_back("key2");
  batch_keys[1].push_back("key3");
  batch_keys[1].push_back("key4");
  batch_keys[2].push_back("key5");
  batch_keys[2].push_back("key6");

  // Test with all WAL processing options
  for (int option = 0;
    option < static_cast<int>(
    WalFilter::WalProcessingOption::kWalProcessingOptionMax);
  option++) {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);
    CreateAndReopenWithCF({ "pikachu" }, options);

    // Write given keys in given batches
    for (size_t i = 0; i < batch_keys.size(); i++) {
      WriteBatch batch;
      for (size_t j = 0; j < batch_keys[i].size(); j++) {
        batch.Put(handles_[0], batch_keys[i][j], DummyString(1024));
      }
      dbfull()->Write(WriteOptions(), &batch);
    }

    WalFilter::WalProcessingOption wal_processing_option =
      static_cast<WalFilter::WalProcessingOption>(option);

    // Create a test filter that would apply wal_processing_option at the first
    // record
    size_t apply_option_for_record_index = 1;
    TestWalFilter test_wal_filter(wal_processing_option,
      apply_option_for_record_index);

    // Reopen database with option to use WAL filter
    options = OptionsForLogIterTest();
    options.wal_filter = &test_wal_filter;
    Status status =
      TryReopenWithColumnFamilies({ "default", "pikachu" }, options);
    if (wal_processing_option ==
      WalFilter::WalProcessingOption::kCorruptedRecord) {
      assert(!status.ok());
      // In case of corruption we can turn off paranoid_checks to reopen
      // databse
      options.paranoid_checks = false;
      ReopenWithColumnFamilies({ "default", "pikachu" }, options);
    }
    else {
      assert(status.ok());
    }

    // Compute which keys we expect to be found
    // and which we expect not to be found after recovery.
    std::vector<Slice> keys_must_exist;
    std::vector<Slice> keys_must_not_exist;
    switch (wal_processing_option) {
    case WalFilter::WalProcessingOption::kCorruptedRecord:
    case WalFilter::WalProcessingOption::kContinueProcessing: {
      fprintf(stderr, "Testing with complete WAL processing\n");
      // we expect all records to be processed
      for (size_t i = 0; i < batch_keys.size(); i++) {
        for (size_t j = 0; j < batch_keys[i].size(); j++) {
          keys_must_exist.push_back(Slice(batch_keys[i][j]));
        }
      }
      break;
    }
    case WalFilter::WalProcessingOption::kIgnoreCurrentRecord: {
      fprintf(stderr,
        "Testing with ignoring record %" ROCKSDB_PRIszt " only\n",
        apply_option_for_record_index);
      // We expect the record with apply_option_for_record_index to be not
      // found.
      for (size_t i = 0; i < batch_keys.size(); i++) {
        for (size_t j = 0; j < batch_keys[i].size(); j++) {
          if (i == apply_option_for_record_index) {
            keys_must_not_exist.push_back(Slice(batch_keys[i][j]));
          }
          else {
            keys_must_exist.push_back(Slice(batch_keys[i][j]));
          }
        }
      }
      break;
    }
    case WalFilter::WalProcessingOption::kStopReplay: {
      fprintf(stderr,
        "Testing with stopping replay from record %" ROCKSDB_PRIszt
        "\n",
        apply_option_for_record_index);
      // We expect records beyond apply_option_for_record_index to be not
      // found.
      for (size_t i = 0; i < batch_keys.size(); i++) {
        for (size_t j = 0; j < batch_keys[i].size(); j++) {
          if (i >= apply_option_for_record_index) {
            keys_must_not_exist.push_back(Slice(batch_keys[i][j]));
          }
          else {
            keys_must_exist.push_back(Slice(batch_keys[i][j]));
          }
        }
      }
      break;
    }
    default:
      assert(false);  // unhandled case
    }

    bool checked_after_reopen = false;

    while (true) {
      // Ensure that expected keys exists
      // and not expected keys don't exist after recovery
      ValidateKeyExistence(db_, keys_must_exist, keys_must_not_exist);

      if (checked_after_reopen) {
        break;
      }

      // reopen database again to make sure previous log(s) are not used
      //(even if they were skipped)
      // reopn database with option to use WAL filter
      options = OptionsForLogIterTest();
      ReopenWithColumnFamilies({ "default", "pikachu" }, options);

      checked_after_reopen = true;
    }
  }
}

TEST_F(DBTest2, WalFilterTestWithChangeBatch) {
  class ChangeBatchHandler : public WriteBatch::Handler {
  private:
    // Batch to insert keys in
    WriteBatch* new_write_batch_;
    // Number of keys to add in the new batch
    size_t num_keys_to_add_in_new_batch_;
    // Number of keys added to new batch
    size_t num_keys_added_;

  public:
    ChangeBatchHandler(WriteBatch* new_write_batch,
      size_t num_keys_to_add_in_new_batch)
      : new_write_batch_(new_write_batch),
      num_keys_to_add_in_new_batch_(num_keys_to_add_in_new_batch),
      num_keys_added_(0) {}
    void Put(const Slice& key, const Slice& value) override {
      if (num_keys_added_ < num_keys_to_add_in_new_batch_) {
        new_write_batch_->Put(key, value);
        ++num_keys_added_;
      }
    }
  };

  class TestWalFilterWithChangeBatch : public WalFilter {
  private:
    // Index at which to start changing records
    size_t change_records_from_index_;
    // Number of keys to add in the new batch
    size_t num_keys_to_add_in_new_batch_;
    // Current record index, incremented with each record encountered.
    size_t current_record_index_;

  public:
    TestWalFilterWithChangeBatch(size_t change_records_from_index,
      size_t num_keys_to_add_in_new_batch)
      : change_records_from_index_(change_records_from_index),
      num_keys_to_add_in_new_batch_(num_keys_to_add_in_new_batch),
      current_record_index_(0) {}

    WalProcessingOption LogRecord(const WriteBatch& batch,
                                  WriteBatch* new_batch,
                                  bool* batch_changed) const override {
      if (current_record_index_ >= change_records_from_index_) {
        ChangeBatchHandler handler(new_batch, num_keys_to_add_in_new_batch_);
        batch.Iterate(&handler);
        *batch_changed = true;
      }

      // Filter is passed as a const object for RocksDB to not modify the
      // object, however we modify it for our own purpose here and hence
      // cast the constness away.
      (const_cast<TestWalFilterWithChangeBatch*>(this)
        ->current_record_index_)++;

      return WalProcessingOption::kContinueProcessing;
    }

    const char* Name() const override { return "TestWalFilterWithChangeBatch"; }
  };

  std::vector<std::vector<std::string>> batch_keys(3);

  batch_keys[0].push_back("key1");
  batch_keys[0].push_back("key2");
  batch_keys[1].push_back("key3");
  batch_keys[1].push_back("key4");
  batch_keys[2].push_back("key5");
  batch_keys[2].push_back("key6");

  Options options = OptionsForLogIterTest();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({ "pikachu" }, options);

  // Write given keys in given batches
  for (size_t i = 0; i < batch_keys.size(); i++) {
    WriteBatch batch;
    for (size_t j = 0; j < batch_keys[i].size(); j++) {
      batch.Put(handles_[0], batch_keys[i][j], DummyString(1024));
    }
    dbfull()->Write(WriteOptions(), &batch);
  }

  // Create a test filter that would apply wal_processing_option at the first
  // record
  size_t change_records_from_index = 1;
  size_t num_keys_to_add_in_new_batch = 1;
  TestWalFilterWithChangeBatch test_wal_filter_with_change_batch(
    change_records_from_index, num_keys_to_add_in_new_batch);

  // Reopen database with option to use WAL filter
  options = OptionsForLogIterTest();
  options.wal_filter = &test_wal_filter_with_change_batch;
  ReopenWithColumnFamilies({ "default", "pikachu" }, options);

  // Ensure that all keys exist before change_records_from_index_
  // And after that index only single key exists
  // as our filter adds only single key for each batch
  std::vector<Slice> keys_must_exist;
  std::vector<Slice> keys_must_not_exist;

  for (size_t i = 0; i < batch_keys.size(); i++) {
    for (size_t j = 0; j < batch_keys[i].size(); j++) {
      if (i >= change_records_from_index && j >= num_keys_to_add_in_new_batch) {
        keys_must_not_exist.push_back(Slice(batch_keys[i][j]));
      }
      else {
        keys_must_exist.push_back(Slice(batch_keys[i][j]));
      }
    }
  }

  bool checked_after_reopen = false;

  while (true) {
    // Ensure that expected keys exists
    // and not expected keys don't exist after recovery
    ValidateKeyExistence(db_, keys_must_exist, keys_must_not_exist);

    if (checked_after_reopen) {
      break;
    }

    // reopen database again to make sure previous log(s) are not used
    //(even if they were skipped)
    // reopn database with option to use WAL filter
    options = OptionsForLogIterTest();
    ReopenWithColumnFamilies({ "default", "pikachu" }, options);

    checked_after_reopen = true;
  }
}

TEST_F(DBTest2, WalFilterTestWithChangeBatchExtraKeys) {
  class TestWalFilterWithChangeBatchAddExtraKeys : public WalFilter {
  public:
   WalProcessingOption LogRecord(const WriteBatch& batch, WriteBatch* new_batch,
                                 bool* batch_changed) const override {
     *new_batch = batch;
     new_batch->Put("key_extra", "value_extra");
     *batch_changed = true;
     return WalProcessingOption::kContinueProcessing;
   }

   const char* Name() const override {
     return "WalFilterTestWithChangeBatchExtraKeys";
   }
  };

  std::vector<std::vector<std::string>> batch_keys(3);

  batch_keys[0].push_back("key1");
  batch_keys[0].push_back("key2");
  batch_keys[1].push_back("key3");
  batch_keys[1].push_back("key4");
  batch_keys[2].push_back("key5");
  batch_keys[2].push_back("key6");

  Options options = OptionsForLogIterTest();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({ "pikachu" }, options);

  // Write given keys in given batches
  for (size_t i = 0; i < batch_keys.size(); i++) {
    WriteBatch batch;
    for (size_t j = 0; j < batch_keys[i].size(); j++) {
      batch.Put(handles_[0], batch_keys[i][j], DummyString(1024));
    }
    dbfull()->Write(WriteOptions(), &batch);
  }

  // Create a test filter that would add extra keys
  TestWalFilterWithChangeBatchAddExtraKeys test_wal_filter_extra_keys;

  // Reopen database with option to use WAL filter
  options = OptionsForLogIterTest();
  options.wal_filter = &test_wal_filter_extra_keys;
  Status status = TryReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_TRUE(status.IsNotSupported());

  // Reopen without filter, now reopen should succeed - previous
  // attempt to open must not have altered the db.
  options = OptionsForLogIterTest();
  ReopenWithColumnFamilies({ "default", "pikachu" }, options);

  std::vector<Slice> keys_must_exist;
  std::vector<Slice> keys_must_not_exist;  // empty vector

  for (size_t i = 0; i < batch_keys.size(); i++) {
    for (size_t j = 0; j < batch_keys[i].size(); j++) {
      keys_must_exist.push_back(Slice(batch_keys[i][j]));
    }
  }

  ValidateKeyExistence(db_, keys_must_exist, keys_must_not_exist);
}

TEST_F(DBTest2, WalFilterTestWithColumnFamilies) {
  class TestWalFilterWithColumnFamilies : public WalFilter {
  private:
    // column_family_id -> log_number map (provided to WALFilter)
    std::map<uint32_t, uint64_t> cf_log_number_map_;
    // column_family_name -> column_family_id map (provided to WALFilter)
    std::map<std::string, uint32_t> cf_name_id_map_;
    // column_family_name -> keys_found_in_wal map
    // We store keys that are applicable to the column_family
    // during recovery (i.e. aren't already flushed to SST file(s))
    // for verification against the keys we expect.
    std::map<uint32_t, std::vector<std::string>> cf_wal_keys_;
  public:
   void ColumnFamilyLogNumberMap(
       const std::map<uint32_t, uint64_t>& cf_lognumber_map,
       const std::map<std::string, uint32_t>& cf_name_id_map) override {
     cf_log_number_map_ = cf_lognumber_map;
     cf_name_id_map_ = cf_name_id_map;
   }

   WalProcessingOption LogRecordFound(unsigned long long log_number,
                                      const std::string& /*log_file_name*/,
                                      const WriteBatch& batch,
                                      WriteBatch* /*new_batch*/,
                                      bool* /*batch_changed*/) override {
     class LogRecordBatchHandler : public WriteBatch::Handler {
      private:
        const std::map<uint32_t, uint64_t> & cf_log_number_map_;
        std::map<uint32_t, std::vector<std::string>> & cf_wal_keys_;
        unsigned long long log_number_;
      public:
        LogRecordBatchHandler(unsigned long long current_log_number,
          const std::map<uint32_t, uint64_t> & cf_log_number_map,
          std::map<uint32_t, std::vector<std::string>> & cf_wal_keys) :
          cf_log_number_map_(cf_log_number_map),
          cf_wal_keys_(cf_wal_keys),
          log_number_(current_log_number){}

        Status PutCF(uint32_t column_family_id, const Slice& key,
                     const Slice& /*value*/) override {
          auto it = cf_log_number_map_.find(column_family_id);
          assert(it != cf_log_number_map_.end());
          unsigned long long log_number_for_cf = it->second;
          // If the current record is applicable for column_family_id
          // (i.e. isn't flushed to SST file(s) for column_family_id)
          // add it to the cf_wal_keys_ map for verification.
          if (log_number_ >= log_number_for_cf) {
            cf_wal_keys_[column_family_id].push_back(std::string(key.data(),
              key.size()));
          }
          return Status::OK();
        }
      } handler(log_number, cf_log_number_map_, cf_wal_keys_);

      batch.Iterate(&handler);

      return WalProcessingOption::kContinueProcessing;
   }

   const char* Name() const override {
     return "WalFilterTestWithColumnFamilies";
   }

    const std::map<uint32_t, std::vector<std::string>>& GetColumnFamilyKeys() {
      return cf_wal_keys_;
    }

    const std::map<std::string, uint32_t> & GetColumnFamilyNameIdMap() {
      return cf_name_id_map_;
    }
  };

  std::vector<std::vector<std::string>> batch_keys_pre_flush(3);

  batch_keys_pre_flush[0].push_back("key1");
  batch_keys_pre_flush[0].push_back("key2");
  batch_keys_pre_flush[1].push_back("key3");
  batch_keys_pre_flush[1].push_back("key4");
  batch_keys_pre_flush[2].push_back("key5");
  batch_keys_pre_flush[2].push_back("key6");

  Options options = OptionsForLogIterTest();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({ "pikachu" }, options);

  // Write given keys in given batches
  for (size_t i = 0; i < batch_keys_pre_flush.size(); i++) {
    WriteBatch batch;
    for (size_t j = 0; j < batch_keys_pre_flush[i].size(); j++) {
      batch.Put(handles_[0], batch_keys_pre_flush[i][j], DummyString(1024));
      batch.Put(handles_[1], batch_keys_pre_flush[i][j], DummyString(1024));
    }
    dbfull()->Write(WriteOptions(), &batch);
  }

  //Flush default column-family
  db_->Flush(FlushOptions(), handles_[0]);

  // Do some more writes
  std::vector<std::vector<std::string>> batch_keys_post_flush(3);

  batch_keys_post_flush[0].push_back("key7");
  batch_keys_post_flush[0].push_back("key8");
  batch_keys_post_flush[1].push_back("key9");
  batch_keys_post_flush[1].push_back("key10");
  batch_keys_post_flush[2].push_back("key11");
  batch_keys_post_flush[2].push_back("key12");

  // Write given keys in given batches
  for (size_t i = 0; i < batch_keys_post_flush.size(); i++) {
    WriteBatch batch;
    for (size_t j = 0; j < batch_keys_post_flush[i].size(); j++) {
      batch.Put(handles_[0], batch_keys_post_flush[i][j], DummyString(1024));
      batch.Put(handles_[1], batch_keys_post_flush[i][j], DummyString(1024));
    }
    dbfull()->Write(WriteOptions(), &batch);
  }

  // On Recovery we should only find the second batch applicable to default CF
  // But both batches applicable to pikachu CF

  // Create a test filter that would add extra keys
  TestWalFilterWithColumnFamilies test_wal_filter_column_families;

  // Reopen database with option to use WAL filter
  options = OptionsForLogIterTest();
  options.wal_filter = &test_wal_filter_column_families;
  Status status =
    TryReopenWithColumnFamilies({ "default", "pikachu" }, options);
  ASSERT_TRUE(status.ok());

  // verify that handles_[0] only has post_flush keys
  // while handles_[1] has pre and post flush keys
  auto cf_wal_keys = test_wal_filter_column_families.GetColumnFamilyKeys();
  auto name_id_map = test_wal_filter_column_families.GetColumnFamilyNameIdMap();
  size_t index = 0;
  auto keys_cf = cf_wal_keys[name_id_map[kDefaultColumnFamilyName]];
  //default column-family, only post_flush keys are expected
  for (size_t i = 0; i < batch_keys_post_flush.size(); i++) {
    for (size_t j = 0; j < batch_keys_post_flush[i].size(); j++) {
      Slice key_from_the_log(keys_cf[index++]);
      Slice batch_key(batch_keys_post_flush[i][j]);
      ASSERT_TRUE(key_from_the_log.compare(batch_key) == 0);
    }
  }
  ASSERT_TRUE(index == keys_cf.size());

  index = 0;
  keys_cf = cf_wal_keys[name_id_map["pikachu"]];
  //pikachu column-family, all keys are expected
  for (size_t i = 0; i < batch_keys_pre_flush.size(); i++) {
    for (size_t j = 0; j < batch_keys_pre_flush[i].size(); j++) {
      Slice key_from_the_log(keys_cf[index++]);
      Slice batch_key(batch_keys_pre_flush[i][j]);
      ASSERT_TRUE(key_from_the_log.compare(batch_key) == 0);
    }
  }

  for (size_t i = 0; i < batch_keys_post_flush.size(); i++) {
    for (size_t j = 0; j < batch_keys_post_flush[i].size(); j++) {
      Slice key_from_the_log(keys_cf[index++]);
      Slice batch_key(batch_keys_post_flush[i][j]);
      ASSERT_TRUE(key_from_the_log.compare(batch_key) == 0);
    }
  }
  ASSERT_TRUE(index == keys_cf.size());
}

// Temporarily disable it because the test is flaky.
TEST_F(DBTest2, DISABLED_PresetCompressionDict) {
  // Verifies that compression ratio improves when dictionary is enabled, and
  // improves even further when the dictionary is trained by ZSTD.
  const size_t kBlockSizeBytes = 4 << 10;
  const size_t kL0FileBytes = 128 << 10;
  const size_t kApproxPerBlockOverheadBytes = 50;
  const int kNumL0Files = 5;

  Options options;
  options.env = CurrentOptions().env; // Make sure to use any custom env that the test is configured with.
  options.allow_concurrent_memtable_write = false;
  options.arena_block_size = kBlockSizeBytes;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.level0_file_num_compaction_trigger = kNumL0Files;
  options.memtable_factory.reset(
      new SpecialSkipListFactory(kL0FileBytes / kBlockSizeBytes));
  options.num_levels = 2;
  options.target_file_size_base = kL0FileBytes;
  options.target_file_size_multiplier = 2;
  options.write_buffer_size = kL0FileBytes;
  BlockBasedTableOptions table_options;
  table_options.block_size = kBlockSizeBytes;
  std::vector<CompressionType> compression_types;
  if (Zlib_Supported()) {
    compression_types.push_back(kZlibCompression);
  }
#if LZ4_VERSION_NUMBER >= 10400  // r124+
  compression_types.push_back(kLZ4Compression);
  compression_types.push_back(kLZ4HCCompression);
#endif                          // LZ4_VERSION_NUMBER >= 10400
  if (ZSTD_Supported()) {
    compression_types.push_back(kZSTD);
  }

  for (auto compression_type : compression_types) {
    options.compression = compression_type;
    size_t prev_out_bytes;
    for (int i = 0; i < 3; ++i) {
      // First iteration: compress without preset dictionary
      // Second iteration: compress with preset dictionary
      // Third iteration (zstd only): compress with zstd-trained dictionary
      //
      // To make sure the compression dictionary has the intended effect, we
      // verify the compressed size is smaller in successive iterations. Also in
      // the non-first iterations, verify the data we get out is the same data
      // we put in.
      switch (i) {
        case 0:
          options.compression_opts.max_dict_bytes = 0;
          options.compression_opts.zstd_max_train_bytes = 0;
          break;
        case 1:
          options.compression_opts.max_dict_bytes = 4 * kBlockSizeBytes;
          options.compression_opts.zstd_max_train_bytes = 0;
          break;
        case 2:
          if (compression_type != kZSTD) {
            continue;
          }
          options.compression_opts.max_dict_bytes = 4 * kBlockSizeBytes;
          options.compression_opts.zstd_max_train_bytes = kL0FileBytes;
          break;
        default:
          assert(false);
      }

      options.statistics = rocksdb::CreateDBStatistics();
      options.table_factory.reset(NewBlockBasedTableFactory(table_options));
      CreateAndReopenWithCF({"pikachu"}, options);
      Random rnd(301);
      std::string seq_datas[10];
      for (int j = 0; j < 10; ++j) {
        seq_datas[j] =
            RandomString(&rnd, kBlockSizeBytes - kApproxPerBlockOverheadBytes);
      }

      ASSERT_EQ(0, NumTableFilesAtLevel(0, 1));
      for (int j = 0; j < kNumL0Files; ++j) {
        for (size_t k = 0; k < kL0FileBytes / kBlockSizeBytes + 1; ++k) {
          auto key_num = j * (kL0FileBytes / kBlockSizeBytes) + k;
          ASSERT_OK(Put(1, Key(static_cast<int>(key_num)),
                        seq_datas[(key_num / 10) % 10]));
        }
        dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
        ASSERT_EQ(j + 1, NumTableFilesAtLevel(0, 1));
      }
      dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1],
                                  true /* disallow_trivial_move */);
      ASSERT_EQ(0, NumTableFilesAtLevel(0, 1));
      ASSERT_GT(NumTableFilesAtLevel(1, 1), 0);

      size_t out_bytes = 0;
      std::vector<std::string> files;
      GetSstFiles(env_, dbname_, &files);
      for (const auto& file : files) {
        uint64_t curr_bytes;
        env_->GetFileSize(dbname_ + "/" + file, &curr_bytes);
        out_bytes += static_cast<size_t>(curr_bytes);
      }

      for (size_t j = 0; j < kNumL0Files * (kL0FileBytes / kBlockSizeBytes);
           j++) {
        ASSERT_EQ(seq_datas[(j / 10) % 10], Get(1, Key(static_cast<int>(j))));
      }
      if (i) {
        ASSERT_GT(prev_out_bytes, out_bytes);
      }
      prev_out_bytes = out_bytes;
      DestroyAndReopen(options);
    }
  }
}

TEST_F(DBTest2, PresetCompressionDictLocality) {
  if (!ZSTD_Supported()) {
    return;
  }
  // Verifies that compression dictionary is generated from local data. The
  // verification simply checks all output SSTs have different compression
  // dictionaries. We do not verify effectiveness as that'd likely be flaky in
  // the future.
  const int kNumEntriesPerFile = 1 << 10;  // 1KB
  const int kNumBytesPerEntry = 1 << 10;   // 1KB
  const int kNumFiles = 4;
  Options options = CurrentOptions();
  options.compression = kZSTD;
  options.compression_opts.max_dict_bytes = 1 << 14;        // 16KB
  options.compression_opts.zstd_max_train_bytes = 1 << 18;  // 256KB
  options.statistics = rocksdb::CreateDBStatistics();
  options.target_file_size_base = kNumEntriesPerFile * kNumBytesPerEntry;
  BlockBasedTableOptions table_options;
  table_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  Reopen(options);

  Random rnd(301);
  for (int i = 0; i < kNumFiles; ++i) {
    for (int j = 0; j < kNumEntriesPerFile; ++j) {
      ASSERT_OK(Put(Key(i * kNumEntriesPerFile + j),
                    RandomString(&rnd, kNumBytesPerEntry)));
    }
    ASSERT_OK(Flush());
    MoveFilesToLevel(1);
    ASSERT_EQ(NumTableFilesAtLevel(1), i + 1);
  }

  // Store all the dictionaries generated during a full compaction.
  std::vector<std::string> compression_dicts;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTableBuilder::WriteCompressionDictBlock:RawDict",
      [&](void* arg) {
        compression_dicts.emplace_back(static_cast<Slice*>(arg)->ToString());
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  CompactRangeOptions compact_range_opts;
  compact_range_opts.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_range_opts, nullptr, nullptr));

  // Dictionary compression should not be so good as to compress four totally
  // random files into one. If it does then there's probably something wrong
  // with the test.
  ASSERT_GT(NumTableFilesAtLevel(1), 1);

  // Furthermore, there should be one compression dictionary generated per file.
  // And they should all be different from each other.
  ASSERT_EQ(NumTableFilesAtLevel(1),
            static_cast<int>(compression_dicts.size()));
  for (size_t i = 1; i < compression_dicts.size(); ++i) {
    std::string& a = compression_dicts[i - 1];
    std::string& b = compression_dicts[i];
    size_t alen = a.size();
    size_t blen = b.size();
    ASSERT_TRUE(alen != blen || memcmp(a.data(), b.data(), alen) != 0);
  }
}

class CompactionCompressionListener : public EventListener {
 public:
  explicit CompactionCompressionListener(Options* db_options)
      : db_options_(db_options) {}

  void OnCompactionCompleted(DB* db, const CompactionJobInfo& ci) override {
    // Figure out last level with files
    int bottommost_level = 0;
    for (int level = 0; level < db->NumberLevels(); level++) {
      std::string files_at_level;
      ASSERT_TRUE(
          db->GetProperty("rocksdb.num-files-at-level" + NumberToString(level),
                          &files_at_level));
      if (files_at_level != "0") {
        bottommost_level = level;
      }
    }

    if (db_options_->bottommost_compression != kDisableCompressionOption &&
        ci.output_level == bottommost_level) {
      ASSERT_EQ(ci.compression, db_options_->bottommost_compression);
    } else if (db_options_->compression_per_level.size() != 0) {
      ASSERT_EQ(ci.compression,
                db_options_->compression_per_level[ci.output_level]);
    } else {
      ASSERT_EQ(ci.compression, db_options_->compression);
    }
    max_level_checked = std::max(max_level_checked, ci.output_level);
  }

  int max_level_checked = 0;
  const Options* db_options_;
};

TEST_F(DBTest2, CompressionOptions) {
  if (!Zlib_Supported() || !Snappy_Supported()) {
    return;
  }

  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 2;
  options.max_bytes_for_level_base = 100;
  options.max_bytes_for_level_multiplier = 2;
  options.num_levels = 7;
  options.max_background_compactions = 1;

  CompactionCompressionListener* listener =
      new CompactionCompressionListener(&options);
  options.listeners.emplace_back(listener);

  const int kKeySize = 5;
  const int kValSize = 20;
  Random rnd(301);

  for (int iter = 0; iter <= 2; iter++) {
    listener->max_level_checked = 0;

    if (iter == 0) {
      // Use different compression algorithms for different levels but
      // always use Zlib for bottommost level
      options.compression_per_level = {kNoCompression,     kNoCompression,
                                       kNoCompression,     kSnappyCompression,
                                       kSnappyCompression, kSnappyCompression,
                                       kZlibCompression};
      options.compression = kNoCompression;
      options.bottommost_compression = kZlibCompression;
    } else if (iter == 1) {
      // Use Snappy except for bottommost level use ZLib
      options.compression_per_level = {};
      options.compression = kSnappyCompression;
      options.bottommost_compression = kZlibCompression;
    } else if (iter == 2) {
      // Use Snappy everywhere
      options.compression_per_level = {};
      options.compression = kSnappyCompression;
      options.bottommost_compression = kDisableCompressionOption;
    }

    DestroyAndReopen(options);
    // Write 10 random files
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 5; j++) {
        ASSERT_OK(
            Put(RandomString(&rnd, kKeySize), RandomString(&rnd, kValSize)));
      }
      ASSERT_OK(Flush());
      dbfull()->TEST_WaitForCompact();
    }

    // Make sure that we wrote enough to check all 7 levels
    ASSERT_EQ(listener->max_level_checked, 6);
  }
}

class CompactionStallTestListener : public EventListener {
 public:
  CompactionStallTestListener() : compacting_files_cnt_(0), compacted_files_cnt_(0) {}

  void OnCompactionBegin(DB* /*db*/, const CompactionJobInfo& ci) override {
    ASSERT_EQ(ci.cf_name, "default");
    ASSERT_EQ(ci.base_input_level, 0);
    ASSERT_EQ(ci.compaction_reason, CompactionReason::kLevelL0FilesNum);
    compacting_files_cnt_ += ci.input_files.size();
  }

  void OnCompactionCompleted(DB* /*db*/, const CompactionJobInfo& ci) override {
    ASSERT_EQ(ci.cf_name, "default");
    ASSERT_EQ(ci.base_input_level, 0);
    ASSERT_EQ(ci.compaction_reason, CompactionReason::kLevelL0FilesNum);
    compacted_files_cnt_ += ci.input_files.size();
  }

  std::atomic<size_t> compacting_files_cnt_;
  std::atomic<size_t> compacted_files_cnt_;
};

TEST_F(DBTest2, CompactionStall) {
  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BGWorkCompaction", "DBTest2::CompactionStall:0"},
       {"DBImpl::BGWorkCompaction", "DBTest2::CompactionStall:1"},
       {"DBTest2::CompactionStall:2",
        "DBImpl::NotifyOnCompactionBegin::UnlockMutex"},
       {"DBTest2::CompactionStall:3",
        "DBImpl::NotifyOnCompactionCompleted::UnlockMutex"}});
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 4;
  options.max_background_compactions = 40;
  CompactionStallTestListener* listener = new CompactionStallTestListener();
  options.listeners.emplace_back(listener);
  DestroyAndReopen(options);
  // make sure all background compaction jobs can be scheduled
  auto stop_token =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();

  Random rnd(301);

  // 4 Files in L0
  for (int i = 0; i < 4; i++) {
    for (int j = 0; j < 10; j++) {
      ASSERT_OK(Put(RandomString(&rnd, 10), RandomString(&rnd, 10)));
    }
    ASSERT_OK(Flush());
  }

  // Wait for compaction to be triggered
  TEST_SYNC_POINT("DBTest2::CompactionStall:0");

  // Clear "DBImpl::BGWorkCompaction" SYNC_POINT since we want to hold it again
  // at DBTest2::CompactionStall::1
  rocksdb::SyncPoint::GetInstance()->ClearTrace();

  // Another 6 L0 files to trigger compaction again
  for (int i = 0; i < 6; i++) {
    for (int j = 0; j < 10; j++) {
      ASSERT_OK(Put(RandomString(&rnd, 10), RandomString(&rnd, 10)));
    }
    ASSERT_OK(Flush());
  }

  // Wait for another compaction to be triggered
  TEST_SYNC_POINT("DBTest2::CompactionStall:1");

  // Hold NotifyOnCompactionBegin in the unlock mutex section
  TEST_SYNC_POINT("DBTest2::CompactionStall:2");

  // Hold NotifyOnCompactionCompleted in the unlock mutex section
  TEST_SYNC_POINT("DBTest2::CompactionStall:3");

  dbfull()->TEST_WaitForCompact();
  ASSERT_LT(NumTableFilesAtLevel(0),
            options.level0_file_num_compaction_trigger);
  ASSERT_GT(listener->compacted_files_cnt_.load(),
            10 - options.level0_file_num_compaction_trigger);
  ASSERT_EQ(listener->compacting_files_cnt_.load(), listener->compacted_files_cnt_.load());

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

#endif  // ROCKSDB_LITE

TEST_F(DBTest2, FirstSnapshotTest) {
  Options options;
  options.write_buffer_size = 100000;  // Small write buffer
  options = CurrentOptions(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  // This snapshot will have sequence number 0 what is expected behaviour.
  const Snapshot* s1 = db_->GetSnapshot();

  Put(1, "k1", std::string(100000, 'x'));  // Fill memtable
  Put(1, "k2", std::string(100000, 'y'));  // Trigger flush

  db_->ReleaseSnapshot(s1);
}

#ifndef ROCKSDB_LITE
TEST_F(DBTest2, DuplicateSnapshot) {
  Options options;
  options = CurrentOptions(options);
  std::vector<const Snapshot*> snapshots;
  DBImpl* dbi = reinterpret_cast<DBImpl*>(db_);
  SequenceNumber oldest_ww_snap, first_ww_snap;

  Put("k", "v");  // inc seq
  snapshots.push_back(db_->GetSnapshot());
  snapshots.push_back(db_->GetSnapshot());
  Put("k", "v");  // inc seq
  snapshots.push_back(db_->GetSnapshot());
  snapshots.push_back(dbi->GetSnapshotForWriteConflictBoundary());
  first_ww_snap = snapshots.back()->GetSequenceNumber();
  Put("k", "v");  // inc seq
  snapshots.push_back(dbi->GetSnapshotForWriteConflictBoundary());
  snapshots.push_back(db_->GetSnapshot());
  Put("k", "v");  // inc seq
  snapshots.push_back(db_->GetSnapshot());

  {
    InstrumentedMutexLock l(dbi->mutex());
    auto seqs = dbi->snapshots().GetAll(&oldest_ww_snap);
    ASSERT_EQ(seqs.size(), 4);  // duplicates are not counted
    ASSERT_EQ(oldest_ww_snap, first_ww_snap);
  }

  for (auto s : snapshots) {
    db_->ReleaseSnapshot(s);
  }
}
#endif  // ROCKSDB_LITE

class PinL0IndexAndFilterBlocksTest
    : public DBTestBase,
      public testing::WithParamInterface<std::tuple<bool, bool>> {
 public:
  PinL0IndexAndFilterBlocksTest() : DBTestBase("/db_pin_l0_index_bloom_test") {}
  void SetUp() override {
    infinite_max_files_ = std::get<0>(GetParam());
    disallow_preload_ = std::get<1>(GetParam());
  }

  void CreateTwoLevels(Options* options, bool close_afterwards) {
    if (infinite_max_files_) {
      options->max_open_files = -1;
    }
    options->create_if_missing = true;
    options->statistics = rocksdb::CreateDBStatistics();
    BlockBasedTableOptions table_options;
    table_options.cache_index_and_filter_blocks = true;
    table_options.pin_l0_filter_and_index_blocks_in_cache = true;
    table_options.filter_policy.reset(NewBloomFilterPolicy(20));
    options->table_factory.reset(new BlockBasedTableFactory(table_options));
    CreateAndReopenWithCF({"pikachu"}, *options);

    Put(1, "a", "begin");
    Put(1, "z", "end");
    ASSERT_OK(Flush(1));
    // move this table to L1
    dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1]);

    // reset block cache
    table_options.block_cache = NewLRUCache(64 * 1024);
    options->table_factory.reset(NewBlockBasedTableFactory(table_options));
    TryReopenWithColumnFamilies({"default", "pikachu"}, *options);
    // create new table at L0
    Put(1, "a2", "begin2");
    Put(1, "z2", "end2");
    ASSERT_OK(Flush(1));

    if (close_afterwards) {
      Close();  // This ensures that there is no ref to block cache entries
    }
    table_options.block_cache->EraseUnRefEntries();
  }

  bool infinite_max_files_;
  bool disallow_preload_;
};

TEST_P(PinL0IndexAndFilterBlocksTest,
       IndexAndFilterBlocksOfNewTableAddedToCacheWithPinning) {
  Options options = CurrentOptions();
  if (infinite_max_files_) {
    options.max_open_files = -1;
  }
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.cache_index_and_filter_blocks = true;
  table_options.pin_l0_filter_and_index_blocks_in_cache = true;
  table_options.filter_policy.reset(NewBloomFilterPolicy(20));
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "key", "val"));
  // Create a new table.
  ASSERT_OK(Flush(1));

  // index/filter blocks added to block cache right after table creation.
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));

  // only index/filter were added
  ASSERT_EQ(2, TestGetTickerCount(options, BLOCK_CACHE_ADD));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_DATA_MISS));

  std::string value;
  // Miss and hit count should remain the same, they're all pinned.
  db_->KeyMayExist(ReadOptions(), handles_[1], "key", &value);
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));

  // Miss and hit count should remain the same, they're all pinned.
  value = Get(1, "key");
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));
}

TEST_P(PinL0IndexAndFilterBlocksTest,
       MultiLevelIndexAndFilterBlocksCachedWithPinning) {
  Options options = CurrentOptions();
  PinL0IndexAndFilterBlocksTest::CreateTwoLevels(&options, false);
  // get base cache values
  uint64_t fm = TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS);
  uint64_t fh = TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT);
  uint64_t im = TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS);
  uint64_t ih = TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT);

  std::string value;
  // this should be read from L0
  // so cache values don't change
  value = Get(1, "a2");
  ASSERT_EQ(fm, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(fh, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
  ASSERT_EQ(im, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
  ASSERT_EQ(ih, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));

  // this should be read from L1
  // the file is opened, prefetching results in a cache filter miss
  // the block is loaded and added to the cache,
  // then the get results in a cache hit for L1
  // When we have inifinite max_files, there is still cache miss because we have
  // reset the block cache
  value = Get(1, "a");
  ASSERT_EQ(fm + 1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(im + 1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
}

TEST_P(PinL0IndexAndFilterBlocksTest, DisablePrefetchingNonL0IndexAndFilter) {
  Options options = CurrentOptions();
  // This ensures that db does not ref anything in the block cache, so
  // EraseUnRefEntries could clear them up.
  bool close_afterwards = true;
  PinL0IndexAndFilterBlocksTest::CreateTwoLevels(&options, close_afterwards);

  // Get base cache values
  uint64_t fm = TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS);
  uint64_t fh = TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT);
  uint64_t im = TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS);
  uint64_t ih = TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT);

  if (disallow_preload_) {
    // Now we have two files. We narrow the max open files to allow 3 entries
    // so that preloading SST files won't happen.
    options.max_open_files = 13;
    // RocksDB sanitize max open files to at least 20. Modify it back.
    rocksdb::SyncPoint::GetInstance()->SetCallBack(
        "SanitizeOptions::AfterChangeMaxOpenFiles", [&](void* arg) {
          int* max_open_files = static_cast<int*>(arg);
          *max_open_files = 13;
        });
  }
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  // Reopen database. If max_open_files is set as -1, table readers will be
  // preloaded. This will trigger a BlockBasedTable::Open() and prefetch
  // L0 index and filter. Level 1's prefetching is disabled in DB::Open()
  TryReopenWithColumnFamilies({"default", "pikachu"}, options);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();

  if (!disallow_preload_) {
    // After reopen, cache miss are increased by one because we read (and only
    // read) filter and index on L0
    ASSERT_EQ(fm + 1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
    ASSERT_EQ(fh, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
    ASSERT_EQ(im + 1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
    ASSERT_EQ(ih, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));
  } else {
    // If max_open_files is not -1, we do not preload table readers, so there is
    // no change.
    ASSERT_EQ(fm, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
    ASSERT_EQ(fh, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
    ASSERT_EQ(im, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
    ASSERT_EQ(ih, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));
  }
  std::string value;
  // this should be read from L0
  value = Get(1, "a2");
  // If max_open_files is -1, we have pinned index and filter in Rep, so there
  // will not be changes in index and filter misses or hits. If max_open_files
  // is not -1, Get() will open a TableReader and prefetch index and filter.
  ASSERT_EQ(fm + 1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(fh, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
  ASSERT_EQ(im + 1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
  ASSERT_EQ(ih, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));

  // this should be read from L1
  value = Get(1, "a");
  if (!disallow_preload_) {
    // In inifinite max files case, there's a cache miss in executing Get()
    // because index and filter are not prefetched before.
    ASSERT_EQ(fm + 2, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
    ASSERT_EQ(fh, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
    ASSERT_EQ(im + 2, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
    ASSERT_EQ(ih, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));
  } else {
    // In this case, cache miss will be increased by one in
    // BlockBasedTable::Open() because this is not in DB::Open() code path so we
    // will prefetch L1's index and filter. Cache hit will also be increased by
    // one because Get() will read index and filter from the block cache
    // prefetched in previous Open() call.
    ASSERT_EQ(fm + 2, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
    ASSERT_EQ(fh + 1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
    ASSERT_EQ(im + 2, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
    ASSERT_EQ(ih + 1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));
  }

  // Force a full compaction to one single file. There will be a block
  // cache read for both of index and filter. If prefetch doesn't explicitly
  // happen, it will happen when verifying the file.
  Compact(1, "a", "zzzzz");
  dbfull()->TEST_WaitForCompact();

  if (!disallow_preload_) {
    ASSERT_EQ(fm + 3, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
    ASSERT_EQ(fh, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
    ASSERT_EQ(im + 3, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
    ASSERT_EQ(ih + 2, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));
  } else {
    ASSERT_EQ(fm + 3, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
    ASSERT_EQ(fh + 1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
    ASSERT_EQ(im + 3, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
    ASSERT_EQ(ih + 3, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));
  }

  // Bloom and index hit will happen when a Get() happens.
  value = Get(1, "a");
  if (!disallow_preload_) {
    ASSERT_EQ(fm + 3, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
    ASSERT_EQ(fh + 1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
    ASSERT_EQ(im + 3, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
    ASSERT_EQ(ih + 3, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));
  } else {
    ASSERT_EQ(fm + 3, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
    ASSERT_EQ(fh + 2, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
    ASSERT_EQ(im + 3, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
    ASSERT_EQ(ih + 4, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));
  }
}

INSTANTIATE_TEST_CASE_P(PinL0IndexAndFilterBlocksTest,
                        PinL0IndexAndFilterBlocksTest,
                        ::testing::Values(std::make_tuple(true, false),
                                          std::make_tuple(false, false),
                                          std::make_tuple(false, true)));

#ifndef ROCKSDB_LITE
TEST_F(DBTest2, MaxCompactionBytesTest) {
  Options options = CurrentOptions();
  options.memtable_factory.reset(
      new SpecialSkipListFactory(DBTestBase::kNumKeysByGenerateNewRandomFile));
  options.compaction_style = kCompactionStyleLevel;
  options.write_buffer_size = 200 << 10;
  options.arena_block_size = 4 << 10;
  options.level0_file_num_compaction_trigger = 4;
  options.num_levels = 4;
  options.compression = kNoCompression;
  options.max_bytes_for_level_base = 450 << 10;
  options.target_file_size_base = 100 << 10;
  // Infinite for full compaction.
  options.max_compaction_bytes = options.target_file_size_base * 100;

  Reopen(options);

  Random rnd(301);

  for (int num = 0; num < 8; num++) {
    GenerateNewRandomFile(&rnd);
  }
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,8", FilesPerLevel(0));

  // When compact from Ln -> Ln+1, cut a file if the file overlaps with
  // more than three files in Ln+1.
  options.max_compaction_bytes = options.target_file_size_base * 3;
  Reopen(options);

  GenerateNewRandomFile(&rnd);
  // Add three more small files that overlap with the previous file
  for (int i = 0; i < 3; i++) {
    Put("a", "z");
    ASSERT_OK(Flush());
  }
  dbfull()->TEST_WaitForCompact();

  // Output files to L1 are cut to three pieces, according to
  // options.max_compaction_bytes
  ASSERT_EQ("0,3,8", FilesPerLevel(0));
}

static void UniqueIdCallback(void* arg) {
  int* result = reinterpret_cast<int*>(arg);
  if (*result == -1) {
    *result = 0;
  }

  rocksdb::SyncPoint::GetInstance()->ClearTrace();
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "GetUniqueIdFromFile:FS_IOC_GETVERSION", UniqueIdCallback);
}

class MockPersistentCache : public PersistentCache {
 public:
  explicit MockPersistentCache(const bool is_compressed, const size_t max_size)
      : is_compressed_(is_compressed), max_size_(max_size) {
    rocksdb::SyncPoint::GetInstance()->EnableProcessing();
    rocksdb::SyncPoint::GetInstance()->SetCallBack(
        "GetUniqueIdFromFile:FS_IOC_GETVERSION", UniqueIdCallback);
  }

  ~MockPersistentCache() override {}

  PersistentCache::StatsType Stats() override {
    return PersistentCache::StatsType();
  }

  Status Insert(const Slice& page_key, const char* data,
                const size_t size) override {
    MutexLock _(&lock_);

    if (size_ > max_size_) {
      size_ -= data_.begin()->second.size();
      data_.erase(data_.begin());
    }

    data_.insert(std::make_pair(page_key.ToString(), std::string(data, size)));
    size_ += size;
    return Status::OK();
  }

  Status Lookup(const Slice& page_key, std::unique_ptr<char[]>* data,
                size_t* size) override {
    MutexLock _(&lock_);
    auto it = data_.find(page_key.ToString());
    if (it == data_.end()) {
      return Status::NotFound();
    }

    assert(page_key.ToString() == it->first);
    data->reset(new char[it->second.size()]);
    memcpy(data->get(), it->second.c_str(), it->second.size());
    *size = it->second.size();
    return Status::OK();
  }

  bool IsCompressed() override { return is_compressed_; }

  std::string GetPrintableOptions() const override {
    return "MockPersistentCache";
  }

  port::Mutex lock_;
  std::map<std::string, std::string> data_;
  const bool is_compressed_ = true;
  size_t size_ = 0;
  const size_t max_size_ = 10 * 1024;  // 10KiB
};

#ifdef OS_LINUX
// Make sure that in CPU time perf context counters, Env::NowCPUNanos()
// is used, rather than Env::CPUNanos();
TEST_F(DBTest2, TestPerfContextCpuTime) {
  // force resizing table cache so table handle is not preloaded so that
  // we can measure find_table_nanos during Get().
  dbfull()->TEST_table_cache()->SetCapacity(0);
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Flush());
  env_->now_cpu_count_.store(0);

  // CPU timing is not enabled with kEnableTimeExceptForMutex
  SetPerfLevel(PerfLevel::kEnableTimeExceptForMutex);
  ASSERT_EQ("bar", Get("foo"));
  ASSERT_EQ(0, get_perf_context()->get_cpu_nanos);
  ASSERT_EQ(0, env_->now_cpu_count_.load());

  uint64_t kDummyAddonTime = uint64_t{1000000000000};

  // Add time to NowNanos() reading.
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "TableCache::FindTable:0",
      [&](void* /*arg*/) { env_->addon_time_.fetch_add(kDummyAddonTime); });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  SetPerfLevel(PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);
  ASSERT_EQ("bar", Get("foo"));
  ASSERT_GT(env_->now_cpu_count_.load(), 2);
  ASSERT_LT(get_perf_context()->get_cpu_nanos, kDummyAddonTime);
  ASSERT_GT(get_perf_context()->find_table_nanos, kDummyAddonTime);

  SetPerfLevel(PerfLevel::kDisable);
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}
#endif  // OS_LINUX

#ifndef OS_SOLARIS // GetUniqueIdFromFile is not implemented
TEST_F(DBTest2, PersistentCache) {
  int num_iter = 80;

  Options options;
  options.write_buffer_size = 64 * 1024;  // small write buffer
  options.statistics = rocksdb::CreateDBStatistics();
  options = CurrentOptions(options);

  auto bsizes = {/*no block cache*/ 0, /*1M*/ 1 * 1024 * 1024};
  auto types = {/*compressed*/ 1, /*uncompressed*/ 0};
  for (auto bsize : bsizes) {
    for (auto type : types) {
      BlockBasedTableOptions table_options;
      table_options.persistent_cache.reset(
          new MockPersistentCache(type, 10 * 1024));
      table_options.no_block_cache = true;
      table_options.block_cache = bsize ? NewLRUCache(bsize) : nullptr;
      table_options.block_cache_compressed = nullptr;
      options.table_factory.reset(NewBlockBasedTableFactory(table_options));

      DestroyAndReopen(options);
      CreateAndReopenWithCF({"pikachu"}, options);
      // default column family doesn't have block cache
      Options no_block_cache_opts;
      no_block_cache_opts.statistics = options.statistics;
      no_block_cache_opts = CurrentOptions(no_block_cache_opts);
      BlockBasedTableOptions table_options_no_bc;
      table_options_no_bc.no_block_cache = true;
      no_block_cache_opts.table_factory.reset(
          NewBlockBasedTableFactory(table_options_no_bc));
      ReopenWithColumnFamilies(
          {"default", "pikachu"},
          std::vector<Options>({no_block_cache_opts, options}));

      Random rnd(301);

      // Write 8MB (80 values, each 100K)
      ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
      std::vector<std::string> values;
      std::string str;
      for (int i = 0; i < num_iter; i++) {
        if (i % 4 == 0) {  // high compression ratio
          str = RandomString(&rnd, 1000);
        }
        values.push_back(str);
        ASSERT_OK(Put(1, Key(i), values[i]));
      }

      // flush all data from memtable so that reads are from block cache
      ASSERT_OK(Flush(1));

      for (int i = 0; i < num_iter; i++) {
        ASSERT_EQ(Get(1, Key(i)), values[i]);
      }

      auto hit = options.statistics->getTickerCount(PERSISTENT_CACHE_HIT);
      auto miss = options.statistics->getTickerCount(PERSISTENT_CACHE_MISS);

      ASSERT_GT(hit, 0);
      ASSERT_GT(miss, 0);
    }
  }
}
#endif // !OS_SOLARIS

namespace {
void CountSyncPoint() {
  TEST_SYNC_POINT_CALLBACK("DBTest2::MarkedPoint", nullptr /* arg */);
}
}  // namespace

TEST_F(DBTest2, SyncPointMarker) {
  std::atomic<int> sync_point_called(0);
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBTest2::MarkedPoint",
      [&](void* /*arg*/) { sync_point_called.fetch_add(1); });

  // The first dependency enforces Marker can be loaded before MarkedPoint.
  // The second checks that thread 1's MarkedPoint should be disabled here.
  // Execution order:
  // |   Thread 1    |  Thread 2   |
  // |               |   Marker    |
  // |  MarkedPoint  |             |
  // | Thread1First  |             |
  // |               | MarkedPoint |
  rocksdb::SyncPoint::GetInstance()->LoadDependencyAndMarkers(
      {{"DBTest2::SyncPointMarker:Thread1First", "DBTest2::MarkedPoint"}},
      {{"DBTest2::SyncPointMarker:Marker", "DBTest2::MarkedPoint"}});

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  std::function<void()> func1 = [&]() {
    CountSyncPoint();
    TEST_SYNC_POINT("DBTest2::SyncPointMarker:Thread1First");
  };

  std::function<void()> func2 = [&]() {
    TEST_SYNC_POINT("DBTest2::SyncPointMarker:Marker");
    CountSyncPoint();
  };

  auto thread1 = port::Thread(func1);
  auto thread2 = port::Thread(func2);
  thread1.join();
  thread2.join();

  // Callback is only executed once
  ASSERT_EQ(sync_point_called.load(), 1);
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}
#endif

size_t GetEncodedEntrySize(size_t key_size, size_t value_size) {
  std::string buffer;

  PutVarint32(&buffer, static_cast<uint32_t>(0));
  PutVarint32(&buffer, static_cast<uint32_t>(key_size));
  PutVarint32(&buffer, static_cast<uint32_t>(value_size));

  return buffer.size() + key_size + value_size;
}

TEST_F(DBTest2, ReadAmpBitmap) {
  Options options = CurrentOptions();
  BlockBasedTableOptions bbto;
  uint32_t bytes_per_bit[2] = {1, 16};
  for (size_t k = 0; k < 2; k++) {
    // Disable delta encoding to make it easier to calculate read amplification
    bbto.use_delta_encoding = false;
    // Huge block cache to make it easier to calculate read amplification
    bbto.block_cache = NewLRUCache(1024 * 1024 * 1024);
    bbto.read_amp_bytes_per_bit = bytes_per_bit[k];
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));
    options.statistics = rocksdb::CreateDBStatistics();
    DestroyAndReopen(options);

    const size_t kNumEntries = 10000;

    Random rnd(301);
    for (size_t i = 0; i < kNumEntries; i++) {
      ASSERT_OK(Put(Key(static_cast<int>(i)), RandomString(&rnd, 100)));
    }
    ASSERT_OK(Flush());

    Close();
    Reopen(options);

    // Read keys/values randomly and verify that reported read amp error
    // is less than 2%
    uint64_t total_useful_bytes = 0;
    std::set<int> read_keys;
    std::string value;
    for (size_t i = 0; i < kNumEntries * 5; i++) {
      int key_idx = rnd.Next() % kNumEntries;
      std::string key = Key(key_idx);
      ASSERT_OK(db_->Get(ReadOptions(), key, &value));

      if (read_keys.find(key_idx) == read_keys.end()) {
        auto internal_key = InternalKey(key, 0, ValueType::kTypeValue);
        total_useful_bytes +=
            GetEncodedEntrySize(internal_key.size(), value.size());
        read_keys.insert(key_idx);
      }

      double expected_read_amp =
          static_cast<double>(total_useful_bytes) /
          options.statistics->getTickerCount(READ_AMP_TOTAL_READ_BYTES);

      double read_amp =
          static_cast<double>(options.statistics->getTickerCount(
              READ_AMP_ESTIMATE_USEFUL_BYTES)) /
          options.statistics->getTickerCount(READ_AMP_TOTAL_READ_BYTES);

      double error_pct = fabs(expected_read_amp - read_amp) * 100;
      // Error between reported read amp and real read amp should be less than
      // 2%
      EXPECT_LE(error_pct, 2);
    }

    // Make sure we read every thing in the DB (which is smaller than our cache)
    Iterator* iter = db_->NewIterator(ReadOptions());
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_EQ(iter->value().ToString(), Get(iter->key().ToString()));
    }
    delete iter;

    // Read amp is on average 100% since we read all what we loaded in memory
    if (k == 0) {
      ASSERT_EQ(
          options.statistics->getTickerCount(READ_AMP_ESTIMATE_USEFUL_BYTES),
          options.statistics->getTickerCount(READ_AMP_TOTAL_READ_BYTES));
    } else {
      ASSERT_NEAR(
          options.statistics->getTickerCount(READ_AMP_ESTIMATE_USEFUL_BYTES) *
              1.0f /
              options.statistics->getTickerCount(READ_AMP_TOTAL_READ_BYTES),
          1, .01);
    }
  }
}

#ifndef OS_SOLARIS // GetUniqueIdFromFile is not implemented
TEST_F(DBTest2, ReadAmpBitmapLiveInCacheAfterDBClose) {
  {
    const int kIdBufLen = 100;
    char id_buf[kIdBufLen];
#ifndef OS_WIN
    // You can't open a directory on windows using random access file
    std::unique_ptr<RandomAccessFile> file;
    ASSERT_OK(env_->NewRandomAccessFile(dbname_, &file, EnvOptions()));
    if (file->GetUniqueId(id_buf, kIdBufLen) == 0) {
      // fs holding db directory doesn't support getting a unique file id,
      // this means that running this test will fail because lru_cache will load
      // the blocks again regardless of them being already in the cache
      return;
    }
#else
    std::unique_ptr<Directory> dir;
    ASSERT_OK(env_->NewDirectory(dbname_, &dir));
    if (dir->GetUniqueId(id_buf, kIdBufLen) == 0) {
      // fs holding db directory doesn't support getting a unique file id,
      // this means that running this test will fail because lru_cache will load
      // the blocks again regardless of them being already in the cache
      return;
    }
#endif
  }
  uint32_t bytes_per_bit[2] = {1, 16};
  for (size_t k = 0; k < 2; k++) {
    std::shared_ptr<Cache> lru_cache = NewLRUCache(1024 * 1024 * 1024);
    std::shared_ptr<Statistics> stats = rocksdb::CreateDBStatistics();

    Options options = CurrentOptions();
    BlockBasedTableOptions bbto;
    // Disable delta encoding to make it easier to calculate read amplification
    bbto.use_delta_encoding = false;
    // Huge block cache to make it easier to calculate read amplification
    bbto.block_cache = lru_cache;
    bbto.read_amp_bytes_per_bit = bytes_per_bit[k];
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));
    options.statistics = stats;
    DestroyAndReopen(options);

    const int kNumEntries = 10000;

    Random rnd(301);
    for (int i = 0; i < kNumEntries; i++) {
      ASSERT_OK(Put(Key(i), RandomString(&rnd, 100)));
    }
    ASSERT_OK(Flush());

    Close();
    Reopen(options);

    uint64_t total_useful_bytes = 0;
    std::set<int> read_keys;
    std::string value;
    // Iter1: Read half the DB, Read even keys
    // Key(0), Key(2), Key(4), Key(6), Key(8), ...
    for (int i = 0; i < kNumEntries; i += 2) {
      std::string key = Key(i);
      ASSERT_OK(db_->Get(ReadOptions(), key, &value));

      if (read_keys.find(i) == read_keys.end()) {
        auto internal_key = InternalKey(key, 0, ValueType::kTypeValue);
        total_useful_bytes +=
            GetEncodedEntrySize(internal_key.size(), value.size());
        read_keys.insert(i);
      }
    }

    size_t total_useful_bytes_iter1 =
        options.statistics->getTickerCount(READ_AMP_ESTIMATE_USEFUL_BYTES);
    size_t total_loaded_bytes_iter1 =
        options.statistics->getTickerCount(READ_AMP_TOTAL_READ_BYTES);

    Close();
    std::shared_ptr<Statistics> new_statistics = rocksdb::CreateDBStatistics();
    // Destroy old statistics obj that the blocks in lru_cache are pointing to
    options.statistics.reset();
    // Use the statistics object that we just created
    options.statistics = new_statistics;
    Reopen(options);

    // Iter2: Read half the DB, Read odd keys
    // Key(1), Key(3), Key(5), Key(7), Key(9), ...
    for (int i = 1; i < kNumEntries; i += 2) {
      std::string key = Key(i);
      ASSERT_OK(db_->Get(ReadOptions(), key, &value));

      if (read_keys.find(i) == read_keys.end()) {
        auto internal_key = InternalKey(key, 0, ValueType::kTypeValue);
        total_useful_bytes +=
            GetEncodedEntrySize(internal_key.size(), value.size());
        read_keys.insert(i);
      }
    }

    size_t total_useful_bytes_iter2 =
        options.statistics->getTickerCount(READ_AMP_ESTIMATE_USEFUL_BYTES);
    size_t total_loaded_bytes_iter2 =
        options.statistics->getTickerCount(READ_AMP_TOTAL_READ_BYTES);


    // Read amp is on average 100% since we read all what we loaded in memory
    if (k == 0) {
      ASSERT_EQ(total_useful_bytes_iter1 + total_useful_bytes_iter2,
                total_loaded_bytes_iter1 + total_loaded_bytes_iter2);
    } else {
      ASSERT_NEAR((total_useful_bytes_iter1 + total_useful_bytes_iter2) * 1.0f /
                      (total_loaded_bytes_iter1 + total_loaded_bytes_iter2),
                  1, .01);
    }
  }
}
#endif // !OS_SOLARIS

#ifndef ROCKSDB_LITE
TEST_F(DBTest2, AutomaticCompactionOverlapManualCompaction) {
  Options options = CurrentOptions();
  options.num_levels = 3;
  options.IncreaseParallelism(20);
  DestroyAndReopen(options);

  ASSERT_OK(Put(Key(0), "a"));
  ASSERT_OK(Put(Key(5), "a"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put(Key(10), "a"));
  ASSERT_OK(Put(Key(15), "a"));
  ASSERT_OK(Flush());

  CompactRangeOptions cro;
  cro.change_level = true;
  cro.target_level = 2;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  auto get_stat = [](std::string level_str, LevelStatType type,
                     std::map<std::string, std::string> props) {
    auto prop_str =
        "compaction." + level_str + "." +
        InternalStats::compaction_level_stats.at(type).property_name.c_str();
    auto prop_item = props.find(prop_str);
    return prop_item == props.end() ? 0 : std::stod(prop_item->second);
  };

  // Trivial move 2 files to L2
  ASSERT_EQ("0,0,2", FilesPerLevel());
  // Also test that the stats GetMapProperty API reporting the same result
  {
    std::map<std::string, std::string> prop;
    ASSERT_TRUE(dbfull()->GetMapProperty("rocksdb.cfstats", &prop));
    ASSERT_EQ(0, get_stat("L0", LevelStatType::NUM_FILES, prop));
    ASSERT_EQ(0, get_stat("L1", LevelStatType::NUM_FILES, prop));
    ASSERT_EQ(2, get_stat("L2", LevelStatType::NUM_FILES, prop));
    ASSERT_EQ(2, get_stat("Sum", LevelStatType::NUM_FILES, prop));
  }

  // While the compaction is running, we will create 2 new files that
  // can fit in L2, these 2 files will be moved to L2 and overlap with
  // the running compaction and break the LSM consistency.
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run():Start", [&](void* /*arg*/) {
        ASSERT_OK(
            dbfull()->SetOptions({{"level0_file_num_compaction_trigger", "2"},
                                  {"max_bytes_for_level_base", "1"}}));
        ASSERT_OK(Put(Key(6), "a"));
        ASSERT_OK(Put(Key(7), "a"));
        ASSERT_OK(Flush());

        ASSERT_OK(Put(Key(8), "a"));
        ASSERT_OK(Put(Key(9), "a"));
        ASSERT_OK(Flush());
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  // Run a manual compaction that will compact the 2 files in L2
  // into 1 file in L2
  cro.exclusive_manual_compaction = false;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();

  // Test that the stats GetMapProperty API reporting 1 file in L2
  {
    std::map<std::string, std::string> prop;
    ASSERT_TRUE(dbfull()->GetMapProperty("rocksdb.cfstats", &prop));
    ASSERT_EQ(1, get_stat("L2", LevelStatType::NUM_FILES, prop));
  }
}

TEST_F(DBTest2, ManualCompactionOverlapManualCompaction) {
  Options options = CurrentOptions();
  options.num_levels = 2;
  options.IncreaseParallelism(20);
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put(Key(0), "a"));
  ASSERT_OK(Put(Key(5), "a"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put(Key(10), "a"));
  ASSERT_OK(Put(Key(15), "a"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Trivial move 2 files to L1
  ASSERT_EQ("0,2", FilesPerLevel());

  std::function<void()> bg_manual_compact = [&]() {
    std::string k1 = Key(6);
    std::string k2 = Key(9);
    Slice k1s(k1);
    Slice k2s(k2);
    CompactRangeOptions cro;
    cro.exclusive_manual_compaction = false;
    ASSERT_OK(db_->CompactRange(cro, &k1s, &k2s));
  };
  rocksdb::port::Thread bg_thread;

  // While the compaction is running, we will create 2 new files that
  // can fit in L1, these 2 files will be moved to L1 and overlap with
  // the running compaction and break the LSM consistency.
  std::atomic<bool> flag(false);
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run():Start", [&](void* /*arg*/) {
        if (flag.exchange(true)) {
          // We want to make sure to call this callback only once
          return;
        }
        ASSERT_OK(Put(Key(6), "a"));
        ASSERT_OK(Put(Key(7), "a"));
        ASSERT_OK(Flush());

        ASSERT_OK(Put(Key(8), "a"));
        ASSERT_OK(Put(Key(9), "a"));
        ASSERT_OK(Flush());

        // Start a non-exclusive manual compaction in a bg thread
        bg_thread = port::Thread(bg_manual_compact);
        // This manual compaction conflict with the other manual compaction
        // so it should wait until the first compaction finish
        env_->SleepForMicroseconds(1000000);
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  // Run a manual compaction that will compact the 2 files in L1
  // into 1 file in L1
  CompactRangeOptions cro;
  cro.exclusive_manual_compaction = false;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  bg_thread.join();

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest2, OptimizeForPointLookup) {
  Options options = CurrentOptions();
  Close();
  options.OptimizeForPointLookup(2);
  ASSERT_OK(DB::Open(options, dbname_, &db_));

  ASSERT_OK(Put("foo", "v1"));
  ASSERT_EQ("v1", Get("foo"));
  Flush();
  ASSERT_EQ("v1", Get("foo"));
}

#endif  // ROCKSDB_LITE

TEST_F(DBTest2, GetRaceFlush1) {
  ASSERT_OK(Put("foo", "v1"));

  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::GetImpl:1", "DBTest2::GetRaceFlush:1"},
       {"DBTest2::GetRaceFlush:2", "DBImpl::GetImpl:2"}});

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  rocksdb::port::Thread t1([&] {
    TEST_SYNC_POINT("DBTest2::GetRaceFlush:1");
    ASSERT_OK(Put("foo", "v2"));
    Flush();
    TEST_SYNC_POINT("DBTest2::GetRaceFlush:2");
  });

  // Get() is issued after the first Put(), so it should see either
  // "v1" or "v2".
  ASSERT_NE("NOT_FOUND", Get("foo"));
  t1.join();
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest2, GetRaceFlush2) {
  ASSERT_OK(Put("foo", "v1"));

  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::GetImpl:3", "DBTest2::GetRaceFlush:1"},
       {"DBTest2::GetRaceFlush:2", "DBImpl::GetImpl:4"}});

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  port::Thread t1([&] {
    TEST_SYNC_POINT("DBTest2::GetRaceFlush:1");
    ASSERT_OK(Put("foo", "v2"));
    Flush();
    TEST_SYNC_POINT("DBTest2::GetRaceFlush:2");
  });

  // Get() is issued after the first Put(), so it should see either
  // "v1" or "v2".
  ASSERT_NE("NOT_FOUND", Get("foo"));
  t1.join();
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest2, DirectIO) {
  if (!IsDirectIOSupported()) {
    return;
  }
  Options options = CurrentOptions();
  options.use_direct_reads = options.use_direct_io_for_flush_and_compaction =
      true;
  options.allow_mmap_reads = options.allow_mmap_writes = false;
  DestroyAndReopen(options);

  ASSERT_OK(Put(Key(0), "a"));
  ASSERT_OK(Put(Key(5), "a"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put(Key(10), "a"));
  ASSERT_OK(Put(Key(15), "a"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  Reopen(options);
}

TEST_F(DBTest2, MemtableOnlyIterator) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "foo", "first"));
  ASSERT_OK(Put(1, "bar", "second"));

  ReadOptions ropt;
  ropt.read_tier = kMemtableTier;
  std::string value;
  Iterator* it = nullptr;

  // Before flushing
  // point lookups
  ASSERT_OK(db_->Get(ropt, handles_[1], "foo", &value));
  ASSERT_EQ("first", value);
  ASSERT_OK(db_->Get(ropt, handles_[1], "bar", &value));
  ASSERT_EQ("second", value);

  // Memtable-only iterator (read_tier=kMemtableTier); data not flushed yet.
  it = db_->NewIterator(ropt, handles_[1]);
  int count = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    ASSERT_TRUE(it->Valid());
    count++;
  }
  ASSERT_TRUE(!it->Valid());
  ASSERT_EQ(2, count);
  delete it;

  Flush(1);

  // After flushing
  // point lookups
  ASSERT_OK(db_->Get(ropt, handles_[1], "foo", &value));
  ASSERT_EQ("first", value);
  ASSERT_OK(db_->Get(ropt, handles_[1], "bar", &value));
  ASSERT_EQ("second", value);
  // nothing should be returned using memtable-only iterator after flushing.
  it = db_->NewIterator(ropt, handles_[1]);
  count = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    ASSERT_TRUE(it->Valid());
    count++;
  }
  ASSERT_TRUE(!it->Valid());
  ASSERT_EQ(0, count);
  delete it;

  // Add a key to memtable
  ASSERT_OK(Put(1, "foobar", "third"));
  it = db_->NewIterator(ropt, handles_[1]);
  count = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ("foobar", it->key().ToString());
    ASSERT_EQ("third", it->value().ToString());
    count++;
  }
  ASSERT_TRUE(!it->Valid());
  ASSERT_EQ(1, count);
  delete it;
}

TEST_F(DBTest2, LowPriWrite) {
  Options options = CurrentOptions();
  // Compaction pressure should trigger since 6 files
  options.level0_file_num_compaction_trigger = 4;
  options.level0_slowdown_writes_trigger = 12;
  options.level0_stop_writes_trigger = 30;
  options.delayed_write_rate = 8 * 1024 * 1024;
  Reopen(options);

  std::atomic<int> rate_limit_count(0);

  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "GenericRateLimiter::Request:1", [&](void* arg) {
        rate_limit_count.fetch_add(1);
        int64_t* rate_bytes_per_sec = static_cast<int64_t*>(arg);
        ASSERT_EQ(1024 * 1024, *rate_bytes_per_sec);
      });
  // Block compaction
  rocksdb::SyncPoint::GetInstance()->LoadDependency({
      {"DBTest.LowPriWrite:0", "DBImpl::BGWorkCompaction"},
  });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  WriteOptions wo;
  for (int i = 0; i < 6; i++) {
    wo.low_pri = false;
    Put("", "", wo);
    wo.low_pri = true;
    Put("", "", wo);
    Flush();
  }
  ASSERT_EQ(0, rate_limit_count.load());
  wo.low_pri = true;
  Put("", "", wo);
  ASSERT_EQ(1, rate_limit_count.load());
  wo.low_pri = false;
  Put("", "", wo);
  ASSERT_EQ(1, rate_limit_count.load());

  TEST_SYNC_POINT("DBTest.LowPriWrite:0");
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();

  dbfull()->TEST_WaitForCompact();
  wo.low_pri = true;
  Put("", "", wo);
  ASSERT_EQ(1, rate_limit_count.load());
  wo.low_pri = false;
  Put("", "", wo);
  ASSERT_EQ(1, rate_limit_count.load());
}

#ifndef ROCKSDB_LITE
TEST_F(DBTest2, RateLimitedCompactionReads) {
  // compaction input has 512KB data
  const int kNumKeysPerFile = 128;
  const int kBytesPerKey = 1024;
  const int kNumL0Files = 4;

  for (auto use_direct_io : {false, true}) {
    if (use_direct_io && !IsDirectIOSupported()) {
      continue;
    }
    Options options = CurrentOptions();
    options.compression = kNoCompression;
    options.level0_file_num_compaction_trigger = kNumL0Files;
    options.memtable_factory.reset(new SpecialSkipListFactory(kNumKeysPerFile));
    options.new_table_reader_for_compaction_inputs = true;
    // takes roughly one second, split into 100 x 10ms intervals. Each interval
    // permits 5.12KB, which is smaller than the block size, so this test
    // exercises the code for chunking reads.
    options.rate_limiter.reset(NewGenericRateLimiter(
        static_cast<int64_t>(kNumL0Files * kNumKeysPerFile *
                             kBytesPerKey) /* rate_bytes_per_sec */,
        10 * 1000 /* refill_period_us */, 10 /* fairness */,
        RateLimiter::Mode::kReadsOnly));
    options.use_direct_reads = options.use_direct_io_for_flush_and_compaction =
        use_direct_io;
    BlockBasedTableOptions bbto;
    bbto.block_size = 16384;
    bbto.no_block_cache = true;
    options.table_factory.reset(new BlockBasedTableFactory(bbto));
    DestroyAndReopen(options);

    for (int i = 0; i < kNumL0Files; ++i) {
      for (int j = 0; j <= kNumKeysPerFile; ++j) {
        ASSERT_OK(Put(Key(j), DummyString(kBytesPerKey)));
      }
      dbfull()->TEST_WaitForFlushMemTable();
      ASSERT_EQ(i + 1, NumTableFilesAtLevel(0));
    }
    dbfull()->TEST_WaitForCompact();
    ASSERT_EQ(0, NumTableFilesAtLevel(0));

    ASSERT_EQ(0, options.rate_limiter->GetTotalBytesThrough(Env::IO_HIGH));
    // should be slightly above 512KB due to non-data blocks read. Arbitrarily
    // chose 1MB as the upper bound on the total bytes read.
    size_t rate_limited_bytes =
        options.rate_limiter->GetTotalBytesThrough(Env::IO_LOW);
    // Include the explicit prefetch of the footer in direct I/O case.
    size_t direct_io_extra = use_direct_io ? 512 * 1024 : 0;
    ASSERT_GE(
        rate_limited_bytes,
        static_cast<size_t>(kNumKeysPerFile * kBytesPerKey * kNumL0Files));
    ASSERT_LT(
        rate_limited_bytes,
        static_cast<size_t>(2 * kNumKeysPerFile * kBytesPerKey * kNumL0Files +
                            direct_io_extra));

    Iterator* iter = db_->NewIterator(ReadOptions());
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_EQ(iter->value().ToString(), DummyString(kBytesPerKey));
    }
    delete iter;
    // bytes read for user iterator shouldn't count against the rate limit.
    ASSERT_EQ(rate_limited_bytes,
              static_cast<size_t>(
                  options.rate_limiter->GetTotalBytesThrough(Env::IO_LOW)));
  }
}
#endif  // ROCKSDB_LITE

// Make sure DB can be reopen with reduced number of levels, given no file
// is on levels higher than the new num_levels.
TEST_F(DBTest2, ReduceLevel) {
  Options options;
  options.disable_auto_compactions = true;
  options.num_levels = 7;
  Reopen(options);
  Put("foo", "bar");
  Flush();
  MoveFilesToLevel(6);
#ifndef ROCKSDB_LITE
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());
#endif  // !ROCKSDB_LITE
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 1;
  dbfull()->CompactRange(compact_options, nullptr, nullptr);
#ifndef ROCKSDB_LITE
  ASSERT_EQ("0,1", FilesPerLevel());
#endif  // !ROCKSDB_LITE
  options.num_levels = 3;
  Reopen(options);
#ifndef ROCKSDB_LITE
  ASSERT_EQ("0,1", FilesPerLevel());
#endif  // !ROCKSDB_LITE
}

// Test that ReadCallback is actually used in both memtbale and sst tables
TEST_F(DBTest2, ReadCallbackTest) {
  Options options;
  options.disable_auto_compactions = true;
  options.num_levels = 7;
  Reopen(options);
  std::vector<const Snapshot*> snapshots;
  // Try to create a db with multiple layers and a memtable
  const std::string key = "foo";
  const std::string value = "bar";
  // This test assumes that the seq start with 1 and increased by 1 after each
  // write batch of size 1. If that behavior changes, the test needs to be
  // updated as well.
  // TODO(myabandeh): update this test to use the seq number that is returned by
  // the DB instead of assuming what seq the DB used.
  int i = 1;
  for (; i < 10; i++) {
    Put(key, value + std::to_string(i));
    // Take a snapshot to avoid the value being removed during compaction
    auto snapshot = dbfull()->GetSnapshot();
    snapshots.push_back(snapshot);
  }
  Flush();
  for (; i < 20; i++) {
    Put(key, value + std::to_string(i));
    // Take a snapshot to avoid the value being removed during compaction
    auto snapshot = dbfull()->GetSnapshot();
    snapshots.push_back(snapshot);
  }
  Flush();
  MoveFilesToLevel(6);
#ifndef ROCKSDB_LITE
  ASSERT_EQ("0,0,0,0,0,0,2", FilesPerLevel());
#endif  // !ROCKSDB_LITE
  for (; i < 30; i++) {
    Put(key, value + std::to_string(i));
    auto snapshot = dbfull()->GetSnapshot();
    snapshots.push_back(snapshot);
  }
  Flush();
#ifndef ROCKSDB_LITE
  ASSERT_EQ("1,0,0,0,0,0,2", FilesPerLevel());
#endif  // !ROCKSDB_LITE
  // And also add some values to the memtable
  for (; i < 40; i++) {
    Put(key, value + std::to_string(i));
    auto snapshot = dbfull()->GetSnapshot();
    snapshots.push_back(snapshot);
  }

  class TestReadCallback : public ReadCallback {
   public:
    explicit TestReadCallback(SequenceNumber snapshot) : snapshot_(snapshot) {}
    bool IsVisible(SequenceNumber seq) override { return seq <= snapshot_; }

   private:
    SequenceNumber snapshot_;
  };

  for (int seq = 1; seq < i; seq++) {
    PinnableSlice pinnable_val;
    ReadOptions roptions;
    TestReadCallback callback(seq);
    bool dont_care = true;
    Status s = dbfull()->GetImpl(roptions, dbfull()->DefaultColumnFamily(), key,
                                 &pinnable_val, &dont_care, &callback);
    ASSERT_TRUE(s.ok());
    // Assuming that after each Put the DB increased seq by one, the value and
    // seq number must be equal since we also inc value by 1 after each Put.
    ASSERT_EQ(value + std::to_string(seq), pinnable_val.ToString());
  }

  for (auto snapshot : snapshots) {
    dbfull()->ReleaseSnapshot(snapshot);
  }
}

#ifndef ROCKSDB_LITE

TEST_F(DBTest2, LiveFilesOmitObsoleteFiles) {
  // Regression test for race condition where an obsolete file is returned to
  // user as a "live file" but then deleted, all while file deletions are
  // disabled.
  //
  // It happened like this:
  //
  // 1. [flush thread] Log file "x.log" found by FindObsoleteFiles
  // 2. [user thread] DisableFileDeletions, GetSortedWalFiles are called and the
  //    latter returned "x.log"
  // 3. [flush thread] PurgeObsoleteFiles deleted "x.log"
  // 4. [user thread] Reading "x.log" failed
  //
  // Unfortunately the only regression test I can come up with involves sleep.
  // We cannot set SyncPoints to repro since, once the fix is applied, the
  // SyncPoints would cause a deadlock as the repro's sequence of events is now
  // prohibited.
  //
  // Instead, if we sleep for a second between Find and Purge, and ensure the
  // read attempt happens after purge, then the sequence of events will almost
  // certainly happen on the old code.
  rocksdb::SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::BackgroundCallFlush:FilesFound",
       "DBTest2::LiveFilesOmitObsoleteFiles:FlushTriggered"},
      {"DBImpl::PurgeObsoleteFiles:End",
       "DBTest2::LiveFilesOmitObsoleteFiles:LiveFilesCaptured"},
  });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PurgeObsoleteFiles:Begin",
      [&](void* /*arg*/) { env_->SleepForMicroseconds(1000000); });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Put("key", "val");
  FlushOptions flush_opts;
  flush_opts.wait = false;
  db_->Flush(flush_opts);
  TEST_SYNC_POINT("DBTest2::LiveFilesOmitObsoleteFiles:FlushTriggered");

  db_->DisableFileDeletions();
  VectorLogPtr log_files;
  db_->GetSortedWalFiles(log_files);
  TEST_SYNC_POINT("DBTest2::LiveFilesOmitObsoleteFiles:LiveFilesCaptured");
  for (const auto& log_file : log_files) {
    ASSERT_OK(env_->FileExists(LogFileName(dbname_, log_file->LogNumber())));
  }

  db_->EnableFileDeletions();
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest2, TestNumPread) {
  Options options = CurrentOptions();
  // disable block cache
  BlockBasedTableOptions table_options;
  table_options.no_block_cache = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);
  env_->count_random_reads_ = true;

  env_->random_file_open_counter_.store(0);
  ASSERT_OK(Put("bar", "foo"));
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Flush());
  // After flush, we'll open the file and read footer, meta block,
  // property block and index block.
  ASSERT_EQ(4, env_->random_read_counter_.Read());
  ASSERT_EQ(1, env_->random_file_open_counter_.load());

  // One pread per a normal data block read
  env_->random_file_open_counter_.store(0);
  env_->random_read_counter_.Reset();
  ASSERT_EQ("bar", Get("foo"));
  ASSERT_EQ(1, env_->random_read_counter_.Read());
  // All files are already opened.
  ASSERT_EQ(0, env_->random_file_open_counter_.load());

  env_->random_file_open_counter_.store(0);
  env_->random_read_counter_.Reset();
  ASSERT_OK(Put("bar2", "foo2"));
  ASSERT_OK(Put("foo2", "bar2"));
  ASSERT_OK(Flush());
  // After flush, we'll open the file and read footer, meta block,
  // property block and index block.
  ASSERT_EQ(4, env_->random_read_counter_.Read());
  ASSERT_EQ(1, env_->random_file_open_counter_.load());

  // Compaction needs two input blocks, which requires 2 preads, and
  // generate a new SST file which needs 4 preads (footer, meta block,
  // property block and index block). In total 6.
  env_->random_file_open_counter_.store(0);
  env_->random_read_counter_.Reset();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(6, env_->random_read_counter_.Read());
  // All compactin input files should have already been opened.
  ASSERT_EQ(1, env_->random_file_open_counter_.load());

  // One pread per a normal data block read
  env_->random_file_open_counter_.store(0);
  env_->random_read_counter_.Reset();
  ASSERT_EQ("foo2", Get("bar2"));
  ASSERT_EQ(1, env_->random_read_counter_.Read());
  // SST files are already opened.
  ASSERT_EQ(0, env_->random_file_open_counter_.load());
}

TEST_F(DBTest2, TraceAndReplay) {
  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreatePutOperator();
  ReadOptions ro;
  WriteOptions wo;
  TraceOptions trace_opts;
  EnvOptions env_opts;
  CreateAndReopenWithCF({"pikachu"}, options);
  Random rnd(301);
  Iterator* single_iter = nullptr;

  std::string trace_filename = dbname_ + "/rocksdb.trace";
  std::unique_ptr<TraceWriter> trace_writer;
  ASSERT_OK(NewFileTraceWriter(env_, env_opts, trace_filename, &trace_writer));
  ASSERT_OK(db_->StartTrace(trace_opts, std::move(trace_writer)));

  ASSERT_OK(Put(0, "a", "1"));
  ASSERT_OK(Merge(0, "b", "2"));
  ASSERT_OK(Delete(0, "c"));
  ASSERT_OK(SingleDelete(0, "d"));
  ASSERT_OK(db_->DeleteRange(wo, dbfull()->DefaultColumnFamily(), "e", "f"));

  WriteBatch batch;
  ASSERT_OK(batch.Put("f", "11"));
  ASSERT_OK(batch.Merge("g", "12"));
  ASSERT_OK(batch.Delete("h"));
  ASSERT_OK(batch.SingleDelete("i"));
  ASSERT_OK(batch.DeleteRange("j", "k"));
  ASSERT_OK(db_->Write(wo, &batch));

  single_iter = db_->NewIterator(ro);
  single_iter->Seek("f");
  single_iter->SeekForPrev("g");
  delete single_iter;

  ASSERT_EQ("1", Get(0, "a"));
  ASSERT_EQ("12", Get(0, "g"));

  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(1, "rocksdb", "rocks"));
  ASSERT_EQ("NOT_FOUND", Get(1, "leveldb"));

  ASSERT_OK(db_->EndTrace());
  // These should not get into the trace file as it is after EndTrace.
  Put("hello", "world");
  Merge("foo", "bar");

  // Open another db, replay, and verify the data
  std::string value;
  std::string dbname2 = test::TmpDir(env_) + "/db_replay";
  ASSERT_OK(DestroyDB(dbname2, options));

  // Using a different name than db2, to pacify infer's use-after-lifetime
  // warnings (http://fbinfer.com).
  DB* db2_init = nullptr;
  options.create_if_missing = true;
  ASSERT_OK(DB::Open(options, dbname2, &db2_init));
  ColumnFamilyHandle* cf;
  ASSERT_OK(
      db2_init->CreateColumnFamily(ColumnFamilyOptions(), "pikachu", &cf));
  delete cf;
  delete db2_init;

  DB* db2 = nullptr;
  std::vector<ColumnFamilyDescriptor> column_families;
  ColumnFamilyOptions cf_options;
  cf_options.merge_operator = MergeOperators::CreatePutOperator();
  column_families.push_back(ColumnFamilyDescriptor("default", cf_options));
  column_families.push_back(
      ColumnFamilyDescriptor("pikachu", ColumnFamilyOptions()));
  std::vector<ColumnFamilyHandle*> handles;
  ASSERT_OK(DB::Open(DBOptions(), dbname2, column_families, &handles, &db2));

  env_->SleepForMicroseconds(100);
  // Verify that the keys don't already exist
  ASSERT_TRUE(db2->Get(ro, handles[0], "a", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "g", &value).IsNotFound());

  std::unique_ptr<TraceReader> trace_reader;
  ASSERT_OK(NewFileTraceReader(env_, env_opts, trace_filename, &trace_reader));
  Replayer replayer(db2, handles_, std::move(trace_reader));
  ASSERT_OK(replayer.Replay());

  ASSERT_OK(db2->Get(ro, handles[0], "a", &value));
  ASSERT_EQ("1", value);
  ASSERT_OK(db2->Get(ro, handles[0], "g", &value));
  ASSERT_EQ("12", value);
  ASSERT_TRUE(db2->Get(ro, handles[0], "hello", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "world", &value).IsNotFound());

  ASSERT_OK(db2->Get(ro, handles[1], "foo", &value));
  ASSERT_EQ("bar", value);
  ASSERT_OK(db2->Get(ro, handles[1], "rocksdb", &value));
  ASSERT_EQ("rocks", value);

  for (auto handle : handles) {
    delete handle;
  }
  delete db2;
  ASSERT_OK(DestroyDB(dbname2, options));
}

TEST_F(DBTest2, TraceWithLimit) {
  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreatePutOperator();
  ReadOptions ro;
  WriteOptions wo;
  TraceOptions trace_opts;
  EnvOptions env_opts;
  CreateAndReopenWithCF({"pikachu"}, options);
  Random rnd(301);

  // test the max trace file size options
  trace_opts.max_trace_file_size = 5;
  std::string trace_filename = dbname_ + "/rocksdb.trace1";
  std::unique_ptr<TraceWriter> trace_writer;
  ASSERT_OK(NewFileTraceWriter(env_, env_opts, trace_filename, &trace_writer));
  ASSERT_OK(db_->StartTrace(trace_opts, std::move(trace_writer)));
  ASSERT_OK(Put(0, "a", "1"));
  ASSERT_OK(Put(0, "b", "1"));
  ASSERT_OK(Put(0, "c", "1"));
  ASSERT_OK(db_->EndTrace());

  std::string dbname2 = test::TmpDir(env_) + "/db_replay2";
  std::string value;
  ASSERT_OK(DestroyDB(dbname2, options));

  // Using a different name than db2, to pacify infer's use-after-lifetime
  // warnings (http://fbinfer.com).
  DB* db2_init = nullptr;
  options.create_if_missing = true;
  ASSERT_OK(DB::Open(options, dbname2, &db2_init));
  ColumnFamilyHandle* cf;
  ASSERT_OK(
      db2_init->CreateColumnFamily(ColumnFamilyOptions(), "pikachu", &cf));
  delete cf;
  delete db2_init;

  DB* db2 = nullptr;
  std::vector<ColumnFamilyDescriptor> column_families;
  ColumnFamilyOptions cf_options;
  cf_options.merge_operator = MergeOperators::CreatePutOperator();
  column_families.push_back(ColumnFamilyDescriptor("default", cf_options));
  column_families.push_back(
      ColumnFamilyDescriptor("pikachu", ColumnFamilyOptions()));
  std::vector<ColumnFamilyHandle*> handles;
  ASSERT_OK(DB::Open(DBOptions(), dbname2, column_families, &handles, &db2));

  env_->SleepForMicroseconds(100);
  // Verify that the keys don't already exist
  ASSERT_TRUE(db2->Get(ro, handles[0], "a", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "b", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "c", &value).IsNotFound());

  std::unique_ptr<TraceReader> trace_reader;
  ASSERT_OK(NewFileTraceReader(env_, env_opts, trace_filename, &trace_reader));
  Replayer replayer(db2, handles_, std::move(trace_reader));
  ASSERT_OK(replayer.Replay());

  ASSERT_TRUE(db2->Get(ro, handles[0], "a", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "b", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "c", &value).IsNotFound());

  for (auto handle : handles) {
    delete handle;
  }
  delete db2;
  ASSERT_OK(DestroyDB(dbname2, options));
}

TEST_F(DBTest2, TraceWithSampling) {
  Options options = CurrentOptions();
  ReadOptions ro;
  WriteOptions wo;
  TraceOptions trace_opts;
  EnvOptions env_opts;
  CreateAndReopenWithCF({"pikachu"}, options);
  Random rnd(301);

  // test the trace file sampling options
  trace_opts.sampling_frequency = 2;
  std::string trace_filename = dbname_ + "/rocksdb.trace_sampling";
  std::unique_ptr<TraceWriter> trace_writer;
  ASSERT_OK(NewFileTraceWriter(env_, env_opts, trace_filename, &trace_writer));
  ASSERT_OK(db_->StartTrace(trace_opts, std::move(trace_writer)));
  ASSERT_OK(Put(0, "a", "1"));
  ASSERT_OK(Put(0, "b", "2"));
  ASSERT_OK(Put(0, "c", "3"));
  ASSERT_OK(Put(0, "d", "4"));
  ASSERT_OK(Put(0, "e", "5"));
  ASSERT_OK(db_->EndTrace());

  std::string dbname2 = test::TmpDir(env_) + "/db_replay_sampling";
  std::string value;
  ASSERT_OK(DestroyDB(dbname2, options));

  // Using a different name than db2, to pacify infer's use-after-lifetime
  // warnings (http://fbinfer.com).
  DB* db2_init = nullptr;
  options.create_if_missing = true;
  ASSERT_OK(DB::Open(options, dbname2, &db2_init));
  ColumnFamilyHandle* cf;
  ASSERT_OK(
      db2_init->CreateColumnFamily(ColumnFamilyOptions(), "pikachu", &cf));
  delete cf;
  delete db2_init;

  DB* db2 = nullptr;
  std::vector<ColumnFamilyDescriptor> column_families;
  ColumnFamilyOptions cf_options;
  column_families.push_back(ColumnFamilyDescriptor("default", cf_options));
  column_families.push_back(
      ColumnFamilyDescriptor("pikachu", ColumnFamilyOptions()));
  std::vector<ColumnFamilyHandle*> handles;
  ASSERT_OK(DB::Open(DBOptions(), dbname2, column_families, &handles, &db2));

  env_->SleepForMicroseconds(100);
  ASSERT_TRUE(db2->Get(ro, handles[0], "a", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "b", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "c", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "d", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "e", &value).IsNotFound());

  std::unique_ptr<TraceReader> trace_reader;
  ASSERT_OK(NewFileTraceReader(env_, env_opts, trace_filename, &trace_reader));
  Replayer replayer(db2, handles_, std::move(trace_reader));
  ASSERT_OK(replayer.Replay());

  ASSERT_TRUE(db2->Get(ro, handles[0], "a", &value).IsNotFound());
  ASSERT_FALSE(db2->Get(ro, handles[0], "b", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "c", &value).IsNotFound());
  ASSERT_FALSE(db2->Get(ro, handles[0], "d", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "e", &value).IsNotFound());

  for (auto handle : handles) {
    delete handle;
  }
  delete db2;
  ASSERT_OK(DestroyDB(dbname2, options));
}

#endif  // ROCKSDB_LITE

TEST_F(DBTest2, PinnableSliceAndMmapReads) {
  Options options = CurrentOptions();
  options.allow_mmap_reads = true;
  options.max_open_files = 100;
  options.compression = kNoCompression;
  Reopen(options);

  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Flush());

  PinnableSlice pinned_value;
  ASSERT_EQ(Get("foo", &pinned_value), Status::OK());
  // It is not safe to pin mmap files as they might disappear by compaction
  ASSERT_FALSE(pinned_value.IsPinned());
  ASSERT_EQ(pinned_value.ToString(), "bar");

  dbfull()->TEST_CompactRange(0 /* level */, nullptr /* begin */,
                              nullptr /* end */, nullptr /* column_family */,
                              true /* disallow_trivial_move */);

  // Ensure pinned_value doesn't rely on memory munmap'd by the above
  // compaction. It crashes if it does.
  ASSERT_EQ(pinned_value.ToString(), "bar");

#ifndef ROCKSDB_LITE
  pinned_value.Reset();
  // Unsafe to pin mmap files when they could be kicked out of table cache
  Close();
  ASSERT_OK(ReadOnlyReopen(options));
  ASSERT_EQ(Get("foo", &pinned_value), Status::OK());
  ASSERT_FALSE(pinned_value.IsPinned());
  ASSERT_EQ(pinned_value.ToString(), "bar");

  pinned_value.Reset();
  // In read-only mode with infinite capacity on table cache it should pin the
  // value and avoid the memcpy
  Close();
  options.max_open_files = -1;
  ASSERT_OK(ReadOnlyReopen(options));
  ASSERT_EQ(Get("foo", &pinned_value), Status::OK());
  ASSERT_TRUE(pinned_value.IsPinned());
  ASSERT_EQ(pinned_value.ToString(), "bar");
#endif
}

TEST_F(DBTest2, DISABLED_IteratorPinnedMemory) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  BlockBasedTableOptions bbto;
  bbto.no_block_cache = false;
  bbto.cache_index_and_filter_blocks = false;
  bbto.block_cache = NewLRUCache(100000);
  bbto.block_size = 400;  // small block size
  options.table_factory.reset(new BlockBasedTableFactory(bbto));
  Reopen(options);

  Random rnd(301);
  std::string v = RandomString(&rnd, 400);

  // Since v is the size of a block, each key should take a block
  // of 400+ bytes.
  Put("1", v);
  Put("3", v);
  Put("5", v);
  Put("7", v);
  ASSERT_OK(Flush());

  ASSERT_EQ(0, bbto.block_cache->GetPinnedUsage());

  // Verify that iterators don't pin more than one data block in block cache
  // at each time.
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    iter->SeekToFirst();

    for (int i = 0; i < 4; i++) {
      ASSERT_TRUE(iter->Valid());
      // Block cache should contain exactly one block.
      ASSERT_GT(bbto.block_cache->GetPinnedUsage(), 0);
      ASSERT_LT(bbto.block_cache->GetPinnedUsage(), 800);
      iter->Next();
    }
    ASSERT_FALSE(iter->Valid());

    iter->Seek("4");
    ASSERT_TRUE(iter->Valid());

    ASSERT_GT(bbto.block_cache->GetPinnedUsage(), 0);
    ASSERT_LT(bbto.block_cache->GetPinnedUsage(), 800);

    iter->Seek("3");
    ASSERT_TRUE(iter->Valid());

    ASSERT_GT(bbto.block_cache->GetPinnedUsage(), 0);
    ASSERT_LT(bbto.block_cache->GetPinnedUsage(), 800);
  }
  ASSERT_EQ(0, bbto.block_cache->GetPinnedUsage());

  // Test compaction case
  Put("2", v);
  Put("5", v);
  Put("6", v);
  Put("8", v);
  ASSERT_OK(Flush());

  // Clear existing data in block cache
  bbto.block_cache->SetCapacity(0);
  bbto.block_cache->SetCapacity(100000);

  // Verify compaction input iterators don't hold more than one data blocks at
  // one time.
  std::atomic<bool> finished(false);
  std::atomic<int> block_newed(0);
  std::atomic<int> block_destroyed(0);
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "Block::Block:0", [&](void* /*arg*/) {
        if (finished) {
          return;
        }
        // Two iterators. At most 2 outstanding blocks.
        EXPECT_GE(block_newed.load(), block_destroyed.load());
        EXPECT_LE(block_newed.load(), block_destroyed.load() + 1);
        block_newed.fetch_add(1);
      });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "Block::~Block", [&](void* /*arg*/) {
        if (finished) {
          return;
        }
        // Two iterators. At most 2 outstanding blocks.
        EXPECT_GE(block_newed.load(), block_destroyed.load() + 1);
        EXPECT_LE(block_newed.load(), block_destroyed.load() + 2);
        block_destroyed.fetch_add(1);
      });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run:BeforeVerify",
      [&](void* /*arg*/) { finished = true; });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Two input files. Each of them has 4 data blocks.
  ASSERT_EQ(8, block_newed.load());
  ASSERT_EQ(8, block_destroyed.load());

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest2, TestBBTTailPrefetch) {
  std::atomic<bool> called(false);
  size_t expected_lower_bound = 512 * 1024;
  size_t expected_higher_bound = 512 * 1024;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::Open::TailPrefetchLen", [&](void* arg) {
        size_t* prefetch_size = static_cast<size_t*>(arg);
        EXPECT_LE(expected_lower_bound, *prefetch_size);
        EXPECT_GE(expected_higher_bound, *prefetch_size);
        called = true;
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Put("1", "1");
  Put("9", "1");
  Flush();

  expected_lower_bound = 0;
  expected_higher_bound = 8 * 1024;

  Put("1", "1");
  Put("9", "1");
  Flush();

  Put("1", "1");
  Put("9", "1");
  Flush();

  // Full compaction to make sure there is no L0 file after the open.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ASSERT_TRUE(called.load());
  called = false;

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();

  std::atomic<bool> first_call(true);
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::Open::TailPrefetchLen", [&](void* arg) {
        size_t* prefetch_size = static_cast<size_t*>(arg);
        if (first_call) {
          EXPECT_EQ(4 * 1024, *prefetch_size);
          first_call = false;
        } else {
          EXPECT_GE(4 * 1024, *prefetch_size);
        }
        called = true;
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.max_file_opening_threads = 1;  // one thread
  BlockBasedTableOptions table_options;
  table_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.max_open_files = -1;
  Reopen(options);

  Put("1", "1");
  Put("9", "1");
  Flush();

  Put("1", "1");
  Put("9", "1");
  Flush();

  ASSERT_TRUE(called.load());
  called = false;

  // Parallel loading SST files
  options.max_file_opening_threads = 16;
  Reopen(options);

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ASSERT_TRUE(called.load());

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBTest2, TestGetColumnFamilyHandleUnlocked) {
  // Setup sync point dependency to reproduce the race condition of
  // DBImpl::GetColumnFamilyHandleUnlocked
  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      { {"TestGetColumnFamilyHandleUnlocked::GetColumnFamilyHandleUnlocked1",
         "TestGetColumnFamilyHandleUnlocked::PreGetColumnFamilyHandleUnlocked2"},
        {"TestGetColumnFamilyHandleUnlocked::GetColumnFamilyHandleUnlocked2",
         "TestGetColumnFamilyHandleUnlocked::ReadColumnFamilyHandle1"},
      });
  SyncPoint::GetInstance()->EnableProcessing();

  CreateColumnFamilies({"test1", "test2"}, Options());
  ASSERT_EQ(handles_.size(), 2);

  DBImpl* dbi = reinterpret_cast<DBImpl*>(db_);
  port::Thread user_thread1([&]() {
    auto cfh = dbi->GetColumnFamilyHandleUnlocked(handles_[0]->GetID());
    ASSERT_EQ(cfh->GetID(), handles_[0]->GetID());
    TEST_SYNC_POINT("TestGetColumnFamilyHandleUnlocked::GetColumnFamilyHandleUnlocked1");
    TEST_SYNC_POINT("TestGetColumnFamilyHandleUnlocked::ReadColumnFamilyHandle1");
    ASSERT_EQ(cfh->GetID(), handles_[0]->GetID());
  });

  port::Thread user_thread2([&]() {
    TEST_SYNC_POINT("TestGetColumnFamilyHandleUnlocked::PreGetColumnFamilyHandleUnlocked2");
    auto cfh = dbi->GetColumnFamilyHandleUnlocked(handles_[1]->GetID());
    ASSERT_EQ(cfh->GetID(), handles_[1]->GetID());
    TEST_SYNC_POINT("TestGetColumnFamilyHandleUnlocked::GetColumnFamilyHandleUnlocked2");
    ASSERT_EQ(cfh->GetID(), handles_[1]->GetID());
  });

  user_thread1.join();
  user_thread2.join();

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
}

#ifndef ROCKSDB_LITE
TEST_F(DBTest2, TestCompactFiles) {
  // Setup sync point dependency to reproduce the race condition of
  // DBImpl::GetColumnFamilyHandleUnlocked
  rocksdb::SyncPoint::GetInstance()->LoadDependency({
      {"TestCompactFiles::IngestExternalFile1",
       "TestCompactFiles::IngestExternalFile2"},
  });
  SyncPoint::GetInstance()->EnableProcessing();

  Options options;
  options.num_levels = 2;
  options.disable_auto_compactions = true;
  Reopen(options);
  auto* handle = db_->DefaultColumnFamily();
  ASSERT_EQ(db_->NumberLevels(handle), 2);

  rocksdb::SstFileWriter sst_file_writer{rocksdb::EnvOptions(), options};
  std::string external_file1 = dbname_ + "/test_compact_files1.sst_t";
  std::string external_file2 = dbname_ + "/test_compact_files2.sst_t";
  std::string external_file3 = dbname_ + "/test_compact_files3.sst_t";

  ASSERT_OK(sst_file_writer.Open(external_file1));
  ASSERT_OK(sst_file_writer.Put("1", "1"));
  ASSERT_OK(sst_file_writer.Put("2", "2"));
  ASSERT_OK(sst_file_writer.Finish());

  ASSERT_OK(sst_file_writer.Open(external_file2));
  ASSERT_OK(sst_file_writer.Put("3", "3"));
  ASSERT_OK(sst_file_writer.Put("4", "4"));
  ASSERT_OK(sst_file_writer.Finish());

  ASSERT_OK(sst_file_writer.Open(external_file3));
  ASSERT_OK(sst_file_writer.Put("5", "5"));
  ASSERT_OK(sst_file_writer.Put("6", "6"));
  ASSERT_OK(sst_file_writer.Finish());

  ASSERT_OK(db_->IngestExternalFile(handle, {external_file1, external_file3},
                                    IngestExternalFileOptions()));
  ASSERT_EQ(NumTableFilesAtLevel(1, 0), 2);
  std::vector<std::string> files;
  GetSstFiles(env_, dbname_, &files);
  ASSERT_EQ(files.size(), 2);

  port::Thread user_thread1(
      [&]() { db_->CompactFiles(CompactionOptions(), handle, files, 1); });

  port::Thread user_thread2([&]() {
    ASSERT_OK(db_->IngestExternalFile(handle, {external_file2},
                                      IngestExternalFileOptions()));
    TEST_SYNC_POINT("TestCompactFiles::IngestExternalFile1");
  });

  user_thread1.join();
  user_thread2.join();

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
}
#endif  // ROCKSDB_LITE

// TODO: figure out why this test fails in appveyor
#ifndef OS_WIN
TEST_F(DBTest2, MultiDBParallelOpenTest) {
  const int kNumDbs = 2;
  Options options = CurrentOptions();
  std::vector<std::string> dbnames;
  for (int i = 0; i < kNumDbs; ++i) {
    dbnames.emplace_back(test::TmpDir(env_) + "/db" + ToString(i));
    ASSERT_OK(DestroyDB(dbnames.back(), options));
  }

  // Verify empty DBs can be created in parallel
  std::vector<std::thread> open_threads;
  std::vector<DB*> dbs{static_cast<unsigned int>(kNumDbs), nullptr};
  options.create_if_missing = true;
  for (int i = 0; i < kNumDbs; ++i) {
    open_threads.emplace_back(
        [&](int dbnum) {
          ASSERT_OK(DB::Open(options, dbnames[dbnum], &dbs[dbnum]));
        },
        i);
  }

  // Now add some data and close, so next we can verify non-empty DBs can be
  // recovered in parallel
  for (int i = 0; i < kNumDbs; ++i) {
    open_threads[i].join();
    ASSERT_OK(dbs[i]->Put(WriteOptions(), "xi", "gua"));
    delete dbs[i];
  }

  // Verify non-empty DBs can be recovered in parallel
  dbs.clear();
  open_threads.clear();
  for (int i = 0; i < kNumDbs; ++i) {
    open_threads.emplace_back(
        [&](int dbnum) {
          ASSERT_OK(DB::Open(options, dbnames[dbnum], &dbs[dbnum]));
        },
        i);
  }

  // Wait and cleanup
  for (int i = 0; i < kNumDbs; ++i) {
    open_threads[i].join();
    delete dbs[i];
    ASSERT_OK(DestroyDB(dbnames[i], options));
  }
}
#endif  // OS_WIN

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
