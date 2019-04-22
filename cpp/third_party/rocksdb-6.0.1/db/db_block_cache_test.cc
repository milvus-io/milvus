//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <cstdlib>
#include "cache/lru_cache.h"
#include "db/db_test_util.h"
#include "port/stack_trace.h"

namespace rocksdb {

class DBBlockCacheTest : public DBTestBase {
 private:
  size_t miss_count_ = 0;
  size_t hit_count_ = 0;
  size_t insert_count_ = 0;
  size_t failure_count_ = 0;
  size_t compressed_miss_count_ = 0;
  size_t compressed_hit_count_ = 0;
  size_t compressed_insert_count_ = 0;
  size_t compressed_failure_count_ = 0;

 public:
  const size_t kNumBlocks = 10;
  const size_t kValueSize = 100;

  DBBlockCacheTest() : DBTestBase("/db_block_cache_test") {}

  BlockBasedTableOptions GetTableOptions() {
    BlockBasedTableOptions table_options;
    // Set a small enough block size so that each key-value get its own block.
    table_options.block_size = 1;
    return table_options;
  }

  Options GetOptions(const BlockBasedTableOptions& table_options) {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.avoid_flush_during_recovery = false;
    // options.compression = kNoCompression;
    options.statistics = rocksdb::CreateDBStatistics();
    options.table_factory.reset(new BlockBasedTableFactory(table_options));
    return options;
  }

  void InitTable(const Options& /*options*/) {
    std::string value(kValueSize, 'a');
    for (size_t i = 0; i < kNumBlocks; i++) {
      ASSERT_OK(Put(ToString(i), value.c_str()));
    }
  }

  void RecordCacheCounters(const Options& options) {
    miss_count_ = TestGetTickerCount(options, BLOCK_CACHE_MISS);
    hit_count_ = TestGetTickerCount(options, BLOCK_CACHE_HIT);
    insert_count_ = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    failure_count_ = TestGetTickerCount(options, BLOCK_CACHE_ADD_FAILURES);
    compressed_miss_count_ =
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_MISS);
    compressed_hit_count_ =
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_HIT);
    compressed_insert_count_ =
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_ADD);
    compressed_failure_count_ =
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_ADD_FAILURES);
  }

  void CheckCacheCounters(const Options& options, size_t expected_misses,
                          size_t expected_hits, size_t expected_inserts,
                          size_t expected_failures) {
    size_t new_miss_count = TestGetTickerCount(options, BLOCK_CACHE_MISS);
    size_t new_hit_count = TestGetTickerCount(options, BLOCK_CACHE_HIT);
    size_t new_insert_count = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    size_t new_failure_count =
        TestGetTickerCount(options, BLOCK_CACHE_ADD_FAILURES);
    ASSERT_EQ(miss_count_ + expected_misses, new_miss_count);
    ASSERT_EQ(hit_count_ + expected_hits, new_hit_count);
    ASSERT_EQ(insert_count_ + expected_inserts, new_insert_count);
    ASSERT_EQ(failure_count_ + expected_failures, new_failure_count);
    miss_count_ = new_miss_count;
    hit_count_ = new_hit_count;
    insert_count_ = new_insert_count;
    failure_count_ = new_failure_count;
  }

  void CheckCompressedCacheCounters(const Options& options,
                                    size_t expected_misses,
                                    size_t expected_hits,
                                    size_t expected_inserts,
                                    size_t expected_failures) {
    size_t new_miss_count =
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_MISS);
    size_t new_hit_count =
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_HIT);
    size_t new_insert_count =
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_ADD);
    size_t new_failure_count =
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_ADD_FAILURES);
    ASSERT_EQ(compressed_miss_count_ + expected_misses, new_miss_count);
    ASSERT_EQ(compressed_hit_count_ + expected_hits, new_hit_count);
    ASSERT_EQ(compressed_insert_count_ + expected_inserts, new_insert_count);
    ASSERT_EQ(compressed_failure_count_ + expected_failures, new_failure_count);
    compressed_miss_count_ = new_miss_count;
    compressed_hit_count_ = new_hit_count;
    compressed_insert_count_ = new_insert_count;
    compressed_failure_count_ = new_failure_count;
  }
};

TEST_F(DBBlockCacheTest, IteratorBlockCacheUsage) {
  ReadOptions read_options;
  read_options.fill_cache = false;
  auto table_options = GetTableOptions();
  auto options = GetOptions(table_options);
  InitTable(options);

  std::shared_ptr<Cache> cache = NewLRUCache(0, 0, false);
  table_options.block_cache = cache;
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  Reopen(options);
  RecordCacheCounters(options);

  std::vector<std::unique_ptr<Iterator>> iterators(kNumBlocks - 1);
  Iterator* iter = nullptr;

  ASSERT_EQ(0, cache->GetUsage());
  iter = db_->NewIterator(read_options);
  iter->Seek(ToString(0));
  ASSERT_LT(0, cache->GetUsage());
  delete iter;
  iter = nullptr;
  ASSERT_EQ(0, cache->GetUsage());
}

TEST_F(DBBlockCacheTest, TestWithoutCompressedBlockCache) {
  ReadOptions read_options;
  auto table_options = GetTableOptions();
  auto options = GetOptions(table_options);
  InitTable(options);

  std::shared_ptr<Cache> cache = NewLRUCache(0, 0, false);
  table_options.block_cache = cache;
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  Reopen(options);
  RecordCacheCounters(options);

  std::vector<std::unique_ptr<Iterator>> iterators(kNumBlocks - 1);
  Iterator* iter = nullptr;

  // Load blocks into cache.
  for (size_t i = 0; i < kNumBlocks - 1; i++) {
    iter = db_->NewIterator(read_options);
    iter->Seek(ToString(i));
    ASSERT_OK(iter->status());
    CheckCacheCounters(options, 1, 0, 1, 0);
    iterators[i].reset(iter);
  }
  size_t usage = cache->GetUsage();
  ASSERT_LT(0, usage);
  cache->SetCapacity(usage);
  ASSERT_EQ(usage, cache->GetPinnedUsage());

  // Test with strict capacity limit.
  cache->SetStrictCapacityLimit(true);
  iter = db_->NewIterator(read_options);
  iter->Seek(ToString(kNumBlocks - 1));
  ASSERT_TRUE(iter->status().IsIncomplete());
  CheckCacheCounters(options, 1, 0, 0, 1);
  delete iter;
  iter = nullptr;

  // Release iterators and access cache again.
  for (size_t i = 0; i < kNumBlocks - 1; i++) {
    iterators[i].reset();
    CheckCacheCounters(options, 0, 0, 0, 0);
  }
  ASSERT_EQ(0, cache->GetPinnedUsage());
  for (size_t i = 0; i < kNumBlocks - 1; i++) {
    iter = db_->NewIterator(read_options);
    iter->Seek(ToString(i));
    ASSERT_OK(iter->status());
    CheckCacheCounters(options, 0, 1, 0, 0);
    iterators[i].reset(iter);
  }
}

#ifdef SNAPPY
TEST_F(DBBlockCacheTest, TestWithCompressedBlockCache) {
  ReadOptions read_options;
  auto table_options = GetTableOptions();
  auto options = GetOptions(table_options);
  options.compression = CompressionType::kSnappyCompression;
  InitTable(options);

  std::shared_ptr<Cache> cache = NewLRUCache(0, 0, false);
  std::shared_ptr<Cache> compressed_cache = NewLRUCache(1 << 25, 0, false);
  table_options.block_cache = cache;
  table_options.block_cache_compressed = compressed_cache;
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  Reopen(options);
  RecordCacheCounters(options);

  std::vector<std::unique_ptr<Iterator>> iterators(kNumBlocks - 1);
  Iterator* iter = nullptr;

  // Load blocks into cache.
  for (size_t i = 0; i < kNumBlocks - 1; i++) {
    iter = db_->NewIterator(read_options);
    iter->Seek(ToString(i));
    ASSERT_OK(iter->status());
    CheckCacheCounters(options, 1, 0, 1, 0);
    CheckCompressedCacheCounters(options, 1, 0, 1, 0);
    iterators[i].reset(iter);
  }
  size_t usage = cache->GetUsage();
  ASSERT_LT(0, usage);
  ASSERT_EQ(usage, cache->GetPinnedUsage());
  size_t compressed_usage = compressed_cache->GetUsage();
  ASSERT_LT(0, compressed_usage);
  // Compressed block cache cannot be pinned.
  ASSERT_EQ(0, compressed_cache->GetPinnedUsage());

  // Set strict capacity limit flag. Now block will only load into compressed
  // block cache.
  cache->SetCapacity(usage);
  cache->SetStrictCapacityLimit(true);
  ASSERT_EQ(usage, cache->GetPinnedUsage());
  iter = db_->NewIterator(read_options);
  iter->Seek(ToString(kNumBlocks - 1));
  ASSERT_TRUE(iter->status().IsIncomplete());
  CheckCacheCounters(options, 1, 0, 0, 1);
  CheckCompressedCacheCounters(options, 1, 0, 1, 0);
  delete iter;
  iter = nullptr;

  // Clear strict capacity limit flag. This time we shall hit compressed block
  // cache.
  cache->SetStrictCapacityLimit(false);
  iter = db_->NewIterator(read_options);
  iter->Seek(ToString(kNumBlocks - 1));
  ASSERT_OK(iter->status());
  CheckCacheCounters(options, 1, 0, 1, 0);
  CheckCompressedCacheCounters(options, 0, 1, 0, 0);
  delete iter;
  iter = nullptr;
}
#endif  // SNAPPY

#ifndef ROCKSDB_LITE

// Make sure that when options.block_cache is set, after a new table is
// created its index/filter blocks are added to block cache.
TEST_F(DBBlockCacheTest, IndexAndFilterBlocksOfNewTableAddedToCache) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.cache_index_and_filter_blocks = true;
  table_options.filter_policy.reset(NewBloomFilterPolicy(20));
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "key", "val"));
  // Create a new table.
  ASSERT_OK(Flush(1));

  // index/filter blocks added to block cache right after table creation.
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(2, /* only index/filter were added */
            TestGetTickerCount(options, BLOCK_CACHE_ADD));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_DATA_MISS));
  uint64_t int_num;
  ASSERT_TRUE(
      dbfull()->GetIntProperty("rocksdb.estimate-table-readers-mem", &int_num));
  ASSERT_EQ(int_num, 0U);

  // Make sure filter block is in cache.
  std::string value;
  ReadOptions ropt;
  db_->KeyMayExist(ReadOptions(), handles_[1], "key", &value);

  // Miss count should remain the same.
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));

  db_->KeyMayExist(ReadOptions(), handles_[1], "key", &value);
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(2, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));

  // Make sure index block is in cache.
  auto index_block_hit = TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT);
  value = Get(1, "key");
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
  ASSERT_EQ(index_block_hit + 1,
            TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));

  value = Get(1, "key");
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
  ASSERT_EQ(index_block_hit + 2,
            TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));
}

// With fill_cache = false, fills up the cache, then iterates over the entire
// db, verify dummy entries inserted in `BlockBasedTable::NewDataBlockIterator`
// does not cause heap-use-after-free errors in COMPILE_WITH_ASAN=1 runs
TEST_F(DBBlockCacheTest, FillCacheAndIterateDB) {
  ReadOptions read_options;
  read_options.fill_cache = false;
  auto table_options = GetTableOptions();
  auto options = GetOptions(table_options);
  InitTable(options);

  std::shared_ptr<Cache> cache = NewLRUCache(10, 0, true);
  table_options.block_cache = cache;
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  Reopen(options);
  ASSERT_OK(Put("key1", "val1"));
  ASSERT_OK(Put("key2", "val2"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("key3", "val3"));
  ASSERT_OK(Put("key4", "val4"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("key5", "val5"));
  ASSERT_OK(Put("key6", "val6"));
  ASSERT_OK(Flush());

  Iterator* iter = nullptr;

  iter = db_->NewIterator(read_options);
  iter->Seek(ToString(0));
  while (iter->Valid()) {
    iter->Next();
  }
  delete iter;
  iter = nullptr;
}

TEST_F(DBBlockCacheTest, IndexAndFilterBlocksStats) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.cache_index_and_filter_blocks = true;
  // 200 bytes are enough to hold the first two blocks
  std::shared_ptr<Cache> cache = NewLRUCache(200, 0, false);
  table_options.block_cache = cache;
  table_options.filter_policy.reset(NewBloomFilterPolicy(20, true));
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "key", "val"));
  // Create a new table
  ASSERT_OK(Flush(1));
  size_t index_bytes_insert =
      TestGetTickerCount(options, BLOCK_CACHE_INDEX_BYTES_INSERT);
  size_t filter_bytes_insert =
      TestGetTickerCount(options, BLOCK_CACHE_FILTER_BYTES_INSERT);
  ASSERT_GT(index_bytes_insert, 0);
  ASSERT_GT(filter_bytes_insert, 0);
  ASSERT_EQ(cache->GetUsage(), index_bytes_insert + filter_bytes_insert);
  // set the cache capacity to the current usage
  cache->SetCapacity(index_bytes_insert + filter_bytes_insert);
  ASSERT_EQ(TestGetTickerCount(options, BLOCK_CACHE_INDEX_BYTES_EVICT), 0);
  ASSERT_EQ(TestGetTickerCount(options, BLOCK_CACHE_FILTER_BYTES_EVICT), 0);
  ASSERT_OK(Put(1, "key2", "val"));
  // Create a new table
  ASSERT_OK(Flush(1));
  // cache evicted old index and block entries
  ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_INDEX_BYTES_INSERT),
            index_bytes_insert);
  ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_FILTER_BYTES_INSERT),
            filter_bytes_insert);
  ASSERT_EQ(TestGetTickerCount(options, BLOCK_CACHE_INDEX_BYTES_EVICT),
            index_bytes_insert);
  ASSERT_EQ(TestGetTickerCount(options, BLOCK_CACHE_FILTER_BYTES_EVICT),
            filter_bytes_insert);
}

namespace {

// A mock cache wraps LRUCache, and record how many entries have been
// inserted for each priority.
class MockCache : public LRUCache {
 public:
  static uint32_t high_pri_insert_count;
  static uint32_t low_pri_insert_count;

  MockCache()
      : LRUCache((size_t)1 << 25 /*capacity*/, 0 /*num_shard_bits*/,
                 false /*strict_capacity_limit*/, 0.0 /*high_pri_pool_ratio*/) {
  }

  Status Insert(const Slice& key, void* value, size_t charge,
                void (*deleter)(const Slice& key, void* value), Handle** handle,
                Priority priority) override {
    if (priority == Priority::LOW) {
      low_pri_insert_count++;
    } else {
      high_pri_insert_count++;
    }
    return LRUCache::Insert(key, value, charge, deleter, handle, priority);
  }
};

uint32_t MockCache::high_pri_insert_count = 0;
uint32_t MockCache::low_pri_insert_count = 0;

}  // anonymous namespace

TEST_F(DBBlockCacheTest, IndexAndFilterBlocksCachePriority) {
  for (auto priority : {Cache::Priority::LOW, Cache::Priority::HIGH}) {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.statistics = rocksdb::CreateDBStatistics();
    BlockBasedTableOptions table_options;
    table_options.cache_index_and_filter_blocks = true;
    table_options.block_cache.reset(new MockCache());
    table_options.filter_policy.reset(NewBloomFilterPolicy(20));
    table_options.cache_index_and_filter_blocks_with_high_priority =
        priority == Cache::Priority::HIGH ? true : false;
    options.table_factory.reset(new BlockBasedTableFactory(table_options));
    DestroyAndReopen(options);

    MockCache::high_pri_insert_count = 0;
    MockCache::low_pri_insert_count = 0;

    // Create a new table.
    ASSERT_OK(Put("foo", "value"));
    ASSERT_OK(Put("bar", "value"));
    ASSERT_OK(Flush());
    ASSERT_EQ(1, NumTableFilesAtLevel(0));

    // index/filter blocks added to block cache right after table creation.
    ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
    ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
    ASSERT_EQ(2, /* only index/filter were added */
              TestGetTickerCount(options, BLOCK_CACHE_ADD));
    ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_DATA_MISS));
    if (priority == Cache::Priority::LOW) {
      ASSERT_EQ(0, MockCache::high_pri_insert_count);
      ASSERT_EQ(2, MockCache::low_pri_insert_count);
    } else {
      ASSERT_EQ(2, MockCache::high_pri_insert_count);
      ASSERT_EQ(0, MockCache::low_pri_insert_count);
    }

    // Access data block.
    ASSERT_EQ("value", Get("foo"));

    ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
    ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
    ASSERT_EQ(3, /*adding data block*/
              TestGetTickerCount(options, BLOCK_CACHE_ADD));
    ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_DATA_MISS));

    // Data block should be inserted with low priority.
    if (priority == Cache::Priority::LOW) {
      ASSERT_EQ(0, MockCache::high_pri_insert_count);
      ASSERT_EQ(3, MockCache::low_pri_insert_count);
    } else {
      ASSERT_EQ(2, MockCache::high_pri_insert_count);
      ASSERT_EQ(1, MockCache::low_pri_insert_count);
    }
  }
}

TEST_F(DBBlockCacheTest, ParanoidFileChecks) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  options.level0_file_num_compaction_trigger = 2;
  options.paranoid_file_checks = true;
  BlockBasedTableOptions table_options;
  table_options.cache_index_and_filter_blocks = false;
  table_options.filter_policy.reset(NewBloomFilterPolicy(20));
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "1_key", "val"));
  ASSERT_OK(Put(1, "9_key", "val"));
  // Create a new table.
  ASSERT_OK(Flush(1));
  ASSERT_EQ(1, /* read and cache data block */
            TestGetTickerCount(options, BLOCK_CACHE_ADD));

  ASSERT_OK(Put(1, "1_key2", "val2"));
  ASSERT_OK(Put(1, "9_key2", "val2"));
  // Create a new SST file. This will further trigger a compaction
  // and generate another file.
  ASSERT_OK(Flush(1));
  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(3, /* Totally 3 files created up to now */
            TestGetTickerCount(options, BLOCK_CACHE_ADD));

  // After disabling options.paranoid_file_checks. NO further block
  // is added after generating a new file.
  ASSERT_OK(
      dbfull()->SetOptions(handles_[1], {{"paranoid_file_checks", "false"}}));

  ASSERT_OK(Put(1, "1_key3", "val3"));
  ASSERT_OK(Put(1, "9_key3", "val3"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "1_key4", "val4"));
  ASSERT_OK(Put(1, "9_key4", "val4"));
  ASSERT_OK(Flush(1));
  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(3, /* Totally 3 files created up to now */
            TestGetTickerCount(options, BLOCK_CACHE_ADD));
}

TEST_F(DBBlockCacheTest, CompressedCache) {
  if (!Snappy_Supported()) {
    return;
  }
  int num_iter = 80;

  // Run this test three iterations.
  // Iteration 1: only a uncompressed block cache
  // Iteration 2: only a compressed block cache
  // Iteration 3: both block cache and compressed cache
  // Iteration 4: both block cache and compressed cache, but DB is not
  // compressed
  for (int iter = 0; iter < 4; iter++) {
    Options options = CurrentOptions();
    options.write_buffer_size = 64 * 1024;  // small write buffer
    options.statistics = rocksdb::CreateDBStatistics();

    BlockBasedTableOptions table_options;
    switch (iter) {
      case 0:
        // only uncompressed block cache
        table_options.block_cache = NewLRUCache(8 * 1024);
        table_options.block_cache_compressed = nullptr;
        options.table_factory.reset(NewBlockBasedTableFactory(table_options));
        break;
      case 1:
        // no block cache, only compressed cache
        table_options.no_block_cache = true;
        table_options.block_cache = nullptr;
        table_options.block_cache_compressed = NewLRUCache(8 * 1024);
        options.table_factory.reset(NewBlockBasedTableFactory(table_options));
        break;
      case 2:
        // both compressed and uncompressed block cache
        table_options.block_cache = NewLRUCache(1024);
        table_options.block_cache_compressed = NewLRUCache(8 * 1024);
        options.table_factory.reset(NewBlockBasedTableFactory(table_options));
        break;
      case 3:
        // both block cache and compressed cache, but DB is not compressed
        // also, make block cache sizes bigger, to trigger block cache hits
        table_options.block_cache = NewLRUCache(1024 * 1024);
        table_options.block_cache_compressed = NewLRUCache(8 * 1024 * 1024);
        options.table_factory.reset(NewBlockBasedTableFactory(table_options));
        options.compression = kNoCompression;
        break;
      default:
        FAIL();
    }
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

    // check that we triggered the appropriate code paths in the cache
    switch (iter) {
      case 0:
        // only uncompressed block cache
        ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_MISS), 0);
        ASSERT_EQ(TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_MISS), 0);
        break;
      case 1:
        // no block cache, only compressed cache
        ASSERT_EQ(TestGetTickerCount(options, BLOCK_CACHE_MISS), 0);
        ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_MISS), 0);
        break;
      case 2:
        // both compressed and uncompressed block cache
        ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_MISS), 0);
        ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_MISS), 0);
        break;
      case 3:
        // both compressed and uncompressed block cache
        ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_MISS), 0);
        ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_HIT), 0);
        ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_MISS), 0);
        // compressed doesn't have any hits since blocks are not compressed on
        // storage
        ASSERT_EQ(TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_HIT), 0);
        break;
      default:
        FAIL();
    }

    options.create_if_missing = true;
    DestroyAndReopen(options);
  }
}

TEST_F(DBBlockCacheTest, CacheCompressionDict) {
  const int kNumFiles = 4;
  const int kNumEntriesPerFile = 128;
  const int kNumBytesPerEntry = 1024;

  // Try all the available libraries that support dictionary compression
  std::vector<CompressionType> compression_types;
#ifdef ZLIB
  compression_types.push_back(kZlibCompression);
#endif  // ZLIB
#if LZ4_VERSION_NUMBER >= 10400
  compression_types.push_back(kLZ4Compression);
  compression_types.push_back(kLZ4HCCompression);
#endif  // LZ4_VERSION_NUMBER >= 10400
#if ZSTD_VERSION_NUMBER >= 500
  compression_types.push_back(kZSTD);
#endif  // ZSTD_VERSION_NUMBER >= 500
  Random rnd(301);
  for (auto compression_type : compression_types) {
    Options options = CurrentOptions();
    options.compression = compression_type;
    options.compression_opts.max_dict_bytes = 4096;
    options.create_if_missing = true;
    options.num_levels = 2;
    options.statistics = rocksdb::CreateDBStatistics();
    options.target_file_size_base = kNumEntriesPerFile * kNumBytesPerEntry;
    BlockBasedTableOptions table_options;
    table_options.cache_index_and_filter_blocks = true;
    table_options.block_cache.reset(new MockCache());
    options.table_factory.reset(new BlockBasedTableFactory(table_options));
    DestroyAndReopen(options);

    for (int i = 0; i < kNumFiles; ++i) {
      ASSERT_EQ(i, NumTableFilesAtLevel(0, 0));
      for (int j = 0; j < kNumEntriesPerFile; ++j) {
        std::string value = RandomString(&rnd, kNumBytesPerEntry);
        ASSERT_OK(Put(Key(j * kNumFiles + i), value.c_str()));
      }
      ASSERT_OK(Flush());
    }
    dbfull()->TEST_WaitForCompact();
    ASSERT_EQ(0, NumTableFilesAtLevel(0));
    ASSERT_EQ(kNumFiles, NumTableFilesAtLevel(1));

    // Seek to a key in a file. It should cause the SST's dictionary meta-block
    // to be read.
    RecordCacheCounters(options);
    ASSERT_EQ(0,
              TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_MISS));
    ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_ADD));
    ASSERT_EQ(
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT),
        0);
    ReadOptions read_options;
    ASSERT_NE("NOT_FOUND", Get(Key(kNumFiles * kNumEntriesPerFile - 1)));
    // Two blocks missed/added: dictionary and data block
    // One block hit: index since it's prefetched
    CheckCacheCounters(options, 2 /* expected_misses */, 1 /* expected_hits */,
                       2 /* expected_inserts */, 0 /* expected_failures */);
    ASSERT_EQ(1,
              TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_MISS));
    ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_ADD));
    ASSERT_GT(
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT),
        0);
  }
}

#endif  // ROCKSDB_LITE

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
