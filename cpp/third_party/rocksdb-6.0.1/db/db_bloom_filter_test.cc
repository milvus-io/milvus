//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/perf_context.h"

namespace rocksdb {

// DB tests related to bloom filter.

class DBBloomFilterTest : public DBTestBase {
 public:
  DBBloomFilterTest() : DBTestBase("/db_bloom_filter_test") {}
};

class DBBloomFilterTestWithParam
    : public DBTestBase,
      public testing::WithParamInterface<std::tuple<bool, bool, uint32_t>> {
  //                             public testing::WithParamInterface<bool> {
 protected:
  bool use_block_based_filter_;
  bool partition_filters_;
  uint32_t format_version_;

 public:
  DBBloomFilterTestWithParam() : DBTestBase("/db_bloom_filter_tests") {}

  ~DBBloomFilterTestWithParam() override {}

  void SetUp() override {
    use_block_based_filter_ = std::get<0>(GetParam());
    partition_filters_ = std::get<1>(GetParam());
    format_version_ = std::get<2>(GetParam());
  }
};

class DBBloomFilterTestDefFormatVersion : public DBBloomFilterTestWithParam {};

class SliceTransformLimitedDomainGeneric : public SliceTransform {
  const char* Name() const override {
    return "SliceTransformLimitedDomainGeneric";
  }

  Slice Transform(const Slice& src) const override {
    return Slice(src.data(), 5);
  }

  bool InDomain(const Slice& src) const override {
    // prefix will be x????
    return src.size() >= 5;
  }

  bool InRange(const Slice& dst) const override {
    // prefix will be x????
    return dst.size() == 5;
  }
};

// KeyMayExist can lead to a few false positives, but not false negatives.
// To make test deterministic, use a much larger number of bits per key-20 than
// bits in the key, so that false positives are eliminated
TEST_P(DBBloomFilterTestDefFormatVersion, KeyMayExist) {
  do {
    ReadOptions ropts;
    std::string value;
    anon::OptionsOverride options_override;
    options_override.filter_policy.reset(
        NewBloomFilterPolicy(20, use_block_based_filter_));
    options_override.partition_filters = partition_filters_;
    options_override.metadata_block_size = 32;
    Options options = CurrentOptions(options_override);
    if (partition_filters_ &&
        static_cast<BlockBasedTableOptions*>(
            options.table_factory->GetOptions())
                ->index_type != BlockBasedTableOptions::kTwoLevelIndexSearch) {
      // In the current implementation partitioned filters depend on partitioned
      // indexes
      continue;
    }
    options.statistics = rocksdb::CreateDBStatistics();
    CreateAndReopenWithCF({"pikachu"}, options);

    ASSERT_TRUE(!db_->KeyMayExist(ropts, handles_[1], "a", &value));

    ASSERT_OK(Put(1, "a", "b"));
    bool value_found = false;
    ASSERT_TRUE(
        db_->KeyMayExist(ropts, handles_[1], "a", &value, &value_found));
    ASSERT_TRUE(value_found);
    ASSERT_EQ("b", value);

    ASSERT_OK(Flush(1));
    value.clear();

    uint64_t numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    uint64_t cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    ASSERT_TRUE(
        db_->KeyMayExist(ropts, handles_[1], "a", &value, &value_found));
    ASSERT_TRUE(!value_found);
    // assert that no new files were opened and no new blocks were
    // read into block cache.
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));

    ASSERT_OK(Delete(1, "a"));

    numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    ASSERT_TRUE(!db_->KeyMayExist(ropts, handles_[1], "a", &value));
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));

    ASSERT_OK(Flush(1));
    dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1],
                                true /* disallow trivial move */);

    numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    ASSERT_TRUE(!db_->KeyMayExist(ropts, handles_[1], "a", &value));
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));

    ASSERT_OK(Delete(1, "c"));

    numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    ASSERT_TRUE(!db_->KeyMayExist(ropts, handles_[1], "c", &value));
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));

    // KeyMayExist function only checks data in block caches, which is not used
    // by plain table format.
  } while (
      ChangeOptions(kSkipPlainTable | kSkipHashIndex | kSkipFIFOCompaction));
}

TEST_F(DBBloomFilterTest, GetFilterByPrefixBloomCustomPrefixExtractor) {
  for (bool partition_filters : {true, false}) {
    Options options = last_options_;
    options.prefix_extractor =
        std::make_shared<SliceTransformLimitedDomainGeneric>();
    options.statistics = rocksdb::CreateDBStatistics();
    get_perf_context()->EnablePerLevelPerfContext();
    BlockBasedTableOptions bbto;
    bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
    if (partition_filters) {
      bbto.partition_filters = true;
      bbto.index_type = BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
    }
    bbto.whole_key_filtering = false;
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));
    DestroyAndReopen(options);

    WriteOptions wo;
    ReadOptions ro;
    FlushOptions fo;
    fo.wait = true;
    std::string value;

    ASSERT_OK(dbfull()->Put(wo, "barbarbar", "foo"));
    ASSERT_OK(dbfull()->Put(wo, "barbarbar2", "foo2"));
    ASSERT_OK(dbfull()->Put(wo, "foofoofoo", "bar"));

    dbfull()->Flush(fo);

    ASSERT_EQ("foo", Get("barbarbar"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);
    ASSERT_EQ(
        0,
        (*(get_perf_context()->level_to_perf_context))[0].bloom_filter_useful);
    ASSERT_EQ("foo2", Get("barbarbar2"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);
    ASSERT_EQ(
        0,
        (*(get_perf_context()->level_to_perf_context))[0].bloom_filter_useful);
    ASSERT_EQ("NOT_FOUND", Get("barbarbar3"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);
    ASSERT_EQ(
        0,
        (*(get_perf_context()->level_to_perf_context))[0].bloom_filter_useful);

    ASSERT_EQ("NOT_FOUND", Get("barfoofoo"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
    ASSERT_EQ(
        1,
        (*(get_perf_context()->level_to_perf_context))[0].bloom_filter_useful);

    ASSERT_EQ("NOT_FOUND", Get("foobarbar"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 2);
    ASSERT_EQ(
        2,
        (*(get_perf_context()->level_to_perf_context))[0].bloom_filter_useful);

    ro.total_order_seek = true;
    ASSERT_TRUE(db_->Get(ro, "foobarbar", &value).IsNotFound());
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 2);
    ASSERT_EQ(
        2,
        (*(get_perf_context()->level_to_perf_context))[0].bloom_filter_useful);
    get_perf_context()->Reset();
  }
}

TEST_F(DBBloomFilterTest, GetFilterByPrefixBloom) {
  for (bool partition_filters : {true, false}) {
    Options options = last_options_;
    options.prefix_extractor.reset(NewFixedPrefixTransform(8));
    options.statistics = rocksdb::CreateDBStatistics();
    get_perf_context()->EnablePerLevelPerfContext();
    BlockBasedTableOptions bbto;
    bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
    if (partition_filters) {
      bbto.partition_filters = true;
      bbto.index_type = BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
    }
    bbto.whole_key_filtering = false;
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));
    DestroyAndReopen(options);

    WriteOptions wo;
    ReadOptions ro;
    FlushOptions fo;
    fo.wait = true;
    std::string value;

    ASSERT_OK(dbfull()->Put(wo, "barbarbar", "foo"));
    ASSERT_OK(dbfull()->Put(wo, "barbarbar2", "foo2"));
    ASSERT_OK(dbfull()->Put(wo, "foofoofoo", "bar"));

    dbfull()->Flush(fo);

    ASSERT_EQ("foo", Get("barbarbar"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);
    ASSERT_EQ("foo2", Get("barbarbar2"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);
    ASSERT_EQ("NOT_FOUND", Get("barbarbar3"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);

    ASSERT_EQ("NOT_FOUND", Get("barfoofoo"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);

    ASSERT_EQ("NOT_FOUND", Get("foobarbar"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 2);

    ro.total_order_seek = true;
    ASSERT_TRUE(db_->Get(ro, "foobarbar", &value).IsNotFound());
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 2);
    ASSERT_EQ(
        2,
        (*(get_perf_context()->level_to_perf_context))[0].bloom_filter_useful);
    get_perf_context()->Reset();
  }
}

TEST_F(DBBloomFilterTest, WholeKeyFilterProp) {
  for (bool partition_filters : {true, false}) {
    Options options = last_options_;
    options.prefix_extractor.reset(NewFixedPrefixTransform(3));
    options.statistics = rocksdb::CreateDBStatistics();
    get_perf_context()->EnablePerLevelPerfContext();

    BlockBasedTableOptions bbto;
    bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
    bbto.whole_key_filtering = false;
    if (partition_filters) {
      bbto.partition_filters = true;
      bbto.index_type = BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
    }
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));
    DestroyAndReopen(options);

    WriteOptions wo;
    ReadOptions ro;
    FlushOptions fo;
    fo.wait = true;
    std::string value;

    ASSERT_OK(dbfull()->Put(wo, "foobar", "foo"));
    // Needs insert some keys to make sure files are not filtered out by key
    // ranges.
    ASSERT_OK(dbfull()->Put(wo, "aaa", ""));
    ASSERT_OK(dbfull()->Put(wo, "zzz", ""));
    dbfull()->Flush(fo);

    Reopen(options);
    ASSERT_EQ("NOT_FOUND", Get("foo"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);
    ASSERT_EQ("NOT_FOUND", Get("bar"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
    ASSERT_EQ("foo", Get("foobar"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);

    // Reopen with whole key filtering enabled and prefix extractor
    // NULL. Bloom filter should be off for both of whole key and
    // prefix bloom.
    bbto.whole_key_filtering = true;
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));
    options.prefix_extractor.reset();
    Reopen(options);

    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
    ASSERT_EQ("NOT_FOUND", Get("foo"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
    ASSERT_EQ("NOT_FOUND", Get("bar"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
    ASSERT_EQ("foo", Get("foobar"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
    // Write DB with only full key filtering.
    ASSERT_OK(dbfull()->Put(wo, "foobar", "foo"));
    // Needs insert some keys to make sure files are not filtered out by key
    // ranges.
    ASSERT_OK(dbfull()->Put(wo, "aaa", ""));
    ASSERT_OK(dbfull()->Put(wo, "zzz", ""));
    db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);

    // Reopen with both of whole key off and prefix extractor enabled.
    // Still no bloom filter should be used.
    options.prefix_extractor.reset(NewFixedPrefixTransform(3));
    bbto.whole_key_filtering = false;
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));
    Reopen(options);

    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
    ASSERT_EQ("NOT_FOUND", Get("foo"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
    ASSERT_EQ("NOT_FOUND", Get("bar"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
    ASSERT_EQ("foo", Get("foobar"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);

    // Try to create a DB with mixed files:
    ASSERT_OK(dbfull()->Put(wo, "foobar", "foo"));
    // Needs insert some keys to make sure files are not filtered out by key
    // ranges.
    ASSERT_OK(dbfull()->Put(wo, "aaa", ""));
    ASSERT_OK(dbfull()->Put(wo, "zzz", ""));
    db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);

    options.prefix_extractor.reset();
    bbto.whole_key_filtering = true;
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));
    Reopen(options);

    // Try to create a DB with mixed files.
    ASSERT_OK(dbfull()->Put(wo, "barfoo", "bar"));
    // In this case needs insert some keys to make sure files are
    // not filtered out by key ranges.
    ASSERT_OK(dbfull()->Put(wo, "aaa", ""));
    ASSERT_OK(dbfull()->Put(wo, "zzz", ""));
    Flush();

    // Now we have two files:
    // File 1: An older file with prefix bloom.
    // File 2: A newer file with whole bloom filter.
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
    ASSERT_EQ("NOT_FOUND", Get("foo"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 2);
    ASSERT_EQ("NOT_FOUND", Get("bar"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 3);
    ASSERT_EQ("foo", Get("foobar"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 4);
    ASSERT_EQ("bar", Get("barfoo"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 4);

    // Reopen with the same setting: only whole key is used
    Reopen(options);
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 4);
    ASSERT_EQ("NOT_FOUND", Get("foo"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 5);
    ASSERT_EQ("NOT_FOUND", Get("bar"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 6);
    ASSERT_EQ("foo", Get("foobar"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 7);
    ASSERT_EQ("bar", Get("barfoo"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 7);

    // Restart with both filters are allowed
    options.prefix_extractor.reset(NewFixedPrefixTransform(3));
    bbto.whole_key_filtering = true;
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));
    Reopen(options);
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 7);
    // File 1 will has it filtered out.
    // File 2 will not, as prefix `foo` exists in the file.
    ASSERT_EQ("NOT_FOUND", Get("foo"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 8);
    ASSERT_EQ("NOT_FOUND", Get("bar"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 10);
    ASSERT_EQ("foo", Get("foobar"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 11);
    ASSERT_EQ("bar", Get("barfoo"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 11);

    // Restart with only prefix bloom is allowed.
    options.prefix_extractor.reset(NewFixedPrefixTransform(3));
    bbto.whole_key_filtering = false;
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));
    Reopen(options);
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 11);
    ASSERT_EQ("NOT_FOUND", Get("foo"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 11);
    ASSERT_EQ("NOT_FOUND", Get("bar"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 12);
    ASSERT_EQ("foo", Get("foobar"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 12);
    ASSERT_EQ("bar", Get("barfoo"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 12);
    uint64_t bloom_filter_useful_all_levels = 0;
    for (auto& kv : (*(get_perf_context()->level_to_perf_context))) {
      if (kv.second.bloom_filter_useful > 0) {
        bloom_filter_useful_all_levels += kv.second.bloom_filter_useful;
      }
    }
    ASSERT_EQ(12, bloom_filter_useful_all_levels);
    get_perf_context()->Reset();
  }
}

TEST_P(DBBloomFilterTestWithParam, BloomFilter) {
  do {
    Options options = CurrentOptions();
    env_->count_random_reads_ = true;
    options.env = env_;
    // ChangeCompactOptions() only changes compaction style, which does not
    // trigger reset of table_factory
    BlockBasedTableOptions table_options;
    table_options.no_block_cache = true;
    table_options.filter_policy.reset(
        NewBloomFilterPolicy(10, use_block_based_filter_));
    table_options.partition_filters = partition_filters_;
    if (partition_filters_) {
      table_options.index_type =
          BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
    }
    table_options.format_version = format_version_;
    if (format_version_ >= 4) {
      // value delta encoding challenged more with index interval > 1
      table_options.index_block_restart_interval = 8;
    }
    table_options.metadata_block_size = 32;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    CreateAndReopenWithCF({"pikachu"}, options);

    // Populate multiple layers
    const int N = 10000;
    for (int i = 0; i < N; i++) {
      ASSERT_OK(Put(1, Key(i), Key(i)));
    }
    Compact(1, "a", "z");
    for (int i = 0; i < N; i += 100) {
      ASSERT_OK(Put(1, Key(i), Key(i)));
    }
    Flush(1);

    // Prevent auto compactions triggered by seeks
    env_->delay_sstable_sync_.store(true, std::memory_order_release);

    // Lookup present keys.  Should rarely read from small sstable.
    env_->random_read_counter_.Reset();
    for (int i = 0; i < N; i++) {
      ASSERT_EQ(Key(i), Get(1, Key(i)));
    }
    int reads = env_->random_read_counter_.Read();
    fprintf(stderr, "%d present => %d reads\n", N, reads);
    ASSERT_GE(reads, N);
    if (partition_filters_) {
      // Without block cache, we read an extra partition filter per each
      // level*read and a partition index per each read
      ASSERT_LE(reads, 4 * N + 2 * N / 100);
    } else {
      ASSERT_LE(reads, N + 2 * N / 100);
    }

    // Lookup present keys.  Should rarely read from either sstable.
    env_->random_read_counter_.Reset();
    for (int i = 0; i < N; i++) {
      ASSERT_EQ("NOT_FOUND", Get(1, Key(i) + ".missing"));
    }
    reads = env_->random_read_counter_.Read();
    fprintf(stderr, "%d missing => %d reads\n", N, reads);
    if (partition_filters_) {
      // With partitioned filter we read one extra filter per level per each
      // missed read.
      ASSERT_LE(reads, 2 * N + 3 * N / 100);
    } else {
      ASSERT_LE(reads, 3 * N / 100);
    }

    env_->delay_sstable_sync_.store(false, std::memory_order_release);
    Close();
  } while (ChangeCompactOptions());
}

#ifndef ROCKSDB_VALGRIND_RUN
INSTANTIATE_TEST_CASE_P(
    FormatDef, DBBloomFilterTestDefFormatVersion,
    ::testing::Values(std::make_tuple(true, false, test::kDefaultFormatVersion),
                      std::make_tuple(false, true, test::kDefaultFormatVersion),
                      std::make_tuple(false, false,
                                      test::kDefaultFormatVersion)));

INSTANTIATE_TEST_CASE_P(
    FormatDef, DBBloomFilterTestWithParam,
    ::testing::Values(std::make_tuple(true, false, test::kDefaultFormatVersion),
                      std::make_tuple(false, true, test::kDefaultFormatVersion),
                      std::make_tuple(false, false,
                                      test::kDefaultFormatVersion)));

INSTANTIATE_TEST_CASE_P(
    FormatLatest, DBBloomFilterTestWithParam,
    ::testing::Values(std::make_tuple(true, false, test::kLatestFormatVersion),
                      std::make_tuple(false, true, test::kLatestFormatVersion),
                      std::make_tuple(false, false,
                                      test::kLatestFormatVersion)));
#endif  // ROCKSDB_VALGRIND_RUN

TEST_F(DBBloomFilterTest, BloomFilterRate) {
  while (ChangeFilterOptions()) {
    Options options = CurrentOptions();
    options.statistics = rocksdb::CreateDBStatistics();
    get_perf_context()->EnablePerLevelPerfContext();
    CreateAndReopenWithCF({"pikachu"}, options);

    const int maxKey = 10000;
    for (int i = 0; i < maxKey; i++) {
      ASSERT_OK(Put(1, Key(i), Key(i)));
    }
    // Add a large key to make the file contain wide range
    ASSERT_OK(Put(1, Key(maxKey + 55555), Key(maxKey + 55555)));
    Flush(1);

    // Check if they can be found
    for (int i = 0; i < maxKey; i++) {
      ASSERT_EQ(Key(i), Get(1, Key(i)));
    }
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);

    // Check if filter is useful
    for (int i = 0; i < maxKey; i++) {
      ASSERT_EQ("NOT_FOUND", Get(1, Key(i + 33333)));
    }
    ASSERT_GE(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), maxKey * 0.98);
    ASSERT_GE(
        (*(get_perf_context()->level_to_perf_context))[0].bloom_filter_useful,
        maxKey * 0.98);
    get_perf_context()->Reset();
  }
}

TEST_F(DBBloomFilterTest, BloomFilterCompatibility) {
  Options options = CurrentOptions();
  options.statistics = rocksdb::CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, true));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  // Create with block based filter
  CreateAndReopenWithCF({"pikachu"}, options);

  const int maxKey = 10000;
  for (int i = 0; i < maxKey; i++) {
    ASSERT_OK(Put(1, Key(i), Key(i)));
  }
  ASSERT_OK(Put(1, Key(maxKey + 55555), Key(maxKey + 55555)));
  Flush(1);

  // Check db with full filter
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  // Check if they can be found
  for (int i = 0; i < maxKey; i++) {
    ASSERT_EQ(Key(i), Get(1, Key(i)));
  }
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);

  // Check db with partitioned full filter
  table_options.partition_filters = true;
  table_options.index_type =
      BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  // Check if they can be found
  for (int i = 0; i < maxKey; i++) {
    ASSERT_EQ(Key(i), Get(1, Key(i)));
  }
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);
}

TEST_F(DBBloomFilterTest, BloomFilterReverseCompatibility) {
  for (bool partition_filters : {true, false}) {
    Options options = CurrentOptions();
    options.statistics = rocksdb::CreateDBStatistics();
    BlockBasedTableOptions table_options;
    if (partition_filters) {
      table_options.partition_filters = true;
      table_options.index_type =
          BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
    }
    table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    DestroyAndReopen(options);

    // Create with full filter
    CreateAndReopenWithCF({"pikachu"}, options);

    const int maxKey = 10000;
    for (int i = 0; i < maxKey; i++) {
      ASSERT_OK(Put(1, Key(i), Key(i)));
    }
    ASSERT_OK(Put(1, Key(maxKey + 55555), Key(maxKey + 55555)));
    Flush(1);

    // Check db with block_based filter
    table_options.filter_policy.reset(NewBloomFilterPolicy(10, true));
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    ReopenWithColumnFamilies({"default", "pikachu"}, options);

    // Check if they can be found
    for (int i = 0; i < maxKey; i++) {
      ASSERT_EQ(Key(i), Get(1, Key(i)));
    }
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);
  }
}

namespace {
// A wrapped bloom over default FilterPolicy
class WrappedBloom : public FilterPolicy {
 public:
  explicit WrappedBloom(int bits_per_key)
      : filter_(NewBloomFilterPolicy(bits_per_key)), counter_(0) {}

  ~WrappedBloom() override { delete filter_; }

  const char* Name() const override { return "WrappedRocksDbFilterPolicy"; }

  void CreateFilter(const rocksdb::Slice* keys, int n,
                    std::string* dst) const override {
    std::unique_ptr<rocksdb::Slice[]> user_keys(new rocksdb::Slice[n]);
    for (int i = 0; i < n; ++i) {
      user_keys[i] = convertKey(keys[i]);
    }
    return filter_->CreateFilter(user_keys.get(), n, dst);
  }

  bool KeyMayMatch(const rocksdb::Slice& key,
                   const rocksdb::Slice& filter) const override {
    counter_++;
    return filter_->KeyMayMatch(convertKey(key), filter);
  }

  uint32_t GetCounter() { return counter_; }

 private:
  const FilterPolicy* filter_;
  mutable uint32_t counter_;

  rocksdb::Slice convertKey(const rocksdb::Slice& key) const { return key; }
};
}  // namespace

TEST_F(DBBloomFilterTest, BloomFilterWrapper) {
  Options options = CurrentOptions();
  options.statistics = rocksdb::CreateDBStatistics();

  BlockBasedTableOptions table_options;
  WrappedBloom* policy = new WrappedBloom(10);
  table_options.filter_policy.reset(policy);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  CreateAndReopenWithCF({"pikachu"}, options);

  const int maxKey = 10000;
  for (int i = 0; i < maxKey; i++) {
    ASSERT_OK(Put(1, Key(i), Key(i)));
  }
  // Add a large key to make the file contain wide range
  ASSERT_OK(Put(1, Key(maxKey + 55555), Key(maxKey + 55555)));
  ASSERT_EQ(0U, policy->GetCounter());
  Flush(1);

  // Check if they can be found
  for (int i = 0; i < maxKey; i++) {
    ASSERT_EQ(Key(i), Get(1, Key(i)));
  }
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);
  ASSERT_EQ(1U * maxKey, policy->GetCounter());

  // Check if filter is useful
  for (int i = 0; i < maxKey; i++) {
    ASSERT_EQ("NOT_FOUND", Get(1, Key(i + 33333)));
  }
  ASSERT_GE(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), maxKey * 0.98);
  ASSERT_EQ(2U * maxKey, policy->GetCounter());
}

class SliceTransformLimitedDomain : public SliceTransform {
  const char* Name() const override { return "SliceTransformLimitedDomain"; }

  Slice Transform(const Slice& src) const override {
    return Slice(src.data(), 5);
  }

  bool InDomain(const Slice& src) const override {
    // prefix will be x????
    return src.size() >= 5 && src[0] == 'x';
  }

  bool InRange(const Slice& dst) const override {
    // prefix will be x????
    return dst.size() == 5 && dst[0] == 'x';
  }
};

TEST_F(DBBloomFilterTest, PrefixExtractorFullFilter) {
  BlockBasedTableOptions bbto;
  // Full Filter Block
  bbto.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
  bbto.whole_key_filtering = false;

  Options options = CurrentOptions();
  options.prefix_extractor = std::make_shared<SliceTransformLimitedDomain>();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  DestroyAndReopen(options);

  ASSERT_OK(Put("x1111_AAAA", "val1"));
  ASSERT_OK(Put("x1112_AAAA", "val2"));
  ASSERT_OK(Put("x1113_AAAA", "val3"));
  ASSERT_OK(Put("x1114_AAAA", "val4"));
  // Not in domain, wont be added to filter
  ASSERT_OK(Put("zzzzz_AAAA", "val5"));

  ASSERT_OK(Flush());

  ASSERT_EQ(Get("x1111_AAAA"), "val1");
  ASSERT_EQ(Get("x1112_AAAA"), "val2");
  ASSERT_EQ(Get("x1113_AAAA"), "val3");
  ASSERT_EQ(Get("x1114_AAAA"), "val4");
  // Was not added to filter but rocksdb will try to read it from the filter
  ASSERT_EQ(Get("zzzzz_AAAA"), "val5");
}

TEST_F(DBBloomFilterTest, PrefixExtractorBlockFilter) {
  BlockBasedTableOptions bbto;
  // Block Filter Block
  bbto.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));

  Options options = CurrentOptions();
  options.prefix_extractor = std::make_shared<SliceTransformLimitedDomain>();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  DestroyAndReopen(options);

  ASSERT_OK(Put("x1113_AAAA", "val3"));
  ASSERT_OK(Put("x1114_AAAA", "val4"));
  // Not in domain, wont be added to filter
  ASSERT_OK(Put("zzzzz_AAAA", "val1"));
  ASSERT_OK(Put("zzzzz_AAAB", "val2"));
  ASSERT_OK(Put("zzzzz_AAAC", "val3"));
  ASSERT_OK(Put("zzzzz_AAAD", "val4"));

  ASSERT_OK(Flush());

  std::vector<std::string> iter_res;
  auto iter = db_->NewIterator(ReadOptions());
  // Seek to a key that was not in Domain
  for (iter->Seek("zzzzz_AAAA"); iter->Valid(); iter->Next()) {
    iter_res.emplace_back(iter->value().ToString());
  }

  std::vector<std::string> expected_res = {"val1", "val2", "val3", "val4"};
  ASSERT_EQ(iter_res, expected_res);
  delete iter;
}

TEST_F(DBBloomFilterTest, MemtableWholeKeyBloomFilter) {
  // regression test for #2743. the range delete tombstones in memtable should
  // be added even when Get() skips searching due to its prefix bloom filter
  const int kMemtableSize = 1 << 20;              // 1MB
  const int kMemtablePrefixFilterSize = 1 << 13;  // 8KB
  const int kPrefixLen = 4;
  Options options = CurrentOptions();
  options.memtable_prefix_bloom_size_ratio =
      static_cast<double>(kMemtablePrefixFilterSize) / kMemtableSize;
  options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(kPrefixLen));
  options.write_buffer_size = kMemtableSize;
  options.memtable_whole_key_filtering = false;
  Reopen(options);
  std::string key1("AAAABBBB");
  std::string key2("AAAACCCC");  // not in DB
  std::string key3("AAAADDDD");
  std::string key4("AAAAEEEE");
  std::string value1("Value1");
  std::string value3("Value3");
  std::string value4("Value4");

  ASSERT_OK(Put(key1, value1, WriteOptions()));

  // check memtable bloom stats
  ASSERT_EQ("NOT_FOUND", Get(key2));
  ASSERT_EQ(0, get_perf_context()->bloom_memtable_miss_count);
  // same prefix, bloom filter false positive
  ASSERT_EQ(1, get_perf_context()->bloom_memtable_hit_count);

  // enable whole key bloom filter
  options.memtable_whole_key_filtering = true;
  Reopen(options);
  // check memtable bloom stats
  ASSERT_OK(Put(key3, value3, WriteOptions()));
  ASSERT_EQ("NOT_FOUND", Get(key2));
  // whole key bloom filter kicks in and determines it's a miss
  ASSERT_EQ(1, get_perf_context()->bloom_memtable_miss_count);
  ASSERT_EQ(1, get_perf_context()->bloom_memtable_hit_count);

  // verify whole key filtering does not depend on prefix_extractor
  options.prefix_extractor.reset();
  Reopen(options);
  // check memtable bloom stats
  ASSERT_OK(Put(key4, value4, WriteOptions()));
  ASSERT_EQ("NOT_FOUND", Get(key2));
  // whole key bloom filter kicks in and determines it's a miss
  ASSERT_EQ(2, get_perf_context()->bloom_memtable_miss_count);
  ASSERT_EQ(1, get_perf_context()->bloom_memtable_hit_count);
}

#ifndef ROCKSDB_LITE
class BloomStatsTestWithParam
    : public DBBloomFilterTest,
      public testing::WithParamInterface<std::tuple<bool, bool, bool>> {
 public:
  BloomStatsTestWithParam() {
    use_block_table_ = std::get<0>(GetParam());
    use_block_based_builder_ = std::get<1>(GetParam());
    partition_filters_ = std::get<2>(GetParam());

    options_.create_if_missing = true;
    options_.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(4));
    options_.memtable_prefix_bloom_size_ratio =
        8.0 * 1024.0 / static_cast<double>(options_.write_buffer_size);
    if (use_block_table_) {
      BlockBasedTableOptions table_options;
      table_options.hash_index_allow_collision = false;
      if (partition_filters_) {
        assert(!use_block_based_builder_);
        table_options.partition_filters = partition_filters_;
        table_options.index_type =
            BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
      }
      table_options.filter_policy.reset(
          NewBloomFilterPolicy(10, use_block_based_builder_));
      options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
    } else {
      assert(!partition_filters_);  // not supported in plain table
      PlainTableOptions table_options;
      options_.table_factory.reset(NewPlainTableFactory(table_options));
    }
    options_.env = env_;

    get_perf_context()->Reset();
    DestroyAndReopen(options_);
  }

  ~BloomStatsTestWithParam() override {
    get_perf_context()->Reset();
    Destroy(options_);
  }

  // Required if inheriting from testing::WithParamInterface<>
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

  bool use_block_table_;
  bool use_block_based_builder_;
  bool partition_filters_;
  Options options_;
};

// 1 Insert 2 K-V pairs into DB
// 2 Call Get() for both keys - expext memtable bloom hit stat to be 2
// 3 Call Get() for nonexisting key - expect memtable bloom miss stat to be 1
// 4 Call Flush() to create SST
// 5 Call Get() for both keys - expext SST bloom hit stat to be 2
// 6 Call Get() for nonexisting key - expect SST bloom miss stat to be 1
// Test both: block and plain SST
TEST_P(BloomStatsTestWithParam, BloomStatsTest) {
  std::string key1("AAAA");
  std::string key2("RXDB");  // not in DB
  std::string key3("ZBRA");
  std::string value1("Value1");
  std::string value3("Value3");

  ASSERT_OK(Put(key1, value1, WriteOptions()));
  ASSERT_OK(Put(key3, value3, WriteOptions()));

  // check memtable bloom stats
  ASSERT_EQ(value1, Get(key1));
  ASSERT_EQ(1, get_perf_context()->bloom_memtable_hit_count);
  ASSERT_EQ(value3, Get(key3));
  ASSERT_EQ(2, get_perf_context()->bloom_memtable_hit_count);
  ASSERT_EQ(0, get_perf_context()->bloom_memtable_miss_count);

  ASSERT_EQ("NOT_FOUND", Get(key2));
  ASSERT_EQ(1, get_perf_context()->bloom_memtable_miss_count);
  ASSERT_EQ(2, get_perf_context()->bloom_memtable_hit_count);

  // sanity checks
  ASSERT_EQ(0, get_perf_context()->bloom_sst_hit_count);
  ASSERT_EQ(0, get_perf_context()->bloom_sst_miss_count);

  Flush();

  // sanity checks
  ASSERT_EQ(0, get_perf_context()->bloom_sst_hit_count);
  ASSERT_EQ(0, get_perf_context()->bloom_sst_miss_count);

  // check SST bloom stats
  ASSERT_EQ(value1, Get(key1));
  ASSERT_EQ(1, get_perf_context()->bloom_sst_hit_count);
  ASSERT_EQ(value3, Get(key3));
  ASSERT_EQ(2, get_perf_context()->bloom_sst_hit_count);

  ASSERT_EQ("NOT_FOUND", Get(key2));
  ASSERT_EQ(1, get_perf_context()->bloom_sst_miss_count);
}

// Same scenario as in BloomStatsTest but using an iterator
TEST_P(BloomStatsTestWithParam, BloomStatsTestWithIter) {
  std::string key1("AAAA");
  std::string key2("RXDB");  // not in DB
  std::string key3("ZBRA");
  std::string value1("Value1");
  std::string value3("Value3");

  ASSERT_OK(Put(key1, value1, WriteOptions()));
  ASSERT_OK(Put(key3, value3, WriteOptions()));

  std::unique_ptr<Iterator> iter(dbfull()->NewIterator(ReadOptions()));

  // check memtable bloom stats
  iter->Seek(key1);
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(value1, iter->value().ToString());
  ASSERT_EQ(1, get_perf_context()->bloom_memtable_hit_count);
  ASSERT_EQ(0, get_perf_context()->bloom_memtable_miss_count);

  iter->Seek(key3);
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(value3, iter->value().ToString());
  ASSERT_EQ(2, get_perf_context()->bloom_memtable_hit_count);
  ASSERT_EQ(0, get_perf_context()->bloom_memtable_miss_count);

  iter->Seek(key2);
  ASSERT_OK(iter->status());
  ASSERT_TRUE(!iter->Valid());
  ASSERT_EQ(1, get_perf_context()->bloom_memtable_miss_count);
  ASSERT_EQ(2, get_perf_context()->bloom_memtable_hit_count);

  Flush();

  iter.reset(dbfull()->NewIterator(ReadOptions()));

  // Check SST bloom stats
  iter->Seek(key1);
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(value1, iter->value().ToString());
  ASSERT_EQ(1, get_perf_context()->bloom_sst_hit_count);

  iter->Seek(key3);
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(value3, iter->value().ToString());
  ASSERT_EQ(2, get_perf_context()->bloom_sst_hit_count);

  iter->Seek(key2);
  ASSERT_OK(iter->status());
  ASSERT_TRUE(!iter->Valid());
  ASSERT_EQ(1, get_perf_context()->bloom_sst_miss_count);
  ASSERT_EQ(2, get_perf_context()->bloom_sst_hit_count);
}

INSTANTIATE_TEST_CASE_P(BloomStatsTestWithParam, BloomStatsTestWithParam,
                        ::testing::Values(std::make_tuple(true, true, false),
                                          std::make_tuple(true, false, false),
                                          std::make_tuple(true, false, true),
                                          std::make_tuple(false, false,
                                                          false)));

namespace {
void PrefixScanInit(DBBloomFilterTest* dbtest) {
  char buf[100];
  std::string keystr;
  const int small_range_sstfiles = 5;
  const int big_range_sstfiles = 5;

  // Generate 11 sst files with the following prefix ranges.
  // GROUP 0: [0,10]                              (level 1)
  // GROUP 1: [1,2], [2,3], [3,4], [4,5], [5, 6]  (level 0)
  // GROUP 2: [0,6], [0,7], [0,8], [0,9], [0,10]  (level 0)
  //
  // A seek with the previous API would do 11 random I/Os (to all the
  // files).  With the new API and a prefix filter enabled, we should
  // only do 2 random I/O, to the 2 files containing the key.

  // GROUP 0
  snprintf(buf, sizeof(buf), "%02d______:start", 0);
  keystr = std::string(buf);
  ASSERT_OK(dbtest->Put(keystr, keystr));
  snprintf(buf, sizeof(buf), "%02d______:end", 10);
  keystr = std::string(buf);
  ASSERT_OK(dbtest->Put(keystr, keystr));
  dbtest->Flush();
  dbtest->dbfull()->CompactRange(CompactRangeOptions(), nullptr,
                                 nullptr);  // move to level 1

  // GROUP 1
  for (int i = 1; i <= small_range_sstfiles; i++) {
    snprintf(buf, sizeof(buf), "%02d______:start", i);
    keystr = std::string(buf);
    ASSERT_OK(dbtest->Put(keystr, keystr));
    snprintf(buf, sizeof(buf), "%02d______:end", i + 1);
    keystr = std::string(buf);
    ASSERT_OK(dbtest->Put(keystr, keystr));
    dbtest->Flush();
  }

  // GROUP 2
  for (int i = 1; i <= big_range_sstfiles; i++) {
    snprintf(buf, sizeof(buf), "%02d______:start", 0);
    keystr = std::string(buf);
    ASSERT_OK(dbtest->Put(keystr, keystr));
    snprintf(buf, sizeof(buf), "%02d______:end", small_range_sstfiles + i + 1);
    keystr = std::string(buf);
    ASSERT_OK(dbtest->Put(keystr, keystr));
    dbtest->Flush();
  }
}
}  // namespace

TEST_F(DBBloomFilterTest, PrefixScan) {
  while (ChangeFilterOptions()) {
    int count;
    Slice prefix;
    Slice key;
    char buf[100];
    Iterator* iter;
    snprintf(buf, sizeof(buf), "03______:");
    prefix = Slice(buf, 8);
    key = Slice(buf, 9);
    ASSERT_EQ(key.difference_offset(prefix), 8);
    ASSERT_EQ(prefix.difference_offset(key), 8);
    // db configs
    env_->count_random_reads_ = true;
    Options options = CurrentOptions();
    options.env = env_;
    options.prefix_extractor.reset(NewFixedPrefixTransform(8));
    options.disable_auto_compactions = true;
    options.max_background_compactions = 2;
    options.create_if_missing = true;
    options.memtable_factory.reset(NewHashSkipListRepFactory(16));
    options.allow_concurrent_memtable_write = false;

    BlockBasedTableOptions table_options;
    table_options.no_block_cache = true;
    table_options.filter_policy.reset(NewBloomFilterPolicy(10));
    table_options.whole_key_filtering = false;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    // 11 RAND I/Os
    DestroyAndReopen(options);
    PrefixScanInit(this);
    count = 0;
    env_->random_read_counter_.Reset();
    iter = db_->NewIterator(ReadOptions());
    for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
      if (!iter->key().starts_with(prefix)) {
        break;
      }
      count++;
    }
    ASSERT_OK(iter->status());
    delete iter;
    ASSERT_EQ(count, 2);
    ASSERT_EQ(env_->random_read_counter_.Read(), 2);
    Close();
  }  // end of while
}

TEST_F(DBBloomFilterTest, OptimizeFiltersForHits) {
  Options options = CurrentOptions();
  options.write_buffer_size = 64 * 1024;
  options.arena_block_size = 4 * 1024;
  options.target_file_size_base = 64 * 1024;
  options.level0_file_num_compaction_trigger = 2;
  options.level0_slowdown_writes_trigger = 2;
  options.level0_stop_writes_trigger = 4;
  options.max_bytes_for_level_base = 256 * 1024;
  options.max_write_buffer_number = 2;
  options.max_background_compactions = 8;
  options.max_background_flushes = 8;
  options.compression = kNoCompression;
  options.compaction_style = kCompactionStyleLevel;
  options.level_compaction_dynamic_level_bytes = true;
  BlockBasedTableOptions bbto;
  bbto.cache_index_and_filter_blocks = true;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, true));
  bbto.whole_key_filtering = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  options.optimize_filters_for_hits = true;
  options.statistics = rocksdb::CreateDBStatistics();
  get_perf_context()->EnablePerLevelPerfContext();
  CreateAndReopenWithCF({"mypikachu"}, options);

  int numkeys = 200000;

  // Generate randomly shuffled keys, so the updates are almost
  // random.
  std::vector<int> keys;
  keys.reserve(numkeys);
  for (int i = 0; i < numkeys; i += 2) {
    keys.push_back(i);
  }
  std::random_shuffle(std::begin(keys), std::end(keys));

  int num_inserted = 0;
  for (int key : keys) {
    ASSERT_OK(Put(1, Key(key), "val"));
    if (++num_inserted % 1000 == 0) {
      dbfull()->TEST_WaitForFlushMemTable();
      dbfull()->TEST_WaitForCompact();
    }
  }
  ASSERT_OK(Put(1, Key(0), "val"));
  ASSERT_OK(Put(1, Key(numkeys), "val"));
  ASSERT_OK(Flush(1));
  dbfull()->TEST_WaitForCompact();

  if (NumTableFilesAtLevel(0, 1) == 0) {
    // No Level 0 file. Create one.
    ASSERT_OK(Put(1, Key(0), "val"));
    ASSERT_OK(Put(1, Key(numkeys), "val"));
    ASSERT_OK(Flush(1));
    dbfull()->TEST_WaitForCompact();
  }

  for (int i = 1; i < numkeys; i += 2) {
    ASSERT_EQ(Get(1, Key(i)), "NOT_FOUND");
  }

  ASSERT_EQ(0, TestGetTickerCount(options, GET_HIT_L0));
  ASSERT_EQ(0, TestGetTickerCount(options, GET_HIT_L1));
  ASSERT_EQ(0, TestGetTickerCount(options, GET_HIT_L2_AND_UP));

  // Now we have three sorted run, L0, L5 and L6 with most files in L6 have
  // no bloom filter. Most keys be checked bloom filters twice.
  ASSERT_GT(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 65000 * 2);
  ASSERT_LT(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 120000 * 2);
  uint64_t bloom_filter_useful_all_levels = 0;
  for (auto& kv : (*(get_perf_context()->level_to_perf_context))) {
    if (kv.second.bloom_filter_useful > 0) {
      bloom_filter_useful_all_levels += kv.second.bloom_filter_useful;
    }
  }
  ASSERT_GT(bloom_filter_useful_all_levels, 65000 * 2);
  ASSERT_LT(bloom_filter_useful_all_levels, 120000 * 2);

  for (int i = 0; i < numkeys; i += 2) {
    ASSERT_EQ(Get(1, Key(i)), "val");
  }

  // Part 2 (read path): rewrite last level with blooms, then verify they get
  // cached only if !optimize_filters_for_hits
  options.disable_auto_compactions = true;
  options.num_levels = 9;
  options.optimize_filters_for_hits = false;
  options.statistics = CreateDBStatistics();
  bbto.block_cache.reset();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  ReopenWithColumnFamilies({"default", "mypikachu"}, options);
  MoveFilesToLevel(7 /* level */, 1 /* column family index */);

  std::string value = Get(1, Key(0));
  uint64_t prev_cache_filter_hits =
      TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT);
  value = Get(1, Key(0));
  ASSERT_EQ(prev_cache_filter_hits + 1,
            TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));

  // Now that we know the filter blocks exist in the last level files, see if
  // filter caching is skipped for this optimization
  options.optimize_filters_for_hits = true;
  options.statistics = CreateDBStatistics();
  bbto.block_cache.reset();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  ReopenWithColumnFamilies({"default", "mypikachu"}, options);

  value = Get(1, Key(0));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
  ASSERT_EQ(2 /* index and data block */,
            TestGetTickerCount(options, BLOCK_CACHE_ADD));

  // Check filter block ignored for files preloaded during DB::Open()
  options.max_open_files = -1;
  options.statistics = CreateDBStatistics();
  bbto.block_cache.reset();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  ReopenWithColumnFamilies({"default", "mypikachu"}, options);

  uint64_t prev_cache_filter_misses =
      TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS);
  prev_cache_filter_hits = TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT);
  Get(1, Key(0));
  ASSERT_EQ(prev_cache_filter_misses,
            TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(prev_cache_filter_hits,
            TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));

  // Check filter block ignored for file trivially-moved to bottom level
  bbto.block_cache.reset();
  options.max_open_files = 100;  // setting > -1 makes it not preload all files
  options.statistics = CreateDBStatistics();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  ReopenWithColumnFamilies({"default", "mypikachu"}, options);

  ASSERT_OK(Put(1, Key(numkeys + 1), "val"));
  ASSERT_OK(Flush(1));

  int32_t trivial_move = 0;
  int32_t non_trivial_move = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:TrivialMove",
      [&](void* /*arg*/) { trivial_move++; });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial",
      [&](void* /*arg*/) { non_trivial_move++; });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kSkip;
  compact_options.change_level = true;
  compact_options.target_level = 7;
  db_->CompactRange(compact_options, handles_[1], nullptr, nullptr);

  ASSERT_EQ(trivial_move, 1);
  ASSERT_EQ(non_trivial_move, 0);

  prev_cache_filter_hits = TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT);
  prev_cache_filter_misses =
      TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS);
  value = Get(1, Key(numkeys + 1));
  ASSERT_EQ(prev_cache_filter_hits,
            TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
  ASSERT_EQ(prev_cache_filter_misses,
            TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));

  // Check filter block not cached for iterator
  bbto.block_cache.reset();
  options.statistics = CreateDBStatistics();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  ReopenWithColumnFamilies({"default", "mypikachu"}, options);

  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions(), handles_[1]));
  iter->SeekToFirst();
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
  ASSERT_EQ(2 /* index and data block */,
            TestGetTickerCount(options, BLOCK_CACHE_ADD));
  get_perf_context()->Reset();
}

int CountIter(std::unique_ptr<Iterator>& iter, const Slice& key) {
  int count = 0;
  for (iter->Seek(key); iter->Valid() && iter->status() == Status::OK();
       iter->Next()) {
    count++;
  }
  return count;
}

// use iterate_upper_bound to hint compatiability of existing bloom filters.
// The BF is considered compatible if 1) upper bound and seek key transform
// into the same string, or 2) the transformed seek key is of the same length
// as the upper bound and two keys are adjacent according to the comparator.
TEST_F(DBBloomFilterTest, DynamicBloomFilterUpperBound) {
  int iteration = 0;
  for (bool use_block_based_builder : {true, false}) {
    Options options;
    options.create_if_missing = true;
    options.prefix_extractor.reset(NewCappedPrefixTransform(4));
    options.disable_auto_compactions = true;
    options.statistics = CreateDBStatistics();
    // Enable prefix bloom for SST files
    BlockBasedTableOptions table_options;
    table_options.cache_index_and_filter_blocks = true;
    table_options.filter_policy.reset(
        NewBloomFilterPolicy(10, use_block_based_builder));
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    DestroyAndReopen(options);

    ASSERT_OK(Put("abcdxxx0", "val1"));
    ASSERT_OK(Put("abcdxxx1", "val2"));
    ASSERT_OK(Put("abcdxxx2", "val3"));
    ASSERT_OK(Put("abcdxxx3", "val4"));
    dbfull()->Flush(FlushOptions());
    {
      // prefix_extractor has not changed, BF will always be read
      Slice upper_bound("abce");
      ReadOptions read_options;
      read_options.prefix_same_as_start = true;
      read_options.iterate_upper_bound = &upper_bound;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "abcd0000"), 4);
    }
    {
      Slice upper_bound("abcdzzzz");
      ReadOptions read_options;
      read_options.prefix_same_as_start = true;
      read_options.iterate_upper_bound = &upper_bound;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "abcd0000"), 4);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED), 2);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
    }
    ASSERT_OK(dbfull()->SetOptions({{"prefix_extractor", "fixed:5"}}));
    ASSERT_EQ(0, strcmp(dbfull()->GetOptions().prefix_extractor->Name(),
                        "rocksdb.FixedPrefix.5"));
    {
      // BF changed, [abcdxx00, abce) is a valid bound, will trigger BF read
      Slice upper_bound("abce");
      ReadOptions read_options;
      read_options.prefix_same_as_start = true;
      read_options.iterate_upper_bound = &upper_bound;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "abcdxx00"), 4);
      // should check bloom filter since upper bound meets requirement
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
                2 + iteration);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
    }
    {
      // [abcdxx01, abcey) is not valid bound since upper bound is too long for
      // the BF in SST (capped:4)
      Slice upper_bound("abcey");
      ReadOptions read_options;
      read_options.prefix_same_as_start = true;
      read_options.iterate_upper_bound = &upper_bound;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "abcdxx01"), 4);
      // should skip bloom filter since upper bound is too long
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
                2 + iteration);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
    }
    {
      // [abcdxx02, abcdy) is a valid bound since the prefix is the same
      Slice upper_bound("abcdy");
      ReadOptions read_options;
      read_options.prefix_same_as_start = true;
      read_options.iterate_upper_bound = &upper_bound;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "abcdxx02"), 4);
      // should check bloom filter since upper bound matches transformed seek
      // key
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
                2 + iteration * 2);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
    }
    {
      // [aaaaaaaa, abce) is not a valid bound since 1) they don't share the
      // same prefix, 2) the prefixes are not consecutive
      Slice upper_bound("abce");
      ReadOptions read_options;
      read_options.prefix_same_as_start = true;
      read_options.iterate_upper_bound = &upper_bound;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "aaaaaaaa"), 0);
      // should skip bloom filter since mismatch is found
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
                2 + iteration * 2);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
    }
    ASSERT_OK(dbfull()->SetOptions({{"prefix_extractor", "fixed:3"}}));
    {
      // [abc, abd) is not a valid bound since the upper bound is too short
      // for BF (capped:4)
      Slice upper_bound("abd");
      ReadOptions read_options;
      read_options.prefix_same_as_start = true;
      read_options.iterate_upper_bound = &upper_bound;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "abc"), 4);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
                2 + iteration * 2);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
    }
    ASSERT_OK(dbfull()->SetOptions({{"prefix_extractor", "capped:4"}}));
    {
      // set back to capped:4 and verify BF is always read
      Slice upper_bound("abd");
      ReadOptions read_options;
      read_options.prefix_same_as_start = true;
      read_options.iterate_upper_bound = &upper_bound;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "abc"), 0);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
                3 + iteration * 2);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 1);
    }
    iteration++;
  }
}

// Create multiple SST files each with a different prefix_extractor config,
// verify iterators can read all SST files using the latest config.
TEST_F(DBBloomFilterTest, DynamicBloomFilterMultipleSST) {
  int iteration = 0;
  for (bool use_block_based_builder : {true, false}) {
    Options options;
    options.create_if_missing = true;
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    options.disable_auto_compactions = true;
    options.statistics = CreateDBStatistics();
    // Enable prefix bloom for SST files
    BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(
        NewBloomFilterPolicy(10, use_block_based_builder));
    table_options.cache_index_and_filter_blocks = true;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    DestroyAndReopen(options);

    Slice upper_bound("foz90000");
    ReadOptions read_options;
    read_options.prefix_same_as_start = true;

    // first SST with fixed:1 BF
    ASSERT_OK(Put("foo2", "bar2"));
    ASSERT_OK(Put("foo", "bar"));
    ASSERT_OK(Put("foq1", "bar1"));
    ASSERT_OK(Put("fpa", "0"));
    dbfull()->Flush(FlushOptions());
    std::unique_ptr<Iterator> iter_old(db_->NewIterator(read_options));
    ASSERT_EQ(CountIter(iter_old, "foo"), 4);
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED), 1);

    ASSERT_OK(dbfull()->SetOptions({{"prefix_extractor", "capped:3"}}));
    ASSERT_EQ(0, strcmp(dbfull()->GetOptions().prefix_extractor->Name(),
                        "rocksdb.CappedPrefix.3"));
    read_options.iterate_upper_bound = &upper_bound;
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_EQ(CountIter(iter, "foo"), 2);
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
              1 + iteration);
    ASSERT_EQ(CountIter(iter, "gpk"), 0);
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
              1 + iteration);
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);

    // second SST with capped:3 BF
    ASSERT_OK(Put("foo3", "bar3"));
    ASSERT_OK(Put("foo4", "bar4"));
    ASSERT_OK(Put("foq5", "bar5"));
    ASSERT_OK(Put("fpb", "1"));
    dbfull()->Flush(FlushOptions());
    {
      // BF is cappped:3 now
      std::unique_ptr<Iterator> iter_tmp(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter_tmp, "foo"), 4);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
                2 + iteration * 2);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
      ASSERT_EQ(CountIter(iter_tmp, "gpk"), 0);
      // both counters are incremented because BF is "not changed" for 1 of the
      // 2 SST files, so filter is checked once and found no match.
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
                3 + iteration * 2);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 1);
    }

    ASSERT_OK(dbfull()->SetOptions({{"prefix_extractor", "fixed:2"}}));
    ASSERT_EQ(0, strcmp(dbfull()->GetOptions().prefix_extractor->Name(),
                        "rocksdb.FixedPrefix.2"));
    // third SST with fixed:2 BF
    ASSERT_OK(Put("foo6", "bar6"));
    ASSERT_OK(Put("foo7", "bar7"));
    ASSERT_OK(Put("foq8", "bar8"));
    ASSERT_OK(Put("fpc", "2"));
    dbfull()->Flush(FlushOptions());
    {
      // BF is fixed:2 now
      std::unique_ptr<Iterator> iter_tmp(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter_tmp, "foo"), 9);
      // the first and last BF are checked
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
                4 + iteration * 3);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 1);
      ASSERT_EQ(CountIter(iter_tmp, "gpk"), 0);
      // only last BF is checked and not found
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
                5 + iteration * 3);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 2);
    }

    // iter_old can only see the first SST, so checked plus 1
    ASSERT_EQ(CountIter(iter_old, "foo"), 4);
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
              6 + iteration * 3);
    // iter was created after the first setoptions call so only full filter
    // will check the filter
    ASSERT_EQ(CountIter(iter, "foo"), 2);
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
              6 + iteration * 4);

    {
      // keys in all three SSTs are visible to iterator
      // The range of [foo, foz90000] is compatible with (fixed:1) and (fixed:2)
      // so +2 for checked counter
      std::unique_ptr<Iterator> iter_all(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter_all, "foo"), 9);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
                7 + iteration * 5);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 2);
      ASSERT_EQ(CountIter(iter_all, "gpk"), 0);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
                8 + iteration * 5);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 3);
    }
    ASSERT_OK(dbfull()->SetOptions({{"prefix_extractor", "capped:3"}}));
    ASSERT_EQ(0, strcmp(dbfull()->GetOptions().prefix_extractor->Name(),
                        "rocksdb.CappedPrefix.3"));
    {
      std::unique_ptr<Iterator> iter_all(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter_all, "foo"), 6);
      // all three SST are checked because the current options has the same as
      // the remaining SST (capped:3)
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
                9 + iteration * 7);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 3);
      ASSERT_EQ(CountIter(iter_all, "gpk"), 0);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
                10 + iteration * 7);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 4);
    }
    // TODO(Zhongyi): Maybe also need to add Get calls to test point look up?
    iteration++;
  }
}

// Create a new column family in a running DB, change prefix_extractor
// dynamically, verify the iterator created on the new column family behaves
// as expected
TEST_F(DBBloomFilterTest, DynamicBloomFilterNewColumnFamily) {
  int iteration = 0;
  for (bool use_block_based_builder : {true, false}) {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    options.disable_auto_compactions = true;
    options.statistics = CreateDBStatistics();
    // Enable prefix bloom for SST files
    BlockBasedTableOptions table_options;
    table_options.cache_index_and_filter_blocks = true;
    table_options.filter_policy.reset(
        NewBloomFilterPolicy(10, use_block_based_builder));
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    CreateAndReopenWithCF({"pikachu" + std::to_string(iteration)}, options);
    ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    // create a new CF and set prefix_extractor dynamically
    options.prefix_extractor.reset(NewCappedPrefixTransform(3));
    CreateColumnFamilies({"ramen_dojo_" + std::to_string(iteration)}, options);
    ASSERT_EQ(0,
              strcmp(dbfull()->GetOptions(handles_[2]).prefix_extractor->Name(),
                     "rocksdb.CappedPrefix.3"));
    ASSERT_OK(Put(2, "foo3", "bar3"));
    ASSERT_OK(Put(2, "foo4", "bar4"));
    ASSERT_OK(Put(2, "foo5", "bar5"));
    ASSERT_OK(Put(2, "foq6", "bar6"));
    ASSERT_OK(Put(2, "fpq7", "bar7"));
    dbfull()->Flush(FlushOptions());
    {
      std::unique_ptr<Iterator> iter(
          db_->NewIterator(read_options, handles_[2]));
      ASSERT_EQ(CountIter(iter, "foo"), 3);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED), 0);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
    }
    ASSERT_OK(
        dbfull()->SetOptions(handles_[2], {{"prefix_extractor", "fixed:2"}}));
    ASSERT_EQ(0,
              strcmp(dbfull()->GetOptions(handles_[2]).prefix_extractor->Name(),
                     "rocksdb.FixedPrefix.2"));
    {
      std::unique_ptr<Iterator> iter(
          db_->NewIterator(read_options, handles_[2]));
      ASSERT_EQ(CountIter(iter, "foo"), 4);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED), 0);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
    }
    ASSERT_OK(dbfull()->DropColumnFamily(handles_[2]));
    dbfull()->DestroyColumnFamilyHandle(handles_[2]);
    handles_[2] = nullptr;
    ASSERT_OK(dbfull()->DropColumnFamily(handles_[1]));
    dbfull()->DestroyColumnFamilyHandle(handles_[1]);
    handles_[1] = nullptr;
    iteration++;
  }
}

// Verify it's possible to change prefix_extractor at runtime and iterators
// behaves as expected
TEST_F(DBBloomFilterTest, DynamicBloomFilterOptions) {
  int iteration = 0;
  for (bool use_block_based_builder : {true, false}) {
    Options options;
    options.create_if_missing = true;
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    options.disable_auto_compactions = true;
    options.statistics = CreateDBStatistics();
    // Enable prefix bloom for SST files
    BlockBasedTableOptions table_options;
    table_options.cache_index_and_filter_blocks = true;
    table_options.filter_policy.reset(
        NewBloomFilterPolicy(10, use_block_based_builder));
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    DestroyAndReopen(options);

    ASSERT_OK(Put("foo2", "bar2"));
    ASSERT_OK(Put("foo", "bar"));
    ASSERT_OK(Put("foo1", "bar1"));
    ASSERT_OK(Put("fpa", "0"));
    dbfull()->Flush(FlushOptions());
    ASSERT_OK(Put("foo3", "bar3"));
    ASSERT_OK(Put("foo4", "bar4"));
    ASSERT_OK(Put("foo5", "bar5"));
    ASSERT_OK(Put("fpb", "1"));
    dbfull()->Flush(FlushOptions());
    ASSERT_OK(Put("foo6", "bar6"));
    ASSERT_OK(Put("foo7", "bar7"));
    ASSERT_OK(Put("foo8", "bar8"));
    ASSERT_OK(Put("fpc", "2"));
    dbfull()->Flush(FlushOptions());

    ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    {
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "foo"), 12);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED), 3);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
    }
    std::unique_ptr<Iterator> iter_old(db_->NewIterator(read_options));
    ASSERT_EQ(CountIter(iter_old, "foo"), 12);
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED), 6);
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);

    ASSERT_OK(dbfull()->SetOptions({{"prefix_extractor", "capped:3"}}));
    ASSERT_EQ(0, strcmp(dbfull()->GetOptions().prefix_extractor->Name(),
                        "rocksdb.CappedPrefix.3"));
    {
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      // "fp*" should be skipped
      ASSERT_EQ(CountIter(iter, "foo"), 9);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED), 6);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
    }

    // iterator created before should not be affected and see all keys
    ASSERT_EQ(CountIter(iter_old, "foo"), 12);
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED), 9);
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
    ASSERT_EQ(CountIter(iter_old, "abc"), 0);
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED), 12);
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 3);
    iteration++;
  }
}

#endif  // ROCKSDB_LITE

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
