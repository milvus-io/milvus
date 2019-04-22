//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run this test... Skipping...\n");
  return 0;
}
#else

#include <algorithm>
#include <iostream>
#include <vector>

#include "db/db_impl.h"
#include "monitoring/histogram.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "util/coding.h"
#include "util/gflags_compat.h"
#include "util/random.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "util/testharness.h"
#include "utilities/merge_operators.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;

DEFINE_bool(trigger_deadlock, false,
            "issue delete in range scan to trigger PrefixHashMap deadlock");
DEFINE_int32(bucket_count, 100000, "number of buckets");
DEFINE_uint64(num_locks, 10001, "number of locks");
DEFINE_bool(random_prefix, false, "randomize prefix");
DEFINE_uint64(total_prefixes, 100000, "total number of prefixes");
DEFINE_uint64(items_per_prefix, 1, "total number of values per prefix");
DEFINE_int64(write_buffer_size, 33554432, "");
DEFINE_int32(max_write_buffer_number, 2, "");
DEFINE_int32(min_write_buffer_number_to_merge, 1, "");
DEFINE_int32(skiplist_height, 4, "");
DEFINE_double(memtable_prefix_bloom_size_ratio, 0.1, "");
DEFINE_int32(memtable_huge_page_size, 2 * 1024 * 1024, "");
DEFINE_int32(value_size, 40, "");
DEFINE_bool(enable_print, false, "Print options generated to console.");

// Path to the database on file system
const std::string kDbName = rocksdb::test::PerThreadDBPath("prefix_test");

namespace rocksdb {

struct TestKey {
  uint64_t prefix;
  uint64_t sorted;

  TestKey(uint64_t _prefix, uint64_t _sorted)
      : prefix(_prefix), sorted(_sorted) {}
};

// return a slice backed by test_key
inline Slice TestKeyToSlice(std::string &s, const TestKey& test_key) {
  s.clear();
  PutFixed64(&s, test_key.prefix);
  PutFixed64(&s, test_key.sorted);
  return Slice(s.c_str(), s.size());
}

inline const TestKey SliceToTestKey(const Slice& slice) {
  return TestKey(DecodeFixed64(slice.data()),
    DecodeFixed64(slice.data() + 8));
}

class TestKeyComparator : public Comparator {
 public:

  // Compare needs to be aware of the possibility of a and/or b is
  // prefix only
  int Compare(const Slice& a, const Slice& b) const override {
    const TestKey kkey_a = SliceToTestKey(a);
    const TestKey kkey_b = SliceToTestKey(b);
    const TestKey *key_a = &kkey_a;
    const TestKey *key_b = &kkey_b;
    if (key_a->prefix != key_b->prefix) {
      if (key_a->prefix < key_b->prefix) return -1;
      if (key_a->prefix > key_b->prefix) return 1;
    } else {
      EXPECT_TRUE(key_a->prefix == key_b->prefix);
      // note, both a and b could be prefix only
      if (a.size() != b.size()) {
        // one of them is prefix
        EXPECT_TRUE(
            (a.size() == sizeof(uint64_t) && b.size() == sizeof(TestKey)) ||
            (b.size() == sizeof(uint64_t) && a.size() == sizeof(TestKey)));
        if (a.size() < b.size()) return -1;
        if (a.size() > b.size()) return 1;
      } else {
        // both a and b are prefix
        if (a.size() == sizeof(uint64_t)) {
          return 0;
        }

        // both a and b are whole key
        EXPECT_TRUE(a.size() == sizeof(TestKey) && b.size() == sizeof(TestKey));
        if (key_a->sorted < key_b->sorted) return -1;
        if (key_a->sorted > key_b->sorted) return 1;
        if (key_a->sorted == key_b->sorted) return 0;
      }
    }
    return 0;
  }

  bool operator()(const TestKey& a, const TestKey& b) const {
    std::string sa, sb;
    return Compare(TestKeyToSlice(sa, a), TestKeyToSlice(sb, b)) < 0;
  }

  const char* Name() const override { return "TestKeyComparator"; }

  void FindShortestSeparator(std::string* /*start*/,
                             const Slice& /*limit*/) const override {}

  void FindShortSuccessor(std::string* /*key*/) const override {}
};

namespace {
void PutKey(DB* db, WriteOptions write_options, uint64_t prefix,
            uint64_t suffix, const Slice& value) {
  TestKey test_key(prefix, suffix);
  std::string s;
  Slice key = TestKeyToSlice(s, test_key);
  ASSERT_OK(db->Put(write_options, key, value));
}

void PutKey(DB* db, WriteOptions write_options, const TestKey& test_key,
            const Slice& value) {
  std::string s;
  Slice key = TestKeyToSlice(s, test_key);
  ASSERT_OK(db->Put(write_options, key, value));
}

void MergeKey(DB* db, WriteOptions write_options, const TestKey& test_key,
              const Slice& value) {
  std::string s;
  Slice key = TestKeyToSlice(s, test_key);
  ASSERT_OK(db->Merge(write_options, key, value));
}

void DeleteKey(DB* db, WriteOptions write_options, const TestKey& test_key) {
  std::string s;
  Slice key = TestKeyToSlice(s, test_key);
  ASSERT_OK(db->Delete(write_options, key));
}

void SeekIterator(Iterator* iter, uint64_t prefix, uint64_t suffix) {
  TestKey test_key(prefix, suffix);
  std::string s;
  Slice key = TestKeyToSlice(s, test_key);
  iter->Seek(key);
}

const std::string kNotFoundResult = "NOT_FOUND";

std::string Get(DB* db, const ReadOptions& read_options, uint64_t prefix,
                uint64_t suffix) {
  TestKey test_key(prefix, suffix);
  std::string s2;
  Slice key = TestKeyToSlice(s2, test_key);

  std::string result;
  Status s = db->Get(read_options, key, &result);
  if (s.IsNotFound()) {
    result = kNotFoundResult;
  } else if (!s.ok()) {
    result = s.ToString();
  }
  return result;
}

class SamePrefixTransform : public SliceTransform {
 private:
  const Slice prefix_;
  std::string name_;

 public:
  explicit SamePrefixTransform(const Slice& prefix)
      : prefix_(prefix), name_("rocksdb.SamePrefix." + prefix.ToString()) {}

  const char* Name() const override { return name_.c_str(); }

  Slice Transform(const Slice& src) const override {
    assert(InDomain(src));
    return prefix_;
  }

  bool InDomain(const Slice& src) const override {
    if (src.size() >= prefix_.size()) {
      return Slice(src.data(), prefix_.size()) == prefix_;
    }
    return false;
  }

  bool InRange(const Slice& dst) const override { return dst == prefix_; }

  bool FullLengthEnabled(size_t* /*len*/) const override { return false; }
};

}  // namespace

class PrefixTest : public testing::Test {
 public:
  std::shared_ptr<DB> OpenDb() {
    DB* db;

    options.create_if_missing = true;
    options.write_buffer_size = FLAGS_write_buffer_size;
    options.max_write_buffer_number = FLAGS_max_write_buffer_number;
    options.min_write_buffer_number_to_merge =
      FLAGS_min_write_buffer_number_to_merge;

    options.memtable_prefix_bloom_size_ratio =
        FLAGS_memtable_prefix_bloom_size_ratio;
    options.memtable_huge_page_size = FLAGS_memtable_huge_page_size;

    options.prefix_extractor.reset(NewFixedPrefixTransform(8));
    BlockBasedTableOptions bbto;
    bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
    bbto.whole_key_filtering = false;
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));
    options.allow_concurrent_memtable_write = false;

    Status s = DB::Open(options, kDbName,  &db);
    EXPECT_OK(s);
    return std::shared_ptr<DB>(db);
  }

  void FirstOption() {
    option_config_ = kBegin;
  }

  bool NextOptions(int bucket_count) {
    // skip some options
    option_config_++;
    if (option_config_ < kEnd) {
      options.prefix_extractor.reset(NewFixedPrefixTransform(8));
      switch(option_config_) {
        case kHashSkipList:
          options.memtable_factory.reset(
              NewHashSkipListRepFactory(bucket_count, FLAGS_skiplist_height));
          return true;
        case kHashLinkList:
          options.memtable_factory.reset(
              NewHashLinkListRepFactory(bucket_count));
          return true;
        case kHashLinkListHugePageTlb:
          options.memtable_factory.reset(
              NewHashLinkListRepFactory(bucket_count, 2 * 1024 * 1024));
          return true;
        case kHashLinkListTriggerSkipList:
          options.memtable_factory.reset(
              NewHashLinkListRepFactory(bucket_count, 0, 3));
          return true;
        default:
          return false;
      }
    }
    return false;
  }

  PrefixTest() : option_config_(kBegin) {
    options.comparator = new TestKeyComparator();
  }
  ~PrefixTest() override { delete options.comparator; }

 protected:
  enum OptionConfig {
    kBegin,
    kHashSkipList,
    kHashLinkList,
    kHashLinkListHugePageTlb,
    kHashLinkListTriggerSkipList,
    kEnd
  };
  int option_config_;
  Options options;
};

TEST(SamePrefixTest, InDomainTest) {
  DB* db;
  Options options;
  options.create_if_missing = true;
  options.prefix_extractor.reset(new SamePrefixTransform("HHKB"));
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  WriteOptions write_options;
  ReadOptions read_options;
  {
    ASSERT_OK(DestroyDB(kDbName, Options()));
    ASSERT_OK(DB::Open(options, kDbName, &db));
    ASSERT_OK(db->Put(write_options, "HHKB pro2", "Mar 24, 2006"));
    ASSERT_OK(db->Put(write_options, "HHKB pro2 Type-S", "June 29, 2011"));
    ASSERT_OK(db->Put(write_options, "Realforce 87u", "idk"));
    db->Flush(FlushOptions());
    std::string result;
    auto db_iter = db->NewIterator(ReadOptions());

    db_iter->Seek("Realforce 87u");
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_OK(db_iter->status());
    ASSERT_EQ(db_iter->key(), "Realforce 87u");
    ASSERT_EQ(db_iter->value(), "idk");

    delete db_iter;
    delete db;
    ASSERT_OK(DestroyDB(kDbName, Options()));
  }

  {
    ASSERT_OK(DB::Open(options, kDbName, &db));
    ASSERT_OK(db->Put(write_options, "pikachu", "1"));
    ASSERT_OK(db->Put(write_options, "Meowth", "1"));
    ASSERT_OK(db->Put(write_options, "Mewtwo", "idk"));
    db->Flush(FlushOptions());
    std::string result;
    auto db_iter = db->NewIterator(ReadOptions());

    db_iter->Seek("Mewtwo");
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_OK(db_iter->status());
    delete db_iter;
    delete db;
    ASSERT_OK(DestroyDB(kDbName, Options()));
  }
}

TEST_F(PrefixTest, TestResult) {
  for (int num_buckets = 1; num_buckets <= 2; num_buckets++) {
    FirstOption();
    while (NextOptions(num_buckets)) {
      std::cout << "*** Mem table: " << options.memtable_factory->Name()
                << " number of buckets: " << num_buckets
                << std::endl;
      DestroyDB(kDbName, Options());
      auto db = OpenDb();
      WriteOptions write_options;
      ReadOptions read_options;

      // 1. Insert one row.
      Slice v16("v16");
      PutKey(db.get(), write_options, 1, 6, v16);
      std::unique_ptr<Iterator> iter(db->NewIterator(read_options));
      SeekIterator(iter.get(), 1, 6);
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v16 == iter->value());
      SeekIterator(iter.get(), 1, 5);
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v16 == iter->value());
      SeekIterator(iter.get(), 1, 5);
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v16 == iter->value());
      iter->Next();
      ASSERT_TRUE(!iter->Valid());

      SeekIterator(iter.get(), 2, 0);
      ASSERT_TRUE(!iter->Valid());

      ASSERT_EQ(v16.ToString(), Get(db.get(), read_options, 1, 6));
      ASSERT_EQ(kNotFoundResult, Get(db.get(), read_options, 1, 5));
      ASSERT_EQ(kNotFoundResult, Get(db.get(), read_options, 1, 7));
      ASSERT_EQ(kNotFoundResult, Get(db.get(), read_options, 0, 6));
      ASSERT_EQ(kNotFoundResult, Get(db.get(), read_options, 2, 6));

      // 2. Insert an entry for the same prefix as the last entry in the bucket.
      Slice v17("v17");
      PutKey(db.get(), write_options, 1, 7, v17);
      iter.reset(db->NewIterator(read_options));
      SeekIterator(iter.get(), 1, 7);
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v17 == iter->value());

      SeekIterator(iter.get(), 1, 6);
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v16 == iter->value());
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v17 == iter->value());
      iter->Next();
      ASSERT_TRUE(!iter->Valid());

      SeekIterator(iter.get(), 2, 0);
      ASSERT_TRUE(!iter->Valid());

      // 3. Insert an entry for the same prefix as the head of the bucket.
      Slice v15("v15");
      PutKey(db.get(), write_options, 1, 5, v15);
      iter.reset(db->NewIterator(read_options));

      SeekIterator(iter.get(), 1, 7);
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v17 == iter->value());

      SeekIterator(iter.get(), 1, 5);
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v15 == iter->value());
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v16 == iter->value());
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v17 == iter->value());

      SeekIterator(iter.get(), 1, 5);
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v15 == iter->value());

      ASSERT_EQ(v15.ToString(), Get(db.get(), read_options, 1, 5));
      ASSERT_EQ(v16.ToString(), Get(db.get(), read_options, 1, 6));
      ASSERT_EQ(v17.ToString(), Get(db.get(), read_options, 1, 7));

      // 4. Insert an entry with a larger prefix
      Slice v22("v22");
      PutKey(db.get(), write_options, 2, 2, v22);
      iter.reset(db->NewIterator(read_options));

      SeekIterator(iter.get(), 2, 2);
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v22 == iter->value());
      SeekIterator(iter.get(), 2, 0);
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v22 == iter->value());

      SeekIterator(iter.get(), 1, 5);
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v15 == iter->value());

      SeekIterator(iter.get(), 1, 7);
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v17 == iter->value());

      // 5. Insert an entry with a smaller prefix
      Slice v02("v02");
      PutKey(db.get(), write_options, 0, 2, v02);
      iter.reset(db->NewIterator(read_options));

      SeekIterator(iter.get(), 0, 2);
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v02 == iter->value());
      SeekIterator(iter.get(), 0, 0);
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v02 == iter->value());

      SeekIterator(iter.get(), 2, 0);
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v22 == iter->value());

      SeekIterator(iter.get(), 1, 5);
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v15 == iter->value());

      SeekIterator(iter.get(), 1, 7);
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v17 == iter->value());

      // 6. Insert to the beginning and the end of the first prefix
      Slice v13("v13");
      Slice v18("v18");
      PutKey(db.get(), write_options, 1, 3, v13);
      PutKey(db.get(), write_options, 1, 8, v18);
      iter.reset(db->NewIterator(read_options));
      SeekIterator(iter.get(), 1, 7);
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v17 == iter->value());

      SeekIterator(iter.get(), 1, 3);
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v13 == iter->value());
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v15 == iter->value());
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v16 == iter->value());
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v17 == iter->value());
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v18 == iter->value());

      SeekIterator(iter.get(), 0, 0);
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v02 == iter->value());

      SeekIterator(iter.get(), 2, 0);
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v22 == iter->value());

      ASSERT_EQ(v22.ToString(), Get(db.get(), read_options, 2, 2));
      ASSERT_EQ(v02.ToString(), Get(db.get(), read_options, 0, 2));
      ASSERT_EQ(v13.ToString(), Get(db.get(), read_options, 1, 3));
      ASSERT_EQ(v15.ToString(), Get(db.get(), read_options, 1, 5));
      ASSERT_EQ(v16.ToString(), Get(db.get(), read_options, 1, 6));
      ASSERT_EQ(v17.ToString(), Get(db.get(), read_options, 1, 7));
      ASSERT_EQ(v18.ToString(), Get(db.get(), read_options, 1, 8));
    }
  }
}

// Show results in prefix
TEST_F(PrefixTest, PrefixValid) {
  for (int num_buckets = 1; num_buckets <= 2; num_buckets++) {
    FirstOption();
    while (NextOptions(num_buckets)) {
      std::cout << "*** Mem table: " << options.memtable_factory->Name()
                << " number of buckets: " << num_buckets << std::endl;
      DestroyDB(kDbName, Options());
      auto db = OpenDb();
      WriteOptions write_options;
      ReadOptions read_options;

      // Insert keys with common prefix and one key with different
      Slice v16("v16");
      Slice v17("v17");
      Slice v18("v18");
      Slice v19("v19");
      PutKey(db.get(), write_options, 12345, 6, v16);
      PutKey(db.get(), write_options, 12345, 7, v17);
      PutKey(db.get(), write_options, 12345, 8, v18);
      PutKey(db.get(), write_options, 12345, 9, v19);
      PutKey(db.get(), write_options, 12346, 8, v16);
      db->Flush(FlushOptions());
      TestKey test_key(12346, 8);
      std::string s;
      db->Delete(write_options, TestKeyToSlice(s, test_key));
      db->Flush(FlushOptions());
      read_options.prefix_same_as_start = true;
      std::unique_ptr<Iterator> iter(db->NewIterator(read_options));
      SeekIterator(iter.get(), 12345, 6);
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v16 == iter->value());

      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v17 == iter->value());

      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v18 == iter->value());

      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_TRUE(v19 == iter->value());
      iter->Next();
      ASSERT_FALSE(iter->Valid());
      ASSERT_EQ(kNotFoundResult, Get(db.get(), read_options, 12346, 8));

      // Verify seeking past the prefix won't return a result.
      SeekIterator(iter.get(), 12345, 10);
      ASSERT_TRUE(!iter->Valid());
    }
  }
}

TEST_F(PrefixTest, DynamicPrefixIterator) {
  while (NextOptions(FLAGS_bucket_count)) {
    std::cout << "*** Mem table: " << options.memtable_factory->Name()
        << std::endl;
    DestroyDB(kDbName, Options());
    auto db = OpenDb();
    WriteOptions write_options;
    ReadOptions read_options;

    std::vector<uint64_t> prefixes;
    for (uint64_t i = 0; i < FLAGS_total_prefixes; ++i) {
      prefixes.push_back(i);
    }

    if (FLAGS_random_prefix) {
      std::random_shuffle(prefixes.begin(), prefixes.end());
    }

    HistogramImpl hist_put_time;
    HistogramImpl hist_put_comparison;

    // insert x random prefix, each with y continuous element.
    for (auto prefix : prefixes) {
       for (uint64_t sorted = 0; sorted < FLAGS_items_per_prefix; sorted++) {
        TestKey test_key(prefix, sorted);

        std::string s;
        Slice key = TestKeyToSlice(s, test_key);
        std::string value(FLAGS_value_size, 0);

        get_perf_context()->Reset();
        StopWatchNano timer(Env::Default(), true);
        ASSERT_OK(db->Put(write_options, key, value));
        hist_put_time.Add(timer.ElapsedNanos());
        hist_put_comparison.Add(get_perf_context()->user_key_comparison_count);
      }
    }

    std::cout << "Put key comparison: \n" << hist_put_comparison.ToString()
              << "Put time: \n" << hist_put_time.ToString();

    // test seek existing keys
    HistogramImpl hist_seek_time;
    HistogramImpl hist_seek_comparison;

    std::unique_ptr<Iterator> iter(db->NewIterator(read_options));

    for (auto prefix : prefixes) {
      TestKey test_key(prefix, FLAGS_items_per_prefix / 2);
      std::string s;
      Slice key = TestKeyToSlice(s, test_key);
      std::string value = "v" + ToString(0);

      get_perf_context()->Reset();
      StopWatchNano timer(Env::Default(), true);
      auto key_prefix = options.prefix_extractor->Transform(key);
      uint64_t total_keys = 0;
      for (iter->Seek(key);
           iter->Valid() && iter->key().starts_with(key_prefix);
           iter->Next()) {
        if (FLAGS_trigger_deadlock) {
          std::cout << "Behold the deadlock!\n";
          db->Delete(write_options, iter->key());
        }
        total_keys++;
      }
      hist_seek_time.Add(timer.ElapsedNanos());
      hist_seek_comparison.Add(get_perf_context()->user_key_comparison_count);
      ASSERT_EQ(total_keys, FLAGS_items_per_prefix - FLAGS_items_per_prefix/2);
    }

    std::cout << "Seek key comparison: \n"
              << hist_seek_comparison.ToString()
              << "Seek time: \n"
              << hist_seek_time.ToString();

    // test non-existing keys
    HistogramImpl hist_no_seek_time;
    HistogramImpl hist_no_seek_comparison;

    for (auto prefix = FLAGS_total_prefixes;
         prefix < FLAGS_total_prefixes + 10000;
         prefix++) {
      TestKey test_key(prefix, 0);
      std::string s;
      Slice key = TestKeyToSlice(s, test_key);

      get_perf_context()->Reset();
      StopWatchNano timer(Env::Default(), true);
      iter->Seek(key);
      hist_no_seek_time.Add(timer.ElapsedNanos());
      hist_no_seek_comparison.Add(get_perf_context()->user_key_comparison_count);
      ASSERT_TRUE(!iter->Valid());
    }

    std::cout << "non-existing Seek key comparison: \n"
              << hist_no_seek_comparison.ToString()
              << "non-existing Seek time: \n"
              << hist_no_seek_time.ToString();
  }
}

TEST_F(PrefixTest, PrefixSeekModePrev) {
  // Only for SkipListFactory
  options.memtable_factory.reset(new SkipListFactory);
  options.merge_operator = MergeOperators::CreatePutOperator();
  options.write_buffer_size = 1024 * 1024;
  Random rnd(1);
  for (size_t m = 1; m < 100; m++) {
    std::cout << "[" + std::to_string(m) + "]" + "*** Mem table: "
              << options.memtable_factory->Name() << std::endl;
    DestroyDB(kDbName, Options());
    auto db = OpenDb();
    WriteOptions write_options;
    ReadOptions read_options;
    std::map<TestKey, std::string, TestKeyComparator> entry_maps[3], whole_map;
    for (uint64_t i = 0; i < 10; i++) {
      int div = i % 3 + 1;
      for (uint64_t j = 0; j < 10; j++) {
        whole_map[TestKey(i, j)] = entry_maps[rnd.Uniform(div)][TestKey(i, j)] =
            'v' + std::to_string(i) + std::to_string(j);
      }
    }

    std::map<TestKey, std::string, TestKeyComparator> type_map;
    for (size_t i = 0; i < 3; i++) {
      for (auto& kv : entry_maps[i]) {
        if (rnd.OneIn(3)) {
          PutKey(db.get(), write_options, kv.first, kv.second);
          type_map[kv.first] = "value";
        } else {
          MergeKey(db.get(), write_options, kv.first, kv.second);
          type_map[kv.first] = "merge";
        }
      }
      if (i < 2) {
        db->Flush(FlushOptions());
      }
    }

    for (size_t i = 0; i < 2; i++) {
      for (auto& kv : entry_maps[i]) {
        if (rnd.OneIn(10)) {
          whole_map.erase(kv.first);
          DeleteKey(db.get(), write_options, kv.first);
          entry_maps[2][kv.first] = "delete";
        }
      }
    }

    if (FLAGS_enable_print) {
      for (size_t i = 0; i < 3; i++) {
        for (auto& kv : entry_maps[i]) {
          std::cout << "[" << i << "]" << kv.first.prefix << kv.first.sorted
                    << " " << kv.second + " " + type_map[kv.first] << std::endl;
        }
      }
    }

    std::unique_ptr<Iterator> iter(db->NewIterator(read_options));
    for (uint64_t prefix = 0; prefix < 10; prefix++) {
      uint64_t start_suffix = rnd.Uniform(9);
      SeekIterator(iter.get(), prefix, start_suffix);
      auto it = whole_map.find(TestKey(prefix, start_suffix));
      if (it == whole_map.end()) {
        continue;
      }
      ASSERT_NE(it, whole_map.end());
      ASSERT_TRUE(iter->Valid());
      if (FLAGS_enable_print) {
        std::cout << "round " << prefix
                  << " iter: " << SliceToTestKey(iter->key()).prefix
                  << SliceToTestKey(iter->key()).sorted
                  << " | map: " << it->first.prefix << it->first.sorted << " | "
                  << iter->value().ToString() << " " << it->second << std::endl;
      }
      ASSERT_EQ(iter->value(), it->second);
      uint64_t stored_prefix = prefix;
      for (size_t k = 0; k < 9; k++) {
        if (rnd.OneIn(2) || it == whole_map.begin()) {
          iter->Next();
          it++;
          if (FLAGS_enable_print) {
            std::cout << "Next >> ";
          }
        } else {
          iter->Prev();
          it--;
          if (FLAGS_enable_print) {
            std::cout << "Prev >> ";
          }
        }
        if (!iter->Valid() ||
            SliceToTestKey(iter->key()).prefix != stored_prefix) {
          break;
        }
        stored_prefix = SliceToTestKey(iter->key()).prefix;
        ASSERT_TRUE(iter->Valid());
        ASSERT_NE(it, whole_map.end());
        ASSERT_EQ(iter->value(), it->second);
        if (FLAGS_enable_print) {
          std::cout << "iter: " << SliceToTestKey(iter->key()).prefix
                    << SliceToTestKey(iter->key()).sorted
                    << " | map: " << it->first.prefix << it->first.sorted
                    << " | " << iter->value().ToString() << " " << it->second
                    << std::endl;
        }
      }
    }
  }
}

TEST_F(PrefixTest, PrefixSeekModePrev2) {
  // Only for SkipListFactory
  // test the case
  //        iter1                iter2
  // | prefix | suffix |  | prefix | suffix |
  // |   1    |   1    |  |   1    |   2    |
  // |   1    |   3    |  |   1    |   4    |
  // |   2    |   1    |  |   3    |   3    |
  // |   2    |   2    |  |   3    |   4    |
  // after seek(15), iter1 will be at 21 and iter2 will be 33.
  // Then if call Prev() in prefix mode where SeekForPrev(21) gets called,
  // iter2 should turn to invalid state because of bloom filter.
  options.memtable_factory.reset(new SkipListFactory);
  options.write_buffer_size = 1024 * 1024;
  std::string v13("v13");
  DestroyDB(kDbName, Options());
  auto db = OpenDb();
  WriteOptions write_options;
  ReadOptions read_options;
  PutKey(db.get(), write_options, TestKey(1, 2), "v12");
  PutKey(db.get(), write_options, TestKey(1, 4), "v14");
  PutKey(db.get(), write_options, TestKey(3, 3), "v33");
  PutKey(db.get(), write_options, TestKey(3, 4), "v34");
  db->Flush(FlushOptions());
  reinterpret_cast<DBImpl*>(db.get())->TEST_WaitForFlushMemTable();
  PutKey(db.get(), write_options, TestKey(1, 1), "v11");
  PutKey(db.get(), write_options, TestKey(1, 3), "v13");
  PutKey(db.get(), write_options, TestKey(2, 1), "v21");
  PutKey(db.get(), write_options, TestKey(2, 2), "v22");
  db->Flush(FlushOptions());
  reinterpret_cast<DBImpl*>(db.get())->TEST_WaitForFlushMemTable();
  std::unique_ptr<Iterator> iter(db->NewIterator(read_options));
  SeekIterator(iter.get(), 1, 5);
  iter->Prev();
  ASSERT_EQ(iter->value(), v13);
}

TEST_F(PrefixTest, PrefixSeekModePrev3) {
  // Only for SkipListFactory
  // test SeekToLast() with iterate_upper_bound_ in prefix_seek_mode
  options.memtable_factory.reset(new SkipListFactory);
  options.write_buffer_size = 1024 * 1024;
  std::string v14("v14");
  TestKey upper_bound_key = TestKey(1, 5);
  std::string s;
  Slice upper_bound = TestKeyToSlice(s, upper_bound_key);

  {
    DestroyDB(kDbName, Options());
    auto db = OpenDb();
    WriteOptions write_options;
    ReadOptions read_options;
    read_options.iterate_upper_bound = &upper_bound;
    PutKey(db.get(), write_options, TestKey(1, 2), "v12");
    PutKey(db.get(), write_options, TestKey(1, 4), "v14");
    db->Flush(FlushOptions());
    reinterpret_cast<DBImpl*>(db.get())->TEST_WaitForFlushMemTable();
    PutKey(db.get(), write_options, TestKey(1, 1), "v11");
    PutKey(db.get(), write_options, TestKey(1, 3), "v13");
    PutKey(db.get(), write_options, TestKey(2, 1), "v21");
    PutKey(db.get(), write_options, TestKey(2, 2), "v22");
    db->Flush(FlushOptions());
    reinterpret_cast<DBImpl*>(db.get())->TEST_WaitForFlushMemTable();
    std::unique_ptr<Iterator> iter(db->NewIterator(read_options));
    iter->SeekToLast();
    ASSERT_EQ(iter->value(), v14);
  }
  {
    DestroyDB(kDbName, Options());
    auto db = OpenDb();
    WriteOptions write_options;
    ReadOptions read_options;
    read_options.iterate_upper_bound = &upper_bound;
    PutKey(db.get(), write_options, TestKey(1, 2), "v12");
    PutKey(db.get(), write_options, TestKey(1, 4), "v14");
    PutKey(db.get(), write_options, TestKey(3, 3), "v33");
    PutKey(db.get(), write_options, TestKey(3, 4), "v34");
    db->Flush(FlushOptions());
    reinterpret_cast<DBImpl*>(db.get())->TEST_WaitForFlushMemTable();
    PutKey(db.get(), write_options, TestKey(1, 1), "v11");
    PutKey(db.get(), write_options, TestKey(1, 3), "v13");
    db->Flush(FlushOptions());
    reinterpret_cast<DBImpl*>(db.get())->TEST_WaitForFlushMemTable();
    std::unique_ptr<Iterator> iter(db->NewIterator(read_options));
    iter->SeekToLast();
    ASSERT_EQ(iter->value(), v14);
  }
}

}  // end namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}

#endif  // GFLAGS

#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr,
          "SKIPPED as HashSkipList and HashLinkList are not supported in "
          "ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE
