//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include <array>
#include <map>
#include <string>

#include "memtable/stl_wrappers.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "util/hash.h"
#include "util/kv_map.h"
#include "util/string_util.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "utilities/merge_operators.h"

using std::unique_ptr;

namespace rocksdb {
namespace {

static const Comparator* comparator;

class KVIter : public Iterator {
 public:
  explicit KVIter(const stl_wrappers::KVMap* map)
      : map_(map), iter_(map_->end()) {}
  bool Valid() const override { return iter_ != map_->end(); }
  void SeekToFirst() override { iter_ = map_->begin(); }
  void SeekToLast() override {
    if (map_->empty()) {
      iter_ = map_->end();
    } else {
      iter_ = map_->find(map_->rbegin()->first);
    }
  }
  void Seek(const Slice& k) override {
    iter_ = map_->lower_bound(k.ToString());
  }
  void SeekForPrev(const Slice& k) override {
    iter_ = map_->upper_bound(k.ToString());
    Prev();
  }
  void Next() override { ++iter_; }
  void Prev() override {
    if (iter_ == map_->begin()) {
      iter_ = map_->end();
      return;
    }
    --iter_;
  }

  Slice key() const override { return iter_->first; }
  Slice value() const override { return iter_->second; }
  Status status() const override { return Status::OK(); }

 private:
  const stl_wrappers::KVMap* const map_;
  stl_wrappers::KVMap::const_iterator iter_;
};

void AssertItersEqual(Iterator* iter1, Iterator* iter2) {
  ASSERT_EQ(iter1->Valid(), iter2->Valid());
  if (iter1->Valid()) {
    ASSERT_EQ(iter1->key().ToString(), iter2->key().ToString());
    ASSERT_EQ(iter1->value().ToString(), iter2->value().ToString());
  }
}

// Measuring operations on DB (expect to be empty).
// source_strings are candidate keys
void DoRandomIteraratorTest(DB* db, std::vector<std::string> source_strings,
                            Random* rnd, int num_writes, int num_iter_ops,
                            int num_trigger_flush) {
  stl_wrappers::KVMap map((stl_wrappers::LessOfComparator(comparator)));

  for (int i = 0; i < num_writes; i++) {
    if (num_trigger_flush > 0 && i != 0 && i % num_trigger_flush == 0) {
      db->Flush(FlushOptions());
    }

    int type = rnd->Uniform(2);
    int index = rnd->Uniform(static_cast<int>(source_strings.size()));
    auto& key = source_strings[index];
    switch (type) {
      case 0:
        // put
        map[key] = key;
        ASSERT_OK(db->Put(WriteOptions(), key, key));
        break;
      case 1:
        // delete
        if (map.find(key) != map.end()) {
          map.erase(key);
        }
        ASSERT_OK(db->Delete(WriteOptions(), key));
        break;
      default:
        assert(false);
    }
  }

  std::unique_ptr<Iterator> iter(db->NewIterator(ReadOptions()));
  std::unique_ptr<Iterator> result_iter(new KVIter(&map));

  bool is_valid = false;
  for (int i = 0; i < num_iter_ops; i++) {
    // Random walk and make sure iter and result_iter returns the
    // same key and value
    int type = rnd->Uniform(6);
    ASSERT_OK(iter->status());
    switch (type) {
      case 0:
        // Seek to First
        iter->SeekToFirst();
        result_iter->SeekToFirst();
        break;
      case 1:
        // Seek to last
        iter->SeekToLast();
        result_iter->SeekToLast();
        break;
      case 2: {
        // Seek to random key
        auto key_idx = rnd->Uniform(static_cast<int>(source_strings.size()));
        auto key = source_strings[key_idx];
        iter->Seek(key);
        result_iter->Seek(key);
        break;
      }
      case 3:
        // Next
        if (is_valid) {
          iter->Next();
          result_iter->Next();
        } else {
          continue;
        }
        break;
      case 4:
        // Prev
        if (is_valid) {
          iter->Prev();
          result_iter->Prev();
        } else {
          continue;
        }
        break;
      default: {
        assert(type == 5);
        auto key_idx = rnd->Uniform(static_cast<int>(source_strings.size()));
        auto key = source_strings[key_idx];
        std::string result;
        auto status = db->Get(ReadOptions(), key, &result);
        if (map.find(key) == map.end()) {
          ASSERT_TRUE(status.IsNotFound());
        } else {
          ASSERT_EQ(map[key], result);
        }
        break;
      }
    }
    AssertItersEqual(iter.get(), result_iter.get());
    is_valid = iter->Valid();
  }
}

class DoubleComparator : public Comparator {
 public:
  DoubleComparator() {}

  const char* Name() const override { return "DoubleComparator"; }

  int Compare(const Slice& a, const Slice& b) const override {
#ifndef CYGWIN
    double da = std::stod(a.ToString());
    double db = std::stod(b.ToString());
#else
    double da = std::strtod(a.ToString().c_str(), 0 /* endptr */);
    double db = std::strtod(a.ToString().c_str(), 0 /* endptr */);
#endif
    if (da == db) {
      return a.compare(b);
    } else if (da > db) {
      return 1;
    } else {
      return -1;
    }
  }
  void FindShortestSeparator(std::string* /*start*/,
                             const Slice& /*limit*/) const override {}

  void FindShortSuccessor(std::string* /*key*/) const override {}
};

class HashComparator : public Comparator {
 public:
  HashComparator() {}

  const char* Name() const override { return "HashComparator"; }

  int Compare(const Slice& a, const Slice& b) const override {
    uint32_t ha = Hash(a.data(), a.size(), 66);
    uint32_t hb = Hash(b.data(), b.size(), 66);
    if (ha == hb) {
      return a.compare(b);
    } else if (ha > hb) {
      return 1;
    } else {
      return -1;
    }
  }
  void FindShortestSeparator(std::string* /*start*/,
                             const Slice& /*limit*/) const override {}

  void FindShortSuccessor(std::string* /*key*/) const override {}
};

class TwoStrComparator : public Comparator {
 public:
  TwoStrComparator() {}

  const char* Name() const override { return "TwoStrComparator"; }

  int Compare(const Slice& a, const Slice& b) const override {
    assert(a.size() >= 2);
    assert(b.size() >= 2);
    size_t size_a1 = static_cast<size_t>(a[0]);
    size_t size_b1 = static_cast<size_t>(b[0]);
    size_t size_a2 = static_cast<size_t>(a[1]);
    size_t size_b2 = static_cast<size_t>(b[1]);
    assert(size_a1 + size_a2 + 2 == a.size());
    assert(size_b1 + size_b2 + 2 == b.size());

    Slice a1 = Slice(a.data() + 2, size_a1);
    Slice b1 = Slice(b.data() + 2, size_b1);
    Slice a2 = Slice(a.data() + 2 + size_a1, size_a2);
    Slice b2 = Slice(b.data() + 2 + size_b1, size_b2);

    if (a1 != b1) {
      return a1.compare(b1);
    }
    return a2.compare(b2);
  }
  void FindShortestSeparator(std::string* /*start*/,
                             const Slice& /*limit*/) const override {}

  void FindShortSuccessor(std::string* /*key*/) const override {}
};
}  // namespace

class ComparatorDBTest
    : public testing::Test,
      virtual public ::testing::WithParamInterface<uint32_t> {
 private:
  std::string dbname_;
  Env* env_;
  DB* db_;
  Options last_options_;
  std::unique_ptr<const Comparator> comparator_guard;

 public:
  ComparatorDBTest() : env_(Env::Default()), db_(nullptr) {
    comparator = BytewiseComparator();
    dbname_ = test::PerThreadDBPath("comparator_db_test");
    BlockBasedTableOptions toptions;
    toptions.format_version = GetParam();
    last_options_.table_factory.reset(
        rocksdb::NewBlockBasedTableFactory(toptions));
    EXPECT_OK(DestroyDB(dbname_, last_options_));
  }

  ~ComparatorDBTest() override {
    delete db_;
    EXPECT_OK(DestroyDB(dbname_, last_options_));
    comparator = BytewiseComparator();
  }

  DB* GetDB() { return db_; }

  void SetOwnedComparator(const Comparator* cmp, bool owner = true) {
    if (owner) {
      comparator_guard.reset(cmp);
    } else {
      comparator_guard.reset();
    }
    comparator = cmp;
    last_options_.comparator = cmp;
  }

  // Return the current option configuration.
  Options* GetOptions() { return &last_options_; }

  void DestroyAndReopen() {
    // Destroy using last options
    Destroy();
    ASSERT_OK(TryReopen());
  }

  void Destroy() {
    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, last_options_));
  }

  Status TryReopen() {
    delete db_;
    db_ = nullptr;
    last_options_.create_if_missing = true;

    return DB::Open(last_options_, dbname_, &db_);
  }
};

INSTANTIATE_TEST_CASE_P(FormatDef, ComparatorDBTest,
                        testing::Values(test::kDefaultFormatVersion));
INSTANTIATE_TEST_CASE_P(FormatLatest, ComparatorDBTest,
                        testing::Values(test::kLatestFormatVersion));

TEST_P(ComparatorDBTest, Bytewise) {
  for (int rand_seed = 301; rand_seed < 306; rand_seed++) {
    DestroyAndReopen();
    Random rnd(rand_seed);
    DoRandomIteraratorTest(GetDB(),
                           {"a", "b", "c", "d", "e", "f", "g", "h", "i"}, &rnd,
                           8, 100, 3);
  }
}

TEST_P(ComparatorDBTest, SimpleSuffixReverseComparator) {
  SetOwnedComparator(new test::SimpleSuffixReverseComparator());

  for (int rnd_seed = 301; rnd_seed < 316; rnd_seed++) {
    Options* opt = GetOptions();
    opt->comparator = comparator;
    DestroyAndReopen();
    Random rnd(rnd_seed);

    std::vector<std::string> source_strings;
    std::vector<std::string> source_prefixes;
    // Randomly generate 5 prefixes
    for (int i = 0; i < 5; i++) {
      source_prefixes.push_back(test::RandomHumanReadableString(&rnd, 8));
    }
    for (int j = 0; j < 20; j++) {
      int prefix_index = rnd.Uniform(static_cast<int>(source_prefixes.size()));
      std::string key = source_prefixes[prefix_index] +
                        test::RandomHumanReadableString(&rnd, rnd.Uniform(8));
      source_strings.push_back(key);
    }

    DoRandomIteraratorTest(GetDB(), source_strings, &rnd, 30, 600, 66);
  }
}

TEST_P(ComparatorDBTest, Uint64Comparator) {
  SetOwnedComparator(test::Uint64Comparator(), false /* owner */);

  for (int rnd_seed = 301; rnd_seed < 316; rnd_seed++) {
    Options* opt = GetOptions();
    opt->comparator = comparator;
    DestroyAndReopen();
    Random rnd(rnd_seed);
    Random64 rnd64(rnd_seed);

    std::vector<std::string> source_strings;
    // Randomly generate source keys
    for (int i = 0; i < 100; i++) {
      uint64_t r = rnd64.Next();
      std::string str;
      str.resize(8);
      memcpy(&str[0], static_cast<void*>(&r), 8);
      source_strings.push_back(str);
    }

    DoRandomIteraratorTest(GetDB(), source_strings, &rnd, 200, 1000, 66);
  }
}

TEST_P(ComparatorDBTest, DoubleComparator) {
  SetOwnedComparator(new DoubleComparator());

  for (int rnd_seed = 301; rnd_seed < 316; rnd_seed++) {
    Options* opt = GetOptions();
    opt->comparator = comparator;
    DestroyAndReopen();
    Random rnd(rnd_seed);

    std::vector<std::string> source_strings;
    // Randomly generate source keys
    for (int i = 0; i < 100; i++) {
      uint32_t r = rnd.Next();
      uint32_t divide_order = rnd.Uniform(8);
      double to_divide = 1.0;
      for (uint32_t j = 0; j < divide_order; j++) {
        to_divide *= 10.0;
      }
      source_strings.push_back(ToString(r / to_divide));
    }

    DoRandomIteraratorTest(GetDB(), source_strings, &rnd, 200, 1000, 66);
  }
}

TEST_P(ComparatorDBTest, HashComparator) {
  SetOwnedComparator(new HashComparator());

  for (int rnd_seed = 301; rnd_seed < 316; rnd_seed++) {
    Options* opt = GetOptions();
    opt->comparator = comparator;
    DestroyAndReopen();
    Random rnd(rnd_seed);

    std::vector<std::string> source_strings;
    // Randomly generate source keys
    for (int i = 0; i < 100; i++) {
      source_strings.push_back(test::RandomKey(&rnd, 8));
    }

    DoRandomIteraratorTest(GetDB(), source_strings, &rnd, 200, 1000, 66);
  }
}

TEST_P(ComparatorDBTest, TwoStrComparator) {
  SetOwnedComparator(new TwoStrComparator());

  for (int rnd_seed = 301; rnd_seed < 316; rnd_seed++) {
    Options* opt = GetOptions();
    opt->comparator = comparator;
    DestroyAndReopen();
    Random rnd(rnd_seed);

    std::vector<std::string> source_strings;
    // Randomly generate source keys
    for (int i = 0; i < 100; i++) {
      std::string str;
      uint32_t size1 = rnd.Uniform(8);
      uint32_t size2 = rnd.Uniform(8);
      str.append(1, static_cast<char>(size1));
      str.append(1, static_cast<char>(size2));
      str.append(test::RandomKey(&rnd, size1));
      str.append(test::RandomKey(&rnd, size2));
      source_strings.push_back(str);
    }

    DoRandomIteraratorTest(GetDB(), source_strings, &rnd, 200, 1000, 66);
  }
}

TEST_P(ComparatorDBTest, IsSameLengthImmediateSuccessor) {
  {
    // different length
    Slice s("abcxy");
    Slice t("abcxyz");
    ASSERT_FALSE(BytewiseComparator()->IsSameLengthImmediateSuccessor(s, t));
  }
  {
    Slice s("abcxyz");
    Slice t("abcxy");
    ASSERT_FALSE(BytewiseComparator()->IsSameLengthImmediateSuccessor(s, t));
  }
  {
    // not last byte different
    Slice s("abc1xyz");
    Slice t("abc2xyz");
    ASSERT_FALSE(BytewiseComparator()->IsSameLengthImmediateSuccessor(s, t));
  }
  {
    // same string
    Slice s("abcxyz");
    Slice t("abcxyz");
    ASSERT_FALSE(BytewiseComparator()->IsSameLengthImmediateSuccessor(s, t));
  }
  {
    Slice s("abcxy");
    Slice t("abcxz");
    ASSERT_TRUE(BytewiseComparator()->IsSameLengthImmediateSuccessor(s, t));
  }
  {
    Slice s("abcxz");
    Slice t("abcxy");
    ASSERT_FALSE(BytewiseComparator()->IsSameLengthImmediateSuccessor(s, t));
  }
  {
    const char s_array[] = "\x50\x8a\xac";
    const char t_array[] = "\x50\x8a\xad";
    Slice s(s_array);
    Slice t(t_array);
    ASSERT_TRUE(BytewiseComparator()->IsSameLengthImmediateSuccessor(s, t));
  }
  {
    const char s_array[] = "\x50\x8a\xff";
    const char t_array[] = "\x50\x8b\x00";
    Slice s(s_array, 3);
    Slice t(t_array, 3);
    ASSERT_TRUE(BytewiseComparator()->IsSameLengthImmediateSuccessor(s, t));
  }
  {
    const char s_array[] = "\x50\x8a\xff\xff";
    const char t_array[] = "\x50\x8b\x00\x00";
    Slice s(s_array, 4);
    Slice t(t_array, 4);
    ASSERT_TRUE(BytewiseComparator()->IsSameLengthImmediateSuccessor(s, t));
  }
  {
    const char s_array[] = "\x50\x8a\xff\xff";
    const char t_array[] = "\x50\x8b\x00\x01";
    Slice s(s_array, 4);
    Slice t(t_array, 4);
    ASSERT_FALSE(BytewiseComparator()->IsSameLengthImmediateSuccessor(s, t));
  }
}

TEST_P(ComparatorDBTest, FindShortestSeparator) {
  std::string s1 = "abc1xyz";
  std::string s2 = "abc3xy";

  BytewiseComparator()->FindShortestSeparator(&s1, s2);
  ASSERT_EQ("abc2", s1);

  s1 = "abc5xyztt";

  ReverseBytewiseComparator()->FindShortestSeparator(&s1, s2);
  ASSERT_EQ("abc5", s1);

  s1 = "abc3";
  s2 = "abc2xy";
  ReverseBytewiseComparator()->FindShortestSeparator(&s1, s2);
  ASSERT_EQ("abc3", s1);

  s1 = "abc3xyz";
  s2 = "abc2xy";
  ReverseBytewiseComparator()->FindShortestSeparator(&s1, s2);
  ASSERT_EQ("abc3", s1);

  s1 = "abc3xyz";
  s2 = "abc2";
  ReverseBytewiseComparator()->FindShortestSeparator(&s1, s2);
  ASSERT_EQ("abc3", s1);

  std::string old_s1 = s1 = "abc2xy";
  s2 = "abc2";
  ReverseBytewiseComparator()->FindShortestSeparator(&s1, s2);
  ASSERT_TRUE(old_s1 >= s1);
  ASSERT_TRUE(s1 > s2);
}

TEST_P(ComparatorDBTest, SeparatorSuccessorRandomizeTest) {
  // Char list for boundary cases.
  std::array<unsigned char, 6> char_list{{0, 1, 2, 253, 254, 255}};
  Random rnd(301);

  for (int attempts = 0; attempts < 1000; attempts++) {
    uint32_t size1 = rnd.Skewed(4);
    uint32_t size2;

    if (rnd.OneIn(2)) {
      // size2 to be random size
      size2 = rnd.Skewed(4);
    } else {
      // size1 is within [-2, +2] of size1
      int diff = static_cast<int>(rnd.Uniform(5)) - 2;
      int tmp_size2 = static_cast<int>(size1) + diff;
      if (tmp_size2 < 0) {
        tmp_size2 = 0;
      }
      size2 = static_cast<uint32_t>(tmp_size2);
    }

    std::string s1;
    std::string s2;
    for (uint32_t i = 0; i < size1; i++) {
      if (rnd.OneIn(2)) {
        // Use random byte
        s1 += static_cast<char>(rnd.Uniform(256));
      } else {
        // Use one byte in char_list
        char c = static_cast<char>(char_list[rnd.Uniform(sizeof(char_list))]);
        s1 += c;
      }
    }

    // First set s2 to be the same as s1, and then modify s2.
    s2 = s1;
    s2.resize(size2);
    // We start from the back of the string
    if (size2 > 0) {
      uint32_t pos = size2 - 1;
      do {
        if (pos >= size1 || rnd.OneIn(4)) {
          // For 1/4 chance, use random byte
          s2[pos] = static_cast<char>(rnd.Uniform(256));
        } else if (rnd.OneIn(4)) {
          // In 1/4 chance, stop here.
          break;
        } else {
          // Create a char within [-2, +2] of the matching char of s1.
          int diff = static_cast<int>(rnd.Uniform(5)) - 2;
          // char may be signed or unsigned based on platform.
          int s1_char = static_cast<int>(static_cast<unsigned char>(s1[pos]));
          int s2_char = s1_char + diff;
          if (s2_char < 0) {
            s2_char = 0;
          }
          if (s2_char > 255) {
            s2_char = 255;
          }
          s2[pos] = static_cast<char>(s2_char);
        }
      } while (pos-- != 0);
    }

    // Test separators
    for (int rev = 0; rev < 2; rev++) {
      if (rev == 1) {
        // switch s1 and s2
        std::string t = s1;
        s1 = s2;
        s2 = t;
      }
      std::string separator = s1;
      BytewiseComparator()->FindShortestSeparator(&separator, s2);
      std::string rev_separator = s1;
      ReverseBytewiseComparator()->FindShortestSeparator(&rev_separator, s2);

      if (s1 == s2) {
        ASSERT_EQ(s1, separator);
        ASSERT_EQ(s2, rev_separator);
      } else if (s1 < s2) {
        ASSERT_TRUE(s1 <= separator);
        ASSERT_TRUE(s2 > separator);
        ASSERT_LE(separator.size(), std::max(s1.size(), s2.size()));
        ASSERT_EQ(s1, rev_separator);
      } else {
        ASSERT_TRUE(s1 >= rev_separator);
        ASSERT_TRUE(s2 < rev_separator);
        ASSERT_LE(rev_separator.size(), std::max(s1.size(), s2.size()));
        ASSERT_EQ(s1, separator);
      }
    }

    // Test successors
    std::string succ = s1;
    BytewiseComparator()->FindShortSuccessor(&succ);
    ASSERT_TRUE(succ >= s1);

    succ = s1;
    ReverseBytewiseComparator()->FindShortSuccessor(&succ);
    ASSERT_TRUE(succ <= s1);
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
