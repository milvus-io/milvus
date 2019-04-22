//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/full_filter_block.h"

#include "rocksdb/filter_policy.h"
#include "table/full_filter_bits_builder.h"
#include "util/coding.h"
#include "util/hash.h"
#include "util/string_util.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {

class TestFilterBitsBuilder : public FilterBitsBuilder {
 public:
  explicit TestFilterBitsBuilder() {}

  // Add Key to filter
  void AddKey(const Slice& key) override {
    hash_entries_.push_back(Hash(key.data(), key.size(), 1));
  }

  // Generate the filter using the keys that are added
  Slice Finish(std::unique_ptr<const char[]>* buf) override {
    uint32_t len = static_cast<uint32_t>(hash_entries_.size()) * 4;
    char* data = new char[len];
    for (size_t i = 0; i < hash_entries_.size(); i++) {
      EncodeFixed32(data + i * 4, hash_entries_[i]);
    }
    const char* const_data = data;
    buf->reset(const_data);
    return Slice(data, len);
  }

 private:
  std::vector<uint32_t> hash_entries_;
};

class TestFilterBitsReader : public FilterBitsReader {
 public:
  explicit TestFilterBitsReader(const Slice& contents)
      : data_(contents.data()), len_(static_cast<uint32_t>(contents.size())) {}

  bool MayMatch(const Slice& entry) override {
    uint32_t h = Hash(entry.data(), entry.size(), 1);
    for (size_t i = 0; i + 4 <= len_; i += 4) {
      if (h == DecodeFixed32(data_ + i)) {
        return true;
      }
    }
    return false;
  }

 private:
  const char* data_;
  uint32_t len_;
};


class TestHashFilter : public FilterPolicy {
 public:
  const char* Name() const override { return "TestHashFilter"; }

  void CreateFilter(const Slice* keys, int n, std::string* dst) const override {
    for (int i = 0; i < n; i++) {
      uint32_t h = Hash(keys[i].data(), keys[i].size(), 1);
      PutFixed32(dst, h);
    }
  }

  bool KeyMayMatch(const Slice& key, const Slice& filter) const override {
    uint32_t h = Hash(key.data(), key.size(), 1);
    for (unsigned int i = 0; i + 4 <= filter.size(); i += 4) {
      if (h == DecodeFixed32(filter.data() + i)) {
        return true;
      }
    }
    return false;
  }

  FilterBitsBuilder* GetFilterBitsBuilder() const override {
    return new TestFilterBitsBuilder();
  }

  FilterBitsReader* GetFilterBitsReader(const Slice& contents) const override {
    return new TestFilterBitsReader(contents);
  }
};

class PluginFullFilterBlockTest : public testing::Test {
 public:
  BlockBasedTableOptions table_options_;

  PluginFullFilterBlockTest() {
    table_options_.filter_policy.reset(new TestHashFilter());
  }
};

TEST_F(PluginFullFilterBlockTest, PluginEmptyBuilder) {
  FullFilterBlockBuilder builder(
      nullptr, true, table_options_.filter_policy->GetFilterBitsBuilder());
  Slice block = builder.Finish();
  ASSERT_EQ("", EscapeString(block));

  FullFilterBlockReader reader(
      nullptr, true, block,
      table_options_.filter_policy->GetFilterBitsReader(block), nullptr);
  // Remain same symantic with blockbased filter
  ASSERT_TRUE(reader.KeyMayMatch("foo", nullptr));
}

TEST_F(PluginFullFilterBlockTest, PluginSingleChunk) {
  FullFilterBlockBuilder builder(
      nullptr, true, table_options_.filter_policy->GetFilterBitsBuilder());
  builder.Add("foo");
  builder.Add("bar");
  builder.Add("box");
  builder.Add("box");
  builder.Add("hello");
  Slice block = builder.Finish();
  FullFilterBlockReader reader(
      nullptr, true, block,
      table_options_.filter_policy->GetFilterBitsReader(block), nullptr);
  ASSERT_TRUE(reader.KeyMayMatch("foo", nullptr));
  ASSERT_TRUE(reader.KeyMayMatch("bar", nullptr));
  ASSERT_TRUE(reader.KeyMayMatch("box", nullptr));
  ASSERT_TRUE(reader.KeyMayMatch("hello", nullptr));
  ASSERT_TRUE(reader.KeyMayMatch("foo", nullptr));
  ASSERT_TRUE(!reader.KeyMayMatch("missing", nullptr));
  ASSERT_TRUE(!reader.KeyMayMatch("other", nullptr));
}

class FullFilterBlockTest : public testing::Test {
 public:
  BlockBasedTableOptions table_options_;

  FullFilterBlockTest() {
    table_options_.filter_policy.reset(NewBloomFilterPolicy(10, false));
  }

  ~FullFilterBlockTest() override {}
};

TEST_F(FullFilterBlockTest, EmptyBuilder) {
  FullFilterBlockBuilder builder(
      nullptr, true, table_options_.filter_policy->GetFilterBitsBuilder());
  Slice block = builder.Finish();
  ASSERT_EQ("", EscapeString(block));

  FullFilterBlockReader reader(
      nullptr, true, block,
      table_options_.filter_policy->GetFilterBitsReader(block), nullptr);
  // Remain same symantic with blockbased filter
  ASSERT_TRUE(reader.KeyMayMatch("foo", nullptr));
}

TEST_F(FullFilterBlockTest, DuplicateEntries) {
  {  // empty prefixes
    std::unique_ptr<const SliceTransform> prefix_extractor(
        NewFixedPrefixTransform(0));
    auto bits_builder = dynamic_cast<FullFilterBitsBuilder*>(
        table_options_.filter_policy->GetFilterBitsBuilder());
    const bool WHOLE_KEY = true;
    FullFilterBlockBuilder builder(prefix_extractor.get(), WHOLE_KEY,
                                   bits_builder);
    ASSERT_EQ(0, builder.NumAdded());
    builder.Add("key");  // test with empty prefix
    ASSERT_EQ(2, bits_builder->hash_entries_.size());
  }

  // mix of empty and non-empty
  std::unique_ptr<const SliceTransform> prefix_extractor(
      NewFixedPrefixTransform(7));
  auto bits_builder = dynamic_cast<FullFilterBitsBuilder*>(
      table_options_.filter_policy->GetFilterBitsBuilder());
  const bool WHOLE_KEY = true;
  FullFilterBlockBuilder builder(prefix_extractor.get(), WHOLE_KEY,
                                 bits_builder);
  ASSERT_EQ(0, builder.NumAdded());
  builder.Add("");  // test with empty key too
  builder.Add("prefix1key1");
  builder.Add("prefix1key1");
  builder.Add("prefix1key2");
  builder.Add("prefix1key3");
  builder.Add("prefix2key4");
  // two prefix adn 4 keys
  ASSERT_EQ(1 + 2 + 4, bits_builder->hash_entries_.size());
}

TEST_F(FullFilterBlockTest, SingleChunk) {
  FullFilterBlockBuilder builder(
      nullptr, true, table_options_.filter_policy->GetFilterBitsBuilder());
  ASSERT_EQ(0, builder.NumAdded());
  builder.Add("foo");
  builder.Add("bar");
  builder.Add("box");
  builder.Add("box");
  builder.Add("hello");
  ASSERT_EQ(5, builder.NumAdded());
  Slice block = builder.Finish();
  FullFilterBlockReader reader(
      nullptr, true, block,
      table_options_.filter_policy->GetFilterBitsReader(block), nullptr);
  ASSERT_TRUE(reader.KeyMayMatch("foo", nullptr));
  ASSERT_TRUE(reader.KeyMayMatch("bar", nullptr));
  ASSERT_TRUE(reader.KeyMayMatch("box", nullptr));
  ASSERT_TRUE(reader.KeyMayMatch("hello", nullptr));
  ASSERT_TRUE(reader.KeyMayMatch("foo", nullptr));
  ASSERT_TRUE(!reader.KeyMayMatch("missing", nullptr));
  ASSERT_TRUE(!reader.KeyMayMatch("other", nullptr));
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
