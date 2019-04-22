//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <memory>
#include <string>

#include "db/db_test_util.h"
#include "db/memtable.h"
#include "db/range_del_aggregator.h"
#include "port/stack_trace.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/slice_transform.h"

namespace rocksdb {

class DBMemTableTest : public DBTestBase {
 public:
  DBMemTableTest() : DBTestBase("/db_memtable_test") {}
};

class MockMemTableRep : public MemTableRep {
 public:
  explicit MockMemTableRep(Allocator* allocator, MemTableRep* rep)
      : MemTableRep(allocator), rep_(rep), num_insert_with_hint_(0) {}

  KeyHandle Allocate(const size_t len, char** buf) override {
    return rep_->Allocate(len, buf);
  }

  void Insert(KeyHandle handle) override { rep_->Insert(handle); }

  void InsertWithHint(KeyHandle handle, void** hint) override {
    num_insert_with_hint_++;
    EXPECT_NE(nullptr, hint);
    last_hint_in_ = *hint;
    rep_->InsertWithHint(handle, hint);
    last_hint_out_ = *hint;
  }

  bool Contains(const char* key) const override { return rep_->Contains(key); }

  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override {
    rep_->Get(k, callback_args, callback_func);
  }

  size_t ApproximateMemoryUsage() override {
    return rep_->ApproximateMemoryUsage();
  }

  Iterator* GetIterator(Arena* arena) override {
    return rep_->GetIterator(arena);
  }

  void* last_hint_in() { return last_hint_in_; }
  void* last_hint_out() { return last_hint_out_; }
  int num_insert_with_hint() { return num_insert_with_hint_; }

 private:
  std::unique_ptr<MemTableRep> rep_;
  void* last_hint_in_;
  void* last_hint_out_;
  int num_insert_with_hint_;
};

class MockMemTableRepFactory : public MemTableRepFactory {
 public:
  MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator& cmp,
                                 Allocator* allocator,
                                 const SliceTransform* transform,
                                 Logger* logger) override {
    SkipListFactory factory;
    MemTableRep* skiplist_rep =
        factory.CreateMemTableRep(cmp, allocator, transform, logger);
    mock_rep_ = new MockMemTableRep(allocator, skiplist_rep);
    return mock_rep_;
  }

  MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator& cmp,
                                 Allocator* allocator,
                                 const SliceTransform* transform,
                                 Logger* logger,
                                 uint32_t column_family_id) override {
    last_column_family_id_ = column_family_id;
    return CreateMemTableRep(cmp, allocator, transform, logger);
  }

  const char* Name() const override { return "MockMemTableRepFactory"; }

  MockMemTableRep* rep() { return mock_rep_; }

  bool IsInsertConcurrentlySupported() const override { return false; }

  uint32_t GetLastColumnFamilyId() { return last_column_family_id_; }

 private:
  MockMemTableRep* mock_rep_;
  // workaround since there's no port::kMaxUint32 yet.
  uint32_t last_column_family_id_ = static_cast<uint32_t>(-1);
};

class TestPrefixExtractor : public SliceTransform {
 public:
  const char* Name() const override { return "TestPrefixExtractor"; }

  Slice Transform(const Slice& key) const override {
    const char* p = separator(key);
    if (p == nullptr) {
      return Slice();
    }
    return Slice(key.data(), p - key.data() + 1);
  }

  bool InDomain(const Slice& key) const override {
    return separator(key) != nullptr;
  }

  bool InRange(const Slice& /*key*/) const override { return false; }

 private:
  const char* separator(const Slice& key) const {
    return reinterpret_cast<const char*>(memchr(key.data(), '_', key.size()));
  }
};

// Test that ::Add properly returns false when inserting duplicate keys
TEST_F(DBMemTableTest, DuplicateSeq) {
  SequenceNumber seq = 123;
  std::string value;
  Status s;
  MergeContext merge_context;
  Options options;
  InternalKeyComparator ikey_cmp(options.comparator);
  ReadRangeDelAggregator range_del_agg(&ikey_cmp,
                                       kMaxSequenceNumber /* upper_bound */);

  // Create a MemTable
  InternalKeyComparator cmp(BytewiseComparator());
  auto factory = std::make_shared<SkipListFactory>();
  options.memtable_factory = factory;
  ImmutableCFOptions ioptions(options);
  WriteBufferManager wb(options.db_write_buffer_size);
  MemTable* mem = new MemTable(cmp, ioptions, MutableCFOptions(options), &wb,
                               kMaxSequenceNumber, 0 /* column_family_id */);

  // Write some keys and make sure it returns false on duplicates
  bool res;
  res = mem->Add(seq, kTypeValue, "key", "value2");
  ASSERT_TRUE(res);
  res = mem->Add(seq, kTypeValue, "key", "value2");
  ASSERT_FALSE(res);
  // Changing the type should still cause the duplicatae key
  res = mem->Add(seq, kTypeMerge, "key", "value2");
  ASSERT_FALSE(res);
  // Changing the seq number will make the key fresh
  res = mem->Add(seq + 1, kTypeMerge, "key", "value2");
  ASSERT_TRUE(res);
  // Test with different types for duplicate keys
  res = mem->Add(seq, kTypeDeletion, "key", "");
  ASSERT_FALSE(res);
  res = mem->Add(seq, kTypeSingleDeletion, "key", "");
  ASSERT_FALSE(res);

  // Test the duplicate keys under stress
  for (int i = 0; i < 10000; i++) {
    bool insert_dup = i % 10 == 1;
    if (!insert_dup) {
      seq++;
    }
    res = mem->Add(seq, kTypeValue, "foo", "value" + ToString(seq));
    if (insert_dup) {
      ASSERT_FALSE(res);
    } else {
      ASSERT_TRUE(res);
    }
  }
  delete mem;

  // Test with InsertWithHint
  options.memtable_insert_with_hint_prefix_extractor.reset(
      new TestPrefixExtractor());  // which uses _ to extract the prefix
  ioptions = ImmutableCFOptions(options);
  mem = new MemTable(cmp, ioptions, MutableCFOptions(options), &wb,
                     kMaxSequenceNumber, 0 /* column_family_id */);
  // Insert a duplicate key with _ in it
  res = mem->Add(seq, kTypeValue, "key_1", "value");
  ASSERT_TRUE(res);
  res = mem->Add(seq, kTypeValue, "key_1", "value");
  ASSERT_FALSE(res);
  delete mem;

  // Test when InsertConcurrently will be invoked
  options.allow_concurrent_memtable_write = true;
  ioptions = ImmutableCFOptions(options);
  mem = new MemTable(cmp, ioptions, MutableCFOptions(options), &wb,
                     kMaxSequenceNumber, 0 /* column_family_id */);
  MemTablePostProcessInfo post_process_info;
  res = mem->Add(seq, kTypeValue, "key", "value", true, &post_process_info);
  ASSERT_TRUE(res);
  res = mem->Add(seq, kTypeValue, "key", "value", true, &post_process_info);
  ASSERT_FALSE(res);
  delete mem;
}

TEST_F(DBMemTableTest, InsertWithHint) {
  Options options;
  options.allow_concurrent_memtable_write = false;
  options.create_if_missing = true;
  options.memtable_factory.reset(new MockMemTableRepFactory());
  options.memtable_insert_with_hint_prefix_extractor.reset(
      new TestPrefixExtractor());
  options.env = env_;
  Reopen(options);
  MockMemTableRep* rep =
      reinterpret_cast<MockMemTableRepFactory*>(options.memtable_factory.get())
          ->rep();
  ASSERT_OK(Put("foo_k1", "foo_v1"));
  ASSERT_EQ(nullptr, rep->last_hint_in());
  void* hint_foo = rep->last_hint_out();
  ASSERT_OK(Put("foo_k2", "foo_v2"));
  ASSERT_EQ(hint_foo, rep->last_hint_in());
  ASSERT_EQ(hint_foo, rep->last_hint_out());
  ASSERT_OK(Put("foo_k3", "foo_v3"));
  ASSERT_EQ(hint_foo, rep->last_hint_in());
  ASSERT_EQ(hint_foo, rep->last_hint_out());
  ASSERT_OK(Put("bar_k1", "bar_v1"));
  ASSERT_EQ(nullptr, rep->last_hint_in());
  void* hint_bar = rep->last_hint_out();
  ASSERT_NE(hint_foo, hint_bar);
  ASSERT_OK(Put("bar_k2", "bar_v2"));
  ASSERT_EQ(hint_bar, rep->last_hint_in());
  ASSERT_EQ(hint_bar, rep->last_hint_out());
  ASSERT_EQ(5, rep->num_insert_with_hint());
  ASSERT_OK(Put("whitelisted", "vvv"));
  ASSERT_EQ(5, rep->num_insert_with_hint());
  ASSERT_EQ("foo_v1", Get("foo_k1"));
  ASSERT_EQ("foo_v2", Get("foo_k2"));
  ASSERT_EQ("foo_v3", Get("foo_k3"));
  ASSERT_EQ("bar_v1", Get("bar_k1"));
  ASSERT_EQ("bar_v2", Get("bar_k2"));
  ASSERT_EQ("vvv", Get("whitelisted"));
}

TEST_F(DBMemTableTest, ColumnFamilyId) {
  // Verifies MemTableRepFactory is told the right column family id.
  Options options;
  options.allow_concurrent_memtable_write = false;
  options.create_if_missing = true;
  options.memtable_factory.reset(new MockMemTableRepFactory());
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  for (int cf = 0; cf < 2; ++cf) {
    ASSERT_OK(Put(cf, "key", "val"));
    ASSERT_OK(Flush(cf));
    ASSERT_EQ(
        cf, static_cast<MockMemTableRepFactory*>(options.memtable_factory.get())
                ->GetLastColumnFamilyId());
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
