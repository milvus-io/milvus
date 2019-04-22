//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/db.h"

#include <memory>
#include "db/column_family.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "rocksdb/env.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/scoped_arena_iterator.h"
#include "util/string_util.h"
#include "util/testharness.h"

namespace rocksdb {

static std::string PrintContents(WriteBatch* b) {
  InternalKeyComparator cmp(BytewiseComparator());
  auto factory = std::make_shared<SkipListFactory>();
  Options options;
  options.memtable_factory = factory;
  ImmutableCFOptions ioptions(options);
  WriteBufferManager wb(options.db_write_buffer_size);
  MemTable* mem = new MemTable(cmp, ioptions, MutableCFOptions(options), &wb,
                               kMaxSequenceNumber, 0 /* column_family_id */);
  mem->Ref();
  std::string state;
  ColumnFamilyMemTablesDefault cf_mems_default(mem);
  Status s = WriteBatchInternal::InsertInto(b, &cf_mems_default, nullptr);
  int count = 0;
  int put_count = 0;
  int delete_count = 0;
  int single_delete_count = 0;
  int delete_range_count = 0;
  int merge_count = 0;
  for (int i = 0; i < 2; ++i) {
    Arena arena;
    ScopedArenaIterator arena_iter_guard;
    std::unique_ptr<InternalIterator> iter_guard;
    InternalIterator* iter;
    if (i == 0) {
      iter = mem->NewIterator(ReadOptions(), &arena);
      arena_iter_guard.set(iter);
    } else {
      iter = mem->NewRangeTombstoneIterator(ReadOptions(),
                                            kMaxSequenceNumber /* read_seq */);
      iter_guard.reset(iter);
    }
    if (iter == nullptr) {
      continue;
    }
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ParsedInternalKey ikey;
      ikey.clear();
      EXPECT_TRUE(ParseInternalKey(iter->key(), &ikey));
      switch (ikey.type) {
        case kTypeValue:
          state.append("Put(");
          state.append(ikey.user_key.ToString());
          state.append(", ");
          state.append(iter->value().ToString());
          state.append(")");
          count++;
          put_count++;
          break;
        case kTypeDeletion:
          state.append("Delete(");
          state.append(ikey.user_key.ToString());
          state.append(")");
          count++;
          delete_count++;
          break;
        case kTypeSingleDeletion:
          state.append("SingleDelete(");
          state.append(ikey.user_key.ToString());
          state.append(")");
          count++;
          single_delete_count++;
          break;
        case kTypeRangeDeletion:
          state.append("DeleteRange(");
          state.append(ikey.user_key.ToString());
          state.append(", ");
          state.append(iter->value().ToString());
          state.append(")");
          count++;
          delete_range_count++;
          break;
        case kTypeMerge:
          state.append("Merge(");
          state.append(ikey.user_key.ToString());
          state.append(", ");
          state.append(iter->value().ToString());
          state.append(")");
          count++;
          merge_count++;
          break;
        default:
          assert(false);
          break;
      }
      state.append("@");
      state.append(NumberToString(ikey.sequence));
    }
  }
  EXPECT_EQ(b->HasPut(), put_count > 0);
  EXPECT_EQ(b->HasDelete(), delete_count > 0);
  EXPECT_EQ(b->HasSingleDelete(), single_delete_count > 0);
  EXPECT_EQ(b->HasDeleteRange(), delete_range_count > 0);
  EXPECT_EQ(b->HasMerge(), merge_count > 0);
  if (!s.ok()) {
    state.append(s.ToString());
  } else if (count != WriteBatchInternal::Count(b)) {
    state.append("CountMismatch()");
  }
  delete mem->Unref();
  return state;
}

class WriteBatchTest : public testing::Test {};

TEST_F(WriteBatchTest, Empty) {
  WriteBatch batch;
  ASSERT_EQ("", PrintContents(&batch));
  ASSERT_EQ(0, WriteBatchInternal::Count(&batch));
  ASSERT_EQ(0, batch.Count());
}

TEST_F(WriteBatchTest, Multiple) {
  WriteBatch batch;
  batch.Put(Slice("foo"), Slice("bar"));
  batch.Delete(Slice("box"));
  batch.DeleteRange(Slice("bar"), Slice("foo"));
  batch.Put(Slice("baz"), Slice("boo"));
  WriteBatchInternal::SetSequence(&batch, 100);
  ASSERT_EQ(100U, WriteBatchInternal::Sequence(&batch));
  ASSERT_EQ(4, WriteBatchInternal::Count(&batch));
  ASSERT_EQ(
      "Put(baz, boo)@103"
      "Delete(box)@101"
      "Put(foo, bar)@100"
      "DeleteRange(bar, foo)@102",
      PrintContents(&batch));
  ASSERT_EQ(4, batch.Count());
}

TEST_F(WriteBatchTest, Corruption) {
  WriteBatch batch;
  batch.Put(Slice("foo"), Slice("bar"));
  batch.Delete(Slice("box"));
  WriteBatchInternal::SetSequence(&batch, 200);
  Slice contents = WriteBatchInternal::Contents(&batch);
  WriteBatchInternal::SetContents(&batch,
                                  Slice(contents.data(),contents.size()-1));
  ASSERT_EQ("Put(foo, bar)@200"
            "Corruption: bad WriteBatch Delete",
            PrintContents(&batch));
}

TEST_F(WriteBatchTest, Append) {
  WriteBatch b1, b2;
  WriteBatchInternal::SetSequence(&b1, 200);
  WriteBatchInternal::SetSequence(&b2, 300);
  WriteBatchInternal::Append(&b1, &b2);
  ASSERT_EQ("",
            PrintContents(&b1));
  ASSERT_EQ(0, b1.Count());
  b2.Put("a", "va");
  WriteBatchInternal::Append(&b1, &b2);
  ASSERT_EQ("Put(a, va)@200",
            PrintContents(&b1));
  ASSERT_EQ(1, b1.Count());
  b2.Clear();
  b2.Put("b", "vb");
  WriteBatchInternal::Append(&b1, &b2);
  ASSERT_EQ("Put(a, va)@200"
            "Put(b, vb)@201",
            PrintContents(&b1));
  ASSERT_EQ(2, b1.Count());
  b2.Delete("foo");
  WriteBatchInternal::Append(&b1, &b2);
  ASSERT_EQ("Put(a, va)@200"
            "Put(b, vb)@202"
            "Put(b, vb)@201"
            "Delete(foo)@203",
            PrintContents(&b1));
  ASSERT_EQ(4, b1.Count());
  b2.Clear();
  b2.Put("c", "cc");
  b2.Put("d", "dd");
  b2.MarkWalTerminationPoint();
  b2.Put("e", "ee");
  WriteBatchInternal::Append(&b1, &b2, /*wal only*/ true);
  ASSERT_EQ(
      "Put(a, va)@200"
      "Put(b, vb)@202"
      "Put(b, vb)@201"
      "Put(c, cc)@204"
      "Put(d, dd)@205"
      "Delete(foo)@203",
      PrintContents(&b1));
  ASSERT_EQ(6, b1.Count());
  ASSERT_EQ(
      "Put(c, cc)@0"
      "Put(d, dd)@1"
      "Put(e, ee)@2",
      PrintContents(&b2));
  ASSERT_EQ(3, b2.Count());
}

TEST_F(WriteBatchTest, SingleDeletion) {
  WriteBatch batch;
  WriteBatchInternal::SetSequence(&batch, 100);
  ASSERT_EQ("", PrintContents(&batch));
  ASSERT_EQ(0, batch.Count());
  batch.Put("a", "va");
  ASSERT_EQ("Put(a, va)@100", PrintContents(&batch));
  ASSERT_EQ(1, batch.Count());
  batch.SingleDelete("a");
  ASSERT_EQ(
      "SingleDelete(a)@101"
      "Put(a, va)@100",
      PrintContents(&batch));
  ASSERT_EQ(2, batch.Count());
}

namespace {
  struct TestHandler : public WriteBatch::Handler {
    std::string seen;
    Status PutCF(uint32_t column_family_id, const Slice& key,
                 const Slice& value) override {
      if (column_family_id == 0) {
        seen += "Put(" + key.ToString() + ", " + value.ToString() + ")";
      } else {
        seen += "PutCF(" + ToString(column_family_id) + ", " +
                key.ToString() + ", " + value.ToString() + ")";
      }
      return Status::OK();
    }
    Status DeleteCF(uint32_t column_family_id, const Slice& key) override {
      if (column_family_id == 0) {
        seen += "Delete(" + key.ToString() + ")";
      } else {
        seen += "DeleteCF(" + ToString(column_family_id) + ", " +
                key.ToString() + ")";
      }
      return Status::OK();
    }
    Status SingleDeleteCF(uint32_t column_family_id,
                          const Slice& key) override {
      if (column_family_id == 0) {
        seen += "SingleDelete(" + key.ToString() + ")";
      } else {
        seen += "SingleDeleteCF(" + ToString(column_family_id) + ", " +
                key.ToString() + ")";
      }
      return Status::OK();
    }
    Status DeleteRangeCF(uint32_t column_family_id, const Slice& begin_key,
                         const Slice& end_key) override {
      if (column_family_id == 0) {
        seen += "DeleteRange(" + begin_key.ToString() + ", " +
                end_key.ToString() + ")";
      } else {
        seen += "DeleteRangeCF(" + ToString(column_family_id) + ", " +
                begin_key.ToString() + ", " + end_key.ToString() + ")";
      }
      return Status::OK();
    }
    Status MergeCF(uint32_t column_family_id, const Slice& key,
                   const Slice& value) override {
      if (column_family_id == 0) {
        seen += "Merge(" + key.ToString() + ", " + value.ToString() + ")";
      } else {
        seen += "MergeCF(" + ToString(column_family_id) + ", " +
                key.ToString() + ", " + value.ToString() + ")";
      }
      return Status::OK();
    }
    void LogData(const Slice& blob) override {
      seen += "LogData(" + blob.ToString() + ")";
    }
    Status MarkBeginPrepare(bool unprepare) override {
      seen +=
          "MarkBeginPrepare(" + std::string(unprepare ? "true" : "false") + ")";
      return Status::OK();
    }
    Status MarkEndPrepare(const Slice& xid) override {
      seen += "MarkEndPrepare(" + xid.ToString() + ")";
      return Status::OK();
    }
    Status MarkNoop(bool empty_batch) override {
      seen += "MarkNoop(" + std::string(empty_batch ? "true" : "false") + ")";
      return Status::OK();
    }
    Status MarkCommit(const Slice& xid) override {
      seen += "MarkCommit(" + xid.ToString() + ")";
      return Status::OK();
    }
    Status MarkRollback(const Slice& xid) override {
      seen += "MarkRollback(" + xid.ToString() + ")";
      return Status::OK();
    }
  };
}

TEST_F(WriteBatchTest, PutNotImplemented) {
  WriteBatch batch;
  batch.Put(Slice("k1"), Slice("v1"));
  ASSERT_EQ(1, batch.Count());
  ASSERT_EQ("Put(k1, v1)@0", PrintContents(&batch));

  WriteBatch::Handler handler;
  ASSERT_OK(batch.Iterate(&handler));
}

TEST_F(WriteBatchTest, DeleteNotImplemented) {
  WriteBatch batch;
  batch.Delete(Slice("k2"));
  ASSERT_EQ(1, batch.Count());
  ASSERT_EQ("Delete(k2)@0", PrintContents(&batch));

  WriteBatch::Handler handler;
  ASSERT_OK(batch.Iterate(&handler));
}

TEST_F(WriteBatchTest, SingleDeleteNotImplemented) {
  WriteBatch batch;
  batch.SingleDelete(Slice("k2"));
  ASSERT_EQ(1, batch.Count());
  ASSERT_EQ("SingleDelete(k2)@0", PrintContents(&batch));

  WriteBatch::Handler handler;
  ASSERT_OK(batch.Iterate(&handler));
}

TEST_F(WriteBatchTest, MergeNotImplemented) {
  WriteBatch batch;
  batch.Merge(Slice("foo"), Slice("bar"));
  ASSERT_EQ(1, batch.Count());
  ASSERT_EQ("Merge(foo, bar)@0", PrintContents(&batch));

  WriteBatch::Handler handler;
  ASSERT_OK(batch.Iterate(&handler));
}

TEST_F(WriteBatchTest, Blob) {
  WriteBatch batch;
  batch.Put(Slice("k1"), Slice("v1"));
  batch.Put(Slice("k2"), Slice("v2"));
  batch.Put(Slice("k3"), Slice("v3"));
  batch.PutLogData(Slice("blob1"));
  batch.Delete(Slice("k2"));
  batch.SingleDelete(Slice("k3"));
  batch.PutLogData(Slice("blob2"));
  batch.Merge(Slice("foo"), Slice("bar"));
  ASSERT_EQ(6, batch.Count());
  ASSERT_EQ(
      "Merge(foo, bar)@5"
      "Put(k1, v1)@0"
      "Delete(k2)@3"
      "Put(k2, v2)@1"
      "SingleDelete(k3)@4"
      "Put(k3, v3)@2",
      PrintContents(&batch));

  TestHandler handler;
  batch.Iterate(&handler);
  ASSERT_EQ(
      "Put(k1, v1)"
      "Put(k2, v2)"
      "Put(k3, v3)"
      "LogData(blob1)"
      "Delete(k2)"
      "SingleDelete(k3)"
      "LogData(blob2)"
      "Merge(foo, bar)",
      handler.seen);
}

TEST_F(WriteBatchTest, PrepareCommit) {
  WriteBatch batch;
  WriteBatchInternal::InsertNoop(&batch);
  batch.Put(Slice("k1"), Slice("v1"));
  batch.Put(Slice("k2"), Slice("v2"));
  batch.SetSavePoint();
  WriteBatchInternal::MarkEndPrepare(&batch, Slice("xid1"));
  Status s = batch.RollbackToSavePoint();
  ASSERT_EQ(s, Status::NotFound());
  WriteBatchInternal::MarkCommit(&batch, Slice("xid1"));
  WriteBatchInternal::MarkRollback(&batch, Slice("xid1"));
  ASSERT_EQ(2, batch.Count());

  TestHandler handler;
  batch.Iterate(&handler);
  ASSERT_EQ(
      "MarkBeginPrepare(false)"
      "Put(k1, v1)"
      "Put(k2, v2)"
      "MarkEndPrepare(xid1)"
      "MarkCommit(xid1)"
      "MarkRollback(xid1)",
      handler.seen);
}

// It requires more than 30GB of memory to run the test. With single memory
// allocation of more than 30GB.
// Not all platform can run it. Also it runs a long time. So disable it.
TEST_F(WriteBatchTest, DISABLED_ManyUpdates) {
  // Insert key and value of 3GB and push total batch size to 12GB.
  static const size_t kKeyValueSize = 4u;
  static const uint32_t kNumUpdates = uint32_t(3 << 30);
  std::string raw(kKeyValueSize, 'A');
  WriteBatch batch(kNumUpdates * (4 + kKeyValueSize * 2) + 1024u);
  char c = 'A';
  for (uint32_t i = 0; i < kNumUpdates; i++) {
    if (c > 'Z') {
      c = 'A';
    }
    raw[0] = c;
    raw[raw.length() - 1] = c;
    c++;
    batch.Put(raw, raw);
  }

  ASSERT_EQ(kNumUpdates, batch.Count());

  struct NoopHandler : public WriteBatch::Handler {
    uint32_t num_seen = 0;
    char expected_char = 'A';
    Status PutCF(uint32_t /*column_family_id*/, const Slice& key,
                 const Slice& value) override {
      EXPECT_EQ(kKeyValueSize, key.size());
      EXPECT_EQ(kKeyValueSize, value.size());
      EXPECT_EQ(expected_char, key[0]);
      EXPECT_EQ(expected_char, value[0]);
      EXPECT_EQ(expected_char, key[kKeyValueSize - 1]);
      EXPECT_EQ(expected_char, value[kKeyValueSize - 1]);
      expected_char++;
      if (expected_char > 'Z') {
        expected_char = 'A';
      }
      ++num_seen;
      return Status::OK();
    }
    Status DeleteCF(uint32_t /*column_family_id*/,
                    const Slice& /*key*/) override {
      ADD_FAILURE();
      return Status::OK();
    }
    Status SingleDeleteCF(uint32_t /*column_family_id*/,
                          const Slice& /*key*/) override {
      ADD_FAILURE();
      return Status::OK();
    }
    Status MergeCF(uint32_t /*column_family_id*/, const Slice& /*key*/,
                   const Slice& /*value*/) override {
      ADD_FAILURE();
      return Status::OK();
    }
    void LogData(const Slice& /*blob*/) override { ADD_FAILURE(); }
    bool Continue() override { return num_seen < kNumUpdates; }
  } handler;

  batch.Iterate(&handler);
  ASSERT_EQ(kNumUpdates, handler.num_seen);
}

// The test requires more than 18GB memory to run it, with single memory
// allocation of more than 12GB. Not all the platform can run it. So disable it.
TEST_F(WriteBatchTest, DISABLED_LargeKeyValue) {
  // Insert key and value of 3GB and push total batch size to 12GB.
  static const size_t kKeyValueSize = 3221225472u;
  std::string raw(kKeyValueSize, 'A');
  WriteBatch batch(size_t(12884901888ull + 1024u));
  for (char i = 0; i < 2; i++) {
    raw[0] = 'A' + i;
    raw[raw.length() - 1] = 'A' - i;
    batch.Put(raw, raw);
  }

  ASSERT_EQ(2, batch.Count());

  struct NoopHandler : public WriteBatch::Handler {
    int num_seen = 0;
    Status PutCF(uint32_t /*column_family_id*/, const Slice& key,
                 const Slice& value) override {
      EXPECT_EQ(kKeyValueSize, key.size());
      EXPECT_EQ(kKeyValueSize, value.size());
      EXPECT_EQ('A' + num_seen, key[0]);
      EXPECT_EQ('A' + num_seen, value[0]);
      EXPECT_EQ('A' - num_seen, key[kKeyValueSize - 1]);
      EXPECT_EQ('A' - num_seen, value[kKeyValueSize - 1]);
      ++num_seen;
      return Status::OK();
    }
    Status DeleteCF(uint32_t /*column_family_id*/,
                    const Slice& /*key*/) override {
      ADD_FAILURE();
      return Status::OK();
    }
    Status SingleDeleteCF(uint32_t /*column_family_id*/,
                          const Slice& /*key*/) override {
      ADD_FAILURE();
      return Status::OK();
    }
    Status MergeCF(uint32_t /*column_family_id*/, const Slice& /*key*/,
                   const Slice& /*value*/) override {
      ADD_FAILURE();
      return Status::OK();
    }
    void LogData(const Slice& /*blob*/) override { ADD_FAILURE(); }
    bool Continue() override { return num_seen < 2; }
  } handler;

  batch.Iterate(&handler);
  ASSERT_EQ(2, handler.num_seen);
}

TEST_F(WriteBatchTest, Continue) {
  WriteBatch batch;

  struct Handler : public TestHandler {
    int num_seen = 0;
    Status PutCF(uint32_t column_family_id, const Slice& key,
                 const Slice& value) override {
      ++num_seen;
      return TestHandler::PutCF(column_family_id, key, value);
    }
    Status DeleteCF(uint32_t column_family_id, const Slice& key) override {
      ++num_seen;
      return TestHandler::DeleteCF(column_family_id, key);
    }
    Status SingleDeleteCF(uint32_t column_family_id,
                          const Slice& key) override {
      ++num_seen;
      return TestHandler::SingleDeleteCF(column_family_id, key);
    }
    Status MergeCF(uint32_t column_family_id, const Slice& key,
                   const Slice& value) override {
      ++num_seen;
      return TestHandler::MergeCF(column_family_id, key, value);
    }
    void LogData(const Slice& blob) override {
      ++num_seen;
      TestHandler::LogData(blob);
    }
    bool Continue() override { return num_seen < 5; }
  } handler;

  batch.Put(Slice("k1"), Slice("v1"));
  batch.Put(Slice("k2"), Slice("v2"));
  batch.PutLogData(Slice("blob1"));
  batch.Delete(Slice("k1"));
  batch.SingleDelete(Slice("k2"));
  batch.PutLogData(Slice("blob2"));
  batch.Merge(Slice("foo"), Slice("bar"));
  batch.Iterate(&handler);
  ASSERT_EQ(
      "Put(k1, v1)"
      "Put(k2, v2)"
      "LogData(blob1)"
      "Delete(k1)"
      "SingleDelete(k2)",
      handler.seen);
}

TEST_F(WriteBatchTest, PutGatherSlices) {
  WriteBatch batch;
  batch.Put(Slice("foo"), Slice("bar"));

  {
    // Try a write where the key is one slice but the value is two
    Slice key_slice("baz");
    Slice value_slices[2] = { Slice("header"), Slice("payload") };
    batch.Put(SliceParts(&key_slice, 1),
              SliceParts(value_slices, 2));
  }

  {
    // One where the key is composite but the value is a single slice
    Slice key_slices[3] = { Slice("key"), Slice("part2"), Slice("part3") };
    Slice value_slice("value");
    batch.Put(SliceParts(key_slices, 3),
              SliceParts(&value_slice, 1));
  }

  WriteBatchInternal::SetSequence(&batch, 100);
  ASSERT_EQ("Put(baz, headerpayload)@101"
            "Put(foo, bar)@100"
            "Put(keypart2part3, value)@102",
            PrintContents(&batch));
  ASSERT_EQ(3, batch.Count());
}

namespace {
class ColumnFamilyHandleImplDummy : public ColumnFamilyHandleImpl {
 public:
  explicit ColumnFamilyHandleImplDummy(int id)
      : ColumnFamilyHandleImpl(nullptr, nullptr, nullptr), id_(id) {}
  uint32_t GetID() const override { return id_; }
  const Comparator* GetComparator() const override {
    return BytewiseComparator();
  }

 private:
  uint32_t id_;
};
}  // namespace anonymous

TEST_F(WriteBatchTest, ColumnFamiliesBatchTest) {
  WriteBatch batch;
  ColumnFamilyHandleImplDummy zero(0), two(2), three(3), eight(8);
  batch.Put(&zero, Slice("foo"), Slice("bar"));
  batch.Put(&two, Slice("twofoo"), Slice("bar2"));
  batch.Put(&eight, Slice("eightfoo"), Slice("bar8"));
  batch.Delete(&eight, Slice("eightfoo"));
  batch.SingleDelete(&two, Slice("twofoo"));
  batch.DeleteRange(&two, Slice("3foo"), Slice("4foo"));
  batch.Merge(&three, Slice("threethree"), Slice("3three"));
  batch.Put(&zero, Slice("foo"), Slice("bar"));
  batch.Merge(Slice("omom"), Slice("nom"));

  TestHandler handler;
  batch.Iterate(&handler);
  ASSERT_EQ(
      "Put(foo, bar)"
      "PutCF(2, twofoo, bar2)"
      "PutCF(8, eightfoo, bar8)"
      "DeleteCF(8, eightfoo)"
      "SingleDeleteCF(2, twofoo)"
      "DeleteRangeCF(2, 3foo, 4foo)"
      "MergeCF(3, threethree, 3three)"
      "Put(foo, bar)"
      "Merge(omom, nom)",
      handler.seen);
}

#ifndef ROCKSDB_LITE
TEST_F(WriteBatchTest, ColumnFamiliesBatchWithIndexTest) {
  WriteBatchWithIndex batch;
  ColumnFamilyHandleImplDummy zero(0), two(2), three(3), eight(8);
  batch.Put(&zero, Slice("foo"), Slice("bar"));
  batch.Put(&two, Slice("twofoo"), Slice("bar2"));
  batch.Put(&eight, Slice("eightfoo"), Slice("bar8"));
  batch.Delete(&eight, Slice("eightfoo"));
  batch.SingleDelete(&two, Slice("twofoo"));
  batch.DeleteRange(&two, Slice("twofoo"), Slice("threefoo"));
  batch.Merge(&three, Slice("threethree"), Slice("3three"));
  batch.Put(&zero, Slice("foo"), Slice("bar"));
  batch.Merge(Slice("omom"), Slice("nom"));

  std::unique_ptr<WBWIIterator> iter;

  iter.reset(batch.NewIterator(&eight));
  iter->Seek("eightfoo");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(WriteType::kPutRecord, iter->Entry().type);
  ASSERT_EQ("eightfoo", iter->Entry().key.ToString());
  ASSERT_EQ("bar8", iter->Entry().value.ToString());

  iter->Next();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(WriteType::kDeleteRecord, iter->Entry().type);
  ASSERT_EQ("eightfoo", iter->Entry().key.ToString());

  iter->Next();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(!iter->Valid());

  iter.reset(batch.NewIterator(&two));
  iter->Seek("twofoo");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(WriteType::kPutRecord, iter->Entry().type);
  ASSERT_EQ("twofoo", iter->Entry().key.ToString());
  ASSERT_EQ("bar2", iter->Entry().value.ToString());

  iter->Next();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(WriteType::kSingleDeleteRecord, iter->Entry().type);
  ASSERT_EQ("twofoo", iter->Entry().key.ToString());

  iter->Next();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(WriteType::kDeleteRangeRecord, iter->Entry().type);
  ASSERT_EQ("twofoo", iter->Entry().key.ToString());
  ASSERT_EQ("threefoo", iter->Entry().value.ToString());

  iter->Next();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(!iter->Valid());

  iter.reset(batch.NewIterator());
  iter->Seek("gggg");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(WriteType::kMergeRecord, iter->Entry().type);
  ASSERT_EQ("omom", iter->Entry().key.ToString());
  ASSERT_EQ("nom", iter->Entry().value.ToString());

  iter->Next();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(!iter->Valid());

  iter.reset(batch.NewIterator(&zero));
  iter->Seek("foo");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(WriteType::kPutRecord, iter->Entry().type);
  ASSERT_EQ("foo", iter->Entry().key.ToString());
  ASSERT_EQ("bar", iter->Entry().value.ToString());

  iter->Next();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(WriteType::kPutRecord, iter->Entry().type);
  ASSERT_EQ("foo", iter->Entry().key.ToString());
  ASSERT_EQ("bar", iter->Entry().value.ToString());

  iter->Next();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(WriteType::kMergeRecord, iter->Entry().type);
  ASSERT_EQ("omom", iter->Entry().key.ToString());
  ASSERT_EQ("nom", iter->Entry().value.ToString());

  iter->Next();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(!iter->Valid());

  TestHandler handler;
  batch.GetWriteBatch()->Iterate(&handler);
  ASSERT_EQ(
      "Put(foo, bar)"
      "PutCF(2, twofoo, bar2)"
      "PutCF(8, eightfoo, bar8)"
      "DeleteCF(8, eightfoo)"
      "SingleDeleteCF(2, twofoo)"
      "DeleteRangeCF(2, twofoo, threefoo)"
      "MergeCF(3, threethree, 3three)"
      "Put(foo, bar)"
      "Merge(omom, nom)",
      handler.seen);
}
#endif  // !ROCKSDB_LITE

TEST_F(WriteBatchTest, SavePointTest) {
  Status s;
  WriteBatch batch;
  batch.SetSavePoint();

  batch.Put("A", "a");
  batch.Put("B", "b");
  batch.SetSavePoint();

  batch.Put("C", "c");
  batch.Delete("A");
  batch.SetSavePoint();
  batch.SetSavePoint();

  ASSERT_OK(batch.RollbackToSavePoint());
  ASSERT_EQ(
      "Delete(A)@3"
      "Put(A, a)@0"
      "Put(B, b)@1"
      "Put(C, c)@2",
      PrintContents(&batch));

  ASSERT_OK(batch.RollbackToSavePoint());
  ASSERT_OK(batch.RollbackToSavePoint());
  ASSERT_EQ(
      "Put(A, a)@0"
      "Put(B, b)@1",
      PrintContents(&batch));

  batch.Delete("A");
  batch.Put("B", "bb");

  ASSERT_OK(batch.RollbackToSavePoint());
  ASSERT_EQ("", PrintContents(&batch));

  s = batch.RollbackToSavePoint();
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ("", PrintContents(&batch));

  batch.Put("D", "d");
  batch.Delete("A");

  batch.SetSavePoint();

  batch.Put("A", "aaa");

  ASSERT_OK(batch.RollbackToSavePoint());
  ASSERT_EQ(
      "Delete(A)@1"
      "Put(D, d)@0",
      PrintContents(&batch));

  batch.SetSavePoint();

  batch.Put("D", "d");
  batch.Delete("A");

  ASSERT_OK(batch.RollbackToSavePoint());
  ASSERT_EQ(
      "Delete(A)@1"
      "Put(D, d)@0",
      PrintContents(&batch));

  s = batch.RollbackToSavePoint();
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(
      "Delete(A)@1"
      "Put(D, d)@0",
      PrintContents(&batch));

  WriteBatch batch2;

  s = batch2.RollbackToSavePoint();
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ("", PrintContents(&batch2));

  batch2.Delete("A");
  batch2.SetSavePoint();

  s = batch2.RollbackToSavePoint();
  ASSERT_OK(s);
  ASSERT_EQ("Delete(A)@0", PrintContents(&batch2));

  batch2.Clear();
  ASSERT_EQ("", PrintContents(&batch2));

  batch2.SetSavePoint();

  batch2.Delete("B");
  ASSERT_EQ("Delete(B)@0", PrintContents(&batch2));

  batch2.SetSavePoint();
  s = batch2.RollbackToSavePoint();
  ASSERT_OK(s);
  ASSERT_EQ("Delete(B)@0", PrintContents(&batch2));

  s = batch2.RollbackToSavePoint();
  ASSERT_OK(s);
  ASSERT_EQ("", PrintContents(&batch2));

  s = batch2.RollbackToSavePoint();
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ("", PrintContents(&batch2));

  WriteBatch batch3;

  s = batch3.PopSavePoint();
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ("", PrintContents(&batch3));

  batch3.SetSavePoint();
  batch3.Delete("A");

  s = batch3.PopSavePoint();
  ASSERT_OK(s);
  ASSERT_EQ("Delete(A)@0", PrintContents(&batch3));
}

TEST_F(WriteBatchTest, MemoryLimitTest) {
  Status s;
  // The header size is 12 bytes. The two Puts take 8 bytes which gives total
  // of 12 + 8 * 2 = 28 bytes.
  WriteBatch batch(0, 28);

  ASSERT_OK(batch.Put("a", "...."));
  ASSERT_OK(batch.Put("b", "...."));
  s = batch.Put("c", "....");
  ASSERT_TRUE(s.IsMemoryLimit());
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
