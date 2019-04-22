//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "db/column_family.h"
#include "db/db_iter.h"
#include "db/db_test_util.h"
#include "db/dbformat.h"
#include "db/write_batch_internal.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "util/string_util.h"
#include "utilities/merge_operators.h"

namespace rocksdb {

// kTypeBlobIndex is a value type used by BlobDB only. The base rocksdb
// should accept the value type on write, and report not supported value
// for reads, unless caller request for it explicitly. The base rocksdb
// doesn't understand format of actual blob index (the value).
class DBBlobIndexTest : public DBTestBase {
 public:
  enum Tier {
    kMemtable = 0,
    kImmutableMemtables = 1,
    kL0SstFile = 2,
    kLnSstFile = 3,
  };
  const std::vector<Tier> kAllTiers = {Tier::kMemtable,
                                       Tier::kImmutableMemtables,
                                       Tier::kL0SstFile, Tier::kLnSstFile};

  DBBlobIndexTest() : DBTestBase("/db_blob_index_test") {}

  ColumnFamilyHandle* cfh() { return dbfull()->DefaultColumnFamily(); }

  ColumnFamilyData* cfd() {
    return reinterpret_cast<ColumnFamilyHandleImpl*>(cfh())->cfd();
  }

  Status PutBlobIndex(WriteBatch* batch, const Slice& key,
                      const Slice& blob_index) {
    return WriteBatchInternal::PutBlobIndex(batch, cfd()->GetID(), key,
                                            blob_index);
  }

  Status Write(WriteBatch* batch) {
    return dbfull()->Write(WriteOptions(), batch);
  }

  std::string GetImpl(const Slice& key, bool* is_blob_index = nullptr,
                      const Snapshot* snapshot = nullptr) {
    ReadOptions read_options;
    read_options.snapshot = snapshot;
    PinnableSlice value;
    auto s = dbfull()->GetImpl(read_options, cfh(), key, &value,
                               nullptr /*value_found*/, nullptr /*callback*/,
                               is_blob_index);
    if (s.IsNotFound()) {
      return "NOT_FOUND";
    }
    if (s.IsNotSupported()) {
      return "NOT_SUPPORTED";
    }
    if (!s.ok()) {
      return s.ToString();
    }
    return value.ToString();
  }

  std::string GetBlobIndex(const Slice& key,
                           const Snapshot* snapshot = nullptr) {
    bool is_blob_index = false;
    std::string value = GetImpl(key, &is_blob_index, snapshot);
    if (!is_blob_index) {
      return "NOT_BLOB";
    }
    return value;
  }

  ArenaWrappedDBIter* GetBlobIterator() {
    return dbfull()->NewIteratorImpl(
        ReadOptions(), cfd(), dbfull()->GetLatestSequenceNumber(),
        nullptr /*read_callback*/, true /*allow_blob*/);
  }

  Options GetTestOptions() {
    Options options;
    options.create_if_missing = true;
    options.num_levels = 2;
    options.disable_auto_compactions = true;
    // Disable auto flushes.
    options.max_write_buffer_number = 10;
    options.min_write_buffer_number_to_merge = 10;
    options.merge_operator = MergeOperators::CreateStringAppendOperator();
    return options;
  }

  void MoveDataTo(Tier tier) {
    switch (tier) {
      case Tier::kMemtable:
        break;
      case Tier::kImmutableMemtables:
        ASSERT_OK(dbfull()->TEST_SwitchMemtable());
        break;
      case Tier::kL0SstFile:
        ASSERT_OK(Flush());
        break;
      case Tier::kLnSstFile:
        ASSERT_OK(Flush());
        ASSERT_OK(Put("a", "dummy"));
        ASSERT_OK(Put("z", "dummy"));
        ASSERT_OK(Flush());
        ASSERT_OK(
            dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
#ifndef ROCKSDB_LITE
        ASSERT_EQ("0,1", FilesPerLevel());
#endif  // !ROCKSDB_LITE
        break;
    }
  }
};

// Should be able to write kTypeBlobIndex to memtables and SST files.
TEST_F(DBBlobIndexTest, Write) {
  for (auto tier : kAllTiers) {
    DestroyAndReopen(GetTestOptions());
    for (int i = 1; i <= 5; i++) {
      std::string index = ToString(i);
      WriteBatch batch;
      ASSERT_OK(PutBlobIndex(&batch, "key" + index, "blob" + index));
      ASSERT_OK(Write(&batch));
    }
    MoveDataTo(tier);
    for (int i = 1; i <= 5; i++) {
      std::string index = ToString(i);
      ASSERT_EQ("blob" + index, GetBlobIndex("key" + index));
    }
  }
}

// Get should be able to return blob index if is_blob_index is provided,
// otherwise return Status::NotSupported status.
TEST_F(DBBlobIndexTest, Get) {
  for (auto tier : kAllTiers) {
    DestroyAndReopen(GetTestOptions());
    WriteBatch batch;
    ASSERT_OK(batch.Put("key", "value"));
    ASSERT_OK(PutBlobIndex(&batch, "blob_key", "blob_index"));
    ASSERT_OK(Write(&batch));
    MoveDataTo(tier);
    // Verify normal value
    bool is_blob_index = false;
    PinnableSlice value;
    ASSERT_EQ("value", Get("key"));
    ASSERT_EQ("value", GetImpl("key"));
    ASSERT_EQ("value", GetImpl("key", &is_blob_index));
    ASSERT_FALSE(is_blob_index);
    // Verify blob index
    ASSERT_TRUE(Get("blob_key", &value).IsNotSupported());
    ASSERT_EQ("NOT_SUPPORTED", GetImpl("blob_key"));
    ASSERT_EQ("blob_index", GetImpl("blob_key", &is_blob_index));
    ASSERT_TRUE(is_blob_index);
  }
}

// Get should NOT return Status::NotSupported if blob index is updated with
// a normal value.
TEST_F(DBBlobIndexTest, Updated) {
  for (auto tier : kAllTiers) {
    DestroyAndReopen(GetTestOptions());
    WriteBatch batch;
    for (int i = 0; i < 10; i++) {
      ASSERT_OK(PutBlobIndex(&batch, "key" + ToString(i), "blob_index"));
    }
    ASSERT_OK(Write(&batch));
    // Avoid blob values from being purged.
    const Snapshot* snapshot = dbfull()->GetSnapshot();
    ASSERT_OK(Put("key1", "new_value"));
    ASSERT_OK(Merge("key2", "a"));
    ASSERT_OK(Merge("key2", "b"));
    ASSERT_OK(Merge("key2", "c"));
    ASSERT_OK(Delete("key3"));
    ASSERT_OK(SingleDelete("key4"));
    ASSERT_OK(Delete("key5"));
    ASSERT_OK(Merge("key5", "a"));
    ASSERT_OK(Merge("key5", "b"));
    ASSERT_OK(Merge("key5", "c"));
    ASSERT_OK(dbfull()->DeleteRange(WriteOptions(), cfh(), "key6", "key9"));
    MoveDataTo(tier);
    for (int i = 0; i < 10; i++) {
      ASSERT_EQ("blob_index", GetBlobIndex("key" + ToString(i), snapshot));
    }
    ASSERT_EQ("new_value", Get("key1"));
    ASSERT_EQ("NOT_SUPPORTED", GetImpl("key2"));
    ASSERT_EQ("NOT_FOUND", Get("key3"));
    ASSERT_EQ("NOT_FOUND", Get("key4"));
    ASSERT_EQ("a,b,c", GetImpl("key5"));
    for (int i = 6; i < 9; i++) {
      ASSERT_EQ("NOT_FOUND", Get("key" + ToString(i)));
    }
    ASSERT_EQ("blob_index", GetBlobIndex("key9"));
    dbfull()->ReleaseSnapshot(snapshot);
  }
}

// Iterator should get blob value if allow_blob flag is set,
// otherwise return Status::NotSupported status.
TEST_F(DBBlobIndexTest, Iterate) {
  const std::vector<std::vector<ValueType>> data = {
      /*00*/ {kTypeValue},
      /*01*/ {kTypeBlobIndex},
      /*02*/ {kTypeValue},
      /*03*/ {kTypeBlobIndex, kTypeValue},
      /*04*/ {kTypeValue},
      /*05*/ {kTypeValue, kTypeBlobIndex},
      /*06*/ {kTypeValue},
      /*07*/ {kTypeDeletion, kTypeBlobIndex},
      /*08*/ {kTypeValue},
      /*09*/ {kTypeSingleDeletion, kTypeBlobIndex},
      /*10*/ {kTypeValue},
      /*11*/ {kTypeMerge, kTypeMerge, kTypeMerge, kTypeBlobIndex},
      /*12*/ {kTypeValue},
      /*13*/
      {kTypeMerge, kTypeMerge, kTypeMerge, kTypeDeletion, kTypeBlobIndex},
      /*14*/ {kTypeValue},
      /*15*/ {kTypeBlobIndex},
      /*16*/ {kTypeValue},
  };

  auto get_key = [](int index) {
    char buf[20];
    snprintf(buf, sizeof(buf), "%02d", index);
    return "key" + std::string(buf);
  };

  auto get_value = [&](int index, int version) {
    return get_key(index) + "_value" + ToString(version);
  };

  auto check_iterator = [&](Iterator* iterator, Status::Code expected_status,
                            const Slice& expected_value) {
    ASSERT_EQ(expected_status, iterator->status().code());
    if (expected_status == Status::kOk) {
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ(expected_value, iterator->value());
    } else {
      ASSERT_FALSE(iterator->Valid());
    }
  };

  auto create_normal_iterator = [&]() -> Iterator* {
    return dbfull()->NewIterator(ReadOptions());
  };

  auto create_blob_iterator = [&]() -> Iterator* { return GetBlobIterator(); };

  auto check_is_blob = [&](bool is_blob) {
    return [is_blob](Iterator* iterator) {
      ASSERT_EQ(is_blob,
                reinterpret_cast<ArenaWrappedDBIter*>(iterator)->IsBlob());
    };
  };

  auto verify = [&](int index, Status::Code expected_status,
                    const Slice& forward_value, const Slice& backward_value,
                    std::function<Iterator*()> create_iterator,
                    std::function<void(Iterator*)> extra_check = nullptr) {
    // Seek
    auto* iterator = create_iterator();
    ASSERT_OK(iterator->Refresh());
    iterator->Seek(get_key(index));
    check_iterator(iterator, expected_status, forward_value);
    if (extra_check) {
      extra_check(iterator);
    }
    delete iterator;

    // Next
    iterator = create_iterator();
    ASSERT_OK(iterator->Refresh());
    iterator->Seek(get_key(index - 1));
    ASSERT_TRUE(iterator->Valid());
    iterator->Next();
    check_iterator(iterator, expected_status, forward_value);
    if (extra_check) {
      extra_check(iterator);
    }
    delete iterator;

    // SeekForPrev
    iterator = create_iterator();
    ASSERT_OK(iterator->Refresh());
    iterator->SeekForPrev(get_key(index));
    check_iterator(iterator, expected_status, backward_value);
    if (extra_check) {
      extra_check(iterator);
    }
    delete iterator;

    // Prev
    iterator = create_iterator();
    iterator->Seek(get_key(index + 1));
    ASSERT_TRUE(iterator->Valid());
    iterator->Prev();
    check_iterator(iterator, expected_status, backward_value);
    if (extra_check) {
      extra_check(iterator);
    }
    delete iterator;
  };

  for (auto tier : {Tier::kMemtable} /*kAllTiers*/) {
    // Avoid values from being purged.
    std::vector<const Snapshot*> snapshots;
    DestroyAndReopen(GetTestOptions());

    // fill data
    for (int i = 0; i < static_cast<int>(data.size()); i++) {
      for (int j = static_cast<int>(data[i].size()) - 1; j >= 0; j--) {
        std::string key = get_key(i);
        std::string value = get_value(i, j);
        WriteBatch batch;
        switch (data[i][j]) {
          case kTypeValue:
            ASSERT_OK(Put(key, value));
            break;
          case kTypeDeletion:
            ASSERT_OK(Delete(key));
            break;
          case kTypeSingleDeletion:
            ASSERT_OK(SingleDelete(key));
            break;
          case kTypeMerge:
            ASSERT_OK(Merge(key, value));
            break;
          case kTypeBlobIndex:
            ASSERT_OK(PutBlobIndex(&batch, key, value));
            ASSERT_OK(Write(&batch));
            break;
          default:
            assert(false);
        };
      }
      snapshots.push_back(dbfull()->GetSnapshot());
    }
    ASSERT_OK(
        dbfull()->DeleteRange(WriteOptions(), cfh(), get_key(15), get_key(16)));
    snapshots.push_back(dbfull()->GetSnapshot());
    MoveDataTo(tier);

    // Normal iterator
    verify(1, Status::kNotSupported, "", "", create_normal_iterator);
    verify(3, Status::kNotSupported, "", "", create_normal_iterator);
    verify(5, Status::kOk, get_value(5, 0), get_value(5, 0),
           create_normal_iterator);
    verify(7, Status::kOk, get_value(8, 0), get_value(6, 0),
           create_normal_iterator);
    verify(9, Status::kOk, get_value(10, 0), get_value(8, 0),
           create_normal_iterator);
    verify(11, Status::kNotSupported, "", "", create_normal_iterator);
    verify(13, Status::kOk,
           get_value(13, 2) + "," + get_value(13, 1) + "," + get_value(13, 0),
           get_value(13, 2) + "," + get_value(13, 1) + "," + get_value(13, 0),
           create_normal_iterator);
    verify(15, Status::kOk, get_value(16, 0), get_value(14, 0),
           create_normal_iterator);

    // Iterator with blob support
    verify(1, Status::kOk, get_value(1, 0), get_value(1, 0),
           create_blob_iterator, check_is_blob(true));
    verify(3, Status::kOk, get_value(3, 0), get_value(3, 0),
           create_blob_iterator, check_is_blob(true));
    verify(5, Status::kOk, get_value(5, 0), get_value(5, 0),
           create_blob_iterator, check_is_blob(false));
    verify(7, Status::kOk, get_value(8, 0), get_value(6, 0),
           create_blob_iterator, check_is_blob(false));
    verify(9, Status::kOk, get_value(10, 0), get_value(8, 0),
           create_blob_iterator, check_is_blob(false));
    verify(11, Status::kNotSupported, "", "", create_blob_iterator);
    verify(13, Status::kOk,
           get_value(13, 2) + "," + get_value(13, 1) + "," + get_value(13, 0),
           get_value(13, 2) + "," + get_value(13, 1) + "," + get_value(13, 0),
           create_blob_iterator, check_is_blob(false));
    verify(15, Status::kOk, get_value(16, 0), get_value(14, 0),
           create_blob_iterator, check_is_blob(false));

    for (auto* snapshot : snapshots) {
      dbfull()->ReleaseSnapshot(snapshot);
    }
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
