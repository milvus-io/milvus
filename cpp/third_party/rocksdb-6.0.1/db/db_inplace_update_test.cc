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

namespace rocksdb {

class DBTestInPlaceUpdate : public DBTestBase {
 public:
  DBTestInPlaceUpdate() : DBTestBase("/db_inplace_update_test") {}
};

TEST_F(DBTestInPlaceUpdate, InPlaceUpdate) {
  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.inplace_update_support = true;
    options.env = env_;
    options.write_buffer_size = 100000;
    options.allow_concurrent_memtable_write = false;
    Reopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    // Update key with values of smaller size
    int numValues = 10;
    for (int i = numValues; i > 0; i--) {
      std::string value = DummyString(i, 'a');
      ASSERT_OK(Put(1, "key", value));
      ASSERT_EQ(value, Get(1, "key"));
    }

    // Only 1 instance for that key.
    validateNumberOfEntries(1, 1);
  } while (ChangeCompactOptions());
}

TEST_F(DBTestInPlaceUpdate, InPlaceUpdateLargeNewValue) {
  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.inplace_update_support = true;
    options.env = env_;
    options.write_buffer_size = 100000;
    options.allow_concurrent_memtable_write = false;
    Reopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    // Update key with values of larger size
    int numValues = 10;
    for (int i = 0; i < numValues; i++) {
      std::string value = DummyString(i, 'a');
      ASSERT_OK(Put(1, "key", value));
      ASSERT_EQ(value, Get(1, "key"));
    }

    // All 10 updates exist in the internal iterator
    validateNumberOfEntries(numValues, 1);
  } while (ChangeCompactOptions());
}

TEST_F(DBTestInPlaceUpdate, InPlaceUpdateCallbackSmallerSize) {
  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.inplace_update_support = true;

    options.env = env_;
    options.write_buffer_size = 100000;
    options.inplace_callback =
      rocksdb::DBTestInPlaceUpdate::updateInPlaceSmallerSize;
    options.allow_concurrent_memtable_write = false;
    Reopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    // Update key with values of smaller size
    int numValues = 10;
    ASSERT_OK(Put(1, "key", DummyString(numValues, 'a')));
    ASSERT_EQ(DummyString(numValues, 'c'), Get(1, "key"));

    for (int i = numValues; i > 0; i--) {
      ASSERT_OK(Put(1, "key", DummyString(i, 'a')));
      ASSERT_EQ(DummyString(i - 1, 'b'), Get(1, "key"));
    }

    // Only 1 instance for that key.
    validateNumberOfEntries(1, 1);
  } while (ChangeCompactOptions());
}

TEST_F(DBTestInPlaceUpdate, InPlaceUpdateCallbackSmallerVarintSize) {
  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.inplace_update_support = true;

    options.env = env_;
    options.write_buffer_size = 100000;
    options.inplace_callback =
      rocksdb::DBTestInPlaceUpdate::updateInPlaceSmallerVarintSize;
    options.allow_concurrent_memtable_write = false;
    Reopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    // Update key with values of smaller varint size
    int numValues = 265;
    ASSERT_OK(Put(1, "key", DummyString(numValues, 'a')));
    ASSERT_EQ(DummyString(numValues, 'c'), Get(1, "key"));

    for (int i = numValues; i > 0; i--) {
      ASSERT_OK(Put(1, "key", DummyString(i, 'a')));
      ASSERT_EQ(DummyString(1, 'b'), Get(1, "key"));
    }

    // Only 1 instance for that key.
    validateNumberOfEntries(1, 1);
  } while (ChangeCompactOptions());
}

TEST_F(DBTestInPlaceUpdate, InPlaceUpdateCallbackLargeNewValue) {
  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.inplace_update_support = true;

    options.env = env_;
    options.write_buffer_size = 100000;
    options.inplace_callback =
      rocksdb::DBTestInPlaceUpdate::updateInPlaceLargerSize;
    options.allow_concurrent_memtable_write = false;
    Reopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    // Update key with values of larger size
    int numValues = 10;
    for (int i = 0; i < numValues; i++) {
      ASSERT_OK(Put(1, "key", DummyString(i, 'a')));
      ASSERT_EQ(DummyString(i, 'c'), Get(1, "key"));
    }

    // No inplace updates. All updates are puts with new seq number
    // All 10 updates exist in the internal iterator
    validateNumberOfEntries(numValues, 1);
  } while (ChangeCompactOptions());
}

TEST_F(DBTestInPlaceUpdate, InPlaceUpdateCallbackNoAction) {
  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.inplace_update_support = true;

    options.env = env_;
    options.write_buffer_size = 100000;
    options.inplace_callback =
        rocksdb::DBTestInPlaceUpdate::updateInPlaceNoAction;
    options.allow_concurrent_memtable_write = false;
    Reopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    // Callback function requests no actions from db
    ASSERT_OK(Put(1, "key", DummyString(1, 'a')));
    ASSERT_EQ(Get(1, "key"), "NOT_FOUND");
  } while (ChangeCompactOptions());
}
}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
