//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <unordered_set>
#include <vector>

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/table_properties_collectors.h"
#include "util/testharness.h"
#include "util/testutil.h"

#ifndef ROCKSDB_LITE

namespace rocksdb {

// A helper function that ensures the table properties returned in
// `GetPropertiesOfAllTablesTest` is correct.
// This test assumes entries size is different for each of the tables.
namespace {

void VerifyTableProperties(DB* db, uint64_t expected_entries_size) {
  TablePropertiesCollection props;
  ASSERT_OK(db->GetPropertiesOfAllTables(&props));

  ASSERT_EQ(4U, props.size());
  std::unordered_set<uint64_t> unique_entries;

  // Indirect test
  uint64_t sum = 0;
  for (const auto& item : props) {
    unique_entries.insert(item.second->num_entries);
    sum += item.second->num_entries;
  }

  ASSERT_EQ(props.size(), unique_entries.size());
  ASSERT_EQ(expected_entries_size, sum);
}
}  // namespace

class DBTablePropertiesTest : public DBTestBase {
 public:
  DBTablePropertiesTest() : DBTestBase("/db_table_properties_test") {}
  TablePropertiesCollection TestGetPropertiesOfTablesInRange(
      std::vector<Range> ranges, std::size_t* num_properties = nullptr,
      std::size_t* num_files = nullptr);
};

TEST_F(DBTablePropertiesTest, GetPropertiesOfAllTablesTest) {
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 8;
  Reopen(options);
  // Create 4 tables
  for (int table = 0; table < 4; ++table) {
    for (int i = 0; i < 10 + table; ++i) {
      db_->Put(WriteOptions(), ToString(table * 100 + i), "val");
    }
    db_->Flush(FlushOptions());
  }

  // 1. Read table properties directly from file
  Reopen(options);
  VerifyTableProperties(db_, 10 + 11 + 12 + 13);

  // 2. Put two tables to table cache and
  Reopen(options);
  // fetch key from 1st and 2nd table, which will internally place that table to
  // the table cache.
  for (int i = 0; i < 2; ++i) {
    Get(ToString(i * 100 + 0));
  }

  VerifyTableProperties(db_, 10 + 11 + 12 + 13);

  // 3. Put all tables to table cache
  Reopen(options);
  // fetch key from 1st and 2nd table, which will internally place that table to
  // the table cache.
  for (int i = 0; i < 4; ++i) {
    Get(ToString(i * 100 + 0));
  }
  VerifyTableProperties(db_, 10 + 11 + 12 + 13);
}

TablePropertiesCollection
DBTablePropertiesTest::TestGetPropertiesOfTablesInRange(
    std::vector<Range> ranges, std::size_t* num_properties,
    std::size_t* num_files) {

  // Since we deref zero element in the vector it can not be empty
  // otherwise we pass an address to some random memory
  EXPECT_GT(ranges.size(), 0U);
  // run the query
  TablePropertiesCollection props;
  EXPECT_OK(db_->GetPropertiesOfTablesInRange(
      db_->DefaultColumnFamily(), &ranges[0], ranges.size(), &props));

  // Make sure that we've received properties for those and for those files
  // only which fall within requested ranges
  std::vector<LiveFileMetaData> vmd;
  db_->GetLiveFilesMetaData(&vmd);
  for (auto& md : vmd) {
    std::string fn = md.db_path + md.name;
    bool in_range = false;
    for (auto& r : ranges) {
      // smallestkey < limit && largestkey >= start
      if (r.limit.compare(md.smallestkey) >= 0 &&
          r.start.compare(md.largestkey) <= 0) {
        in_range = true;
        EXPECT_GT(props.count(fn), 0);
      }
    }
    if (!in_range) {
      EXPECT_EQ(props.count(fn), 0);
    }
  }

  if (num_properties) {
    *num_properties = props.size();
  }

  if (num_files) {
    *num_files = vmd.size();
  }
  return props;
}

TEST_F(DBTablePropertiesTest, GetPropertiesOfTablesInRange) {
  // Fixed random sead
  Random rnd(301);

  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 4096;
  options.max_write_buffer_number = 3;
  options.level0_file_num_compaction_trigger = 2;
  options.level0_slowdown_writes_trigger = 2;
  options.level0_stop_writes_trigger = 4;
  options.target_file_size_base = 2048;
  options.max_bytes_for_level_base = 10240;
  options.max_bytes_for_level_multiplier = 4;
  options.hard_pending_compaction_bytes_limit = 16 * 1024;
  options.num_levels = 8;
  options.env = env_;

  DestroyAndReopen(options);

  // build a decent LSM
  for (int i = 0; i < 10000; i++) {
    ASSERT_OK(Put(test::RandomKey(&rnd, 5), RandomString(&rnd, 102)));
  }
  Flush();
  dbfull()->TEST_WaitForCompact();
  if (NumTableFilesAtLevel(0) == 0) {
    ASSERT_OK(Put(test::RandomKey(&rnd, 5), RandomString(&rnd, 102)));
    Flush();
  }

  db_->PauseBackgroundWork();

  // Ensure that we have at least L0, L1 and L2
  ASSERT_GT(NumTableFilesAtLevel(0), 0);
  ASSERT_GT(NumTableFilesAtLevel(1), 0);
  ASSERT_GT(NumTableFilesAtLevel(2), 0);

  // Query the largest range
  std::size_t num_properties, num_files;
  TestGetPropertiesOfTablesInRange(
      {Range(test::RandomKey(&rnd, 5, test::RandomKeyType::SMALLEST),
             test::RandomKey(&rnd, 5, test::RandomKeyType::LARGEST))},
      &num_properties, &num_files);
  ASSERT_EQ(num_properties, num_files);

  // Query the empty range
  TestGetPropertiesOfTablesInRange(
      {Range(test::RandomKey(&rnd, 5, test::RandomKeyType::LARGEST),
             test::RandomKey(&rnd, 5, test::RandomKeyType::SMALLEST))},
      &num_properties, &num_files);
  ASSERT_GT(num_files, 0);
  ASSERT_EQ(num_properties, 0);

  // Query the middle rangee
  TestGetPropertiesOfTablesInRange(
      {Range(test::RandomKey(&rnd, 5, test::RandomKeyType::MIDDLE),
             test::RandomKey(&rnd, 5, test::RandomKeyType::LARGEST))},
      &num_properties, &num_files);
  ASSERT_GT(num_files, 0);
  ASSERT_GT(num_files, num_properties);
  ASSERT_GT(num_properties, 0);

  // Query a bunch of random ranges
  for (int j = 0; j < 100; j++) {
    // create a bunch of ranges
    std::vector<std::string> random_keys;
    // Random returns numbers with zero included
    // when we pass empty ranges TestGetPropertiesOfTablesInRange()
    // derefs random memory in the empty ranges[0]
    // so want to be greater than zero and even since
    // the below loop requires that random_keys.size() to be even.
    auto n = 2 * (rnd.Uniform(50) + 1);

    for (uint32_t i = 0; i < n; ++i) {
      random_keys.push_back(test::RandomKey(&rnd, 5));
    }

    ASSERT_GT(random_keys.size(), 0U);
    ASSERT_EQ((random_keys.size() % 2), 0U);

    std::vector<Range> ranges;
    auto it = random_keys.begin();
    while (it != random_keys.end()) {
      ranges.push_back(Range(*it, *(it + 1)));
      it += 2;
    }

    TestGetPropertiesOfTablesInRange(std::move(ranges));
  }
}

TEST_F(DBTablePropertiesTest, GetColumnFamilyNameProperty) {
  std::string kExtraCfName = "pikachu";
  CreateAndReopenWithCF({kExtraCfName}, CurrentOptions());

  // Create one table per CF, then verify it was created with the column family
  // name property.
  for (int cf = 0; cf < 2; ++cf) {
    Put(cf, "key", "val");
    Flush(cf);

    TablePropertiesCollection fname_to_props;
    ASSERT_OK(db_->GetPropertiesOfAllTables(handles_[cf], &fname_to_props));
    ASSERT_EQ(1U, fname_to_props.size());

    std::string expected_cf_name;
    if (cf > 0) {
      expected_cf_name = kExtraCfName;
    } else {
      expected_cf_name = kDefaultColumnFamilyName;
    }
    ASSERT_EQ(expected_cf_name,
              fname_to_props.begin()->second->column_family_name);
    ASSERT_EQ(cf, static_cast<uint32_t>(
                      fname_to_props.begin()->second->column_family_id));
  }
}

TEST_F(DBTablePropertiesTest, DeletionTriggeredCompactionMarking) {
  int kNumKeys = 1000;
  int kWindowSize = 100;
  int kNumDelsTrigger = 90;
  std::shared_ptr<TablePropertiesCollectorFactory> compact_on_del =
    NewCompactOnDeletionCollectorFactory(kWindowSize, kNumDelsTrigger);

  Options opts = CurrentOptions();
  opts.table_properties_collector_factories.emplace_back(compact_on_del);
  Reopen(opts);

  // add an L1 file to prevent tombstones from dropping due to obsolescence
  // during flush
  Put(Key(0), "val");
  Flush();
  MoveFilesToLevel(1);

  for (int i = 0; i < kNumKeys; ++i) {
    if (i >= kNumKeys - kWindowSize &&
        i < kNumKeys - kWindowSize + kNumDelsTrigger) {
      Delete(Key(i));
    } else {
      Put(Key(i), "val");
    }
  }
  Flush();

  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_GT(NumTableFilesAtLevel(1), 0);

  // Change the window size and deletion trigger and ensure new values take
  // effect
  kWindowSize = 50;
  kNumDelsTrigger = 40;
  static_cast<CompactOnDeletionCollectorFactory*>
    (compact_on_del.get())->SetWindowSize(kWindowSize);
  static_cast<CompactOnDeletionCollectorFactory*>
    (compact_on_del.get())->SetDeletionTrigger(kNumDelsTrigger);
  for (int i = 0; i < kNumKeys; ++i) {
    if (i >= kNumKeys - kWindowSize &&
        i < kNumKeys - kWindowSize + kNumDelsTrigger) {
      Delete(Key(i));
    } else {
      Put(Key(i), "val");
    }
  }
  Flush();

  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_GT(NumTableFilesAtLevel(1), 0);

  // Change the window size to disable delete triggered compaction
  kWindowSize = 0;
  static_cast<CompactOnDeletionCollectorFactory*>
    (compact_on_del.get())->SetWindowSize(kWindowSize);
  static_cast<CompactOnDeletionCollectorFactory*>
    (compact_on_del.get())->SetDeletionTrigger(kNumDelsTrigger);
  for (int i = 0; i < kNumKeys; ++i) {
    if (i >= kNumKeys - kWindowSize &&
        i < kNumKeys - kWindowSize + kNumDelsTrigger) {
      Delete(Key(i));
    } else {
      Put(Key(i), "val");
    }
  }
  Flush();

  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(1, NumTableFilesAtLevel(0));

}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
