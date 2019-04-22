//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/utilities/option_change_migration.h"
#include <set>
#include "db/db_test_util.h"
#include "port/stack_trace.h"
namespace rocksdb {

class DBOptionChangeMigrationTests
    : public DBTestBase,
      public testing::WithParamInterface<
          std::tuple<int, int, bool, int, int, bool>> {
 public:
  DBOptionChangeMigrationTests()
      : DBTestBase("/db_option_change_migration_test") {
    level1_ = std::get<0>(GetParam());
    compaction_style1_ = std::get<1>(GetParam());
    is_dynamic1_ = std::get<2>(GetParam());

    level2_ = std::get<3>(GetParam());
    compaction_style2_ = std::get<4>(GetParam());
    is_dynamic2_ = std::get<5>(GetParam());
  }

  // Required if inheriting from testing::WithParamInterface<>
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

  int level1_;
  int compaction_style1_;
  bool is_dynamic1_;

  int level2_;
  int compaction_style2_;
  bool is_dynamic2_;
};

#ifndef ROCKSDB_LITE
TEST_P(DBOptionChangeMigrationTests, Migrate1) {
  Options old_options = CurrentOptions();
  old_options.compaction_style =
      static_cast<CompactionStyle>(compaction_style1_);
  if (old_options.compaction_style == CompactionStyle::kCompactionStyleLevel) {
    old_options.level_compaction_dynamic_level_bytes = is_dynamic1_;
  }

  old_options.level0_file_num_compaction_trigger = 3;
  old_options.write_buffer_size = 64 * 1024;
  old_options.target_file_size_base = 128 * 1024;
  // Make level target of L1, L2 to be 200KB and 600KB
  old_options.num_levels = level1_;
  old_options.max_bytes_for_level_multiplier = 3;
  old_options.max_bytes_for_level_base = 200 * 1024;

  Reopen(old_options);

  Random rnd(301);
  int key_idx = 0;

  // Generate at least 2MB of data
  for (int num = 0; num < 20; num++) {
    GenerateNewFile(&rnd, &key_idx);
  }
  dbfull()->TEST_WaitForFlushMemTable();
  dbfull()->TEST_WaitForCompact();

  // Will make sure exactly those keys are in the DB after migration.
  std::set<std::string> keys;
  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToFirst();
    for (; it->Valid(); it->Next()) {
      keys.insert(it->key().ToString());
    }
  }
  Close();

  Options new_options = old_options;
  new_options.compaction_style =
      static_cast<CompactionStyle>(compaction_style2_);
  if (new_options.compaction_style == CompactionStyle::kCompactionStyleLevel) {
    new_options.level_compaction_dynamic_level_bytes = is_dynamic2_;
  }
  new_options.target_file_size_base = 256 * 1024;
  new_options.num_levels = level2_;
  new_options.max_bytes_for_level_base = 150 * 1024;
  new_options.max_bytes_for_level_multiplier = 4;
  ASSERT_OK(OptionChangeMigration(dbname_, old_options, new_options));
  Reopen(new_options);

  // Wait for compaction to finish and make sure it can reopen
  dbfull()->TEST_WaitForFlushMemTable();
  dbfull()->TEST_WaitForCompact();
  Reopen(new_options);

  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToFirst();
    for (std::string key : keys) {
      ASSERT_TRUE(it->Valid());
      ASSERT_EQ(key, it->key().ToString());
      it->Next();
    }
    ASSERT_TRUE(!it->Valid());
  }
}

TEST_P(DBOptionChangeMigrationTests, Migrate2) {
  Options old_options = CurrentOptions();
  old_options.compaction_style =
      static_cast<CompactionStyle>(compaction_style2_);
  if (old_options.compaction_style == CompactionStyle::kCompactionStyleLevel) {
    old_options.level_compaction_dynamic_level_bytes = is_dynamic2_;
  }
  old_options.level0_file_num_compaction_trigger = 3;
  old_options.write_buffer_size = 64 * 1024;
  old_options.target_file_size_base = 128 * 1024;
  // Make level target of L1, L2 to be 200KB and 600KB
  old_options.num_levels = level2_;
  old_options.max_bytes_for_level_multiplier = 3;
  old_options.max_bytes_for_level_base = 200 * 1024;

  Reopen(old_options);

  Random rnd(301);
  int key_idx = 0;

  // Generate at least 2MB of data
  for (int num = 0; num < 20; num++) {
    GenerateNewFile(&rnd, &key_idx);
  }
  dbfull()->TEST_WaitForFlushMemTable();
  dbfull()->TEST_WaitForCompact();

  // Will make sure exactly those keys are in the DB after migration.
  std::set<std::string> keys;
  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToFirst();
    for (; it->Valid(); it->Next()) {
      keys.insert(it->key().ToString());
    }
  }

  Close();

  Options new_options = old_options;
  new_options.compaction_style =
      static_cast<CompactionStyle>(compaction_style1_);
  if (new_options.compaction_style == CompactionStyle::kCompactionStyleLevel) {
    new_options.level_compaction_dynamic_level_bytes = is_dynamic1_;
  }
  new_options.target_file_size_base = 256 * 1024;
  new_options.num_levels = level1_;
  new_options.max_bytes_for_level_base = 150 * 1024;
  new_options.max_bytes_for_level_multiplier = 4;
  ASSERT_OK(OptionChangeMigration(dbname_, old_options, new_options));
  Reopen(new_options);
  // Wait for compaction to finish and make sure it can reopen
  dbfull()->TEST_WaitForFlushMemTable();
  dbfull()->TEST_WaitForCompact();
  Reopen(new_options);

  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToFirst();
    for (std::string key : keys) {
      ASSERT_TRUE(it->Valid());
      ASSERT_EQ(key, it->key().ToString());
      it->Next();
    }
    ASSERT_TRUE(!it->Valid());
  }
}

TEST_P(DBOptionChangeMigrationTests, Migrate3) {
  Options old_options = CurrentOptions();
  old_options.compaction_style =
      static_cast<CompactionStyle>(compaction_style1_);
  if (old_options.compaction_style == CompactionStyle::kCompactionStyleLevel) {
    old_options.level_compaction_dynamic_level_bytes = is_dynamic1_;
  }

  old_options.level0_file_num_compaction_trigger = 3;
  old_options.write_buffer_size = 64 * 1024;
  old_options.target_file_size_base = 128 * 1024;
  // Make level target of L1, L2 to be 200KB and 600KB
  old_options.num_levels = level1_;
  old_options.max_bytes_for_level_multiplier = 3;
  old_options.max_bytes_for_level_base = 200 * 1024;

  Reopen(old_options);
  Random rnd(301);
  for (int num = 0; num < 20; num++) {
    for (int i = 0; i < 50; i++) {
      ASSERT_OK(Put(Key(num * 100 + i), RandomString(&rnd, 900)));
    }
    Flush();
    dbfull()->TEST_WaitForCompact();
    if (num == 9) {
      // Issue a full compaction to generate some zero-out files
      CompactRangeOptions cro;
      cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
      dbfull()->CompactRange(cro, nullptr, nullptr);
    }
  }
  dbfull()->TEST_WaitForFlushMemTable();
  dbfull()->TEST_WaitForCompact();

  // Will make sure exactly those keys are in the DB after migration.
  std::set<std::string> keys;
  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToFirst();
    for (; it->Valid(); it->Next()) {
      keys.insert(it->key().ToString());
    }
  }
  Close();

  Options new_options = old_options;
  new_options.compaction_style =
      static_cast<CompactionStyle>(compaction_style2_);
  if (new_options.compaction_style == CompactionStyle::kCompactionStyleLevel) {
    new_options.level_compaction_dynamic_level_bytes = is_dynamic2_;
  }
  new_options.target_file_size_base = 256 * 1024;
  new_options.num_levels = level2_;
  new_options.max_bytes_for_level_base = 150 * 1024;
  new_options.max_bytes_for_level_multiplier = 4;
  ASSERT_OK(OptionChangeMigration(dbname_, old_options, new_options));
  Reopen(new_options);

  // Wait for compaction to finish and make sure it can reopen
  dbfull()->TEST_WaitForFlushMemTable();
  dbfull()->TEST_WaitForCompact();
  Reopen(new_options);

  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToFirst();
    for (std::string key : keys) {
      ASSERT_TRUE(it->Valid());
      ASSERT_EQ(key, it->key().ToString());
      it->Next();
    }
    ASSERT_TRUE(!it->Valid());
  }
}

TEST_P(DBOptionChangeMigrationTests, Migrate4) {
  Options old_options = CurrentOptions();
  old_options.compaction_style =
      static_cast<CompactionStyle>(compaction_style2_);
  if (old_options.compaction_style == CompactionStyle::kCompactionStyleLevel) {
    old_options.level_compaction_dynamic_level_bytes = is_dynamic2_;
  }
  old_options.level0_file_num_compaction_trigger = 3;
  old_options.write_buffer_size = 64 * 1024;
  old_options.target_file_size_base = 128 * 1024;
  // Make level target of L1, L2 to be 200KB and 600KB
  old_options.num_levels = level2_;
  old_options.max_bytes_for_level_multiplier = 3;
  old_options.max_bytes_for_level_base = 200 * 1024;

  Reopen(old_options);
  Random rnd(301);
  for (int num = 0; num < 20; num++) {
    for (int i = 0; i < 50; i++) {
      ASSERT_OK(Put(Key(num * 100 + i), RandomString(&rnd, 900)));
    }
    Flush();
    dbfull()->TEST_WaitForCompact();
    if (num == 9) {
      // Issue a full compaction to generate some zero-out files
      CompactRangeOptions cro;
      cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
      dbfull()->CompactRange(cro, nullptr, nullptr);
    }
  }
  dbfull()->TEST_WaitForFlushMemTable();
  dbfull()->TEST_WaitForCompact();

  // Will make sure exactly those keys are in the DB after migration.
  std::set<std::string> keys;
  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToFirst();
    for (; it->Valid(); it->Next()) {
      keys.insert(it->key().ToString());
    }
  }

  Close();

  Options new_options = old_options;
  new_options.compaction_style =
      static_cast<CompactionStyle>(compaction_style1_);
  if (new_options.compaction_style == CompactionStyle::kCompactionStyleLevel) {
    new_options.level_compaction_dynamic_level_bytes = is_dynamic1_;
  }
  new_options.target_file_size_base = 256 * 1024;
  new_options.num_levels = level1_;
  new_options.max_bytes_for_level_base = 150 * 1024;
  new_options.max_bytes_for_level_multiplier = 4;
  ASSERT_OK(OptionChangeMigration(dbname_, old_options, new_options));
  Reopen(new_options);
  // Wait for compaction to finish and make sure it can reopen
  dbfull()->TEST_WaitForFlushMemTable();
  dbfull()->TEST_WaitForCompact();
  Reopen(new_options);

  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToFirst();
    for (std::string key : keys) {
      ASSERT_TRUE(it->Valid());
      ASSERT_EQ(key, it->key().ToString());
      it->Next();
    }
    ASSERT_TRUE(!it->Valid());
  }
}

INSTANTIATE_TEST_CASE_P(
    DBOptionChangeMigrationTests, DBOptionChangeMigrationTests,
    ::testing::Values(std::make_tuple(3, 0, false, 4, 0, false),
                      std::make_tuple(3, 0, true, 4, 0, true),
                      std::make_tuple(3, 0, true, 4, 0, false),
                      std::make_tuple(3, 0, false, 4, 0, true),
                      std::make_tuple(3, 1, false, 4, 1, false),
                      std::make_tuple(1, 1, false, 4, 1, false),
                      std::make_tuple(3, 0, false, 4, 1, false),
                      std::make_tuple(3, 0, false, 1, 1, false),
                      std::make_tuple(3, 0, true, 4, 1, false),
                      std::make_tuple(3, 0, true, 1, 1, false),
                      std::make_tuple(1, 1, false, 4, 0, false),
                      std::make_tuple(4, 0, false, 1, 2, false),
                      std::make_tuple(3, 0, true, 2, 2, false),
                      std::make_tuple(3, 1, false, 3, 2, false),
                      std::make_tuple(1, 1, false, 4, 2, false)));

class DBOptionChangeMigrationTest : public DBTestBase {
 public:
  DBOptionChangeMigrationTest()
      : DBTestBase("/db_option_change_migration_test2") {}
};

TEST_F(DBOptionChangeMigrationTest, CompactedSrcToUniversal) {
  Options old_options = CurrentOptions();
  old_options.compaction_style = CompactionStyle::kCompactionStyleLevel;
  old_options.max_compaction_bytes = 200 * 1024;
  old_options.level_compaction_dynamic_level_bytes = false;
  old_options.level0_file_num_compaction_trigger = 3;
  old_options.write_buffer_size = 64 * 1024;
  old_options.target_file_size_base = 128 * 1024;
  // Make level target of L1, L2 to be 200KB and 600KB
  old_options.num_levels = 4;
  old_options.max_bytes_for_level_multiplier = 3;
  old_options.max_bytes_for_level_base = 200 * 1024;

  Reopen(old_options);
  Random rnd(301);
  for (int num = 0; num < 20; num++) {
    for (int i = 0; i < 50; i++) {
      ASSERT_OK(Put(Key(num * 100 + i), RandomString(&rnd, 900)));
    }
  }
  Flush();
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  dbfull()->CompactRange(cro, nullptr, nullptr);

  // Will make sure exactly those keys are in the DB after migration.
  std::set<std::string> keys;
  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToFirst();
    for (; it->Valid(); it->Next()) {
      keys.insert(it->key().ToString());
    }
  }

  Close();

  Options new_options = old_options;
  new_options.compaction_style = CompactionStyle::kCompactionStyleUniversal;
  new_options.target_file_size_base = 256 * 1024;
  new_options.num_levels = 1;
  new_options.max_bytes_for_level_base = 150 * 1024;
  new_options.max_bytes_for_level_multiplier = 4;
  ASSERT_OK(OptionChangeMigration(dbname_, old_options, new_options));
  Reopen(new_options);
  // Wait for compaction to finish and make sure it can reopen
  dbfull()->TEST_WaitForFlushMemTable();
  dbfull()->TEST_WaitForCompact();
  Reopen(new_options);

  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToFirst();
    for (std::string key : keys) {
      ASSERT_TRUE(it->Valid());
      ASSERT_EQ(key, it->key().ToString());
      it->Next();
    }
    ASSERT_TRUE(!it->Valid());
  }
}

#endif  // ROCKSDB_LITE
}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
