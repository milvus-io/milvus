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
#if !defined(ROCKSDB_LITE)
#include "rocksdb/utilities/table_properties_collectors.h"
#include "util/sync_point.h"

namespace rocksdb {

static std::string CompressibleString(Random* rnd, int len) {
  std::string r;
  test::CompressibleString(rnd, 0.8, len, &r);
  return r;
}

class DBTestUniversalCompactionBase
    : public DBTestBase,
      public ::testing::WithParamInterface<std::tuple<int, bool>> {
 public:
  explicit DBTestUniversalCompactionBase(
      const std::string& path) : DBTestBase(path) {}
  void SetUp() override {
    num_levels_ = std::get<0>(GetParam());
    exclusive_manual_compaction_ = std::get<1>(GetParam());
  }
  int num_levels_;
  bool exclusive_manual_compaction_;
};

class DBTestUniversalCompaction : public DBTestUniversalCompactionBase {
 public:
  DBTestUniversalCompaction() :
      DBTestUniversalCompactionBase("/db_universal_compaction_test") {}
};

class DBTestUniversalDeleteTrigCompaction : public DBTestBase {
 public:
  DBTestUniversalDeleteTrigCompaction()
      : DBTestBase("/db_universal_compaction_test") {}
};

namespace {
void VerifyCompactionResult(
    const ColumnFamilyMetaData& cf_meta,
    const std::set<std::string>& overlapping_file_numbers) {
#ifndef NDEBUG
  for (auto& level : cf_meta.levels) {
    for (auto& file : level.files) {
      assert(overlapping_file_numbers.find(file.name) ==
             overlapping_file_numbers.end());
    }
  }
#endif
}

class KeepFilter : public CompactionFilter {
 public:
  bool Filter(int /*level*/, const Slice& /*key*/, const Slice& /*value*/,
              std::string* /*new_value*/,
              bool* /*value_changed*/) const override {
    return false;
  }

  const char* Name() const override { return "KeepFilter"; }
};

class KeepFilterFactory : public CompactionFilterFactory {
 public:
  explicit KeepFilterFactory(bool check_context = false)
      : check_context_(check_context) {}

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override {
    if (check_context_) {
      EXPECT_EQ(expect_full_compaction_.load(), context.is_full_compaction);
      EXPECT_EQ(expect_manual_compaction_.load(), context.is_manual_compaction);
    }
    return std::unique_ptr<CompactionFilter>(new KeepFilter());
  }

  const char* Name() const override { return "KeepFilterFactory"; }
  bool check_context_;
  std::atomic_bool expect_full_compaction_;
  std::atomic_bool expect_manual_compaction_;
};

class DelayFilter : public CompactionFilter {
 public:
  explicit DelayFilter(DBTestBase* d) : db_test(d) {}
  bool Filter(int /*level*/, const Slice& /*key*/, const Slice& /*value*/,
              std::string* /*new_value*/,
              bool* /*value_changed*/) const override {
    db_test->env_->addon_time_.fetch_add(1000);
    return true;
  }

  const char* Name() const override { return "DelayFilter"; }

 private:
  DBTestBase* db_test;
};

class DelayFilterFactory : public CompactionFilterFactory {
 public:
  explicit DelayFilterFactory(DBTestBase* d) : db_test(d) {}
  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::unique_ptr<CompactionFilter>(new DelayFilter(db_test));
  }

  const char* Name() const override { return "DelayFilterFactory"; }

 private:
  DBTestBase* db_test;
};
}  // namespace

// Make sure we don't trigger a problem if the trigger condtion is given
// to be 0, which is invalid.
TEST_P(DBTestUniversalCompaction, UniversalCompactionSingleSortedRun) {
  Options options = CurrentOptions();

  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = num_levels_;
  // Config universal compaction to always compact to one single sorted run.
  options.level0_file_num_compaction_trigger = 0;
  options.compaction_options_universal.size_ratio = 10;
  options.compaction_options_universal.min_merge_width = 2;
  options.compaction_options_universal.max_size_amplification_percent = 0;

  options.write_buffer_size = 105 << 10;  // 105KB
  options.arena_block_size = 4 << 10;
  options.target_file_size_base = 32 << 10;  // 32KB
  // trigger compaction if there are >= 4 files
  KeepFilterFactory* filter = new KeepFilterFactory(true);
  filter->expect_manual_compaction_.store(false);
  options.compaction_filter_factory.reset(filter);

  DestroyAndReopen(options);
  ASSERT_EQ(1, db_->GetOptions().level0_file_num_compaction_trigger);

  Random rnd(301);
  int key_idx = 0;

  filter->expect_full_compaction_.store(true);

  for (int num = 0; num < 16; num++) {
    // Write 100KB file. And immediately it should be compacted to one file.
    GenerateNewFile(&rnd, &key_idx);
    dbfull()->TEST_WaitForCompact();
    ASSERT_EQ(NumSortedRuns(0), 1);
  }
  ASSERT_OK(Put(Key(key_idx), ""));
  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(NumSortedRuns(0), 1);
}

TEST_P(DBTestUniversalCompaction, OptimizeFiltersForHits) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.compaction_options_universal.size_ratio = 5;
  options.num_levels = num_levels_;
  options.write_buffer_size = 105 << 10;  // 105KB
  options.arena_block_size = 4 << 10;
  options.target_file_size_base = 32 << 10;  // 32KB
  // trigger compaction if there are >= 4 files
  options.level0_file_num_compaction_trigger = 4;
  BlockBasedTableOptions bbto;
  bbto.cache_index_and_filter_blocks = true;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.whole_key_filtering = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  options.optimize_filters_for_hits = true;
  options.statistics = rocksdb::CreateDBStatistics();
  options.memtable_factory.reset(new SpecialSkipListFactory(3));

  DestroyAndReopen(options);

  // block compaction from happening
  env_->SetBackgroundThreads(1, Env::LOW);
  test::SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);

  for (int num = 0; num < options.level0_file_num_compaction_trigger; num++) {
    Put(Key(num * 10), "val");
    if (num) {
      dbfull()->TEST_WaitForFlushMemTable();
    }
    Put(Key(30 + num * 10), "val");
    Put(Key(60 + num * 10), "val");
  }
  Put("", "");
  dbfull()->TEST_WaitForFlushMemTable();

  // Query set of non existing keys
  for (int i = 5; i < 90; i += 10) {
    ASSERT_EQ(Get(Key(i)), "NOT_FOUND");
  }

  // Make sure bloom filter is used at least once.
  ASSERT_GT(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);
  auto prev_counter = TestGetTickerCount(options, BLOOM_FILTER_USEFUL);

  // Make sure bloom filter is used for all but the last L0 file when looking
  // up a non-existent key that's in the range of all L0 files.
  ASSERT_EQ(Get(Key(35)), "NOT_FOUND");
  ASSERT_EQ(prev_counter + NumTableFilesAtLevel(0) - 1,
            TestGetTickerCount(options, BLOOM_FILTER_USEFUL));
  prev_counter = TestGetTickerCount(options, BLOOM_FILTER_USEFUL);

  // Unblock compaction and wait it for happening.
  sleeping_task_low.WakeUp();
  dbfull()->TEST_WaitForCompact();

  // The same queries will not trigger bloom filter
  for (int i = 5; i < 90; i += 10) {
    ASSERT_EQ(Get(Key(i)), "NOT_FOUND");
  }
  ASSERT_EQ(prev_counter, TestGetTickerCount(options, BLOOM_FILTER_USEFUL));
}

// TODO(kailiu) The tests on UniversalCompaction has some issues:
//  1. A lot of magic numbers ("11" or "12").
//  2. Made assumption on the memtable flush conditions, which may change from
//     time to time.
TEST_P(DBTestUniversalCompaction, UniversalCompactionTrigger) {
  Options options;
  options.compaction_style = kCompactionStyleUniversal;
  options.compaction_options_universal.size_ratio = 5;
  options.num_levels = num_levels_;
  options.write_buffer_size = 105 << 10;  // 105KB
  options.arena_block_size = 4 << 10;
  options.target_file_size_base = 32 << 10;  // 32KB
  // trigger compaction if there are >= 4 files
  options.level0_file_num_compaction_trigger = 4;
  KeepFilterFactory* filter = new KeepFilterFactory(true);
  filter->expect_manual_compaction_.store(false);
  options.compaction_filter_factory.reset(filter);

  options = CurrentOptions(options);
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBTestWritableFile.GetPreallocationStatus", [&](void* arg) {
        ASSERT_TRUE(arg != nullptr);
        size_t preallocation_size = *(static_cast<size_t*>(arg));
        if (num_levels_ > 3) {
          ASSERT_LE(preallocation_size, options.target_file_size_base * 1.1);
        }
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Random rnd(301);
  int key_idx = 0;

  filter->expect_full_compaction_.store(true);
  // Stage 1:
  //   Generate a set of files at level 0, but don't trigger level-0
  //   compaction.
  for (int num = 0; num < options.level0_file_num_compaction_trigger - 1;
       num++) {
    // Write 100KB
    GenerateNewFile(1, &rnd, &key_idx);
  }

  // Generate one more file at level-0, which should trigger level-0
  // compaction.
  GenerateNewFile(1, &rnd, &key_idx);
  // Suppose each file flushed from mem table has size 1. Now we compact
  // (level0_file_num_compaction_trigger+1)=4 files and should have a big
  // file of size 4.
  ASSERT_EQ(NumSortedRuns(1), 1);

  // Stage 2:
  //   Now we have one file at level 0, with size 4. We also have some data in
  //   mem table. Let's continue generating new files at level 0, but don't
  //   trigger level-0 compaction.
  //   First, clean up memtable before inserting new data. This will generate
  //   a level-0 file, with size around 0.4 (according to previously written
  //   data amount).
  filter->expect_full_compaction_.store(false);
  ASSERT_OK(Flush(1));
  for (int num = 0; num < options.level0_file_num_compaction_trigger - 3;
       num++) {
    GenerateNewFile(1, &rnd, &key_idx);
    ASSERT_EQ(NumSortedRuns(1), num + 3);
  }

  // Generate one more file at level-0, which should trigger level-0
  // compaction.
  GenerateNewFile(1, &rnd, &key_idx);
  // Before compaction, we have 4 files at level 0, with size 4, 0.4, 1, 1.
  // After compaction, we should have 2 files, with size 4, 2.4.
  ASSERT_EQ(NumSortedRuns(1), 2);

  // Stage 3:
  //   Now we have 2 files at level 0, with size 4 and 2.4. Continue
  //   generating new files at level 0.
  for (int num = 0; num < options.level0_file_num_compaction_trigger - 3;
       num++) {
    GenerateNewFile(1, &rnd, &key_idx);
    ASSERT_EQ(NumSortedRuns(1), num + 3);
  }

  // Generate one more file at level-0, which should trigger level-0
  // compaction.
  GenerateNewFile(1, &rnd, &key_idx);
  // Before compaction, we have 4 files at level 0, with size 4, 2.4, 1, 1.
  // After compaction, we should have 3 files, with size 4, 2.4, 2.
  ASSERT_EQ(NumSortedRuns(1), 3);

  // Stage 4:
  //   Now we have 3 files at level 0, with size 4, 2.4, 2. Let's generate a
  //   new file of size 1.
  GenerateNewFile(1, &rnd, &key_idx);
  dbfull()->TEST_WaitForCompact();
  // Level-0 compaction is triggered, but no file will be picked up.
  ASSERT_EQ(NumSortedRuns(1), 4);

  // Stage 5:
  //   Now we have 4 files at level 0, with size 4, 2.4, 2, 1. Let's generate
  //   a new file of size 1.
  filter->expect_full_compaction_.store(true);
  GenerateNewFile(1, &rnd, &key_idx);
  dbfull()->TEST_WaitForCompact();
  // All files at level 0 will be compacted into a single one.
  ASSERT_EQ(NumSortedRuns(1), 1);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(DBTestUniversalCompaction, UniversalCompactionSizeAmplification) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = num_levels_;
  options.write_buffer_size = 100 << 10;     // 100KB
  options.target_file_size_base = 32 << 10;  // 32KB
  options.level0_file_num_compaction_trigger = 3;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  // Trigger compaction if size amplification exceeds 110%
  options.compaction_options_universal.max_size_amplification_percent = 110;
  options = CurrentOptions(options);
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  Random rnd(301);
  int key_idx = 0;

  //   Generate two files in Level 0. Both files are approx the same size.
  for (int num = 0; num < options.level0_file_num_compaction_trigger - 1;
       num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(1, Key(key_idx), RandomString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
    ASSERT_EQ(NumSortedRuns(1), num + 1);
  }
  ASSERT_EQ(NumSortedRuns(1), 2);

  // Flush whatever is remaining in memtable. This is typically
  // small, which should not trigger size ratio based compaction
  // but will instead trigger size amplification.
  ASSERT_OK(Flush(1));

  dbfull()->TEST_WaitForCompact();

  // Verify that size amplification did occur
  ASSERT_EQ(NumSortedRuns(1), 1);
}

TEST_P(DBTestUniversalCompaction, DynamicUniversalCompactionSizeAmplification) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 1;
  options.write_buffer_size = 100 << 10;     // 100KB
  options.target_file_size_base = 32 << 10;  // 32KB
  options.level0_file_num_compaction_trigger = 3;
  // Initial setup of compaction_options_universal will prevent universal
  // compaction from happening
  options.compaction_options_universal.size_ratio = 100;
  options.compaction_options_universal.min_merge_width = 100;
  DestroyAndReopen(options);

  int total_picked_compactions = 0;
  int total_size_amp_compactions = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "UniversalCompactionPicker::PickCompaction:Return", [&](void* arg) {
        if (arg) {
          total_picked_compactions++;
          Compaction* c = static_cast<Compaction*>(arg);
          if (c->compaction_reason() ==
              CompactionReason::kUniversalSizeAmplification) {
            total_size_amp_compactions++;
          }
        }
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  MutableCFOptions mutable_cf_options;
  CreateAndReopenWithCF({"pikachu"}, options);

  Random rnd(301);
  int key_idx = 0;

  //   Generate two files in Level 0. Both files are approx the same size.
  for (int num = 0; num < options.level0_file_num_compaction_trigger - 1;
       num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(1, Key(key_idx), RandomString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
    ASSERT_EQ(NumSortedRuns(1), num + 1);
  }
  ASSERT_EQ(NumSortedRuns(1), 2);

  // Flush whatever is remaining in memtable. This is typically
  // small, which should not trigger size ratio based compaction
  // but could instead trigger size amplification if it's set
  // to 110.
  ASSERT_OK(Flush(1));
  dbfull()->TEST_WaitForCompact();
  // Verify compaction did not happen
  ASSERT_EQ(NumSortedRuns(1), 3);

  // Trigger compaction if size amplification exceeds 110% without reopening DB
  ASSERT_EQ(dbfull()
                ->GetOptions(handles_[1])
                .compaction_options_universal.max_size_amplification_percent,
            200);
  ASSERT_OK(dbfull()->SetOptions(handles_[1],
                                 {{"compaction_options_universal",
                                   "{max_size_amplification_percent=110;}"}}));
  ASSERT_EQ(dbfull()
                ->GetOptions(handles_[1])
                .compaction_options_universal.max_size_amplification_percent,
            110);
  ASSERT_OK(dbfull()->TEST_GetLatestMutableCFOptions(handles_[1],
                                                     &mutable_cf_options));
  ASSERT_EQ(110, mutable_cf_options.compaction_options_universal
                     .max_size_amplification_percent);

  dbfull()->TEST_WaitForCompact();
  // Verify that size amplification did happen
  ASSERT_EQ(NumSortedRuns(1), 1);
  ASSERT_EQ(total_picked_compactions, 1);
  ASSERT_EQ(total_size_amp_compactions, 1);
}

TEST_P(DBTestUniversalCompaction, DynamicUniversalCompactionReadAmplification) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 1;
  options.write_buffer_size = 100 << 10;     // 100KB
  options.target_file_size_base = 32 << 10;  // 32KB
  options.level0_file_num_compaction_trigger = 3;
  // Initial setup of compaction_options_universal will prevent universal
  // compaction from happening
  options.compaction_options_universal.max_size_amplification_percent = 2000;
  options.compaction_options_universal.size_ratio = 0;
  options.compaction_options_universal.min_merge_width = 100;
  DestroyAndReopen(options);

  int total_picked_compactions = 0;
  int total_size_ratio_compactions = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "UniversalCompactionPicker::PickCompaction:Return", [&](void* arg) {
        if (arg) {
          total_picked_compactions++;
          Compaction* c = static_cast<Compaction*>(arg);
          if (c->compaction_reason() == CompactionReason::kUniversalSizeRatio) {
            total_size_ratio_compactions++;
          }
        }
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  MutableCFOptions mutable_cf_options;
  CreateAndReopenWithCF({"pikachu"}, options);

  Random rnd(301);
  int key_idx = 0;

  // Generate three files in Level 0. All files are approx the same size.
  for (int num = 0; num < options.level0_file_num_compaction_trigger; num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(1, Key(key_idx), RandomString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
    ASSERT_EQ(NumSortedRuns(1), num + 1);
  }
  ASSERT_EQ(NumSortedRuns(1), options.level0_file_num_compaction_trigger);

  // Flush whatever is remaining in memtable. This is typically small, about
  // 30KB.
  ASSERT_OK(Flush(1));
  dbfull()->TEST_WaitForCompact();
  // Verify compaction did not happen
  ASSERT_EQ(NumSortedRuns(1), options.level0_file_num_compaction_trigger + 1);
  ASSERT_EQ(total_picked_compactions, 0);

  ASSERT_OK(dbfull()->SetOptions(
      handles_[1],
      {{"compaction_options_universal",
        "{min_merge_width=2;max_merge_width=2;size_ratio=100;}"}}));
  ASSERT_EQ(dbfull()
                ->GetOptions(handles_[1])
                .compaction_options_universal.min_merge_width,
            2);
  ASSERT_EQ(dbfull()
                ->GetOptions(handles_[1])
                .compaction_options_universal.max_merge_width,
            2);
  ASSERT_EQ(
      dbfull()->GetOptions(handles_[1]).compaction_options_universal.size_ratio,
      100);

  ASSERT_OK(dbfull()->TEST_GetLatestMutableCFOptions(handles_[1],
                                                     &mutable_cf_options));
  ASSERT_EQ(mutable_cf_options.compaction_options_universal.size_ratio, 100);
  ASSERT_EQ(mutable_cf_options.compaction_options_universal.min_merge_width, 2);
  ASSERT_EQ(mutable_cf_options.compaction_options_universal.max_merge_width, 2);

  dbfull()->TEST_WaitForCompact();

  // Files in L0 are approx: 0.3 (30KB), 1, 1, 1.
  // On compaction: the files are below the size amp threshold, so we
  // fallthrough to checking read amp conditions. The configured size ratio is
  // not big enough to take 0.3 into consideration. So the next files 1 and 1
  // are compacted together first as they satisfy size ratio condition and
  // (min_merge_width, max_merge_width) condition, to give out a file size of 2.
  // Next, the newly generated 2 and the last file 1 are compacted together. So
  // at the end: #sortedRuns = 2, #picked_compactions = 2, and all the picked
  // ones are size ratio based compactions.
  ASSERT_EQ(NumSortedRuns(1), 2);
  // If max_merge_width had not been changed dynamically above, and if it
  // continued to be the default value of UINIT_MAX, total_picked_compactions
  // would have been 1.
  ASSERT_EQ(total_picked_compactions, 2);
  ASSERT_EQ(total_size_ratio_compactions, 2);
}

TEST_P(DBTestUniversalCompaction, CompactFilesOnUniversalCompaction) {
  const int kTestKeySize = 16;
  const int kTestValueSize = 984;
  const int kEntrySize = kTestKeySize + kTestValueSize;
  const int kEntriesPerBuffer = 10;

  ChangeCompactOptions();
  Options options;
  options.create_if_missing = true;
  options.compaction_style = kCompactionStyleLevel;
  options.num_levels = 1;
  options.target_file_size_base = options.write_buffer_size;
  options.compression = kNoCompression;
  options = CurrentOptions(options);
  options.write_buffer_size = kEntrySize * kEntriesPerBuffer;
  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_EQ(options.compaction_style, kCompactionStyleUniversal);
  Random rnd(301);
  for (int key = 1024 * kEntriesPerBuffer; key >= 0; --key) {
    ASSERT_OK(Put(1, ToString(key), RandomString(&rnd, kTestValueSize)));
  }
  dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
  dbfull()->TEST_WaitForCompact();
  ColumnFamilyMetaData cf_meta;
  dbfull()->GetColumnFamilyMetaData(handles_[1], &cf_meta);
  std::vector<std::string> compaction_input_file_names;
  for (auto file : cf_meta.levels[0].files) {
    if (rnd.OneIn(2)) {
      compaction_input_file_names.push_back(file.name);
    }
  }

  if (compaction_input_file_names.size() == 0) {
    compaction_input_file_names.push_back(
        cf_meta.levels[0].files[0].name);
  }

  // expect fail since universal compaction only allow L0 output
  ASSERT_FALSE(dbfull()
                   ->CompactFiles(CompactionOptions(), handles_[1],
                                  compaction_input_file_names, 1)
                   .ok());

  // expect ok and verify the compacted files no longer exist.
  ASSERT_OK(dbfull()->CompactFiles(
      CompactionOptions(), handles_[1],
      compaction_input_file_names, 0));

  dbfull()->GetColumnFamilyMetaData(handles_[1], &cf_meta);
  VerifyCompactionResult(
      cf_meta,
      std::set<std::string>(compaction_input_file_names.begin(),
          compaction_input_file_names.end()));

  compaction_input_file_names.clear();

  // Pick the first and the last file, expect everything is
  // compacted into one single file.
  compaction_input_file_names.push_back(
      cf_meta.levels[0].files[0].name);
  compaction_input_file_names.push_back(
      cf_meta.levels[0].files[
          cf_meta.levels[0].files.size() - 1].name);
  ASSERT_OK(dbfull()->CompactFiles(
      CompactionOptions(), handles_[1],
      compaction_input_file_names, 0));

  dbfull()->GetColumnFamilyMetaData(handles_[1], &cf_meta);
  ASSERT_EQ(cf_meta.levels[0].files.size(), 1U);
}

TEST_P(DBTestUniversalCompaction, UniversalCompactionTargetLevel) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 100 << 10;     // 100KB
  options.num_levels = 7;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  // Generate 3 overlapping files
  Random rnd(301);
  for (int i = 0; i < 210; i++) {
    ASSERT_OK(Put(Key(i), RandomString(&rnd, 100)));
  }
  ASSERT_OK(Flush());

  for (int i = 200; i < 300; i++) {
    ASSERT_OK(Put(Key(i), RandomString(&rnd, 100)));
  }
  ASSERT_OK(Flush());

  for (int i = 250; i < 260; i++) {
    ASSERT_OK(Put(Key(i), RandomString(&rnd, 100)));
  }
  ASSERT_OK(Flush());

  ASSERT_EQ("3", FilesPerLevel(0));
  // Compact all files into 1 file and put it in L4
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 4;
  compact_options.exclusive_manual_compaction = exclusive_manual_compaction_;
  db_->CompactRange(compact_options, nullptr, nullptr);
  ASSERT_EQ("0,0,0,0,1", FilesPerLevel(0));
}

#ifndef ROCKSDB_VALGRIND_RUN
class DBTestUniversalCompactionMultiLevels
    : public DBTestUniversalCompactionBase {
 public:
  DBTestUniversalCompactionMultiLevels() :
      DBTestUniversalCompactionBase(
          "/db_universal_compaction_multi_levels_test") {}
};

TEST_P(DBTestUniversalCompactionMultiLevels, UniversalCompactionMultiLevels) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = num_levels_;
  options.write_buffer_size = 100 << 10;  // 100KB
  options.level0_file_num_compaction_trigger = 8;
  options.max_background_compactions = 3;
  options.target_file_size_base = 32 * 1024;
  CreateAndReopenWithCF({"pikachu"}, options);

  // Trigger compaction if size amplification exceeds 110%
  options.compaction_options_universal.max_size_amplification_percent = 110;
  options = CurrentOptions(options);
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  Random rnd(301);
  int num_keys = 100000;
  for (int i = 0; i < num_keys * 2; i++) {
    ASSERT_OK(Put(1, Key(i % num_keys), Key(i)));
  }

  dbfull()->TEST_WaitForCompact();

  for (int i = num_keys; i < num_keys * 2; i++) {
    ASSERT_EQ(Get(1, Key(i % num_keys)), Key(i));
  }
}

// Tests universal compaction with trivial move enabled
TEST_P(DBTestUniversalCompactionMultiLevels, UniversalCompactionTrivialMove) {
  int32_t trivial_move = 0;
  int32_t non_trivial_move = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:TrivialMove",
      [&](void* /*arg*/) { trivial_move++; });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial", [&](void* arg) {
        non_trivial_move++;
        ASSERT_TRUE(arg != nullptr);
        int output_level = *(static_cast<int*>(arg));
        ASSERT_EQ(output_level, 0);
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.compaction_options_universal.allow_trivial_move = true;
  options.num_levels = 3;
  options.write_buffer_size = 100 << 10;  // 100KB
  options.level0_file_num_compaction_trigger = 3;
  options.max_background_compactions = 2;
  options.target_file_size_base = 32 * 1024;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  // Trigger compaction if size amplification exceeds 110%
  options.compaction_options_universal.max_size_amplification_percent = 110;
  options = CurrentOptions(options);
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  Random rnd(301);
  int num_keys = 150000;
  for (int i = 0; i < num_keys; i++) {
    ASSERT_OK(Put(1, Key(i), Key(i)));
  }
  std::vector<std::string> values;

  ASSERT_OK(Flush(1));
  dbfull()->TEST_WaitForCompact();

  ASSERT_GT(trivial_move, 0);
  ASSERT_GT(non_trivial_move, 0);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

INSTANTIATE_TEST_CASE_P(DBTestUniversalCompactionMultiLevels,
                        DBTestUniversalCompactionMultiLevels,
                        ::testing::Combine(::testing::Values(3, 20),
                                           ::testing::Bool()));

class DBTestUniversalCompactionParallel :
    public DBTestUniversalCompactionBase {
 public:
  DBTestUniversalCompactionParallel() :
      DBTestUniversalCompactionBase(
          "/db_universal_compaction_prallel_test") {}
};

TEST_P(DBTestUniversalCompactionParallel, UniversalCompactionParallel) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = num_levels_;
  options.write_buffer_size = 1 << 10;  // 1KB
  options.level0_file_num_compaction_trigger = 3;
  options.max_background_compactions = 3;
  options.max_background_flushes = 3;
  options.target_file_size_base = 1 * 1024;
  options.compaction_options_universal.max_size_amplification_percent = 110;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  // Delay every compaction so multiple compactions will happen.
  std::atomic<int> num_compactions_running(0);
  std::atomic<bool> has_parallel(false);
  rocksdb::SyncPoint::GetInstance()->SetCallBack("CompactionJob::Run():Start",
                                                 [&](void* /*arg*/) {
    if (num_compactions_running.fetch_add(1) > 0) {
      has_parallel.store(true);
      return;
    }
    for (int nwait = 0; nwait < 20000; nwait++) {
      if (has_parallel.load() || num_compactions_running.load() > 1) {
        has_parallel.store(true);
        break;
      }
      env_->SleepForMicroseconds(1000);
    }
  });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run():End",
      [&](void* /*arg*/) { num_compactions_running.fetch_add(-1); });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  options = CurrentOptions(options);
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  Random rnd(301);
  int num_keys = 30000;
  for (int i = 0; i < num_keys * 2; i++) {
    ASSERT_OK(Put(1, Key(i % num_keys), Key(i)));
  }
  dbfull()->TEST_WaitForCompact();

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ(num_compactions_running.load(), 0);
  ASSERT_TRUE(has_parallel.load());

  for (int i = num_keys; i < num_keys * 2; i++) {
    ASSERT_EQ(Get(1, Key(i % num_keys)), Key(i));
  }

  // Reopen and check.
  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  for (int i = num_keys; i < num_keys * 2; i++) {
    ASSERT_EQ(Get(1, Key(i % num_keys)), Key(i));
  }
}

TEST_P(DBTestUniversalCompactionParallel, PickByFileNumberBug) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = num_levels_;
  options.write_buffer_size = 1 * 1024;  // 1KB
  options.level0_file_num_compaction_trigger = 7;
  options.max_background_compactions = 2;
  options.target_file_size_base = 1024 * 1024;  // 1MB

  // Disable size amplifiction compaction
  options.compaction_options_universal.max_size_amplification_percent =
      UINT_MAX;
  DestroyAndReopen(options);

  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"DBTestUniversalCompactionParallel::PickByFileNumberBug:0",
        "BackgroundCallCompaction:0"},
       {"UniversalCompactionPicker::PickCompaction:Return",
        "DBTestUniversalCompactionParallel::PickByFileNumberBug:1"},
       {"DBTestUniversalCompactionParallel::PickByFileNumberBug:2",
        "CompactionJob::Run():Start"}});

  int total_picked_compactions = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "UniversalCompactionPicker::PickCompaction:Return", [&](void* arg) {
        if (arg) {
          total_picked_compactions++;
        }
      });

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  // Write 7 files to trigger compaction
  int key_idx = 1;
  for (int i = 1; i <= 70; i++) {
    std::string k = Key(key_idx++);
    ASSERT_OK(Put(k, k));
    if (i % 10 == 0) {
      ASSERT_OK(Flush());
    }
  }

  // Wait for the 1st background compaction process to start
  TEST_SYNC_POINT("DBTestUniversalCompactionParallel::PickByFileNumberBug:0");
  TEST_SYNC_POINT("DBTestUniversalCompactionParallel::PickByFileNumberBug:1");
  rocksdb::SyncPoint::GetInstance()->ClearTrace();

  // Write 3 files while 1st compaction is held
  // These 3 files have different sizes to avoid compacting based on size_ratio
  int num_keys = 1000;
  for (int i = 0; i < 3; i++) {
    for (int j = 1; j <= num_keys; j++) {
      std::string k = Key(key_idx++);
      ASSERT_OK(Put(k, k));
    }
    ASSERT_OK(Flush());
    num_keys -= 100;
  }

  // Hold the 1st compaction from finishing
  TEST_SYNC_POINT("DBTestUniversalCompactionParallel::PickByFileNumberBug:2");
  dbfull()->TEST_WaitForCompact();

  // There should only be one picked compaction as the score drops below one
  // after the first one is picked.
  EXPECT_EQ(total_picked_compactions, 1);
  EXPECT_EQ(TotalTableFiles(), 4);

  // Stop SyncPoint and destroy the DB and reopen it again
  rocksdb::SyncPoint::GetInstance()->ClearTrace();
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  key_idx = 1;
  total_picked_compactions = 0;
  DestroyAndReopen(options);

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  // Write 7 files to trigger compaction
  for (int i = 1; i <= 70; i++) {
    std::string k = Key(key_idx++);
    ASSERT_OK(Put(k, k));
    if (i % 10 == 0) {
      ASSERT_OK(Flush());
    }
  }

  // Wait for the 1st background compaction process to start
  TEST_SYNC_POINT("DBTestUniversalCompactionParallel::PickByFileNumberBug:0");
  TEST_SYNC_POINT("DBTestUniversalCompactionParallel::PickByFileNumberBug:1");
  rocksdb::SyncPoint::GetInstance()->ClearTrace();

  // Write 8 files while 1st compaction is held
  // These 8 files have different sizes to avoid compacting based on size_ratio
  num_keys = 1000;
  for (int i = 0; i < 8; i++) {
    for (int j = 1; j <= num_keys; j++) {
      std::string k = Key(key_idx++);
      ASSERT_OK(Put(k, k));
    }
    ASSERT_OK(Flush());
    num_keys -= 100;
  }

  // Wait for the 2nd background compaction process to start
  TEST_SYNC_POINT("DBTestUniversalCompactionParallel::PickByFileNumberBug:0");
  TEST_SYNC_POINT("DBTestUniversalCompactionParallel::PickByFileNumberBug:1");

  // Hold the 1st and 2nd compaction from finishing
  TEST_SYNC_POINT("DBTestUniversalCompactionParallel::PickByFileNumberBug:2");
  dbfull()->TEST_WaitForCompact();

  // This time we will trigger a compaction because of size ratio and
  // another compaction because of number of files that are not compacted
  // greater than 7
  EXPECT_GE(total_picked_compactions, 2);
}

INSTANTIATE_TEST_CASE_P(DBTestUniversalCompactionParallel,
                        DBTestUniversalCompactionParallel,
                        ::testing::Combine(::testing::Values(1, 10),
                                           ::testing::Values(false)));
#endif  // ROCKSDB_VALGRIND_RUN

TEST_P(DBTestUniversalCompaction, UniversalCompactionOptions) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 105 << 10;    // 105KB
  options.arena_block_size = 4 << 10;       // 4KB
  options.target_file_size_base = 32 << 10;  // 32KB
  options.level0_file_num_compaction_trigger = 4;
  options.num_levels = num_levels_;
  options.compaction_options_universal.compression_size_percent = -1;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  Random rnd(301);
  int key_idx = 0;

  for (int num = 0; num < options.level0_file_num_compaction_trigger; num++) {
    // Write 100KB (100 values, each 1K)
    for (int i = 0; i < 100; i++) {
      ASSERT_OK(Put(1, Key(key_idx), RandomString(&rnd, 990)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable(handles_[1]);

    if (num < options.level0_file_num_compaction_trigger - 1) {
      ASSERT_EQ(NumSortedRuns(1), num + 1);
    }
  }

  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(NumSortedRuns(1), 1);
}

TEST_P(DBTestUniversalCompaction, UniversalCompactionStopStyleSimilarSize) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 105 << 10;    // 105KB
  options.arena_block_size = 4 << 10;       // 4KB
  options.target_file_size_base = 32 << 10;  // 32KB
  // trigger compaction if there are >= 4 files
  options.level0_file_num_compaction_trigger = 4;
  options.compaction_options_universal.size_ratio = 10;
  options.compaction_options_universal.stop_style =
      kCompactionStopStyleSimilarSize;
  options.num_levels = num_levels_;
  DestroyAndReopen(options);

  Random rnd(301);
  int key_idx = 0;

  // Stage 1:
  //   Generate a set of files at level 0, but don't trigger level-0
  //   compaction.
  for (int num = 0; num < options.level0_file_num_compaction_trigger - 1;
       num++) {
    // Write 100KB (100 values, each 1K)
    for (int i = 0; i < 100; i++) {
      ASSERT_OK(Put(Key(key_idx), RandomString(&rnd, 990)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable();
    ASSERT_EQ(NumSortedRuns(), num + 1);
  }

  // Generate one more file at level-0, which should trigger level-0
  // compaction.
  for (int i = 0; i < 100; i++) {
    ASSERT_OK(Put(Key(key_idx), RandomString(&rnd, 990)));
    key_idx++;
  }
  dbfull()->TEST_WaitForCompact();
  // Suppose each file flushed from mem table has size 1. Now we compact
  // (level0_file_num_compaction_trigger+1)=4 files and should have a big
  // file of size 4.
  ASSERT_EQ(NumSortedRuns(), 1);

  // Stage 2:
  //   Now we have one file at level 0, with size 4. We also have some data in
  //   mem table. Let's continue generating new files at level 0, but don't
  //   trigger level-0 compaction.
  //   First, clean up memtable before inserting new data. This will generate
  //   a level-0 file, with size around 0.4 (according to previously written
  //   data amount).
  dbfull()->Flush(FlushOptions());
  for (int num = 0; num < options.level0_file_num_compaction_trigger - 3;
       num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 100; i++) {
      ASSERT_OK(Put(Key(key_idx), RandomString(&rnd, 990)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable();
    ASSERT_EQ(NumSortedRuns(), num + 3);
  }

  // Generate one more file at level-0, which should trigger level-0
  // compaction.
  for (int i = 0; i < 100; i++) {
    ASSERT_OK(Put(Key(key_idx), RandomString(&rnd, 990)));
    key_idx++;
  }
  dbfull()->TEST_WaitForCompact();
  // Before compaction, we have 4 files at level 0, with size 4, 0.4, 1, 1.
  // After compaction, we should have 3 files, with size 4, 0.4, 2.
  ASSERT_EQ(NumSortedRuns(), 3);
  // Stage 3:
  //   Now we have 3 files at level 0, with size 4, 0.4, 2. Generate one
  //   more file at level-0, which should trigger level-0 compaction.
  for (int i = 0; i < 100; i++) {
    ASSERT_OK(Put(Key(key_idx), RandomString(&rnd, 990)));
    key_idx++;
  }
  dbfull()->TEST_WaitForCompact();
  // Level-0 compaction is triggered, but no file will be picked up.
  ASSERT_EQ(NumSortedRuns(), 4);
}

TEST_P(DBTestUniversalCompaction, UniversalCompactionCompressRatio1) {
  if (!Snappy_Supported()) {
    return;
  }

  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 100 << 10;     // 100KB
  options.target_file_size_base = 32 << 10;  // 32KB
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = num_levels_;
  options.compaction_options_universal.compression_size_percent = 70;
  DestroyAndReopen(options);

  Random rnd(301);
  int key_idx = 0;

  // The first compaction (2) is compressed.
  for (int num = 0; num < 2; num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(Key(key_idx), CompressibleString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
  }
  ASSERT_LT(TotalSize(), 110000U * 2 * 0.9);

  // The second compaction (4) is compressed
  for (int num = 0; num < 2; num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(Key(key_idx), CompressibleString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
  }
  ASSERT_LT(TotalSize(), 110000 * 4 * 0.9);

  // The third compaction (2 4) is compressed since this time it is
  // (1 1 3.2) and 3.2/5.2 doesn't reach ratio.
  for (int num = 0; num < 2; num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(Key(key_idx), CompressibleString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
  }
  ASSERT_LT(TotalSize(), 110000 * 6 * 0.9);

  // When we start for the compaction up to (2 4 8), the latest
  // compressed is not compressed.
  for (int num = 0; num < 8; num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(Key(key_idx), CompressibleString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
  }
  ASSERT_GT(TotalSize(), 110000 * 11 * 0.8 + 110000 * 2);
}

TEST_P(DBTestUniversalCompaction, UniversalCompactionCompressRatio2) {
  if (!Snappy_Supported()) {
    return;
  }
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 100 << 10;     // 100KB
  options.target_file_size_base = 32 << 10;  // 32KB
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = num_levels_;
  options.compaction_options_universal.compression_size_percent = 95;
  DestroyAndReopen(options);

  Random rnd(301);
  int key_idx = 0;

  // When we start for the compaction up to (2 4 8), the latest
  // compressed is compressed given the size ratio to compress.
  for (int num = 0; num < 14; num++) {
    // Write 120KB (12 values, each 10K)
    for (int i = 0; i < 12; i++) {
      ASSERT_OK(Put(Key(key_idx), CompressibleString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
  }
  ASSERT_LT(TotalSize(), 120000U * 12 * 0.82 + 120000 * 2);
}

#ifndef ROCKSDB_VALGRIND_RUN
// Test that checks trivial move in universal compaction
TEST_P(DBTestUniversalCompaction, UniversalCompactionTrivialMoveTest1) {
  int32_t trivial_move = 0;
  int32_t non_trivial_move = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:TrivialMove",
      [&](void* /*arg*/) { trivial_move++; });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial", [&](void* arg) {
        non_trivial_move++;
        ASSERT_TRUE(arg != nullptr);
        int output_level = *(static_cast<int*>(arg));
        ASSERT_EQ(output_level, 0);
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.compaction_options_universal.allow_trivial_move = true;
  options.num_levels = 2;
  options.write_buffer_size = 100 << 10;  // 100KB
  options.level0_file_num_compaction_trigger = 3;
  options.max_background_compactions = 1;
  options.target_file_size_base = 32 * 1024;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  // Trigger compaction if size amplification exceeds 110%
  options.compaction_options_universal.max_size_amplification_percent = 110;
  options = CurrentOptions(options);
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  Random rnd(301);
  int num_keys = 250000;
  for (int i = 0; i < num_keys; i++) {
    ASSERT_OK(Put(1, Key(i), Key(i)));
  }
  std::vector<std::string> values;

  ASSERT_OK(Flush(1));
  dbfull()->TEST_WaitForCompact();

  ASSERT_GT(trivial_move, 0);
  ASSERT_GT(non_trivial_move, 0);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}
// Test that checks trivial move in universal compaction
TEST_P(DBTestUniversalCompaction, UniversalCompactionTrivialMoveTest2) {
  int32_t trivial_move = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:TrivialMove",
      [&](void* /*arg*/) { trivial_move++; });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial", [&](void* arg) {
        ASSERT_TRUE(arg != nullptr);
        int output_level = *(static_cast<int*>(arg));
        ASSERT_EQ(output_level, 0);
      });

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.compaction_options_universal.allow_trivial_move = true;
  options.num_levels = 15;
  options.write_buffer_size = 100 << 10;  // 100KB
  options.level0_file_num_compaction_trigger = 8;
  options.max_background_compactions = 2;
  options.target_file_size_base = 64 * 1024;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  // Trigger compaction if size amplification exceeds 110%
  options.compaction_options_universal.max_size_amplification_percent = 110;
  options = CurrentOptions(options);
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  Random rnd(301);
  int num_keys = 500000;
  for (int i = 0; i < num_keys; i++) {
    ASSERT_OK(Put(1, Key(i), Key(i)));
  }
  std::vector<std::string> values;

  ASSERT_OK(Flush(1));
  dbfull()->TEST_WaitForCompact();

  ASSERT_GT(trivial_move, 0);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}
#endif  // ROCKSDB_VALGRIND_RUN

TEST_P(DBTestUniversalCompaction, UniversalCompactionFourPaths) {
  Options options = CurrentOptions();
  options.db_paths.emplace_back(dbname_, 300 * 1024);
  options.db_paths.emplace_back(dbname_ + "_2", 300 * 1024);
  options.db_paths.emplace_back(dbname_ + "_3", 500 * 1024);
  options.db_paths.emplace_back(dbname_ + "_4", 1024 * 1024 * 1024);
  options.memtable_factory.reset(
      new SpecialSkipListFactory(KNumKeysByGenerateNewFile - 1));
  options.compaction_style = kCompactionStyleUniversal;
  options.compaction_options_universal.size_ratio = 5;
  options.write_buffer_size = 111 << 10;  // 114KB
  options.arena_block_size = 4 << 10;
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 1;

  std::vector<std::string> filenames;
  env_->GetChildren(options.db_paths[1].path, &filenames);
  // Delete archival files.
  for (size_t i = 0; i < filenames.size(); ++i) {
    env_->DeleteFile(options.db_paths[1].path + "/" + filenames[i]);
  }
  env_->DeleteDir(options.db_paths[1].path);
  Reopen(options);

  Random rnd(301);
  int key_idx = 0;

  // First three 110KB files are not going to second path.
  // After that, (100K, 200K)
  for (int num = 0; num < 3; num++) {
    GenerateNewFile(&rnd, &key_idx);
  }

  // Another 110KB triggers a compaction to 400K file to second path
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[2].path));

  // (1, 4)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1,1,4) -> (2, 4)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  // (1, 2, 4) -> (3, 4)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  // (1, 3, 4) -> (8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[3].path));

  // (1, 8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[3].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 1, 8) -> (2, 8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[3].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));

  // (1, 2, 8) -> (3, 8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[3].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  // (1, 3, 8) -> (4, 8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[3].path));

  // (1, 4, 8) -> (5, 8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[3].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  for (int i = 0; i < key_idx; i++) {
    auto v = Get(Key(i));
    ASSERT_NE(v, "NOT_FOUND");
    ASSERT_TRUE(v.size() == 1 || v.size() == 990);
  }

  Reopen(options);

  for (int i = 0; i < key_idx; i++) {
    auto v = Get(Key(i));
    ASSERT_NE(v, "NOT_FOUND");
    ASSERT_TRUE(v.size() == 1 || v.size() == 990);
  }

  Destroy(options);
}

TEST_P(DBTestUniversalCompaction, UniversalCompactionCFPathUse) {
  Options options = CurrentOptions();
  options.db_paths.emplace_back(dbname_, 300 * 1024);
  options.db_paths.emplace_back(dbname_ + "_2", 300 * 1024);
  options.db_paths.emplace_back(dbname_ + "_3", 500 * 1024);
  options.db_paths.emplace_back(dbname_ + "_4", 1024 * 1024 * 1024);
  options.memtable_factory.reset(
      new SpecialSkipListFactory(KNumKeysByGenerateNewFile - 1));
  options.compaction_style = kCompactionStyleUniversal;
  options.compaction_options_universal.size_ratio = 10;
  options.write_buffer_size = 111 << 10;  // 114KB
  options.arena_block_size = 4 << 10;
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 1;

  std::vector<Options> option_vector;
  option_vector.emplace_back(options);
  ColumnFamilyOptions cf_opt1(options), cf_opt2(options);
  // Configure CF1 specific paths.
  cf_opt1.cf_paths.emplace_back(dbname_ + "cf1", 300 * 1024);
  cf_opt1.cf_paths.emplace_back(dbname_ + "cf1_2", 300 * 1024);
  cf_opt1.cf_paths.emplace_back(dbname_ + "cf1_3", 500 * 1024);
  cf_opt1.cf_paths.emplace_back(dbname_ + "cf1_4", 1024 * 1024 * 1024);
  option_vector.emplace_back(DBOptions(options), cf_opt1);
  CreateColumnFamilies({"one"},option_vector[1]);

  // Configura CF2 specific paths.
  cf_opt2.cf_paths.emplace_back(dbname_ + "cf2", 300 * 1024);
  cf_opt2.cf_paths.emplace_back(dbname_ + "cf2_2", 300 * 1024);
  cf_opt2.cf_paths.emplace_back(dbname_ + "cf2_3", 500 * 1024);
  cf_opt2.cf_paths.emplace_back(dbname_ + "cf2_4", 1024 * 1024 * 1024);
  option_vector.emplace_back(DBOptions(options), cf_opt2);
  CreateColumnFamilies({"two"},option_vector[2]);

  ReopenWithColumnFamilies({"default", "one", "two"}, option_vector);

  Random rnd(301);
  int key_idx = 0;
  int key_idx1 = 0;
  int key_idx2 = 0;

  auto generate_file = [&]() {
    GenerateNewFile(0, &rnd, &key_idx);
    GenerateNewFile(1, &rnd, &key_idx1);
    GenerateNewFile(2, &rnd, &key_idx2);
  };

  auto check_sstfilecount = [&](int path_id, int expected) {
    ASSERT_EQ(expected, GetSstFileCount(options.db_paths[path_id].path));
    ASSERT_EQ(expected, GetSstFileCount(cf_opt1.cf_paths[path_id].path));
    ASSERT_EQ(expected, GetSstFileCount(cf_opt2.cf_paths[path_id].path));
  };

  auto check_getvalues = [&]() {
    for (int i = 0; i < key_idx; i++) {
      auto v = Get(0, Key(i));
      ASSERT_NE(v, "NOT_FOUND");
      ASSERT_TRUE(v.size() == 1 || v.size() == 990);
    }

    for (int i = 0; i < key_idx1; i++) {
      auto v = Get(1, Key(i));
      ASSERT_NE(v, "NOT_FOUND");
      ASSERT_TRUE(v.size() == 1 || v.size() == 990);
    }

    for (int i = 0; i < key_idx2; i++) {
      auto v = Get(2, Key(i));
      ASSERT_NE(v, "NOT_FOUND");
      ASSERT_TRUE(v.size() == 1 || v.size() == 990);
    }
  };

  // First three 110KB files are not going to second path.
  // After that, (100K, 200K)
  for (int num = 0; num < 3; num++) {
    generate_file();
  }

  // Another 110KB triggers a compaction to 400K file to second path
  generate_file();
  check_sstfilecount(2, 1);

  // (1, 4)
  generate_file();
  check_sstfilecount(2, 1);
  check_sstfilecount(0, 1);

  // (1,1,4) -> (2, 4)
  generate_file();
  check_sstfilecount(2, 1);
  check_sstfilecount(1, 1);
  check_sstfilecount(0, 0);

  // (1, 2, 4) -> (3, 4)
  generate_file();
  check_sstfilecount(2, 1);
  check_sstfilecount(1, 1);
  check_sstfilecount(0, 0);

  // (1, 3, 4) -> (8)
  generate_file();
  check_sstfilecount(3, 1);

  // (1, 8)
  generate_file();
  check_sstfilecount(3, 1);
  check_sstfilecount(0, 1);

  // (1, 1, 8) -> (2, 8)
  generate_file();
  check_sstfilecount(3, 1);
  check_sstfilecount(1, 1);

  // (1, 2, 8) -> (3, 8)
  generate_file();
  check_sstfilecount(3, 1);
  check_sstfilecount(1, 1);
  check_sstfilecount(0, 0);

  // (1, 3, 8) -> (4, 8)
  generate_file();
  check_sstfilecount(2, 1);
  check_sstfilecount(3, 1);

  // (1, 4, 8) -> (5, 8)
  generate_file();
  check_sstfilecount(3, 1);
  check_sstfilecount(2, 1);
  check_sstfilecount(0, 0);

  check_getvalues();

  ReopenWithColumnFamilies({"default", "one", "two"}, option_vector);

  check_getvalues();

  Destroy(options, true);
}

TEST_P(DBTestUniversalCompaction, IncreaseUniversalCompactionNumLevels) {
  std::function<void(int)> verify_func = [&](int num_keys_in_db) {
    std::string keys_in_db;
    Iterator* iter = dbfull()->NewIterator(ReadOptions(), handles_[1]);
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      keys_in_db.append(iter->key().ToString());
      keys_in_db.push_back(',');
    }
    delete iter;

    std::string expected_keys;
    for (int i = 0; i <= num_keys_in_db; i++) {
      expected_keys.append(Key(i));
      expected_keys.push_back(',');
    }

    ASSERT_EQ(keys_in_db, expected_keys);
  };

  Random rnd(301);
  int max_key1 = 200;
  int max_key2 = 600;
  int max_key3 = 800;
  const int KNumKeysPerFile = 10;

  // Stage 1: open a DB with universal compaction, num_levels=1
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 1;
  options.write_buffer_size = 200 << 10;  // 200KB
  options.level0_file_num_compaction_trigger = 3;
  options.memtable_factory.reset(new SpecialSkipListFactory(KNumKeysPerFile));
  options = CurrentOptions(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  for (int i = 0; i <= max_key1; i++) {
    // each value is 10K
    ASSERT_OK(Put(1, Key(i), RandomString(&rnd, 10000)));
    dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
    dbfull()->TEST_WaitForCompact();
  }
  ASSERT_OK(Flush(1));
  dbfull()->TEST_WaitForCompact();

  // Stage 2: reopen with universal compaction, num_levels=4
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 4;
  options = CurrentOptions(options);
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  verify_func(max_key1);

  // Insert more keys
  for (int i = max_key1 + 1; i <= max_key2; i++) {
    // each value is 10K
    ASSERT_OK(Put(1, Key(i), RandomString(&rnd, 10000)));
    dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
    dbfull()->TEST_WaitForCompact();
  }
  ASSERT_OK(Flush(1));
  dbfull()->TEST_WaitForCompact();

  verify_func(max_key2);
  // Compaction to non-L0 has happened.
  ASSERT_GT(NumTableFilesAtLevel(options.num_levels - 1, 1), 0);

  // Stage 3: Revert it back to one level and revert to num_levels=1.
  options.num_levels = 4;
  options.target_file_size_base = INT_MAX;
  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  // Compact all to level 0
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 0;
  compact_options.exclusive_manual_compaction = exclusive_manual_compaction_;
  dbfull()->CompactRange(compact_options, handles_[1], nullptr, nullptr);
  // Need to restart it once to remove higher level records in manifest.
  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  // Final reopen
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 1;
  options = CurrentOptions(options);
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  // Insert more keys
  for (int i = max_key2 + 1; i <= max_key3; i++) {
    // each value is 10K
    ASSERT_OK(Put(1, Key(i), RandomString(&rnd, 10000)));
    dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
    dbfull()->TEST_WaitForCompact();
  }
  ASSERT_OK(Flush(1));
  dbfull()->TEST_WaitForCompact();
  verify_func(max_key3);
}


TEST_P(DBTestUniversalCompaction, UniversalCompactionSecondPathRatio) {
  if (!Snappy_Supported()) {
    return;
  }
  Options options = CurrentOptions();
  options.db_paths.emplace_back(dbname_, 500 * 1024);
  options.db_paths.emplace_back(dbname_ + "_2", 1024 * 1024 * 1024);
  options.compaction_style = kCompactionStyleUniversal;
  options.compaction_options_universal.size_ratio = 5;
  options.write_buffer_size = 111 << 10;  // 114KB
  options.arena_block_size = 4 << 10;
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 1;
  options.memtable_factory.reset(
      new SpecialSkipListFactory(KNumKeysByGenerateNewFile - 1));

  std::vector<std::string> filenames;
  env_->GetChildren(options.db_paths[1].path, &filenames);
  // Delete archival files.
  for (size_t i = 0; i < filenames.size(); ++i) {
    env_->DeleteFile(options.db_paths[1].path + "/" + filenames[i]);
  }
  env_->DeleteDir(options.db_paths[1].path);
  Reopen(options);

  Random rnd(301);
  int key_idx = 0;

  // First three 110KB files are not going to second path.
  // After that, (100K, 200K)
  for (int num = 0; num < 3; num++) {
    GenerateNewFile(&rnd, &key_idx);
  }

  // Another 110KB triggers a compaction to 400K file to second path
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));

  // (1, 4)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1,1,4) -> (2, 4)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 2, 4) -> (3, 4)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(2, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  // (1, 3, 4) -> (8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  // (1, 8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 1, 8) -> (2, 8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 2, 8) -> (3, 8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(2, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  // (1, 3, 8) -> (4, 8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(2, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  // (1, 4, 8) -> (5, 8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(2, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  for (int i = 0; i < key_idx; i++) {
    auto v = Get(Key(i));
    ASSERT_NE(v, "NOT_FOUND");
    ASSERT_TRUE(v.size() == 1 || v.size() == 990);
  }

  Reopen(options);

  for (int i = 0; i < key_idx; i++) {
    auto v = Get(Key(i));
    ASSERT_NE(v, "NOT_FOUND");
    ASSERT_TRUE(v.size() == 1 || v.size() == 990);
  }

  Destroy(options);
}

TEST_P(DBTestUniversalCompaction, ConcurrentBottomPriLowPriCompactions) {
  if (num_levels_ == 1) {
    // for single-level universal, everything's bottom level so nothing should
    // be executed in bottom-pri thread pool.
    return;
  }
  const int kNumFilesTrigger = 3;
  Env::Default()->SetBackgroundThreads(1, Env::Priority::BOTTOM);
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = num_levels_;
  options.write_buffer_size = 100 << 10;     // 100KB
  options.target_file_size_base = 32 << 10;  // 32KB
  options.level0_file_num_compaction_trigger = kNumFilesTrigger;
  // Trigger compaction if size amplification exceeds 110%
  options.compaction_options_universal.max_size_amplification_percent = 110;
  DestroyAndReopen(options);

  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {// wait for the full compaction to be picked before adding files intended
       // for the second one.
       {"DBImpl::BackgroundCompaction:ForwardToBottomPriPool",
        "DBTestUniversalCompaction:ConcurrentBottomPriLowPriCompactions:0"},
       // the full (bottom-pri) compaction waits until a partial (low-pri)
       // compaction has started to verify they can run in parallel.
       {"DBImpl::BackgroundCompaction:NonTrivial",
        "DBImpl::BGWorkBottomCompaction"}});
  SyncPoint::GetInstance()->EnableProcessing();

  Random rnd(301);
  for (int i = 0; i < 2; ++i) {
    for (int num = 0; num < kNumFilesTrigger; num++) {
      int key_idx = 0;
      GenerateNewFile(&rnd, &key_idx, true /* no_wait */);
      // use no_wait above because that one waits for flush and compaction. We
      // don't want to wait for compaction because the full compaction is
      // intentionally blocked while more files are flushed.
      dbfull()->TEST_WaitForFlushMemTable();
    }
    if (i == 0) {
      TEST_SYNC_POINT(
          "DBTestUniversalCompaction:ConcurrentBottomPriLowPriCompactions:0");
    }
  }
  dbfull()->TEST_WaitForCompact();

  // First compaction should output to bottom level. Second should output to L0
  // since older L0 files pending compaction prevent it from being placed lower.
  ASSERT_EQ(NumSortedRuns(), 2);
  ASSERT_GT(NumTableFilesAtLevel(0), 0);
  ASSERT_GT(NumTableFilesAtLevel(num_levels_ - 1), 0);
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  Env::Default()->SetBackgroundThreads(0, Env::Priority::BOTTOM);
}

TEST_P(DBTestUniversalCompaction, RecalculateScoreAfterPicking) {
  // Regression test for extra compactions scheduled. Once enough compactions
  // have been scheduled to bring the score below one, we should stop
  // scheduling more; otherwise, other CFs/DBs may be delayed unnecessarily.
  const int kNumFilesTrigger = 8;
  Options options = CurrentOptions();
  options.compaction_options_universal.max_merge_width = kNumFilesTrigger / 2;
  options.compaction_options_universal.max_size_amplification_percent =
      static_cast<unsigned int>(-1);
  options.compaction_style = kCompactionStyleUniversal;
  options.level0_file_num_compaction_trigger = kNumFilesTrigger;
  options.num_levels = num_levels_;
  options.write_buffer_size = 100 << 10;  // 100KB
  Reopen(options);

  std::atomic<int> num_compactions_attempted(0);
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:Start", [&](void* /*arg*/) {
        ++num_compactions_attempted;
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Random rnd(301);
  for (int num = 0; num < kNumFilesTrigger; num++) {
    ASSERT_EQ(NumSortedRuns(), num);
    int key_idx = 0;
    GenerateNewFile(&rnd, &key_idx);
  }
  dbfull()->TEST_WaitForCompact();
  // Compacting the first four files was enough to bring the score below one so
  // there's no need to schedule any more compactions.
  ASSERT_EQ(1, num_compactions_attempted);
  ASSERT_EQ(NumSortedRuns(), 5);
}

TEST_P(DBTestUniversalCompaction, FinalSortedRunCompactFilesConflict) {
  // Regression test for conflict between:
  // (1) Running CompactFiles including file in the final sorted run; and
  // (2) Picking universal size-amp-triggered compaction, which always includes
  //     the final sorted run.
  if (exclusive_manual_compaction_) {
    return;
  }

  Options opts = CurrentOptions();
  opts.compaction_style = kCompactionStyleUniversal;
  opts.compaction_options_universal.max_size_amplification_percent = 50;
  opts.compaction_options_universal.min_merge_width = 2;
  opts.compression = kNoCompression;
  opts.level0_file_num_compaction_trigger = 2;
  opts.max_background_compactions = 2;
  opts.num_levels = num_levels_;
  Reopen(opts);

  // make sure compaction jobs can be parallelized
  auto stop_token =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();

  Put("key", "val");
  Flush();
  dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_EQ(NumTableFilesAtLevel(num_levels_ - 1), 1);
  ColumnFamilyMetaData cf_meta;
  ColumnFamilyHandle* default_cfh = db_->DefaultColumnFamily();
  dbfull()->GetColumnFamilyMetaData(default_cfh, &cf_meta);
  ASSERT_EQ(1, cf_meta.levels[num_levels_ - 1].files.size());
  std::string first_sst_filename =
      cf_meta.levels[num_levels_ - 1].files[0].name;

  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"CompactFilesImpl:0",
        "DBTestUniversalCompaction:FinalSortedRunCompactFilesConflict:0"},
       {"DBImpl::BackgroundCompaction():AfterPickCompaction",
        "CompactFilesImpl:1"}});
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  port::Thread compact_files_thread([&]() {
    ASSERT_OK(dbfull()->CompactFiles(CompactionOptions(), default_cfh,
                                     {first_sst_filename}, num_levels_ - 1));
  });

  TEST_SYNC_POINT(
      "DBTestUniversalCompaction:FinalSortedRunCompactFilesConflict:0");
  for (int i = 0; i < 2; ++i) {
    Put("key", "val");
    Flush();
  }
  dbfull()->TEST_WaitForCompact();

  compact_files_thread.join();
}

INSTANTIATE_TEST_CASE_P(UniversalCompactionNumLevels, DBTestUniversalCompaction,
                        ::testing::Combine(::testing::Values(1, 3, 5),
                                           ::testing::Bool()));

class DBTestUniversalManualCompactionOutputPathId
    : public DBTestUniversalCompactionBase {
 public:
  DBTestUniversalManualCompactionOutputPathId() :
      DBTestUniversalCompactionBase(
          "/db_universal_compaction_manual_pid_test") {}
};

TEST_P(DBTestUniversalManualCompactionOutputPathId,
       ManualCompactionOutputPathId) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.db_paths.emplace_back(dbname_, 1000000000);
  options.db_paths.emplace_back(dbname_ + "_2", 1000000000);
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = num_levels_;
  options.target_file_size_base = 1 << 30;  // Big size
  options.level0_file_num_compaction_trigger = 10;
  Destroy(options);
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  MakeTables(3, "p", "q", 1);
  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(2, TotalLiveFiles(1));
  ASSERT_EQ(2, GetSstFileCount(options.db_paths[0].path));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[1].path));

  // Full compaction to DB path 0
  CompactRangeOptions compact_options;
  compact_options.target_path_id = 1;
  compact_options.exclusive_manual_compaction = exclusive_manual_compaction_;
  db_->CompactRange(compact_options, handles_[1], nullptr, nullptr);
  ASSERT_EQ(1, TotalLiveFiles(1));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[0].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));

  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "pikachu"}, options);
  ASSERT_EQ(1, TotalLiveFiles(1));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[0].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));

  MakeTables(1, "p", "q", 1);
  ASSERT_EQ(2, TotalLiveFiles(1));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[0].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));

  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "pikachu"}, options);
  ASSERT_EQ(2, TotalLiveFiles(1));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[0].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));

  // Full compaction to DB path 0
  compact_options.target_path_id = 0;
  compact_options.exclusive_manual_compaction = exclusive_manual_compaction_;
  db_->CompactRange(compact_options, handles_[1], nullptr, nullptr);
  ASSERT_EQ(1, TotalLiveFiles(1));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[0].path));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[1].path));

  // Fail when compacting to an invalid path ID
  compact_options.target_path_id = 2;
  compact_options.exclusive_manual_compaction = exclusive_manual_compaction_;
  ASSERT_TRUE(db_->CompactRange(compact_options, handles_[1], nullptr, nullptr)
                  .IsInvalidArgument());
}

INSTANTIATE_TEST_CASE_P(DBTestUniversalManualCompactionOutputPathId,
                        DBTestUniversalManualCompactionOutputPathId,
                        ::testing::Combine(::testing::Values(1, 8),
                                           ::testing::Bool()));

TEST_F(DBTestUniversalDeleteTrigCompaction, BasicL0toL1) {
  const int kNumKeys = 3000;
  const int kWindowSize = 100;
  const int kNumDelsTrigger = 90;

  Options opts = CurrentOptions();
  opts.table_properties_collector_factories.emplace_back(
      NewCompactOnDeletionCollectorFactory(kWindowSize, kNumDelsTrigger));
  opts.compaction_style = kCompactionStyleUniversal;
  opts.level0_file_num_compaction_trigger = 2;
  opts.compression = kNoCompression;
  opts.compaction_options_universal.size_ratio = 10;
  opts.compaction_options_universal.min_merge_width = 2;
  opts.compaction_options_universal.max_size_amplification_percent = 200;
  Reopen(opts);

  // add an L1 file to prevent tombstones from dropping due to obsolescence
  // during flush
  int i;
  for (i = 0; i < 2000; ++i) {
    Put(Key(i), "val");
  }
  Flush();
  //  MoveFilesToLevel(6);
  dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);

  for (i = 1999; i < kNumKeys; ++i) {
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
  ASSERT_GT(NumTableFilesAtLevel(6), 0);
}

TEST_F(DBTestUniversalDeleteTrigCompaction, SingleLevel) {
  const int kNumKeys = 3000;
  const int kWindowSize = 100;
  const int kNumDelsTrigger = 90;

  Options opts = CurrentOptions();
  opts.table_properties_collector_factories.emplace_back(
      NewCompactOnDeletionCollectorFactory(kWindowSize, kNumDelsTrigger));
  opts.compaction_style = kCompactionStyleUniversal;
  opts.level0_file_num_compaction_trigger = 2;
  opts.compression = kNoCompression;
  opts.num_levels = 1;
  opts.compaction_options_universal.size_ratio = 10;
  opts.compaction_options_universal.min_merge_width = 2;
  opts.compaction_options_universal.max_size_amplification_percent = 200;
  Reopen(opts);

  // add an L1 file to prevent tombstones from dropping due to obsolescence
  // during flush
  int i;
  for (i = 0; i < 2000; ++i) {
    Put(Key(i), "val");
  }
  Flush();

  for (i = 1999; i < kNumKeys; ++i) {
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

TEST_F(DBTestUniversalDeleteTrigCompaction, MultipleLevels) {
  const int kWindowSize = 100;
  const int kNumDelsTrigger = 90;

  Options opts = CurrentOptions();
  opts.table_properties_collector_factories.emplace_back(
      NewCompactOnDeletionCollectorFactory(kWindowSize, kNumDelsTrigger));
  opts.compaction_style = kCompactionStyleUniversal;
  opts.level0_file_num_compaction_trigger = 4;
  opts.compression = kNoCompression;
  opts.compaction_options_universal.size_ratio = 10;
  opts.compaction_options_universal.min_merge_width = 2;
  opts.compaction_options_universal.max_size_amplification_percent = 200;
  Reopen(opts);

  // add an L1 file to prevent tombstones from dropping due to obsolescence
  // during flush
  int i;
  for (i = 0; i < 500; ++i) {
    Put(Key(i), "val");
  }
  Flush();
  for (i = 500; i < 1000; ++i) {
    Put(Key(i), "val");
  }
  Flush();
  for (i = 1000; i < 1500; ++i) {
    Put(Key(i), "val");
  }
  Flush();
  for (i = 1500; i < 2000; ++i) {
    Put(Key(i), "val");
  }
  Flush();

  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_GT(NumTableFilesAtLevel(6), 0);

  for (i = 1999; i < 2333; ++i) {
    Put(Key(i), "val");
  }
  Flush();
  for (i = 2333; i < 2666; ++i) {
    Put(Key(i), "val");
  }
  Flush();
  for (i = 2666; i < 2999; ++i) {
    Put(Key(i), "val");
  }
  Flush();

  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_GT(NumTableFilesAtLevel(6), 0);
  ASSERT_GT(NumTableFilesAtLevel(5), 0);

  for (i = 1900; i < 2100; ++i) {
    Delete(Key(i));
  }
  Flush();

  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_EQ(0, NumTableFilesAtLevel(1));
  ASSERT_EQ(0, NumTableFilesAtLevel(2));
  ASSERT_EQ(0, NumTableFilesAtLevel(3));
  ASSERT_EQ(0, NumTableFilesAtLevel(4));
  ASSERT_EQ(0, NumTableFilesAtLevel(5));
  ASSERT_GT(NumTableFilesAtLevel(6), 0);
}

TEST_F(DBTestUniversalDeleteTrigCompaction, OverlappingL0) {
  const int kWindowSize = 100;
  const int kNumDelsTrigger = 90;

  Options opts = CurrentOptions();
  opts.table_properties_collector_factories.emplace_back(
      NewCompactOnDeletionCollectorFactory(kWindowSize, kNumDelsTrigger));
  opts.compaction_style = kCompactionStyleUniversal;
  opts.level0_file_num_compaction_trigger = 5;
  opts.compression = kNoCompression;
  opts.compaction_options_universal.size_ratio = 10;
  opts.compaction_options_universal.min_merge_width = 2;
  opts.compaction_options_universal.max_size_amplification_percent = 200;
  Reopen(opts);

  // add an L1 file to prevent tombstones from dropping due to obsolescence
  // during flush
  int i;
  for (i = 0; i < 2000; ++i) {
    Put(Key(i), "val");
  }
  Flush();
  for (i = 2000; i < 3000; ++i) {
    Put(Key(i), "val");
  }
  Flush();
  for (i = 3500; i < 4000; ++i) {
    Put(Key(i), "val");
  }
  Flush();
  for (i = 2900; i < 3100; ++i) {
    Delete(Key(i));
  }
  Flush();

  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(2, NumTableFilesAtLevel(0));
  ASSERT_GT(NumTableFilesAtLevel(6), 0);
}

TEST_F(DBTestUniversalDeleteTrigCompaction, IngestBehind) {
  const int kNumKeys = 3000;
  const int kWindowSize = 100;
  const int kNumDelsTrigger = 90;

  Options opts = CurrentOptions();
  opts.table_properties_collector_factories.emplace_back(
      NewCompactOnDeletionCollectorFactory(kWindowSize, kNumDelsTrigger));
  opts.compaction_style = kCompactionStyleUniversal;
  opts.level0_file_num_compaction_trigger = 2;
  opts.compression = kNoCompression;
  opts.allow_ingest_behind = true;
  opts.compaction_options_universal.size_ratio = 10;
  opts.compaction_options_universal.min_merge_width = 2;
  opts.compaction_options_universal.max_size_amplification_percent = 200;
  Reopen(opts);

  // add an L1 file to prevent tombstones from dropping due to obsolescence
  // during flush
  int i;
  for (i = 0; i < 2000; ++i) {
    Put(Key(i), "val");
  }
  Flush();
  //  MoveFilesToLevel(6);
  dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);

  for (i = 1999; i < kNumKeys; ++i) {
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
  ASSERT_EQ(0, NumTableFilesAtLevel(6));
  ASSERT_GT(NumTableFilesAtLevel(5), 0);
}

}  // namespace rocksdb

#endif  // !defined(ROCKSDB_LITE)

int main(int argc, char** argv) {
#if !defined(ROCKSDB_LITE)
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
#else
  (void) argc;
  (void) argv;
  return 0;
#endif
}
