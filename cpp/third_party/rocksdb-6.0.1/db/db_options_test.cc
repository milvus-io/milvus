//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <limits>
#include <string>
#include <unordered_map>

#include "db/column_family.h"
#include "db/db_impl.h"
#include "db/db_test_util.h"
#include "options/options_helper.h"
#include "port/stack_trace.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/stats_history.h"
#include "util/random.h"
#include "util/sync_point.h"
#include "util/testutil.h"

namespace rocksdb {

const int kMicrosInSec = 1000000;

class DBOptionsTest : public DBTestBase {
 public:
  DBOptionsTest() : DBTestBase("/db_options_test") {}

#ifndef ROCKSDB_LITE
  std::unordered_map<std::string, std::string> GetMutableDBOptionsMap(
      const DBOptions& options) {
    std::string options_str;
    GetStringFromDBOptions(&options_str, options);
    std::unordered_map<std::string, std::string> options_map;
    StringToMap(options_str, &options_map);
    std::unordered_map<std::string, std::string> mutable_map;
    for (const auto opt : db_options_type_info) {
      if (opt.second.is_mutable &&
          opt.second.verification != OptionVerificationType::kDeprecated) {
        mutable_map[opt.first] = options_map[opt.first];
      }
    }
    return mutable_map;
  }

  std::unordered_map<std::string, std::string> GetMutableCFOptionsMap(
      const ColumnFamilyOptions& options) {
    std::string options_str;
    GetStringFromColumnFamilyOptions(&options_str, options);
    std::unordered_map<std::string, std::string> options_map;
    StringToMap(options_str, &options_map);
    std::unordered_map<std::string, std::string> mutable_map;
    for (const auto opt : cf_options_type_info) {
      if (opt.second.is_mutable &&
          opt.second.verification != OptionVerificationType::kDeprecated) {
        mutable_map[opt.first] = options_map[opt.first];
      }
    }
    return mutable_map;
  }

  std::unordered_map<std::string, std::string> GetRandomizedMutableCFOptionsMap(
      Random* rnd) {
    Options options;
    options.env = env_;
    ImmutableDBOptions db_options(options);
    test::RandomInitCFOptions(&options, rnd);
    auto sanitized_options = SanitizeOptions(db_options, options);
    auto opt_map = GetMutableCFOptionsMap(sanitized_options);
    delete options.compaction_filter;
    return opt_map;
  }

  std::unordered_map<std::string, std::string> GetRandomizedMutableDBOptionsMap(
      Random* rnd) {
    DBOptions db_options;
    test::RandomInitDBOptions(&db_options, rnd);
    auto sanitized_options = SanitizeOptions(dbname_, db_options);
    return GetMutableDBOptionsMap(sanitized_options);
  }
#endif  // ROCKSDB_LITE
};

// RocksDB lite don't support dynamic options.
#ifndef ROCKSDB_LITE

TEST_F(DBOptionsTest, GetLatestDBOptions) {
  // GetOptions should be able to get latest option changed by SetOptions.
  Options options;
  options.create_if_missing = true;
  options.env = env_;
  Random rnd(228);
  Reopen(options);
  auto new_options = GetRandomizedMutableDBOptionsMap(&rnd);
  ASSERT_OK(dbfull()->SetDBOptions(new_options));
  ASSERT_EQ(new_options, GetMutableDBOptionsMap(dbfull()->GetDBOptions()));
}

TEST_F(DBOptionsTest, GetLatestCFOptions) {
  // GetOptions should be able to get latest option changed by SetOptions.
  Options options;
  options.create_if_missing = true;
  options.env = env_;
  Random rnd(228);
  Reopen(options);
  CreateColumnFamilies({"foo"}, options);
  ReopenWithColumnFamilies({"default", "foo"}, options);
  auto options_default = GetRandomizedMutableCFOptionsMap(&rnd);
  auto options_foo = GetRandomizedMutableCFOptionsMap(&rnd);
  ASSERT_OK(dbfull()->SetOptions(handles_[0], options_default));
  ASSERT_OK(dbfull()->SetOptions(handles_[1], options_foo));
  ASSERT_EQ(options_default,
            GetMutableCFOptionsMap(dbfull()->GetOptions(handles_[0])));
  ASSERT_EQ(options_foo,
            GetMutableCFOptionsMap(dbfull()->GetOptions(handles_[1])));
}

TEST_F(DBOptionsTest, SetBytesPerSync) {
  const size_t kValueSize = 1024 * 1024;  // 1MB
  Options options;
  options.create_if_missing = true;
  options.bytes_per_sync = 1024 * 1024;
  options.use_direct_reads = false;
  options.write_buffer_size = 400 * kValueSize;
  options.disable_auto_compactions = true;
  options.compression = kNoCompression;
  options.env = env_;
  Reopen(options);
  int counter = 0;
  int low_bytes_per_sync = 0;
  int i = 0;
  const std::string kValue(kValueSize, 'v');
  ASSERT_EQ(options.bytes_per_sync, dbfull()->GetDBOptions().bytes_per_sync);
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "WritableFileWriter::RangeSync:0", [&](void* /*arg*/) {
        counter++;
      });

  WriteOptions write_opts;
  // should sync approximately 40MB/1MB ~= 40 times.
  for (i = 0; i < 40; i++) {
    Put(Key(i), kValue, write_opts);
  }
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  low_bytes_per_sync = counter;
  ASSERT_GT(low_bytes_per_sync, 35);
  ASSERT_LT(low_bytes_per_sync, 45);

  counter = 0;
  // 8388608 = 8 * 1024 * 1024
  ASSERT_OK(dbfull()->SetDBOptions({{"bytes_per_sync", "8388608"}}));
  ASSERT_EQ(8388608, dbfull()->GetDBOptions().bytes_per_sync);
  // should sync approximately 40MB*2/8MB ~= 10 times.
  // data will be 40*2MB because of previous Puts too.
  for (i = 0; i < 40; i++) {
    Put(Key(i), kValue, write_opts);
  }
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_GT(counter, 5);
  ASSERT_LT(counter, 15);

  // Redundant assert. But leaving it here just to get the point across that
  // low_bytes_per_sync > counter.
  ASSERT_GT(low_bytes_per_sync, counter);
}

TEST_F(DBOptionsTest, SetWalBytesPerSync) {
  const size_t kValueSize = 1024 * 1024 * 3;
  Options options;
  options.create_if_missing = true;
  options.wal_bytes_per_sync = 512;
  options.write_buffer_size = 100 * kValueSize;
  options.disable_auto_compactions = true;
  options.compression = kNoCompression;
  options.env = env_;
  Reopen(options);
  ASSERT_EQ(512, dbfull()->GetDBOptions().wal_bytes_per_sync);
  int counter = 0;
  int low_bytes_per_sync = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "WritableFileWriter::RangeSync:0", [&](void* /*arg*/) {
        counter++;
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  const std::string kValue(kValueSize, 'v');
  int i = 0;
  for (; i < 10; i++) {
    Put(Key(i), kValue);
  }
  // Do not flush. If we flush here, SwitchWAL will reuse old WAL file since its
  // empty and will not get the new wal_bytes_per_sync value.
  low_bytes_per_sync = counter;
  //5242880 = 1024 * 1024 * 5
  ASSERT_OK(dbfull()->SetDBOptions({{"wal_bytes_per_sync", "5242880"}}));
  ASSERT_EQ(5242880, dbfull()->GetDBOptions().wal_bytes_per_sync);
  counter = 0;
  i = 0;
  for (; i < 10; i++) {
    Put(Key(i), kValue);
  }
  ASSERT_GT(counter, 0);
  ASSERT_GT(low_bytes_per_sync, 0);
  ASSERT_GT(low_bytes_per_sync, counter);
}

TEST_F(DBOptionsTest, WritableFileMaxBufferSize) {
  Options options;
  options.create_if_missing = true;
  options.writable_file_max_buffer_size = 1024 * 1024;
  options.level0_file_num_compaction_trigger = 3;
  options.max_manifest_file_size = 1;
  options.env = env_;
  int buffer_size = 1024 * 1024;
  Reopen(options);
  ASSERT_EQ(buffer_size,
            dbfull()->GetDBOptions().writable_file_max_buffer_size);

  std::atomic<int> match_cnt(0);
  std::atomic<int> unmatch_cnt(0);
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "WritableFileWriter::WritableFileWriter:0", [&](void* arg) {
        int value = static_cast<int>(reinterpret_cast<uintptr_t>(arg));
        if (value == buffer_size) {
          match_cnt++;
        } else {
          unmatch_cnt++;
        }
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  int i = 0;
  for (; i < 3; i++) {
    ASSERT_OK(Put("foo", ToString(i)));
    ASSERT_OK(Put("bar", ToString(i)));
    Flush();
  }
  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(unmatch_cnt, 0);
  ASSERT_GE(match_cnt, 11);

  ASSERT_OK(
      dbfull()->SetDBOptions({{"writable_file_max_buffer_size", "524288"}}));
  buffer_size = 512 * 1024;
  match_cnt = 0;
  unmatch_cnt = 0;  // SetDBOptions() will create a WriteableFileWriter

  ASSERT_EQ(buffer_size,
            dbfull()->GetDBOptions().writable_file_max_buffer_size);
  i = 0;
  for (; i < 3; i++) {
    ASSERT_OK(Put("foo", ToString(i)));
    ASSERT_OK(Put("bar", ToString(i)));
    Flush();
  }
  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(unmatch_cnt, 0);
  ASSERT_GE(match_cnt, 11);
}

TEST_F(DBOptionsTest, SetOptionsAndReopen) {
  Random rnd(1044);
  auto rand_opts = GetRandomizedMutableCFOptionsMap(&rnd);
  ASSERT_OK(dbfull()->SetOptions(rand_opts));
  // Verify if DB can be reopen after setting options.
  Options options;
  options.env = env_;
  ASSERT_OK(TryReopen(options));
}

TEST_F(DBOptionsTest, EnableAutoCompactionAndTriggerStall) {
  const std::string kValue(1024, 'v');
  for (int method_type = 0; method_type < 2; method_type++) {
    for (int option_type = 0; option_type < 4; option_type++) {
      Options options;
      options.create_if_missing = true;
      options.disable_auto_compactions = true;
      options.write_buffer_size = 1024 * 1024 * 10;
      options.compression = CompressionType::kNoCompression;
      options.level0_file_num_compaction_trigger = 1;
      options.level0_stop_writes_trigger = std::numeric_limits<int>::max();
      options.level0_slowdown_writes_trigger = std::numeric_limits<int>::max();
      options.hard_pending_compaction_bytes_limit =
          std::numeric_limits<uint64_t>::max();
      options.soft_pending_compaction_bytes_limit =
          std::numeric_limits<uint64_t>::max();
      options.env = env_;

      DestroyAndReopen(options);
      int i = 0;
      for (; i < 1024; i++) {
        Put(Key(i), kValue);
      }
      Flush();
      for (; i < 1024 * 2; i++) {
        Put(Key(i), kValue);
      }
      Flush();
      dbfull()->TEST_WaitForFlushMemTable();
      ASSERT_EQ(2, NumTableFilesAtLevel(0));
      uint64_t l0_size = SizeAtLevel(0);

      switch (option_type) {
        case 0:
          // test with level0_stop_writes_trigger
          options.level0_stop_writes_trigger = 2;
          options.level0_slowdown_writes_trigger = 2;
          break;
        case 1:
          options.level0_slowdown_writes_trigger = 2;
          break;
        case 2:
          options.hard_pending_compaction_bytes_limit = l0_size;
          options.soft_pending_compaction_bytes_limit = l0_size;
          break;
        case 3:
          options.soft_pending_compaction_bytes_limit = l0_size;
          break;
      }
      Reopen(options);
      dbfull()->TEST_WaitForCompact();
      ASSERT_FALSE(dbfull()->TEST_write_controler().IsStopped());
      ASSERT_FALSE(dbfull()->TEST_write_controler().NeedsDelay());

      SyncPoint::GetInstance()->LoadDependency(
          {{"DBOptionsTest::EnableAutoCompactionAndTriggerStall:1",
            "BackgroundCallCompaction:0"},
           {"DBImpl::BackgroundCompaction():BeforePickCompaction",
            "DBOptionsTest::EnableAutoCompactionAndTriggerStall:2"},
           {"DBOptionsTest::EnableAutoCompactionAndTriggerStall:3",
            "DBImpl::BackgroundCompaction():AfterPickCompaction"}});
      // Block background compaction.
      SyncPoint::GetInstance()->EnableProcessing();

      switch (method_type) {
        case 0:
          ASSERT_OK(
              dbfull()->SetOptions({{"disable_auto_compactions", "false"}}));
          break;
        case 1:
          ASSERT_OK(dbfull()->EnableAutoCompaction(
              {dbfull()->DefaultColumnFamily()}));
          break;
      }
      TEST_SYNC_POINT("DBOptionsTest::EnableAutoCompactionAndTriggerStall:1");
      // Wait for stall condition recalculate.
      TEST_SYNC_POINT("DBOptionsTest::EnableAutoCompactionAndTriggerStall:2");

      switch (option_type) {
        case 0:
          ASSERT_TRUE(dbfull()->TEST_write_controler().IsStopped());
          break;
        case 1:
          ASSERT_FALSE(dbfull()->TEST_write_controler().IsStopped());
          ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
          break;
        case 2:
          ASSERT_TRUE(dbfull()->TEST_write_controler().IsStopped());
          break;
        case 3:
          ASSERT_FALSE(dbfull()->TEST_write_controler().IsStopped());
          ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
          break;
      }
      TEST_SYNC_POINT("DBOptionsTest::EnableAutoCompactionAndTriggerStall:3");

      // Background compaction executed.
      dbfull()->TEST_WaitForCompact();
      ASSERT_FALSE(dbfull()->TEST_write_controler().IsStopped());
      ASSERT_FALSE(dbfull()->TEST_write_controler().NeedsDelay());
    }
  }
}

TEST_F(DBOptionsTest, SetOptionsMayTriggerCompaction) {
  Options options;
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 1000;
  options.env = env_;
  Reopen(options);
  for (int i = 0; i < 3; i++) {
    // Need to insert two keys to avoid trivial move.
    ASSERT_OK(Put("foo", ToString(i)));
    ASSERT_OK(Put("bar", ToString(i)));
    Flush();
  }
  ASSERT_EQ("3", FilesPerLevel());
  ASSERT_OK(
      dbfull()->SetOptions({{"level0_file_num_compaction_trigger", "3"}}));
  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ("0,1", FilesPerLevel());
}

TEST_F(DBOptionsTest, SetBackgroundCompactionThreads) {
  Options options;
  options.create_if_missing = true;
  options.max_background_compactions = 1;   // default value
  options.env = env_;
  Reopen(options);
  ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());
  ASSERT_OK(dbfull()->SetDBOptions({{"max_background_compactions", "3"}}));
  ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());
  auto stop_token = dbfull()->TEST_write_controler().GetStopToken();
  ASSERT_EQ(3, dbfull()->TEST_BGCompactionsAllowed());
}

TEST_F(DBOptionsTest, SetBackgroundJobs) {
  Options options;
  options.create_if_missing = true;
  options.max_background_jobs = 8;
  options.env = env_;
  Reopen(options);

  for (int i = 0; i < 2; ++i) {
    if (i > 0) {
      options.max_background_jobs = 12;
      ASSERT_OK(dbfull()->SetDBOptions(
          {{"max_background_jobs",
            std::to_string(options.max_background_jobs)}}));
    }

    ASSERT_EQ(options.max_background_jobs / 4,
              dbfull()->TEST_BGFlushesAllowed());
    ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());

    auto stop_token = dbfull()->TEST_write_controler().GetStopToken();

    ASSERT_EQ(options.max_background_jobs / 4,
              dbfull()->TEST_BGFlushesAllowed());
    ASSERT_EQ(3 * options.max_background_jobs / 4,
              dbfull()->TEST_BGCompactionsAllowed());
  }
}

TEST_F(DBOptionsTest, AvoidFlushDuringShutdown) {
  Options options;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.env = env_;
  WriteOptions write_without_wal;
  write_without_wal.disableWAL = true;

  ASSERT_FALSE(options.avoid_flush_during_shutdown);
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "v1", write_without_wal));
  Reopen(options);
  ASSERT_EQ("v1", Get("foo"));
  ASSERT_EQ("1", FilesPerLevel());

  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "v2", write_without_wal));
  ASSERT_OK(dbfull()->SetDBOptions({{"avoid_flush_during_shutdown", "true"}}));
  Reopen(options);
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  ASSERT_EQ("", FilesPerLevel());
}

TEST_F(DBOptionsTest, SetDelayedWriteRateOption) {
  Options options;
  options.create_if_missing = true;
  options.delayed_write_rate = 2 * 1024U * 1024U;
  options.env = env_;
  Reopen(options);
  ASSERT_EQ(2 * 1024U * 1024U, dbfull()->TEST_write_controler().max_delayed_write_rate());

  ASSERT_OK(dbfull()->SetDBOptions({{"delayed_write_rate", "20000"}}));
  ASSERT_EQ(20000, dbfull()->TEST_write_controler().max_delayed_write_rate());
}

TEST_F(DBOptionsTest, MaxTotalWalSizeChange) {
  Random rnd(1044);
  const auto value_size = size_t(1024);
  std::string value;
  test::RandomString(&rnd, value_size, &value);

  Options options;
  options.create_if_missing = true;
  options.env = env_;
  CreateColumnFamilies({"1", "2", "3"}, options);
  ReopenWithColumnFamilies({"default", "1", "2", "3"}, options);

  WriteOptions write_options;

  const int key_count = 100;
  for (int i = 0; i < key_count; ++i) {
    for (size_t cf = 0; cf < handles_.size(); ++cf) {
      ASSERT_OK(Put(static_cast<int>(cf), Key(i), value));
    }
  }
  ASSERT_OK(dbfull()->SetDBOptions({{"max_total_wal_size", "10"}}));

  for (size_t cf = 0; cf < handles_.size(); ++cf) {
    dbfull()->TEST_WaitForFlushMemTable(handles_[cf]);
    ASSERT_EQ("1", FilesPerLevel(static_cast<int>(cf)));
  }
}

TEST_F(DBOptionsTest, SetStatsDumpPeriodSec) {
  Options options;
  options.create_if_missing = true;
  options.stats_dump_period_sec = 5;
  options.env = env_;
  Reopen(options);
  ASSERT_EQ(5, dbfull()->GetDBOptions().stats_dump_period_sec);

  for (int i = 0; i < 20; i++) {
    int num = rand() % 5000 + 1;
    ASSERT_OK(
        dbfull()->SetDBOptions({{"stats_dump_period_sec", ToString(num)}}));
    ASSERT_EQ(num, dbfull()->GetDBOptions().stats_dump_period_sec);
  }
  Close();
}

TEST_F(DBOptionsTest, RunStatsDumpPeriodSec) {
  Options options;
  options.create_if_missing = true;
  options.stats_dump_period_sec = 5;
  std::unique_ptr<rocksdb::MockTimeEnv> mock_env;
  mock_env.reset(new rocksdb::MockTimeEnv(env_));
  mock_env->set_current_time(0); // in seconds
  options.env = mock_env.get();
  int counter = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::DumpStats:1", [&](void* /*arg*/) {
        counter++;
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  Reopen(options);
  ASSERT_EQ(5, dbfull()->GetDBOptions().stats_dump_period_sec);
  dbfull()->TEST_WaitForDumpStatsRun([&] { mock_env->set_current_time(5); });
  ASSERT_GE(counter, 1);

  // Test cacel job through SetOptions
  ASSERT_OK(dbfull()->SetDBOptions({{"stats_dump_period_sec", "0"}}));
  int old_val = counter;
  for (int i = 6; i < 20; ++i) {
    dbfull()->TEST_WaitForDumpStatsRun([&] { mock_env->set_current_time(i); });
  }
  ASSERT_EQ(counter, old_val);
  Close();
}

// Test persistent stats background thread scheduling and cancelling
TEST_F(DBOptionsTest, StatsPersistScheduling) {
  Options options;
  options.create_if_missing = true;
  options.stats_persist_period_sec = 5;
  std::unique_ptr<rocksdb::MockTimeEnv> mock_env;
  mock_env.reset(new rocksdb::MockTimeEnv(env_));
  mock_env->set_current_time(0);  // in seconds
  options.env = mock_env.get();
  int counter = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistStats:Entry", [&](void* /*arg*/) { counter++; });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  Reopen(options);
  ASSERT_EQ(5, dbfull()->GetDBOptions().stats_persist_period_sec);
  dbfull()->TEST_WaitForPersistStatsRun([&] { mock_env->set_current_time(5); });
  ASSERT_GE(counter, 1);

  // Test cacel job through SetOptions
  ASSERT_TRUE(dbfull()->TEST_IsPersistentStatsEnabled());
  ASSERT_OK(dbfull()->SetDBOptions({{"stats_persist_period_sec", "0"}}));
  ASSERT_FALSE(dbfull()->TEST_IsPersistentStatsEnabled());
  Close();
}

// Test enabling persistent stats for the first time
TEST_F(DBOptionsTest, PersistentStatsFreshInstall) {
  Options options;
  options.create_if_missing = true;
  options.stats_persist_period_sec = 0;
  std::unique_ptr<rocksdb::MockTimeEnv> mock_env;
  mock_env.reset(new rocksdb::MockTimeEnv(env_));
  mock_env->set_current_time(0);  // in seconds
  options.env = mock_env.get();
  int counter = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistStats:Entry", [&](void* /*arg*/) { counter++; });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  Reopen(options);
  ASSERT_OK(dbfull()->SetDBOptions({{"stats_persist_period_sec", "5"}}));
  ASSERT_EQ(5, dbfull()->GetDBOptions().stats_persist_period_sec);
  dbfull()->TEST_WaitForPersistStatsRun([&] { mock_env->set_current_time(5); });
  ASSERT_GE(counter, 1);
  Close();
}

TEST_F(DBOptionsTest, SetOptionsStatsPersistPeriodSec) {
  Options options;
  options.create_if_missing = true;
  options.stats_persist_period_sec = 5;
  options.env = env_;
  Reopen(options);
  ASSERT_EQ(5, dbfull()->GetDBOptions().stats_persist_period_sec);

  ASSERT_OK(dbfull()->SetDBOptions({{"stats_persist_period_sec", "12345"}}));
  ASSERT_EQ(12345, dbfull()->GetDBOptions().stats_persist_period_sec);
  ASSERT_NOK(dbfull()->SetDBOptions({{"stats_persist_period_sec", "abcde"}}));
  ASSERT_EQ(12345, dbfull()->GetDBOptions().stats_persist_period_sec);
}

TEST_F(DBOptionsTest, GetStatsHistory) {
  Options options;
  options.create_if_missing = true;
  options.stats_persist_period_sec = 5;
  options.statistics = rocksdb::CreateDBStatistics();
  std::unique_ptr<rocksdb::MockTimeEnv> mock_env;
  mock_env.reset(new rocksdb::MockTimeEnv(env_));
  mock_env->set_current_time(0);  // in seconds
  options.env = mock_env.get();
  CreateColumnFamilies({"pikachu"}, options);
  ASSERT_OK(Put("foo", "bar"));
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  int mock_time = 1;
  // Wait for stats persist to finish
  dbfull()->TEST_WaitForPersistStatsRun([&] { mock_env->set_current_time(5); });
  std::unique_ptr<StatsHistoryIterator> stats_iter;
  db_->GetStatsHistory(0, 6 * kMicrosInSec, &stats_iter);
  ASSERT_TRUE(stats_iter != nullptr);
  // disabled stats snapshots
  ASSERT_OK(dbfull()->SetDBOptions({{"stats_persist_period_sec", "0"}}));
  size_t stats_count = 0;
  for (; stats_iter->Valid(); stats_iter->Next()) {
    auto stats_map = stats_iter->GetStatsMap();
    stats_count += stats_map.size();
  }
  ASSERT_GT(stats_count, 0);
  // Wait a bit and verify no more stats are found
  for (mock_time = 6; mock_time < 20; ++mock_time) {
    dbfull()->TEST_WaitForPersistStatsRun(
        [&] { mock_env->set_current_time(mock_time); });
  }
  db_->GetStatsHistory(0, 20 * kMicrosInSec, &stats_iter);
  ASSERT_TRUE(stats_iter != nullptr);
  size_t stats_count_new = 0;
  for (; stats_iter->Valid(); stats_iter->Next()) {
    stats_count_new += stats_iter->GetStatsMap().size();
  }
  ASSERT_EQ(stats_count_new, stats_count);
  Close();
}

TEST_F(DBOptionsTest, InMemoryStatsHistoryPurging) {
  Options options;
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  options.stats_persist_period_sec = 1;
  std::unique_ptr<rocksdb::MockTimeEnv> mock_env;
  mock_env.reset(new rocksdb::MockTimeEnv(env_));
  mock_env->set_current_time(0);  // in seconds
  options.env = mock_env.get();
  CreateColumnFamilies({"pikachu"}, options);
  ASSERT_OK(Put("foo", "bar"));
  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  // some random operation to populate statistics
  ASSERT_OK(Delete("foo"));
  ASSERT_OK(Put("sol", "sol"));
  ASSERT_OK(Put("epic", "epic"));
  ASSERT_OK(Put("ltd", "ltd"));
  ASSERT_EQ("sol", Get("sol"));
  ASSERT_EQ("epic", Get("epic"));
  ASSERT_EQ("ltd", Get("ltd"));
  Iterator* iterator = db_->NewIterator(ReadOptions());
  for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
    ASSERT_TRUE(iterator->key() == iterator->value());
  }
  delete iterator;
  ASSERT_OK(Flush());
  ASSERT_OK(Delete("sol"));
  db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  int mock_time = 1;
  // Wait for stats persist to finish
  for (; mock_time < 5; ++mock_time) {
    dbfull()->TEST_WaitForPersistStatsRun(
        [&] { mock_env->set_current_time(mock_time); });
  }

  // second round of ops
  ASSERT_OK(Put("saigon", "saigon"));
  ASSERT_OK(Put("noodle talk", "noodle talk"));
  ASSERT_OK(Put("ping bistro", "ping bistro"));
  iterator = db_->NewIterator(ReadOptions());
  for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
    ASSERT_TRUE(iterator->key() == iterator->value());
  }
  delete iterator;
  ASSERT_OK(Flush());
  db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  for (; mock_time < 10; ++mock_time) {
    dbfull()->TEST_WaitForPersistStatsRun(
        [&] { mock_env->set_current_time(mock_time); });
  }
  std::unique_ptr<StatsHistoryIterator> stats_iter;
  db_->GetStatsHistory(0, 10 * kMicrosInSec, &stats_iter);
  ASSERT_TRUE(stats_iter != nullptr);
  size_t stats_count = 0;
  int slice_count = 0;
  for (; stats_iter->Valid(); stats_iter->Next()) {
    slice_count++;
    auto stats_map = stats_iter->GetStatsMap();
    stats_count += stats_map.size();
  }
  size_t stats_history_size = dbfull()->TEST_EstiamteStatsHistorySize();
  ASSERT_GE(slice_count, 9);
  ASSERT_GE(stats_history_size, 12000);
  // capping memory cost at 12000 bytes since one slice is around 10000~12000
  ASSERT_OK(dbfull()->SetDBOptions({{"stats_history_buffer_size", "12000"}}));
  ASSERT_EQ(12000, dbfull()->GetDBOptions().stats_history_buffer_size);
  // Wait for stats persist to finish
  for (; mock_time < 20; ++mock_time) {
    dbfull()->TEST_WaitForPersistStatsRun(
        [&] { mock_env->set_current_time(mock_time); });
  }
  db_->GetStatsHistory(0, 20 * kMicrosInSec, &stats_iter);
  ASSERT_TRUE(stats_iter != nullptr);
  size_t stats_count_reopen = 0;
  slice_count = 0;
  for (; stats_iter->Valid(); stats_iter->Next()) {
    slice_count++;
    auto stats_map = stats_iter->GetStatsMap();
    stats_count_reopen += stats_map.size();
  }
  size_t stats_history_size_reopen = dbfull()->TEST_EstiamteStatsHistorySize();
  // only one slice can fit under the new stats_history_buffer_size
  ASSERT_LT(slice_count, 2);
  ASSERT_TRUE(stats_history_size_reopen < 12000 &&
              stats_history_size_reopen > 0);
  ASSERT_TRUE(stats_count_reopen < stats_count && stats_count_reopen > 0);
  Close();
}

static void assert_candidate_files_empty(DBImpl* dbfull, const bool empty) {
  dbfull->TEST_LockMutex();
  JobContext job_context(0);
  dbfull->FindObsoleteFiles(&job_context, false);
  ASSERT_EQ(empty, job_context.full_scan_candidate_files.empty());
  dbfull->TEST_UnlockMutex();
  if (job_context.HaveSomethingToDelete()) {
    // fulfill the contract of FindObsoleteFiles by calling PurgeObsoleteFiles
    // afterwards; otherwise the test may hang on shutdown
    dbfull->PurgeObsoleteFiles(job_context);
  }
  job_context.Clean();
}

TEST_F(DBOptionsTest, DeleteObsoleteFilesPeriodChange) {
  SpecialEnv env(env_);
  env.time_elapse_only_sleep_ = true;
  Options options;
  options.env = &env;
  options.create_if_missing = true;
  ASSERT_OK(TryReopen(options));

  // Verify that candidate files set is empty when no full scan requested.
  assert_candidate_files_empty(dbfull(), true);

  ASSERT_OK(
      dbfull()->SetDBOptions({{"delete_obsolete_files_period_micros", "0"}}));

  // After delete_obsolete_files_period_micros updated to 0, the next call
  // to FindObsoleteFiles should make a full scan
  assert_candidate_files_empty(dbfull(), false);

  ASSERT_OK(
      dbfull()->SetDBOptions({{"delete_obsolete_files_period_micros", "20"}}));

  assert_candidate_files_empty(dbfull(), true);

  env.addon_time_.store(20);
  assert_candidate_files_empty(dbfull(), true);

  env.addon_time_.store(21);
  assert_candidate_files_empty(dbfull(), false);

  Close();
}

TEST_F(DBOptionsTest, MaxOpenFilesChange) {
  SpecialEnv env(env_);
  Options options;
  options.env = CurrentOptions().env;
  options.max_open_files = -1;

  Reopen(options);

  Cache* tc = dbfull()->TEST_table_cache();

  ASSERT_EQ(-1, dbfull()->GetDBOptions().max_open_files);
  ASSERT_LT(2000, tc->GetCapacity());
  ASSERT_OK(dbfull()->SetDBOptions({{"max_open_files", "1024"}}));
  ASSERT_EQ(1024, dbfull()->GetDBOptions().max_open_files);
  // examine the table cache (actual size should be 1014)
  ASSERT_GT(1500, tc->GetCapacity());
  Close();
}

TEST_F(DBOptionsTest, SanitizeDelayedWriteRate) {
  Options options;
  options.delayed_write_rate = 0;
  Reopen(options);
  ASSERT_EQ(16 * 1024 * 1024, dbfull()->GetDBOptions().delayed_write_rate);

  options.rate_limiter.reset(NewGenericRateLimiter(31 * 1024 * 1024));
  Reopen(options);
  ASSERT_EQ(31 * 1024 * 1024, dbfull()->GetDBOptions().delayed_write_rate);
}

TEST_F(DBOptionsTest, SetFIFOCompactionOptions) {
  Options options;
  options.compaction_style = kCompactionStyleFIFO;
  options.write_buffer_size = 10 << 10;  // 10KB
  options.arena_block_size = 4096;
  options.compression = kNoCompression;
  options.create_if_missing = true;
  options.compaction_options_fifo.allow_compaction = false;
  env_->time_elapse_only_sleep_ = false;
  options.env = env_;

  // Test dynamically changing ttl.
  env_->addon_time_.store(0);
  options.ttl = 1 * 60 * 60;  // 1 hour
  ASSERT_OK(TryReopen(options));

  Random rnd(301);
  for (int i = 0; i < 10; i++) {
    // Generate and flush a file about 10KB.
    for (int j = 0; j < 10; j++) {
      ASSERT_OK(Put(ToString(i * 20 + j), RandomString(&rnd, 980)));
    }
    Flush();
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(NumTableFilesAtLevel(0), 10);

  // Add 61 seconds to the time.
  env_->addon_time_.fetch_add(61);

  // No files should be compacted as ttl is set to 1 hour.
  ASSERT_EQ(dbfull()->GetOptions().ttl, 3600);
  dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_EQ(NumTableFilesAtLevel(0), 10);

  // Set ttl to 1 minute. So all files should get deleted.
  ASSERT_OK(dbfull()->SetOptions({{"ttl", "60"}}));
  ASSERT_EQ(dbfull()->GetOptions().ttl, 60);
  dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);

  // Test dynamically changing compaction_options_fifo.max_table_files_size
  env_->addon_time_.store(0);
  options.compaction_options_fifo.max_table_files_size = 500 << 10;  // 00KB
  options.ttl = 0;
  DestroyAndReopen(options);

  for (int i = 0; i < 10; i++) {
    // Generate and flush a file about 10KB.
    for (int j = 0; j < 10; j++) {
      ASSERT_OK(Put(ToString(i * 20 + j), RandomString(&rnd, 980)));
    }
    Flush();
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(NumTableFilesAtLevel(0), 10);

  // No files should be compacted as max_table_files_size is set to 500 KB.
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_fifo.max_table_files_size,
            500 << 10);
  dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_EQ(NumTableFilesAtLevel(0), 10);

  // Set max_table_files_size to 12 KB. So only 1 file should remain now.
  ASSERT_OK(dbfull()->SetOptions(
      {{"compaction_options_fifo", "{max_table_files_size=12288;}"}}));
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_fifo.max_table_files_size,
            12 << 10);
  dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(NumTableFilesAtLevel(0), 1);

  // Test dynamically changing compaction_options_fifo.allow_compaction
  options.compaction_options_fifo.max_table_files_size = 500 << 10;  // 500KB
  options.ttl = 0;
  options.compaction_options_fifo.allow_compaction = false;
  options.level0_file_num_compaction_trigger = 6;
  DestroyAndReopen(options);

  for (int i = 0; i < 10; i++) {
    // Generate and flush a file about 10KB.
    for (int j = 0; j < 10; j++) {
      ASSERT_OK(Put(ToString(i * 20 + j), RandomString(&rnd, 980)));
    }
    Flush();
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(NumTableFilesAtLevel(0), 10);

  // No files should be compacted as max_table_files_size is set to 500 KB and
  // allow_compaction is false
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_fifo.allow_compaction,
            false);
  dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_EQ(NumTableFilesAtLevel(0), 10);

  // Set allow_compaction to true. So number of files should be between 1 and 5.
  ASSERT_OK(dbfull()->SetOptions(
      {{"compaction_options_fifo", "{allow_compaction=true;}"}}));
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_fifo.allow_compaction,
            true);
  dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_GE(NumTableFilesAtLevel(0), 1);
  ASSERT_LE(NumTableFilesAtLevel(0), 5);
}

TEST_F(DBOptionsTest, CompactionReadaheadSizeChange) {
  SpecialEnv env(env_);
  Options options;
  options.env = &env;

  options.compaction_readahead_size = 0;
  options.new_table_reader_for_compaction_inputs = true;
  options.level0_file_num_compaction_trigger = 2;
  const std::string kValue(1024, 'v');
  Reopen(options);

  ASSERT_EQ(0, dbfull()->GetDBOptions().compaction_readahead_size);
  ASSERT_OK(dbfull()->SetDBOptions({{"compaction_readahead_size", "256"}}));
  ASSERT_EQ(256, dbfull()->GetDBOptions().compaction_readahead_size);
  for (int i = 0; i < 1024; i++) {
    Put(Key(i), kValue);
  }
  Flush();
  for (int i = 0; i < 1024 * 2; i++) {
    Put(Key(i), kValue);
  }
  Flush();
  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(256, env_->compaction_readahead_size_);
  Close();
}
#endif  // ROCKSDB_LITE

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
