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

class DBIOFailureTest : public DBTestBase {
 public:
  DBIOFailureTest() : DBTestBase("/db_io_failure_test") {}
};

#ifndef ROCKSDB_LITE
// Check that number of files does not grow when writes are dropped
TEST_F(DBIOFailureTest, DropWrites) {
  do {
    Options options = CurrentOptions();
    options.env = env_;
    options.paranoid_checks = false;
    Reopen(options);

    ASSERT_OK(Put("foo", "v1"));
    ASSERT_EQ("v1", Get("foo"));
    Compact("a", "z");
    const size_t num_files = CountFiles();
    // Force out-of-space errors
    env_->drop_writes_.store(true, std::memory_order_release);
    env_->sleep_counter_.Reset();
    env_->no_slowdown_ = true;
    for (int i = 0; i < 5; i++) {
      if (option_config_ != kUniversalCompactionMultiLevel &&
          option_config_ != kUniversalSubcompactions) {
        for (int level = 0; level < dbfull()->NumberLevels(); level++) {
          if (level > 0 && level == dbfull()->NumberLevels() - 1) {
            break;
          }
          dbfull()->TEST_CompactRange(level, nullptr, nullptr, nullptr,
                                      true /* disallow trivial move */);
        }
      } else {
        dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
      }
    }

    std::string property_value;
    ASSERT_TRUE(db_->GetProperty("rocksdb.background-errors", &property_value));
    ASSERT_EQ("5", property_value);

    env_->drop_writes_.store(false, std::memory_order_release);
    ASSERT_LT(CountFiles(), num_files + 3);

    // Check that compaction attempts slept after errors
    // TODO @krad: Figure out why ASSERT_EQ 5 keeps failing in certain compiler
    // versions
    ASSERT_GE(env_->sleep_counter_.Read(), 4);
  } while (ChangeCompactOptions());
}

// Check background error counter bumped on flush failures.
TEST_F(DBIOFailureTest, DropWritesFlush) {
  do {
    Options options = CurrentOptions();
    options.env = env_;
    options.max_background_flushes = 1;
    Reopen(options);

    ASSERT_OK(Put("foo", "v1"));
    // Force out-of-space errors
    env_->drop_writes_.store(true, std::memory_order_release);

    std::string property_value;
    // Background error count is 0 now.
    ASSERT_TRUE(db_->GetProperty("rocksdb.background-errors", &property_value));
    ASSERT_EQ("0", property_value);

    dbfull()->TEST_FlushMemTable(true);

    ASSERT_TRUE(db_->GetProperty("rocksdb.background-errors", &property_value));
    ASSERT_EQ("1", property_value);

    env_->drop_writes_.store(false, std::memory_order_release);
  } while (ChangeCompactOptions());
}

// Check that CompactRange() returns failure if there is not enough space left
// on device
TEST_F(DBIOFailureTest, NoSpaceCompactRange) {
  do {
    Options options = CurrentOptions();
    options.env = env_;
    options.disable_auto_compactions = true;
    Reopen(options);

    // generate 5 tables
    for (int i = 0; i < 5; ++i) {
      ASSERT_OK(Put(Key(i), Key(i) + "v"));
      ASSERT_OK(Flush());
    }

    // Force out-of-space errors
    env_->no_space_.store(true, std::memory_order_release);

    Status s = dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                                           true /* disallow trivial move */);
    ASSERT_TRUE(s.IsIOError());
    ASSERT_TRUE(s.IsNoSpace());

    env_->no_space_.store(false, std::memory_order_release);
  } while (ChangeCompactOptions());
}
#endif  // ROCKSDB_LITE

TEST_F(DBIOFailureTest, NonWritableFileSystem) {
  do {
    Options options = CurrentOptions();
    options.write_buffer_size = 4096;
    options.arena_block_size = 4096;
    options.env = env_;
    Reopen(options);
    ASSERT_OK(Put("foo", "v1"));
    env_->non_writeable_rate_.store(100);
    std::string big(100000, 'x');
    int errors = 0;
    for (int i = 0; i < 20; i++) {
      if (!Put("foo", big).ok()) {
        errors++;
        env_->SleepForMicroseconds(100000);
      }
    }
    ASSERT_GT(errors, 0);
    env_->non_writeable_rate_.store(0);
  } while (ChangeCompactOptions());
}

#ifndef ROCKSDB_LITE
TEST_F(DBIOFailureTest, ManifestWriteError) {
  // Test for the following problem:
  // (a) Compaction produces file F
  // (b) Log record containing F is written to MANIFEST file, but Sync() fails
  // (c) GC deletes F
  // (d) After reopening DB, reads fail since deleted F is named in log record

  // We iterate twice.  In the second iteration, everything is the
  // same except the log record never makes it to the MANIFEST file.
  for (int iter = 0; iter < 2; iter++) {
    std::atomic<bool>* error_type = (iter == 0) ? &env_->manifest_sync_error_
                                                : &env_->manifest_write_error_;

    // Insert foo=>bar mapping
    Options options = CurrentOptions();
    options.env = env_;
    options.create_if_missing = true;
    options.error_if_exists = false;
    options.paranoid_checks = true;
    DestroyAndReopen(options);
    ASSERT_OK(Put("foo", "bar"));
    ASSERT_EQ("bar", Get("foo"));

    // Memtable compaction (will succeed)
    Flush();
    ASSERT_EQ("bar", Get("foo"));
    const int last = 2;
    MoveFilesToLevel(2);
    ASSERT_EQ(NumTableFilesAtLevel(last), 1);  // foo=>bar is now in last level

    // Merging compaction (will fail)
    error_type->store(true, std::memory_order_release);
    dbfull()->TEST_CompactRange(last, nullptr, nullptr);  // Should fail
    ASSERT_EQ("bar", Get("foo"));

    error_type->store(false, std::memory_order_release);

    // Since paranoid_checks=true, writes should fail
    ASSERT_NOK(Put("foo2", "bar2"));

    // Recovery: should not lose data
    ASSERT_EQ("bar", Get("foo"));

    // Try again with paranoid_checks=false
    Close();
    options.paranoid_checks = false;
    Reopen(options);

    // Merging compaction (will fail)
    error_type->store(true, std::memory_order_release);
    dbfull()->TEST_CompactRange(last, nullptr, nullptr);  // Should fail
    ASSERT_EQ("bar", Get("foo"));

    // Recovery: should not lose data
    error_type->store(false, std::memory_order_release);
    Reopen(options);
    ASSERT_EQ("bar", Get("foo"));

    // Since paranoid_checks=false, writes should succeed
    ASSERT_OK(Put("foo2", "bar2"));
    ASSERT_EQ("bar", Get("foo"));
    ASSERT_EQ("bar2", Get("foo2"));
  }
}

TEST_F(DBIOFailureTest, PutFailsParanoid) {
  // Test the following:
  // (a) A random put fails in paranoid mode (simulate by sync fail)
  // (b) All other puts have to fail, even if writes would succeed
  // (c) All of that should happen ONLY if paranoid_checks = true

  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.error_if_exists = false;
  options.paranoid_checks = true;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  Status s;

  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(1, "foo1", "bar1"));
  // simulate error
  env_->log_write_error_.store(true, std::memory_order_release);
  s = Put(1, "foo2", "bar2");
  ASSERT_TRUE(!s.ok());
  env_->log_write_error_.store(false, std::memory_order_release);
  s = Put(1, "foo3", "bar3");
  // the next put should fail, too
  ASSERT_TRUE(!s.ok());
  // but we're still able to read
  ASSERT_EQ("bar", Get(1, "foo"));

  // do the same thing with paranoid checks off
  options.paranoid_checks = false;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(1, "foo1", "bar1"));
  // simulate error
  env_->log_write_error_.store(true, std::memory_order_release);
  s = Put(1, "foo2", "bar2");
  ASSERT_TRUE(!s.ok());
  env_->log_write_error_.store(false, std::memory_order_release);
  s = Put(1, "foo3", "bar3");
  // the next put should NOT fail
  ASSERT_TRUE(s.ok());
}
#if !(defined NDEBUG) || !defined(OS_WIN)
TEST_F(DBIOFailureTest, FlushSstRangeSyncError) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.error_if_exists = false;
  options.paranoid_checks = true;
  options.write_buffer_size = 256 * 1024 * 1024;
  options.writable_file_max_buffer_size = 128 * 1024;
  options.bytes_per_sync = 128 * 1024;
  options.level0_file_num_compaction_trigger = 4;
  options.memtable_factory.reset(new SpecialSkipListFactory(10));
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  Status s;

  std::atomic<int> range_sync_called(0);
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "SpecialEnv::SStableFile::RangeSync", [&](void* arg) {
        if (range_sync_called.fetch_add(1) == 0) {
          Status* st = static_cast<Status*>(arg);
          *st = Status::IOError("range sync dummy error");
        }
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Random rnd(301);
  std::string rnd_str =
      RandomString(&rnd, static_cast<int>(options.bytes_per_sync / 2));
  std::string rnd_str_512kb = RandomString(&rnd, 512 * 1024);

  ASSERT_OK(Put(1, "foo", "bar"));
  // First 1MB doesn't get range synced
  ASSERT_OK(Put(1, "foo0_0", rnd_str_512kb));
  ASSERT_OK(Put(1, "foo0_1", rnd_str_512kb));
  ASSERT_OK(Put(1, "foo1_1", rnd_str));
  ASSERT_OK(Put(1, "foo1_2", rnd_str));
  ASSERT_OK(Put(1, "foo1_3", rnd_str));
  ASSERT_OK(Put(1, "foo2", "bar"));
  ASSERT_OK(Put(1, "foo3_1", rnd_str));
  ASSERT_OK(Put(1, "foo3_2", rnd_str));
  ASSERT_OK(Put(1, "foo3_3", rnd_str));
  ASSERT_OK(Put(1, "foo4", "bar"));
  dbfull()->TEST_WaitForFlushMemTable(handles_[1]);

  // Following writes should fail as flush failed.
  ASSERT_NOK(Put(1, "foo2", "bar3"));
  ASSERT_EQ("bar", Get(1, "foo"));

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_GE(1, range_sync_called.load());

  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_EQ("bar", Get(1, "foo"));
}

TEST_F(DBIOFailureTest, CompactSstRangeSyncError) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.error_if_exists = false;
  options.paranoid_checks = true;
  options.write_buffer_size = 256 * 1024 * 1024;
  options.writable_file_max_buffer_size = 128 * 1024;
  options.bytes_per_sync = 128 * 1024;
  options.level0_file_num_compaction_trigger = 2;
  options.target_file_size_base = 256 * 1024 * 1024;
  options.disable_auto_compactions = true;
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  Status s;

  Random rnd(301);
  std::string rnd_str =
      RandomString(&rnd, static_cast<int>(options.bytes_per_sync / 2));
  std::string rnd_str_512kb = RandomString(&rnd, 512 * 1024);

  ASSERT_OK(Put(1, "foo", "bar"));
  // First 1MB doesn't get range synced
  ASSERT_OK(Put(1, "foo0_0", rnd_str_512kb));
  ASSERT_OK(Put(1, "foo0_1", rnd_str_512kb));
  ASSERT_OK(Put(1, "foo1_1", rnd_str));
  ASSERT_OK(Put(1, "foo1_2", rnd_str));
  ASSERT_OK(Put(1, "foo1_3", rnd_str));
  Flush(1);
  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(1, "foo3_1", rnd_str));
  ASSERT_OK(Put(1, "foo3_2", rnd_str));
  ASSERT_OK(Put(1, "foo3_3", rnd_str));
  ASSERT_OK(Put(1, "foo4", "bar"));
  Flush(1);
  dbfull()->TEST_WaitForFlushMemTable(handles_[1]);

  std::atomic<int> range_sync_called(0);
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "SpecialEnv::SStableFile::RangeSync", [&](void* arg) {
        if (range_sync_called.fetch_add(1) == 0) {
          Status* st = static_cast<Status*>(arg);
          *st = Status::IOError("range sync dummy error");
        }
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(dbfull()->SetOptions(handles_[1],
                                 {
                                     {"disable_auto_compactions", "false"},
                                 }));
  dbfull()->TEST_WaitForCompact();

  // Following writes should fail as flush failed.
  ASSERT_NOK(Put(1, "foo2", "bar3"));
  ASSERT_EQ("bar", Get(1, "foo"));

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_GE(1, range_sync_called.load());

  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_EQ("bar", Get(1, "foo"));
}

TEST_F(DBIOFailureTest, FlushSstCloseError) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.error_if_exists = false;
  options.paranoid_checks = true;
  options.level0_file_num_compaction_trigger = 4;
  options.memtable_factory.reset(new SpecialSkipListFactory(2));

  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  Status s;
  std::atomic<int> close_called(0);
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "SpecialEnv::SStableFile::Close", [&](void* arg) {
        if (close_called.fetch_add(1) == 0) {
          Status* st = static_cast<Status*>(arg);
          *st = Status::IOError("close dummy error");
        }
      });

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(1, "foo1", "bar1"));
  ASSERT_OK(Put(1, "foo", "bar2"));
  dbfull()->TEST_WaitForFlushMemTable(handles_[1]);

  // Following writes should fail as flush failed.
  ASSERT_NOK(Put(1, "foo2", "bar3"));
  ASSERT_EQ("bar2", Get(1, "foo"));
  ASSERT_EQ("bar1", Get(1, "foo1"));

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();

  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_EQ("bar2", Get(1, "foo"));
  ASSERT_EQ("bar1", Get(1, "foo1"));
}

TEST_F(DBIOFailureTest, CompactionSstCloseError) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.error_if_exists = false;
  options.paranoid_checks = true;
  options.level0_file_num_compaction_trigger = 2;
  options.disable_auto_compactions = true;

  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  Status s;

  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(1, "foo2", "bar"));
  Flush(1);
  ASSERT_OK(Put(1, "foo", "bar2"));
  ASSERT_OK(Put(1, "foo2", "bar"));
  Flush(1);
  ASSERT_OK(Put(1, "foo", "bar3"));
  ASSERT_OK(Put(1, "foo2", "bar"));
  Flush(1);
  dbfull()->TEST_WaitForCompact();

  std::atomic<int> close_called(0);
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "SpecialEnv::SStableFile::Close", [&](void* arg) {
        if (close_called.fetch_add(1) == 0) {
          Status* st = static_cast<Status*>(arg);
          *st = Status::IOError("close dummy error");
        }
      });

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(dbfull()->SetOptions(handles_[1],
                                 {
                                     {"disable_auto_compactions", "false"},
                                 }));
  dbfull()->TEST_WaitForCompact();

  // Following writes should fail as compaction failed.
  ASSERT_NOK(Put(1, "foo2", "bar3"));
  ASSERT_EQ("bar3", Get(1, "foo"));

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();

  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_EQ("bar3", Get(1, "foo"));
}

TEST_F(DBIOFailureTest, FlushSstSyncError) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.error_if_exists = false;
  options.paranoid_checks = true;
  options.use_fsync = false;
  options.level0_file_num_compaction_trigger = 4;
  options.memtable_factory.reset(new SpecialSkipListFactory(2));

  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  Status s;
  std::atomic<int> sync_called(0);
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "SpecialEnv::SStableFile::Sync", [&](void* arg) {
        if (sync_called.fetch_add(1) == 0) {
          Status* st = static_cast<Status*>(arg);
          *st = Status::IOError("sync dummy error");
        }
      });

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(1, "foo1", "bar1"));
  ASSERT_OK(Put(1, "foo", "bar2"));
  dbfull()->TEST_WaitForFlushMemTable(handles_[1]);

  // Following writes should fail as flush failed.
  ASSERT_NOK(Put(1, "foo2", "bar3"));
  ASSERT_EQ("bar2", Get(1, "foo"));
  ASSERT_EQ("bar1", Get(1, "foo1"));

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();

  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_EQ("bar2", Get(1, "foo"));
  ASSERT_EQ("bar1", Get(1, "foo1"));
}

TEST_F(DBIOFailureTest, CompactionSstSyncError) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.error_if_exists = false;
  options.paranoid_checks = true;
  options.level0_file_num_compaction_trigger = 2;
  options.disable_auto_compactions = true;
  options.use_fsync = false;

  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  Status s;

  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(1, "foo2", "bar"));
  Flush(1);
  ASSERT_OK(Put(1, "foo", "bar2"));
  ASSERT_OK(Put(1, "foo2", "bar"));
  Flush(1);
  ASSERT_OK(Put(1, "foo", "bar3"));
  ASSERT_OK(Put(1, "foo2", "bar"));
  Flush(1);
  dbfull()->TEST_WaitForCompact();

  std::atomic<int> sync_called(0);
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "SpecialEnv::SStableFile::Sync", [&](void* arg) {
        if (sync_called.fetch_add(1) == 0) {
          Status* st = static_cast<Status*>(arg);
          *st = Status::IOError("close dummy error");
        }
      });

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(dbfull()->SetOptions(handles_[1],
                                 {
                                     {"disable_auto_compactions", "false"},
                                 }));
  dbfull()->TEST_WaitForCompact();

  // Following writes should fail as compaction failed.
  ASSERT_NOK(Put(1, "foo2", "bar3"));
  ASSERT_EQ("bar3", Get(1, "foo"));

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();

  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_EQ("bar3", Get(1, "foo"));
}
#endif  // !(defined NDEBUG) || !defined(OS_WIN)
#endif  // ROCKSDB_LITE
}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
