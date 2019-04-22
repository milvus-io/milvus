//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_test_util.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/sst_file_manager.h"
#include "util/sst_file_manager_impl.h"

namespace rocksdb {

class DBSSTTest : public DBTestBase {
 public:
  DBSSTTest() : DBTestBase("/db_sst_test") {}
};

#ifndef ROCKSDB_LITE
// A class which remembers the name of each flushed file.
class FlushedFileCollector : public EventListener {
 public:
  FlushedFileCollector() {}
  ~FlushedFileCollector() override {}

  void OnFlushCompleted(DB* /*db*/, const FlushJobInfo& info) override {
    std::lock_guard<std::mutex> lock(mutex_);
    flushed_files_.push_back(info.file_path);
  }

  std::vector<std::string> GetFlushedFiles() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> result;
    for (auto fname : flushed_files_) {
      result.push_back(fname);
    }
    return result;
  }
  void ClearFlushedFiles() {
    std::lock_guard<std::mutex> lock(mutex_);
    flushed_files_.clear();
  }

 private:
  std::vector<std::string> flushed_files_;
  std::mutex mutex_;
};
#endif  // ROCKSDB_LITE

TEST_F(DBSSTTest, DontDeletePendingOutputs) {
  Options options;
  options.env = env_;
  options.create_if_missing = true;
  DestroyAndReopen(options);

  // Every time we write to a table file, call FOF/POF with full DB scan. This
  // will make sure our pending_outputs_ protection work correctly
  std::function<void()> purge_obsolete_files_function = [&]() {
    JobContext job_context(0);
    dbfull()->TEST_LockMutex();
    dbfull()->FindObsoleteFiles(&job_context, true /*force*/);
    dbfull()->TEST_UnlockMutex();
    dbfull()->PurgeObsoleteFiles(job_context);
    job_context.Clean();
  };

  env_->table_write_callback_ = &purge_obsolete_files_function;

  for (int i = 0; i < 2; ++i) {
    ASSERT_OK(Put("a", "begin"));
    ASSERT_OK(Put("z", "end"));
    ASSERT_OK(Flush());
  }

  // If pending output guard does not work correctly, PurgeObsoleteFiles() will
  // delete the file that Compaction is trying to create, causing this: error
  // db/db_test.cc:975: IO error:
  // /tmp/rocksdbtest-1552237650/db_test/000009.sst: No such file or directory
  Compact("a", "b");
}

// 1 Create some SST files by inserting K-V pairs into DB
// 2 Close DB and change suffix from ".sst" to ".ldb" for every other SST file
// 3 Open DB and check if all key can be read
TEST_F(DBSSTTest, SSTsWithLdbSuffixHandling) {
  Options options = CurrentOptions();
  options.write_buffer_size = 110 << 10;  // 110KB
  options.num_levels = 4;
  DestroyAndReopen(options);

  Random rnd(301);
  int key_id = 0;
  for (int i = 0; i < 10; ++i) {
    GenerateNewFile(&rnd, &key_id, false);
  }
  Flush();
  Close();
  int const num_files = GetSstFileCount(dbname_);
  ASSERT_GT(num_files, 0);

  std::vector<std::string> filenames;
  GetSstFiles(env_, dbname_, &filenames);
  int num_ldb_files = 0;
  for (size_t i = 0; i < filenames.size(); ++i) {
    if (i & 1) {
      continue;
    }
    std::string const rdb_name = dbname_ + "/" + filenames[i];
    std::string const ldb_name = Rocks2LevelTableFileName(rdb_name);
    ASSERT_TRUE(env_->RenameFile(rdb_name, ldb_name).ok());
    ++num_ldb_files;
  }
  ASSERT_GT(num_ldb_files, 0);
  ASSERT_EQ(num_files, GetSstFileCount(dbname_));

  Reopen(options);
  for (int k = 0; k < key_id; ++k) {
    ASSERT_NE("NOT_FOUND", Get(Key(k)));
  }
  Destroy(options);
}

#ifndef ROCKSDB_LITE
TEST_F(DBSSTTest, DontDeleteMovedFile) {
  // This test triggers move compaction and verifies that the file is not
  // deleted when it's part of move compaction
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.max_bytes_for_level_base = 1024 * 1024;  // 1 MB
  options.level0_file_num_compaction_trigger =
      2;  // trigger compaction when we have 2 files
  DestroyAndReopen(options);

  Random rnd(301);
  // Create two 1MB sst files
  for (int i = 0; i < 2; ++i) {
    // Create 1MB sst file
    for (int j = 0; j < 100; ++j) {
      ASSERT_OK(Put(Key(i * 50 + j), RandomString(&rnd, 10 * 1024)));
    }
    ASSERT_OK(Flush());
  }
  // this should execute both L0->L1 and L1->(move)->L2 compactions
  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ("0,0,1", FilesPerLevel(0));

  // If the moved file is actually deleted (the move-safeguard in
  // ~Version::Version() is not there), we get this failure:
  // Corruption: Can't access /000009.sst
  Reopen(options);
}

// This reproduces a bug where we don't delete a file because when it was
// supposed to be deleted, it was blocked by pending_outputs
// Consider:
// 1. current file_number is 13
// 2. compaction (1) starts, blocks deletion of all files starting with 13
// (pending outputs)
// 3. file 13 is created by compaction (2)
// 4. file 13 is consumed by compaction (3) and file 15 was created. Since file
// 13 has no references, it is put into VersionSet::obsolete_files_
// 5. FindObsoleteFiles() gets file 13 from VersionSet::obsolete_files_. File 13
// is deleted from obsolete_files_ set.
// 6. PurgeObsoleteFiles() tries to delete file 13, but this file is blocked by
// pending outputs since compaction (1) is still running. It is not deleted and
// it is not present in obsolete_files_ anymore. Therefore, we never delete it.
TEST_F(DBSSTTest, DeleteObsoleteFilesPendingOutputs) {
  Options options = CurrentOptions();
  options.env = env_;
  options.write_buffer_size = 2 * 1024 * 1024;     // 2 MB
  options.max_bytes_for_level_base = 1024 * 1024;  // 1 MB
  options.level0_file_num_compaction_trigger =
      2;  // trigger compaction when we have 2 files
  options.max_background_flushes = 2;
  options.max_background_compactions = 2;

  OnFileDeletionListener* listener = new OnFileDeletionListener();
  options.listeners.emplace_back(listener);

  Reopen(options);

  Random rnd(301);
  // Create two 1MB sst files
  for (int i = 0; i < 2; ++i) {
    // Create 1MB sst file
    for (int j = 0; j < 100; ++j) {
      ASSERT_OK(Put(Key(i * 50 + j), RandomString(&rnd, 10 * 1024)));
    }
    ASSERT_OK(Flush());
  }
  // this should execute both L0->L1 and L1->(move)->L2 compactions
  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ("0,0,1", FilesPerLevel(0));

  test::SleepingBackgroundTask blocking_thread;
  port::Mutex mutex_;
  bool already_blocked(false);

  // block the flush
  std::function<void()> block_first_time = [&]() {
    bool blocking = false;
    {
      MutexLock l(&mutex_);
      if (!already_blocked) {
        blocking = true;
        already_blocked = true;
      }
    }
    if (blocking) {
      blocking_thread.DoSleep();
    }
  };
  env_->table_write_callback_ = &block_first_time;
  // Insert 2.5MB data, which should trigger a flush because we exceed
  // write_buffer_size. The flush will be blocked with block_first_time
  // pending_file is protecting all the files created after
  for (int j = 0; j < 256; ++j) {
    ASSERT_OK(Put(Key(j), RandomString(&rnd, 10 * 1024)));
  }
  blocking_thread.WaitUntilSleeping();

  ASSERT_OK(dbfull()->TEST_CompactRange(2, nullptr, nullptr));

  ASSERT_EQ("0,0,0,1", FilesPerLevel(0));
  std::vector<LiveFileMetaData> metadata;
  db_->GetLiveFilesMetaData(&metadata);
  ASSERT_EQ(metadata.size(), 1U);
  auto file_on_L2 = metadata[0].name;
  listener->SetExpectedFileName(dbname_ + file_on_L2);

  ASSERT_OK(dbfull()->TEST_CompactRange(3, nullptr, nullptr, nullptr,
                                        true /* disallow trivial move */));
  ASSERT_EQ("0,0,0,0,1", FilesPerLevel(0));

  // finish the flush!
  blocking_thread.WakeUp();
  blocking_thread.WaitUntilDone();
  dbfull()->TEST_WaitForFlushMemTable();
  // File just flushed is too big for L0 and L1 so gets moved to L2.
  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ("0,0,1,0,1", FilesPerLevel(0));

  metadata.clear();
  db_->GetLiveFilesMetaData(&metadata);
  ASSERT_EQ(metadata.size(), 2U);

  // This file should have been deleted during last compaction
  ASSERT_EQ(Status::NotFound(), env_->FileExists(dbname_ + file_on_L2));
  listener->VerifyMatchedCount(1);
}

TEST_F(DBSSTTest, DBWithSstFileManager) {
  std::shared_ptr<SstFileManager> sst_file_manager(NewSstFileManager(env_));
  auto sfm = static_cast<SstFileManagerImpl*>(sst_file_manager.get());

  int files_added = 0;
  int files_deleted = 0;
  int files_moved = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::OnAddFile", [&](void* /*arg*/) { files_added++; });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::OnDeleteFile", [&](void* /*arg*/) { files_deleted++; });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::OnMoveFile", [&](void* /*arg*/) { files_moved++; });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.sst_file_manager = sst_file_manager;
  DestroyAndReopen(options);

  Random rnd(301);
  for (int i = 0; i < 25; i++) {
    GenerateNewRandomFile(&rnd);
    ASSERT_OK(Flush());
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
    // Verify that we are tracking all sst files in dbname_
    ASSERT_EQ(sfm->GetTrackedFiles(), GetAllSSTFiles());
  }
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  auto files_in_db = GetAllSSTFiles();
  // Verify that we are tracking all sst files in dbname_
  ASSERT_EQ(sfm->GetTrackedFiles(), files_in_db);
  // Verify the total files size
  uint64_t total_files_size = 0;
  for (auto& file_to_size : files_in_db) {
    total_files_size += file_to_size.second;
  }
  ASSERT_EQ(sfm->GetTotalSize(), total_files_size);
  // We flushed at least 25 files
  ASSERT_GE(files_added, 25);
  // Compaction must have deleted some files
  ASSERT_GT(files_deleted, 0);
  // No files were moved
  ASSERT_EQ(files_moved, 0);

  Close();
  Reopen(options);
  ASSERT_EQ(sfm->GetTrackedFiles(), files_in_db);
  ASSERT_EQ(sfm->GetTotalSize(), total_files_size);

  // Verify that we track all the files again after the DB is closed and opened
  Close();
  sst_file_manager.reset(NewSstFileManager(env_));
  options.sst_file_manager = sst_file_manager;
  sfm = static_cast<SstFileManagerImpl*>(sst_file_manager.get());

  Reopen(options);
  ASSERT_EQ(sfm->GetTrackedFiles(), files_in_db);
  ASSERT_EQ(sfm->GetTotalSize(), total_files_size);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBSSTTest, RateLimitedDelete) {
  Destroy(last_options_);
  rocksdb::SyncPoint::GetInstance()->LoadDependency({
      {"DBSSTTest::RateLimitedDelete:1",
       "DeleteScheduler::BackgroundEmptyTrash"},
  });

  std::vector<uint64_t> penalties;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::BackgroundEmptyTrash:Wait",
      [&](void* arg) { penalties.push_back(*(static_cast<uint64_t*>(arg))); });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "InstrumentedCondVar::TimedWaitInternal", [&](void* arg) {
        // Turn timed wait into a simulated sleep
        uint64_t* abs_time_us = static_cast<uint64_t*>(arg);
        int64_t cur_time = 0;
        env_->GetCurrentTime(&cur_time);
        if (*abs_time_us > static_cast<uint64_t>(cur_time)) {
          env_->addon_time_.fetch_add(*abs_time_us -
                                      static_cast<uint64_t>(cur_time));
        }

        // Randomly sleep shortly
        env_->addon_time_.fetch_add(
            static_cast<uint64_t>(Random::GetTLSInstance()->Uniform(10)));

        // Set wait until time to before current to force not to sleep.
        int64_t real_cur_time = 0;
        Env::Default()->GetCurrentTime(&real_cur_time);
        *abs_time_us = static_cast<uint64_t>(real_cur_time);
      });

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  env_->no_slowdown_ = true;
  env_->time_elapse_only_sleep_ = true;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.env = env_;

  int64_t rate_bytes_per_sec = 1024 * 10;  // 10 Kbs / Sec
  Status s;
  options.sst_file_manager.reset(
      NewSstFileManager(env_, nullptr, "", 0, false, &s, 0));
  ASSERT_OK(s);
  options.sst_file_manager->SetDeleteRateBytesPerSecond(rate_bytes_per_sec);
  auto sfm = static_cast<SstFileManagerImpl*>(options.sst_file_manager.get());
  sfm->delete_scheduler()->SetMaxTrashDBRatio(1.1);

  ASSERT_OK(TryReopen(options));
  // Create 4 files in L0
  for (char v = 'a'; v <= 'd'; v++) {
    ASSERT_OK(Put("Key2", DummyString(1024, v)));
    ASSERT_OK(Put("Key3", DummyString(1024, v)));
    ASSERT_OK(Put("Key4", DummyString(1024, v)));
    ASSERT_OK(Put("Key1", DummyString(1024, v)));
    ASSERT_OK(Put("Key4", DummyString(1024, v)));
    ASSERT_OK(Flush());
  }
  // We created 4 sst files in L0
  ASSERT_EQ("4", FilesPerLevel(0));

  std::vector<LiveFileMetaData> metadata;
  db_->GetLiveFilesMetaData(&metadata);

  // Compaction will move the 4 files in L0 to trash and create 1 L1 file
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact(true));
  ASSERT_EQ("0,1", FilesPerLevel(0));

  uint64_t delete_start_time = env_->NowMicros();
  // Hold BackgroundEmptyTrash
  TEST_SYNC_POINT("DBSSTTest::RateLimitedDelete:1");
  sfm->WaitForEmptyTrash();
  uint64_t time_spent_deleting = env_->NowMicros() - delete_start_time;

  uint64_t total_files_size = 0;
  uint64_t expected_penlty = 0;
  ASSERT_EQ(penalties.size(), metadata.size());
  for (size_t i = 0; i < metadata.size(); i++) {
    total_files_size += metadata[i].size;
    expected_penlty = ((total_files_size * 1000000) / rate_bytes_per_sec);
    ASSERT_EQ(expected_penlty, penalties[i]);
  }
  ASSERT_GT(time_spent_deleting, expected_penlty * 0.9);
  ASSERT_LT(time_spent_deleting, expected_penlty * 1.1);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBSSTTest, OpenDBWithExistingTrash) {
  Options options = CurrentOptions();

  options.sst_file_manager.reset(
      NewSstFileManager(env_, nullptr, "", 1024 * 1024 /* 1 MB/sec */));
  auto sfm = static_cast<SstFileManagerImpl*>(options.sst_file_manager.get());

  Destroy(last_options_);

  // Add some trash files to the db directory so the DB can clean them up
  env_->CreateDirIfMissing(dbname_);
  ASSERT_OK(WriteStringToFile(env_, "abc", dbname_ + "/" + "001.sst.trash"));
  ASSERT_OK(WriteStringToFile(env_, "abc", dbname_ + "/" + "002.sst.trash"));
  ASSERT_OK(WriteStringToFile(env_, "abc", dbname_ + "/" + "003.sst.trash"));

  // Reopen the DB and verify that it deletes existing trash files
  ASSERT_OK(TryReopen(options));
  sfm->WaitForEmptyTrash();
  ASSERT_NOK(env_->FileExists(dbname_ + "/" + "001.sst.trash"));
  ASSERT_NOK(env_->FileExists(dbname_ + "/" + "002.sst.trash"));
  ASSERT_NOK(env_->FileExists(dbname_ + "/" + "003.sst.trash"));
}


// Create a DB with 2 db_paths, and generate multiple files in the 2
// db_paths using CompactRangeOptions, make sure that files that were
// deleted from first db_path were deleted using DeleteScheduler and
// files in the second path were not.
TEST_F(DBSSTTest, DeleteSchedulerMultipleDBPaths) {
  std::atomic<int> bg_delete_file(0);
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:DeleteFile",
      [&](void* /*arg*/) { bg_delete_file++; });
  // The deletion scheduler sometimes skips marking file as trash according to
  // a heuristic. In that case the deletion will go through the below SyncPoint.
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteFile",
      [&](void* /*arg*/) { bg_delete_file++; });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.db_paths.emplace_back(dbname_, 1024 * 100);
  options.db_paths.emplace_back(dbname_ + "_2", 1024 * 100);
  options.env = env_;

  int64_t rate_bytes_per_sec = 1024 * 1024;  // 1 Mb / Sec
  Status s;
  options.sst_file_manager.reset(
      NewSstFileManager(env_, nullptr, "", rate_bytes_per_sec, false, &s,
                        /* max_trash_db_ratio= */ 1.1));

  ASSERT_OK(s);
  auto sfm = static_cast<SstFileManagerImpl*>(options.sst_file_manager.get());

  DestroyAndReopen(options);

  // Create 4 files in L0
  for (int i = 0; i < 4; i++) {
    ASSERT_OK(Put("Key" + ToString(i), DummyString(1024, 'A')));
    ASSERT_OK(Flush());
  }
  // We created 4 sst files in L0
  ASSERT_EQ("4", FilesPerLevel(0));
  // Compaction will delete files from L0 in first db path and generate a new
  // file in L1 in second db path
  CompactRangeOptions compact_options;
  compact_options.target_path_id = 1;
  Slice begin("Key0");
  Slice end("Key3");
  ASSERT_OK(db_->CompactRange(compact_options, &begin, &end));
  ASSERT_EQ("0,1", FilesPerLevel(0));

  // Create 4 files in L0
  for (int i = 4; i < 8; i++) {
    ASSERT_OK(Put("Key" + ToString(i), DummyString(1024, 'B')));
    ASSERT_OK(Flush());
  }
  ASSERT_EQ("4,1", FilesPerLevel(0));

  // Compaction will delete files from L0 in first db path and generate a new
  // file in L1 in second db path
  begin = "Key4";
  end = "Key7";
  ASSERT_OK(db_->CompactRange(compact_options, &begin, &end));
  ASSERT_EQ("0,2", FilesPerLevel(0));

  sfm->WaitForEmptyTrash();
  ASSERT_EQ(bg_delete_file, 8);

  // Compaction will delete both files and regenerate a file in L1 in second
  // db path. The deleted files should still be cleaned up via delete scheduler.
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_EQ("0,1", FilesPerLevel(0));

  sfm->WaitForEmptyTrash();
  ASSERT_EQ(bg_delete_file, 10);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBSSTTest, DestroyDBWithRateLimitedDelete) {
  int bg_delete_file = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:DeleteFile",
      [&](void* /*arg*/) { bg_delete_file++; });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Status s;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.env = env_;
  options.sst_file_manager.reset(
      NewSstFileManager(env_, nullptr, "", 0, false, &s, 0));
  ASSERT_OK(s);
  DestroyAndReopen(options);

  // Create 4 files in L0
  for (int i = 0; i < 4; i++) {
    ASSERT_OK(Put("Key" + ToString(i), DummyString(1024, 'A')));
    ASSERT_OK(Flush());
  }
  // We created 4 sst files in L0
  ASSERT_EQ("4", FilesPerLevel(0));

  // Close DB and destroy it using DeleteScheduler
  Close();

  auto sfm = static_cast<SstFileManagerImpl*>(options.sst_file_manager.get());

  sfm->SetDeleteRateBytesPerSecond(1024 * 1024);
  sfm->delete_scheduler()->SetMaxTrashDBRatio(1.1);
  ASSERT_OK(DestroyDB(dbname_, options));
  sfm->WaitForEmptyTrash();
  // We have deleted the 4 sst files in the delete_scheduler
  ASSERT_EQ(bg_delete_file, 4);
}

TEST_F(DBSSTTest, DBWithMaxSpaceAllowed) {
  std::shared_ptr<SstFileManager> sst_file_manager(NewSstFileManager(env_));
  auto sfm = static_cast<SstFileManagerImpl*>(sst_file_manager.get());

  Options options = CurrentOptions();
  options.sst_file_manager = sst_file_manager;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  Random rnd(301);

  // Generate a file containing 100 keys.
  for (int i = 0; i < 100; i++) {
    ASSERT_OK(Put(Key(i), RandomString(&rnd, 50)));
  }
  ASSERT_OK(Flush());

  uint64_t first_file_size = 0;
  auto files_in_db = GetAllSSTFiles(&first_file_size);
  ASSERT_EQ(sfm->GetTotalSize(), first_file_size);

  // Set the maximum allowed space usage to the current total size
  sfm->SetMaxAllowedSpaceUsage(first_file_size + 1);

  ASSERT_OK(Put("key1", "val1"));
  // This flush will cause bg_error_ and will fail
  ASSERT_NOK(Flush());
}

TEST_F(DBSSTTest, CancellingCompactionsWorks) {
  std::shared_ptr<SstFileManager> sst_file_manager(NewSstFileManager(env_));
  auto sfm = static_cast<SstFileManagerImpl*>(sst_file_manager.get());

  Options options = CurrentOptions();
  options.sst_file_manager = sst_file_manager;
  options.level0_file_num_compaction_trigger = 2;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  int completed_compactions = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction():CancelledCompaction", [&](void* /*arg*/) {
        sfm->SetMaxAllowedSpaceUsage(0);
        ASSERT_EQ(sfm->GetCompactionsReservedSize(), 0);
      });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial:AfterRun",
      [&](void* /*arg*/) { completed_compactions++; });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Random rnd(301);

  // Generate a file containing 10 keys.
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put(Key(i), RandomString(&rnd, 50)));
  }
  ASSERT_OK(Flush());
  uint64_t total_file_size = 0;
  auto files_in_db = GetAllSSTFiles(&total_file_size);
  // Set the maximum allowed space usage to the current total size
  sfm->SetMaxAllowedSpaceUsage(2 * total_file_size + 1);

  // Generate another file to trigger compaction.
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put(Key(i), RandomString(&rnd, 50)));
  }
  ASSERT_OK(Flush());
  dbfull()->TEST_WaitForCompact(true);

  // Because we set a callback in CancelledCompaction, we actually
  // let the compaction run
  ASSERT_GT(completed_compactions, 0);
  ASSERT_EQ(sfm->GetCompactionsReservedSize(), 0);
  // Make sure the stat is bumped
  ASSERT_GT(dbfull()->immutable_db_options().statistics.get()->getTickerCount(COMPACTION_CANCELLED), 0);
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBSSTTest, CancellingManualCompactionsWorks) {
  std::shared_ptr<SstFileManager> sst_file_manager(NewSstFileManager(env_));
  auto sfm = static_cast<SstFileManagerImpl*>(sst_file_manager.get());

  Options options = CurrentOptions();
  options.sst_file_manager = sst_file_manager;
  options.statistics = CreateDBStatistics();

  FlushedFileCollector* collector = new FlushedFileCollector();
  options.listeners.emplace_back(collector);

  DestroyAndReopen(options);

  Random rnd(301);

  // Generate a file containing 10 keys.
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put(Key(i), RandomString(&rnd, 50)));
  }
  ASSERT_OK(Flush());
  uint64_t total_file_size = 0;
  auto files_in_db = GetAllSSTFiles(&total_file_size);
  // Set the maximum allowed space usage to the current total size
  sfm->SetMaxAllowedSpaceUsage(2 * total_file_size + 1);

  // Generate another file to trigger compaction.
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put(Key(i), RandomString(&rnd, 50)));
  }
  ASSERT_OK(Flush());

  // OK, now trigger a manual compaction
  dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);

  // Wait for manual compaction to get scheduled and finish
  dbfull()->TEST_WaitForCompact(true);

  ASSERT_EQ(sfm->GetCompactionsReservedSize(), 0);
  // Make sure the stat is bumped
  ASSERT_EQ(dbfull()->immutable_db_options().statistics.get()->getTickerCount(
                COMPACTION_CANCELLED),
            1);

  // Now make sure CompactFiles also gets cancelled
  auto l0_files = collector->GetFlushedFiles();
  dbfull()->CompactFiles(rocksdb::CompactionOptions(), l0_files, 0);

  // Wait for manual compaction to get scheduled and finish
  dbfull()->TEST_WaitForCompact(true);

  ASSERT_EQ(dbfull()->immutable_db_options().statistics.get()->getTickerCount(
                COMPACTION_CANCELLED),
            2);
  ASSERT_EQ(sfm->GetCompactionsReservedSize(), 0);

  // Now let the flush through and make sure GetCompactionsReservedSize
  // returns to normal
  sfm->SetMaxAllowedSpaceUsage(0);
  int completed_compactions = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "CompactFilesImpl:End", [&](void* /*arg*/) { completed_compactions++; });

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  dbfull()->CompactFiles(rocksdb::CompactionOptions(), l0_files, 0);
  dbfull()->TEST_WaitForCompact(true);

  ASSERT_EQ(sfm->GetCompactionsReservedSize(), 0);
  ASSERT_GT(completed_compactions, 0);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBSSTTest, DBWithMaxSpaceAllowedRandomized) {
  // This test will set a maximum allowed space for the DB, then it will
  // keep filling the DB until the limit is reached and bg_error_ is set.
  // When bg_error_ is set we will verify that the DB size is greater
  // than the limit.

  std::vector<int> max_space_limits_mbs = {1, 10};
  std::atomic<bool> bg_error_set(false);

  std::atomic<int> reached_max_space_on_flush(0);
  std::atomic<int> reached_max_space_on_compaction(0);
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushMemTableToOutputFile:MaxAllowedSpaceReached",
      [&](void* arg) {
        Status* bg_error = static_cast<Status*>(arg);
        bg_error_set = true;
        reached_max_space_on_flush++;
        // clear error to ensure compaction callback is called
        *bg_error = Status::OK();
      });

  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction():CancelledCompaction", [&](void* arg) {
        bool* enough_room = static_cast<bool*>(arg);
        *enough_room = true;
      });

  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::FinishCompactionOutputFile:MaxAllowedSpaceReached",
      [&](void* /*arg*/) {
        bg_error_set = true;
        reached_max_space_on_compaction++;
      });

  for (auto limit_mb : max_space_limits_mbs) {
    bg_error_set = false;
    rocksdb::SyncPoint::GetInstance()->ClearTrace();
    rocksdb::SyncPoint::GetInstance()->EnableProcessing();
    std::shared_ptr<SstFileManager> sst_file_manager(NewSstFileManager(env_));
    auto sfm = static_cast<SstFileManagerImpl*>(sst_file_manager.get());

    Options options = CurrentOptions();
    options.sst_file_manager = sst_file_manager;
    options.write_buffer_size = 1024 * 512;  // 512 Kb
    DestroyAndReopen(options);
    Random rnd(301);

    sfm->SetMaxAllowedSpaceUsage(limit_mb * 1024 * 1024);

    // It is easy to detect if the test is stuck in a loop. No need for
    // complex termination logic.
    while (true) {
      auto s = Put(RandomString(&rnd, 10), RandomString(&rnd, 50));
      if (!s.ok()) {
        break;
      }
    }
    ASSERT_TRUE(bg_error_set);
    uint64_t total_sst_files_size = 0;
    GetAllSSTFiles(&total_sst_files_size);
    ASSERT_GE(total_sst_files_size, limit_mb * 1024 * 1024);
    rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  }

  ASSERT_GT(reached_max_space_on_flush, 0);
  ASSERT_GT(reached_max_space_on_compaction, 0);
}

TEST_F(DBSSTTest, OpenDBWithInfiniteMaxOpenFiles) {
  // Open DB with infinite max open files
  //  - First iteration use 1 thread to open files
  //  - Second iteration use 5 threads to open files
  for (int iter = 0; iter < 2; iter++) {
    Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 100000;
    options.disable_auto_compactions = true;
    options.max_open_files = -1;
    if (iter == 0) {
      options.max_file_opening_threads = 1;
    } else {
      options.max_file_opening_threads = 5;
    }
    options = CurrentOptions(options);
    DestroyAndReopen(options);

    // Create 12 Files in L0 (then move then to L2)
    for (int i = 0; i < 12; i++) {
      std::string k = "L2_" + Key(i);
      ASSERT_OK(Put(k, k + std::string(1000, 'a')));
      ASSERT_OK(Flush());
    }
    CompactRangeOptions compact_options;
    compact_options.change_level = true;
    compact_options.target_level = 2;
    db_->CompactRange(compact_options, nullptr, nullptr);

    // Create 12 Files in L0
    for (int i = 0; i < 12; i++) {
      std::string k = "L0_" + Key(i);
      ASSERT_OK(Put(k, k + std::string(1000, 'a')));
      ASSERT_OK(Flush());
    }
    Close();

    // Reopening the DB will load all existing files
    Reopen(options);
    ASSERT_EQ("12,0,12", FilesPerLevel(0));
    std::vector<std::vector<FileMetaData>> files;
    dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &files);

    for (const auto& level : files) {
      for (const auto& file : level) {
        ASSERT_TRUE(file.table_reader_handle != nullptr);
      }
    }

    for (int i = 0; i < 12; i++) {
      ASSERT_EQ(Get("L0_" + Key(i)), "L0_" + Key(i) + std::string(1000, 'a'));
      ASSERT_EQ(Get("L2_" + Key(i)), "L2_" + Key(i) + std::string(1000, 'a'));
    }
  }
}

TEST_F(DBSSTTest, GetTotalSstFilesSize) {
  // We don't propagate oldest-key-time table property on compaction and
  // just write 0 as default value. This affect the exact table size, since
  // we encode table properties as varint64. Force time to be 0 to work around
  // it. Should remove the workaround after we propagate the property on
  // compaction.
  std::unique_ptr<MockTimeEnv> mock_env(new MockTimeEnv(Env::Default()));
  mock_env->set_current_time(0);

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.compression = kNoCompression;
  options.env = mock_env.get();
  DestroyAndReopen(options);
  // Generate 5 files in L0
  for (int i = 0; i < 5; i++) {
    for (int j = 0; j < 10; j++) {
      std::string val = "val_file_" + ToString(i);
      ASSERT_OK(Put(Key(j), val));
    }
    Flush();
  }
  ASSERT_EQ("5", FilesPerLevel(0));

  std::vector<LiveFileMetaData> live_files_meta;
  dbfull()->GetLiveFilesMetaData(&live_files_meta);
  ASSERT_EQ(live_files_meta.size(), 5);
  uint64_t single_file_size = live_files_meta[0].size;

  uint64_t live_sst_files_size = 0;
  uint64_t total_sst_files_size = 0;
  for (const auto& file_meta : live_files_meta) {
    live_sst_files_size += file_meta.size;
  }

  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.total-sst-files-size",
                                       &total_sst_files_size));
  // Live SST files = 5
  // Total SST files = 5
  ASSERT_EQ(live_sst_files_size, 5 * single_file_size);
  ASSERT_EQ(total_sst_files_size, 5 * single_file_size);

  // hold current version
  std::unique_ptr<Iterator> iter1(dbfull()->NewIterator(ReadOptions()));

  // Compact 5 files into 1 file in L0
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,1", FilesPerLevel(0));

  live_files_meta.clear();
  dbfull()->GetLiveFilesMetaData(&live_files_meta);
  ASSERT_EQ(live_files_meta.size(), 1);

  live_sst_files_size = 0;
  total_sst_files_size = 0;
  for (const auto& file_meta : live_files_meta) {
    live_sst_files_size += file_meta.size;
  }
  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.total-sst-files-size",
                                       &total_sst_files_size));
  // Live SST files = 1 (compacted file)
  // Total SST files = 6 (5 original files + compacted file)
  ASSERT_EQ(live_sst_files_size, 1 * single_file_size);
  ASSERT_EQ(total_sst_files_size, 6 * single_file_size);

  // hold current version
  std::unique_ptr<Iterator> iter2(dbfull()->NewIterator(ReadOptions()));

  // Delete all keys and compact, this will delete all live files
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Delete(Key(i)));
  }
  Flush();
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("", FilesPerLevel(0));

  live_files_meta.clear();
  dbfull()->GetLiveFilesMetaData(&live_files_meta);
  ASSERT_EQ(live_files_meta.size(), 0);

  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.total-sst-files-size",
                                       &total_sst_files_size));
  // Live SST files = 0
  // Total SST files = 6 (5 original files + compacted file)
  ASSERT_EQ(total_sst_files_size, 6 * single_file_size);

  iter1.reset();
  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.total-sst-files-size",
                                       &total_sst_files_size));
  // Live SST files = 0
  // Total SST files = 1 (compacted file)
  ASSERT_EQ(total_sst_files_size, 1 * single_file_size);

  iter2.reset();
  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.total-sst-files-size",
                                       &total_sst_files_size));
  // Live SST files = 0
  // Total SST files = 0
  ASSERT_EQ(total_sst_files_size, 0);

  // Close db before mock_env destruct.
  Close();
}

TEST_F(DBSSTTest, GetTotalSstFilesSizeVersionsFilesShared) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.compression = kNoCompression;
  DestroyAndReopen(options);
  // Generate 5 files in L0
  for (int i = 0; i < 5; i++) {
    ASSERT_OK(Put(Key(i), "val"));
    Flush();
  }
  ASSERT_EQ("5", FilesPerLevel(0));

  std::vector<LiveFileMetaData> live_files_meta;
  dbfull()->GetLiveFilesMetaData(&live_files_meta);
  ASSERT_EQ(live_files_meta.size(), 5);
  uint64_t single_file_size = live_files_meta[0].size;

  uint64_t live_sst_files_size = 0;
  uint64_t total_sst_files_size = 0;
  for (const auto& file_meta : live_files_meta) {
    live_sst_files_size += file_meta.size;
  }

  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.total-sst-files-size",
                                       &total_sst_files_size));

  // Live SST files = 5
  // Total SST files = 5
  ASSERT_EQ(live_sst_files_size, 5 * single_file_size);
  ASSERT_EQ(total_sst_files_size, 5 * single_file_size);

  // hold current version
  std::unique_ptr<Iterator> iter1(dbfull()->NewIterator(ReadOptions()));

  // Compaction will do trivial move from L0 to L1
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,5", FilesPerLevel(0));

  live_files_meta.clear();
  dbfull()->GetLiveFilesMetaData(&live_files_meta);
  ASSERT_EQ(live_files_meta.size(), 5);

  live_sst_files_size = 0;
  total_sst_files_size = 0;
  for (const auto& file_meta : live_files_meta) {
    live_sst_files_size += file_meta.size;
  }
  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.total-sst-files-size",
                                       &total_sst_files_size));
  // Live SST files = 5
  // Total SST files = 5 (used in 2 version)
  ASSERT_EQ(live_sst_files_size, 5 * single_file_size);
  ASSERT_EQ(total_sst_files_size, 5 * single_file_size);

  // hold current version
  std::unique_ptr<Iterator> iter2(dbfull()->NewIterator(ReadOptions()));

  // Delete all keys and compact, this will delete all live files
  for (int i = 0; i < 5; i++) {
    ASSERT_OK(Delete(Key(i)));
  }
  Flush();
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("", FilesPerLevel(0));

  live_files_meta.clear();
  dbfull()->GetLiveFilesMetaData(&live_files_meta);
  ASSERT_EQ(live_files_meta.size(), 0);

  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.total-sst-files-size",
                                       &total_sst_files_size));
  // Live SST files = 0
  // Total SST files = 5 (used in 2 version)
  ASSERT_EQ(total_sst_files_size, 5 * single_file_size);

  iter1.reset();
  iter2.reset();

  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.total-sst-files-size",
                                       &total_sst_files_size));
  // Live SST files = 0
  // Total SST files = 0
  ASSERT_EQ(total_sst_files_size, 0);
}

#endif  // ROCKSDB_LITE

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
