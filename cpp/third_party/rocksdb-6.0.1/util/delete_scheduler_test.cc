//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <atomic>
#include <thread>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "util/delete_scheduler.h"
#include "util/sst_file_manager_impl.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "util/testharness.h"
#include "util/testutil.h"

#ifndef ROCKSDB_LITE

namespace rocksdb {

class DeleteSchedulerTest : public testing::Test {
 public:
  DeleteSchedulerTest() : env_(Env::Default()) {
    const int kNumDataDirs = 3;
    dummy_files_dirs_.reserve(kNumDataDirs);
    for (size_t i = 0; i < kNumDataDirs; ++i) {
      dummy_files_dirs_.emplace_back(
          test::PerThreadDBPath(env_, "delete_scheduler_dummy_data_dir") +
          ToString(i));
      DestroyAndCreateDir(dummy_files_dirs_.back());
    }
  }

  ~DeleteSchedulerTest() override {
    rocksdb::SyncPoint::GetInstance()->DisableProcessing();
    rocksdb::SyncPoint::GetInstance()->LoadDependency({});
    rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
    for (const auto& dummy_files_dir : dummy_files_dirs_) {
      test::DestroyDir(env_, dummy_files_dir);
    }
  }

  void DestroyAndCreateDir(const std::string& dir) {
    ASSERT_OK(test::DestroyDir(env_, dir));
    EXPECT_OK(env_->CreateDir(dir));
  }

  int CountNormalFiles(size_t dummy_files_dirs_idx = 0) {
    std::vector<std::string> files_in_dir;
    EXPECT_OK(env_->GetChildren(dummy_files_dirs_[dummy_files_dirs_idx],
                                &files_in_dir));

    int normal_cnt = 0;
    for (auto& f : files_in_dir) {
      if (!DeleteScheduler::IsTrashFile(f) && f != "." && f != "..") {
        normal_cnt++;
      }
    }
    return normal_cnt;
  }

  int CountTrashFiles(size_t dummy_files_dirs_idx = 0) {
    std::vector<std::string> files_in_dir;
    EXPECT_OK(env_->GetChildren(dummy_files_dirs_[dummy_files_dirs_idx],
                                &files_in_dir));

    int trash_cnt = 0;
    for (auto& f : files_in_dir) {
      if (DeleteScheduler::IsTrashFile(f)) {
        trash_cnt++;
      }
    }
    return trash_cnt;
  }

  std::string NewDummyFile(const std::string& file_name, uint64_t size = 1024,
                           size_t dummy_files_dirs_idx = 0) {
    std::string file_path =
        dummy_files_dirs_[dummy_files_dirs_idx] + "/" + file_name;
    std::unique_ptr<WritableFile> f;
    env_->NewWritableFile(file_path, &f, EnvOptions());
    std::string data(size, 'A');
    EXPECT_OK(f->Append(data));
    EXPECT_OK(f->Close());
    sst_file_mgr_->OnAddFile(file_path, false);
    return file_path;
  }

  void NewDeleteScheduler() {
    // Tests in this file are for DeleteScheduler component and dont create any
    // DBs, so we need to set max_trash_db_ratio to 100% (instead of default
    // 25%)
    sst_file_mgr_.reset(
        new SstFileManagerImpl(env_, nullptr, rate_bytes_per_sec_,
                               /* max_trash_db_ratio= */ 1.1, 128 * 1024));
    delete_scheduler_ = sst_file_mgr_->delete_scheduler();
  }

  Env* env_;
  std::vector<std::string> dummy_files_dirs_;
  int64_t rate_bytes_per_sec_;
  DeleteScheduler* delete_scheduler_;
  std::unique_ptr<SstFileManagerImpl> sst_file_mgr_;
};

// Test the basic functionality of DeleteScheduler (Rate Limiting).
// 1- Create 100 dummy files
// 2- Delete the 100 dummy files using DeleteScheduler
// --- Hold DeleteScheduler::BackgroundEmptyTrash ---
// 3- Wait for DeleteScheduler to delete all files in trash
// 4- Verify that BackgroundEmptyTrash used to correct penlties for the files
// 5- Make sure that all created files were completely deleted
TEST_F(DeleteSchedulerTest, BasicRateLimiting) {
  rocksdb::SyncPoint::GetInstance()->LoadDependency({
      {"DeleteSchedulerTest::BasicRateLimiting:1",
       "DeleteScheduler::BackgroundEmptyTrash"},
  });

  std::vector<uint64_t> penalties;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::BackgroundEmptyTrash:Wait",
      [&](void* arg) { penalties.push_back(*(static_cast<uint64_t*>(arg))); });
  int dir_synced = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile::AfterSyncDir", [&](void* arg) {
        dir_synced++;
        std::string* dir = reinterpret_cast<std::string*>(arg);
        EXPECT_EQ(dummy_files_dirs_[0], *dir);
      });

  int num_files = 100;  // 100 files
  uint64_t file_size = 1024;  // every file is 1 kb
  std::vector<uint64_t> delete_kbs_per_sec = {512, 200, 100, 50, 25};

  for (size_t t = 0; t < delete_kbs_per_sec.size(); t++) {
    penalties.clear();
    rocksdb::SyncPoint::GetInstance()->ClearTrace();
    rocksdb::SyncPoint::GetInstance()->EnableProcessing();

    DestroyAndCreateDir(dummy_files_dirs_[0]);
    rate_bytes_per_sec_ = delete_kbs_per_sec[t] * 1024;
    NewDeleteScheduler();

    dir_synced = 0;
    // Create 100 dummy files, every file is 1 Kb
    std::vector<std::string> generated_files;
    for (int i = 0; i < num_files; i++) {
      std::string file_name = "file" + ToString(i) + ".data";
      generated_files.push_back(NewDummyFile(file_name, file_size));
    }

    // Delete dummy files and measure time spent to empty trash
    for (int i = 0; i < num_files; i++) {
      ASSERT_OK(delete_scheduler_->DeleteFile(generated_files[i],
                                              dummy_files_dirs_[0]));
    }
    ASSERT_EQ(CountNormalFiles(), 0);

    uint64_t delete_start_time = env_->NowMicros();
    TEST_SYNC_POINT("DeleteSchedulerTest::BasicRateLimiting:1");
    delete_scheduler_->WaitForEmptyTrash();
    uint64_t time_spent_deleting = env_->NowMicros() - delete_start_time;

    auto bg_errors = delete_scheduler_->GetBackgroundErrors();
    ASSERT_EQ(bg_errors.size(), 0);

    uint64_t total_files_size = 0;
    uint64_t expected_penlty = 0;
    ASSERT_EQ(penalties.size(), num_files);
    for (int i = 0; i < num_files; i++) {
      total_files_size += file_size;
      expected_penlty = ((total_files_size * 1000000) / rate_bytes_per_sec_);
      ASSERT_EQ(expected_penlty, penalties[i]);
    }
    ASSERT_GT(time_spent_deleting, expected_penlty * 0.9);

    ASSERT_EQ(num_files, dir_synced);

    ASSERT_EQ(CountTrashFiles(), 0);
    rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  }
}

TEST_F(DeleteSchedulerTest, MultiDirectoryDeletionsScheduled) {
  rocksdb::SyncPoint::GetInstance()->LoadDependency({
      {"DeleteSchedulerTest::MultiDbPathDeletionsScheduled:1",
       "DeleteScheduler::BackgroundEmptyTrash"},
  });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  rate_bytes_per_sec_ = 1 << 20;  // 1MB
  NewDeleteScheduler();

  // Generate dummy files in multiple directories
  const size_t kNumFiles = dummy_files_dirs_.size();
  const size_t kFileSize = 1 << 10;  // 1KB
  std::vector<std::string> generated_files;
  for (size_t i = 0; i < kNumFiles; i++) {
    generated_files.push_back(NewDummyFile("file", kFileSize, i));
    ASSERT_EQ(1, CountNormalFiles(i));
  }

  // Mark dummy files as trash
  for (size_t i = 0; i < kNumFiles; i++) {
    ASSERT_OK(delete_scheduler_->DeleteFile(generated_files[i], ""));
    ASSERT_EQ(0, CountNormalFiles(i));
    ASSERT_EQ(1, CountTrashFiles(i));
  }
  TEST_SYNC_POINT("DeleteSchedulerTest::MultiDbPathDeletionsScheduled:1");
  delete_scheduler_->WaitForEmptyTrash();

  // Verify dummy files eventually got deleted
  for (size_t i = 0; i < kNumFiles; i++) {
    ASSERT_EQ(0, CountNormalFiles(i));
    ASSERT_EQ(0, CountTrashFiles(i));
  }

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

// Same as the BasicRateLimiting test but delete files in multiple threads.
// 1- Create 100 dummy files
// 2- Delete the 100 dummy files using DeleteScheduler using 10 threads
// --- Hold DeleteScheduler::BackgroundEmptyTrash ---
// 3- Wait for DeleteScheduler to delete all files in queue
// 4- Verify that BackgroundEmptyTrash used to correct penlties for the files
// 5- Make sure that all created files were completely deleted
TEST_F(DeleteSchedulerTest, RateLimitingMultiThreaded) {
  rocksdb::SyncPoint::GetInstance()->LoadDependency({
      {"DeleteSchedulerTest::RateLimitingMultiThreaded:1",
       "DeleteScheduler::BackgroundEmptyTrash"},
  });

  std::vector<uint64_t> penalties;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::BackgroundEmptyTrash:Wait",
      [&](void* arg) { penalties.push_back(*(static_cast<uint64_t*>(arg))); });

  int thread_cnt = 10;
  int num_files = 10;  // 10 files per thread
  uint64_t file_size = 1024;  // every file is 1 kb

  std::vector<uint64_t> delete_kbs_per_sec = {512, 200, 100, 50, 25};
  for (size_t t = 0; t < delete_kbs_per_sec.size(); t++) {
    penalties.clear();
    rocksdb::SyncPoint::GetInstance()->ClearTrace();
    rocksdb::SyncPoint::GetInstance()->EnableProcessing();

    DestroyAndCreateDir(dummy_files_dirs_[0]);
    rate_bytes_per_sec_ = delete_kbs_per_sec[t] * 1024;
    NewDeleteScheduler();

    // Create 100 dummy files, every file is 1 Kb
    std::vector<std::string> generated_files;
    for (int i = 0; i < num_files * thread_cnt; i++) {
      std::string file_name = "file" + ToString(i) + ".data";
      generated_files.push_back(NewDummyFile(file_name, file_size));
    }

    // Delete dummy files using 10 threads and measure time spent to empty trash
    std::atomic<int> thread_num(0);
    std::vector<port::Thread> threads;
    std::function<void()> delete_thread = [&]() {
      int idx = thread_num.fetch_add(1);
      int range_start = idx * num_files;
      int range_end = range_start + num_files;
      for (int j = range_start; j < range_end; j++) {
        ASSERT_OK(delete_scheduler_->DeleteFile(generated_files[j], ""));
      }
    };

    for (int i = 0; i < thread_cnt; i++) {
      threads.emplace_back(delete_thread);
    }

    for (size_t i = 0; i < threads.size(); i++) {
      threads[i].join();
    }

    uint64_t delete_start_time = env_->NowMicros();
    TEST_SYNC_POINT("DeleteSchedulerTest::RateLimitingMultiThreaded:1");
    delete_scheduler_->WaitForEmptyTrash();
    uint64_t time_spent_deleting = env_->NowMicros() - delete_start_time;

    auto bg_errors = delete_scheduler_->GetBackgroundErrors();
    ASSERT_EQ(bg_errors.size(), 0);

    uint64_t total_files_size = 0;
    uint64_t expected_penlty = 0;
    ASSERT_EQ(penalties.size(), num_files * thread_cnt);
    for (int i = 0; i < num_files * thread_cnt; i++) {
      total_files_size += file_size;
      expected_penlty = ((total_files_size * 1000000) / rate_bytes_per_sec_);
      ASSERT_EQ(expected_penlty, penalties[i]);
    }
    ASSERT_GT(time_spent_deleting, expected_penlty * 0.9);

    ASSERT_EQ(CountNormalFiles(), 0);
    ASSERT_EQ(CountTrashFiles(), 0);
    rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  }
}

// Disable rate limiting by setting rate_bytes_per_sec_ to 0 and make sure
// that when DeleteScheduler delete a file it delete it immediately and dont
// move it to trash
TEST_F(DeleteSchedulerTest, DisableRateLimiting) {
  int bg_delete_file = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:DeleteFile",
      [&](void* /*arg*/) { bg_delete_file++; });

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  rate_bytes_per_sec_ = 0;
  NewDeleteScheduler();

  for (int i = 0; i < 10; i++) {
    // Every file we delete will be deleted immediately
    std::string dummy_file = NewDummyFile("dummy.data");
    ASSERT_OK(delete_scheduler_->DeleteFile(dummy_file, ""));
    ASSERT_TRUE(env_->FileExists(dummy_file).IsNotFound());
    ASSERT_EQ(CountNormalFiles(), 0);
    ASSERT_EQ(CountTrashFiles(), 0);
  }

  ASSERT_EQ(bg_delete_file, 0);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

// Testing that moving files to trash with the same name is not a problem
// 1- Create 10 files with the same name "conflict.data"
// 2- Delete the 10 files using DeleteScheduler
// 3- Make sure that trash directory contain 10 files ("conflict.data" x 10)
// --- Hold DeleteScheduler::BackgroundEmptyTrash ---
// 4- Make sure that files are deleted from trash
TEST_F(DeleteSchedulerTest, ConflictNames) {
  rocksdb::SyncPoint::GetInstance()->LoadDependency({
      {"DeleteSchedulerTest::ConflictNames:1",
       "DeleteScheduler::BackgroundEmptyTrash"},
  });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  rate_bytes_per_sec_ = 1024 * 1024;  // 1 Mb/sec
  NewDeleteScheduler();

  // Create "conflict.data" and move it to trash 10 times
  for (int i = 0; i < 10; i++) {
    std::string dummy_file = NewDummyFile("conflict.data");
    ASSERT_OK(delete_scheduler_->DeleteFile(dummy_file, ""));
  }
  ASSERT_EQ(CountNormalFiles(), 0);
  // 10 files ("conflict.data" x 10) in trash
  ASSERT_EQ(CountTrashFiles(), 10);

  // Hold BackgroundEmptyTrash
  TEST_SYNC_POINT("DeleteSchedulerTest::ConflictNames:1");
  delete_scheduler_->WaitForEmptyTrash();
  ASSERT_EQ(CountTrashFiles(), 0);

  auto bg_errors = delete_scheduler_->GetBackgroundErrors();
  ASSERT_EQ(bg_errors.size(), 0);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

// 1- Create 10 dummy files
// 2- Delete the 10 files using DeleteScheduler (move them to trsah)
// 3- Delete the 10 files directly (using env_->DeleteFile)
// --- Hold DeleteScheduler::BackgroundEmptyTrash ---
// 4- Make sure that DeleteScheduler failed to delete the 10 files and
//    reported 10 background errors
TEST_F(DeleteSchedulerTest, BackgroundError) {
  rocksdb::SyncPoint::GetInstance()->LoadDependency({
      {"DeleteSchedulerTest::BackgroundError:1",
       "DeleteScheduler::BackgroundEmptyTrash"},
  });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  rate_bytes_per_sec_ = 1024 * 1024;  // 1 Mb/sec
  NewDeleteScheduler();

  // Generate 10 dummy files and move them to trash
  for (int i = 0; i < 10; i++) {
    std::string file_name = "data_" + ToString(i) + ".data";
    ASSERT_OK(delete_scheduler_->DeleteFile(NewDummyFile(file_name), ""));
  }
  ASSERT_EQ(CountNormalFiles(), 0);
  ASSERT_EQ(CountTrashFiles(), 10);

  // Delete 10 files from trash, this will cause background errors in
  // BackgroundEmptyTrash since we already deleted the files it was
  // goind to delete
  for (int i = 0; i < 10; i++) {
    std::string file_name = "data_" + ToString(i) + ".data.trash";
    ASSERT_OK(env_->DeleteFile(dummy_files_dirs_[0] + "/" + file_name));
  }

  // Hold BackgroundEmptyTrash
  TEST_SYNC_POINT("DeleteSchedulerTest::BackgroundError:1");
  delete_scheduler_->WaitForEmptyTrash();
  auto bg_errors = delete_scheduler_->GetBackgroundErrors();
  ASSERT_EQ(bg_errors.size(), 10);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

// 1- Create 10 dummy files
// 2- Delete 10 dummy files using DeleteScheduler
// 3- Wait for DeleteScheduler to delete all files in queue
// 4- Make sure all files in trash directory were deleted
// 5- Repeat previous steps 5 times
TEST_F(DeleteSchedulerTest, StartBGEmptyTrashMultipleTimes) {
  int bg_delete_file = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:DeleteFile",
      [&](void* /*arg*/) { bg_delete_file++; });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  rate_bytes_per_sec_ = 1024 * 1024;  // 1 MB / sec
  NewDeleteScheduler();

  // Move files to trash, wait for empty trash, start again
  for (int run = 1; run <= 5; run++) {
    // Generate 10 dummy files and move them to trash
    for (int i = 0; i < 10; i++) {
      std::string file_name = "data_" + ToString(i) + ".data";
      ASSERT_OK(delete_scheduler_->DeleteFile(NewDummyFile(file_name), ""));
    }
    ASSERT_EQ(CountNormalFiles(), 0);
    delete_scheduler_->WaitForEmptyTrash();
    ASSERT_EQ(bg_delete_file, 10 * run);
    ASSERT_EQ(CountTrashFiles(), 0);

    auto bg_errors = delete_scheduler_->GetBackgroundErrors();
    ASSERT_EQ(bg_errors.size(), 0);
  }

  ASSERT_EQ(bg_delete_file, 50);
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
}

TEST_F(DeleteSchedulerTest, DeletePartialFile) {
  int bg_delete_file = 0;
  int bg_fsync = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:DeleteFile",
      [&](void*) { bg_delete_file++; });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:Fsync", [&](void*) { bg_fsync++; });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  rate_bytes_per_sec_ = 1024 * 1024;  // 1 MB / sec
  NewDeleteScheduler();

  // Should delete in 4 batch
  ASSERT_OK(
      delete_scheduler_->DeleteFile(NewDummyFile("data_1", 500 * 1024), ""));
  ASSERT_OK(
      delete_scheduler_->DeleteFile(NewDummyFile("data_2", 100 * 1024), ""));
  // Should delete in 2 batch
  ASSERT_OK(
      delete_scheduler_->DeleteFile(NewDummyFile("data_2", 200 * 1024), ""));

  delete_scheduler_->WaitForEmptyTrash();

  auto bg_errors = delete_scheduler_->GetBackgroundErrors();
  ASSERT_EQ(bg_errors.size(), 0);
  ASSERT_EQ(7, bg_delete_file);
  ASSERT_EQ(4, bg_fsync);
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
}

#ifdef OS_LINUX
TEST_F(DeleteSchedulerTest, NoPartialDeleteWithLink) {
  int bg_delete_file = 0;
  int bg_fsync = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:DeleteFile",
      [&](void*) { bg_delete_file++; });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:Fsync", [&](void*) { bg_fsync++; });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  rate_bytes_per_sec_ = 1024 * 1024;  // 1 MB / sec
  NewDeleteScheduler();

  std::string file1 = NewDummyFile("data_1", 500 * 1024);
  std::string file2 = NewDummyFile("data_2", 100 * 1024);

  ASSERT_OK(env_->LinkFile(file1, dummy_files_dirs_[0] + "/data_1b"));
  ASSERT_OK(env_->LinkFile(file2, dummy_files_dirs_[0] + "/data_2b"));

  // Should delete in 4 batch if there is no hardlink
  ASSERT_OK(delete_scheduler_->DeleteFile(file1, ""));
  ASSERT_OK(delete_scheduler_->DeleteFile(file2, ""));

  delete_scheduler_->WaitForEmptyTrash();

  auto bg_errors = delete_scheduler_->GetBackgroundErrors();
  ASSERT_EQ(bg_errors.size(), 0);
  ASSERT_EQ(2, bg_delete_file);
  ASSERT_EQ(0, bg_fsync);
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
}
#endif

// 1- Create a DeleteScheduler with very slow rate limit (1 Byte / sec)
// 2- Delete 100 files using DeleteScheduler
// 3- Delete the DeleteScheduler (call the destructor while queue is not empty)
// 4- Make sure that not all files were deleted from trash and that
//    DeleteScheduler background thread did not delete all files
TEST_F(DeleteSchedulerTest, DestructorWithNonEmptyQueue) {
  int bg_delete_file = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:DeleteFile",
      [&](void* /*arg*/) { bg_delete_file++; });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  rate_bytes_per_sec_ = 1;  // 1 Byte / sec
  NewDeleteScheduler();

  for (int i = 0; i < 100; i++) {
    std::string file_name = "data_" + ToString(i) + ".data";
    ASSERT_OK(delete_scheduler_->DeleteFile(NewDummyFile(file_name), ""));
  }

  // Deleting 100 files will need >28 hours to delete
  // we will delete the DeleteScheduler while delete queue is not empty
  sst_file_mgr_.reset();

  ASSERT_LT(bg_delete_file, 100);
  ASSERT_GT(CountTrashFiles(), 0);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DeleteSchedulerTest, DISABLED_DynamicRateLimiting1) {
  std::vector<uint64_t> penalties;
  int bg_delete_file = 0;
  int fg_delete_file = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:DeleteFile",
      [&](void* /*arg*/) { bg_delete_file++; });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteFile",
      [&](void* /*arg*/) { fg_delete_file++; });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::BackgroundEmptyTrash:Wait",
      [&](void* arg) { penalties.push_back(*(static_cast<int*>(arg))); });

  rocksdb::SyncPoint::GetInstance()->LoadDependency({
      {"DeleteSchedulerTest::DynamicRateLimiting1:1",
       "DeleteScheduler::BackgroundEmptyTrash"},
  });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  rate_bytes_per_sec_ = 0;  // Disable rate limiting initially
  NewDeleteScheduler();


  int num_files = 10;  // 10 files
  uint64_t file_size = 1024;  // every file is 1 kb

  std::vector<int64_t> delete_kbs_per_sec = {512, 200, 0, 100, 50, -2, 25};
  for (size_t t = 0; t < delete_kbs_per_sec.size(); t++) {
    penalties.clear();
    bg_delete_file = 0;
    fg_delete_file = 0;
    rocksdb::SyncPoint::GetInstance()->ClearTrace();
    rocksdb::SyncPoint::GetInstance()->EnableProcessing();

    DestroyAndCreateDir(dummy_files_dirs_[0]);
    rate_bytes_per_sec_ = delete_kbs_per_sec[t] * 1024;
    delete_scheduler_->SetRateBytesPerSecond(rate_bytes_per_sec_);

    // Create 100 dummy files, every file is 1 Kb
    std::vector<std::string> generated_files;
    for (int i = 0; i < num_files; i++) {
      std::string file_name = "file" + ToString(i) + ".data";
      generated_files.push_back(NewDummyFile(file_name, file_size));
    }

    // Delete dummy files and measure time spent to empty trash
    for (int i = 0; i < num_files; i++) {
      ASSERT_OK(delete_scheduler_->DeleteFile(generated_files[i], ""));
    }
    ASSERT_EQ(CountNormalFiles(), 0);

    if (rate_bytes_per_sec_ > 0) {
      uint64_t delete_start_time = env_->NowMicros();
      TEST_SYNC_POINT("DeleteSchedulerTest::DynamicRateLimiting1:1");
      delete_scheduler_->WaitForEmptyTrash();
      uint64_t time_spent_deleting = env_->NowMicros() - delete_start_time;

      auto bg_errors = delete_scheduler_->GetBackgroundErrors();
      ASSERT_EQ(bg_errors.size(), 0);

      uint64_t total_files_size = 0;
      uint64_t expected_penlty = 0;
      ASSERT_EQ(penalties.size(), num_files);
      for (int i = 0; i < num_files; i++) {
        total_files_size += file_size;
        expected_penlty = ((total_files_size * 1000000) / rate_bytes_per_sec_);
        ASSERT_EQ(expected_penlty, penalties[i]);
      }
      ASSERT_GT(time_spent_deleting, expected_penlty * 0.9);
      ASSERT_EQ(bg_delete_file, num_files);
      ASSERT_EQ(fg_delete_file, 0);
    } else {
      ASSERT_EQ(penalties.size(), 0);
      ASSERT_EQ(bg_delete_file, 0);
      ASSERT_EQ(fg_delete_file, num_files);
    }

    ASSERT_EQ(CountTrashFiles(), 0);
    rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  }
}

TEST_F(DeleteSchedulerTest, ImmediateDeleteOn25PercDBSize) {
  int bg_delete_file = 0;
  int fg_delete_file = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:DeleteFile",
      [&](void* /*arg*/) { bg_delete_file++; });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteFile", [&](void* /*arg*/) { fg_delete_file++; });

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  int num_files = 100;  // 100 files
  uint64_t file_size = 1024 * 10; // 100 KB as a file size
  rate_bytes_per_sec_ = 1;  // 1 byte per sec (very slow trash delete)

  NewDeleteScheduler();
  delete_scheduler_->SetMaxTrashDBRatio(0.25);

  std::vector<std::string> generated_files;
  for (int i = 0; i < num_files; i++) {
    std::string file_name = "file" + ToString(i) + ".data";
    generated_files.push_back(NewDummyFile(file_name, file_size));
  }

  for (std::string& file_name : generated_files) {
    delete_scheduler_->DeleteFile(file_name, "");
  }

  // When we end up with 26 files in trash we will start
  // deleting new files immediately
  ASSERT_EQ(fg_delete_file, 74);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DeleteSchedulerTest, IsTrashCheck) {
  // Trash files
  ASSERT_TRUE(DeleteScheduler::IsTrashFile("x.trash"));
  ASSERT_TRUE(DeleteScheduler::IsTrashFile(".trash"));
  ASSERT_TRUE(DeleteScheduler::IsTrashFile("abc.sst.trash"));
  ASSERT_TRUE(DeleteScheduler::IsTrashFile("/a/b/c/abc..sst.trash"));
  ASSERT_TRUE(DeleteScheduler::IsTrashFile("log.trash"));
  ASSERT_TRUE(DeleteScheduler::IsTrashFile("^^^^^.log.trash"));
  ASSERT_TRUE(DeleteScheduler::IsTrashFile("abc.t.trash"));

  // Not trash files
  ASSERT_FALSE(DeleteScheduler::IsTrashFile("abc.sst"));
  ASSERT_FALSE(DeleteScheduler::IsTrashFile("abc.txt"));
  ASSERT_FALSE(DeleteScheduler::IsTrashFile("/a/b/c/abc.sst"));
  ASSERT_FALSE(DeleteScheduler::IsTrashFile("/a/b/c/abc.sstrash"));
  ASSERT_FALSE(DeleteScheduler::IsTrashFile("^^^^^.trashh"));
  ASSERT_FALSE(DeleteScheduler::IsTrashFile("abc.ttrash"));
  ASSERT_FALSE(DeleteScheduler::IsTrashFile(".ttrash"));
  ASSERT_FALSE(DeleteScheduler::IsTrashFile("abc.trashx"));
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
int main(int /*argc*/, char** /*argv*/) {
  printf("DeleteScheduler is not supported in ROCKSDB_LITE\n");
  return 0;
}
#endif  // ROCKSDB_LITE
