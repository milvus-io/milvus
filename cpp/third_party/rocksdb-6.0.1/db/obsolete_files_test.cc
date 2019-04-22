//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE

#include <stdlib.h>
#include <map>
#include <string>
#include <vector>
#include "db/db_impl.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/transaction_log.h"
#include "util/filename.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "util/testharness.h"
#include "util/testutil.h"

using std::cerr;
using std::cout;
using std::endl;
using std::flush;

namespace rocksdb {

class ObsoleteFilesTest : public testing::Test {
 public:
  std::string dbname_;
  Options options_;
  DB* db_;
  Env* env_;
  int numlevels_;

  ObsoleteFilesTest() {
    db_ = nullptr;
    env_ = Env::Default();
    // Trigger compaction when the number of level 0 files reaches 2.
    options_.level0_file_num_compaction_trigger = 2;
    options_.disable_auto_compactions = false;
    options_.delete_obsolete_files_period_micros = 0;  // always do full purge
    options_.enable_thread_tracking = true;
    options_.write_buffer_size = 1024*1024*1000;
    options_.target_file_size_base = 1024*1024*1000;
    options_.max_bytes_for_level_base = 1024*1024*1000;
    options_.WAL_ttl_seconds = 300; // Used to test log files
    options_.WAL_size_limit_MB = 1024; // Used to test log files
    dbname_ = test::PerThreadDBPath("obsolete_files_test");
    options_.wal_dir = dbname_ + "/wal_files";

    // clean up all the files that might have been there before
    std::vector<std::string> old_files;
    env_->GetChildren(dbname_, &old_files);
    for (auto file : old_files) {
      env_->DeleteFile(dbname_ + "/" + file);
    }
    env_->GetChildren(options_.wal_dir, &old_files);
    for (auto file : old_files) {
      env_->DeleteFile(options_.wal_dir + "/" + file);
    }

    DestroyDB(dbname_, options_);
    numlevels_ = 7;
    EXPECT_OK(ReopenDB(true));
  }

  Status ReopenDB(bool create) {
    delete db_;
    if (create) {
      DestroyDB(dbname_, options_);
    }
    db_ = nullptr;
    options_.create_if_missing = create;
    return DB::Open(options_, dbname_, &db_);
  }

  void CloseDB() {
    delete db_;
    db_ = nullptr;
  }

  void AddKeys(int numkeys, int startkey) {
    WriteOptions options;
    options.sync = false;
    for (int i = startkey; i < (numkeys + startkey) ; i++) {
      std::string temp = ToString(i);
      Slice key(temp);
      Slice value(temp);
      ASSERT_OK(db_->Put(options, key, value));
    }
  }

  int numKeysInLevels(
    std::vector<LiveFileMetaData> &metadata,
    std::vector<int> *keysperlevel = nullptr) {

    if (keysperlevel != nullptr) {
      keysperlevel->resize(numlevels_);
    }

    int numKeys = 0;
    for (size_t i = 0; i < metadata.size(); i++) {
      int startkey = atoi(metadata[i].smallestkey.c_str());
      int endkey = atoi(metadata[i].largestkey.c_str());
      int numkeysinfile = (endkey - startkey + 1);
      numKeys += numkeysinfile;
      if (keysperlevel != nullptr) {
        (*keysperlevel)[(int)metadata[i].level] += numkeysinfile;
      }
      fprintf(stderr, "level %d name %s smallest %s largest %s\n",
              metadata[i].level, metadata[i].name.c_str(),
              metadata[i].smallestkey.c_str(),
              metadata[i].largestkey.c_str());
    }
    return numKeys;
  }

  void createLevel0Files(int numFiles, int numKeysPerFile) {
    int startKey = 0;
    DBImpl* dbi = reinterpret_cast<DBImpl*>(db_);
    for (int i = 0; i < numFiles; i++) {
      AddKeys(numKeysPerFile, startKey);
      startKey += numKeysPerFile;
      ASSERT_OK(dbi->TEST_FlushMemTable());
      ASSERT_OK(dbi->TEST_WaitForFlushMemTable());
    }
  }

  void CheckFileTypeCounts(std::string& dir,
                            int required_log,
                            int required_sst,
                            int required_manifest) {
    std::vector<std::string> filenames;
    env_->GetChildren(dir, &filenames);

    int log_cnt = 0, sst_cnt = 0, manifest_cnt = 0;
    for (auto file : filenames) {
      uint64_t number;
      FileType type;
      if (ParseFileName(file, &number, &type)) {
        log_cnt += (type == kLogFile);
        sst_cnt += (type == kTableFile);
        manifest_cnt += (type == kDescriptorFile);
      }
    }
    ASSERT_EQ(required_log, log_cnt);
    ASSERT_EQ(required_sst, sst_cnt);
    ASSERT_EQ(required_manifest, manifest_cnt);
  }
};

TEST_F(ObsoleteFilesTest, RaceForObsoleteFileDeletion) {
  createLevel0Files(2, 50000);
  CheckFileTypeCounts(options_.wal_dir, 1, 0, 0);

  SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::BackgroundCallCompaction:FoundObsoleteFiles",
       "ObsoleteFilesTest::RaceForObsoleteFileDeletion:1"},
      {"DBImpl::BackgroundCallCompaction:PurgedObsoleteFiles",
       "ObsoleteFilesTest::RaceForObsoleteFileDeletion:2"},
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::DeleteObsoleteFileImpl:AfterDeletion", [&](void* arg) {
        Status* p_status = reinterpret_cast<Status*>(arg);
        ASSERT_TRUE(p_status->ok());
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::CloseHelper:PendingPurgeFinished", [&](void* arg) {
        std::vector<uint64_t>* files_grabbed_for_purge_ptr =
            reinterpret_cast<std::vector<uint64_t>*>(arg);
        ASSERT_TRUE(files_grabbed_for_purge_ptr->empty());
      });
  SyncPoint::GetInstance()->EnableProcessing();

  DBImpl* dbi = reinterpret_cast<DBImpl*>(db_);
  port::Thread user_thread([&]() {
    JobContext jobCxt(0);
    TEST_SYNC_POINT("ObsoleteFilesTest::RaceForObsoleteFileDeletion:1");
    dbi->TEST_LockMutex();
    dbi->FindObsoleteFiles(&jobCxt,
      true /* force=true */, false /* no_full_scan=false */);
    dbi->TEST_UnlockMutex();
    TEST_SYNC_POINT("ObsoleteFilesTest::RaceForObsoleteFileDeletion:2");
    dbi->PurgeObsoleteFiles(jobCxt);
    jobCxt.Clean();
  });

  user_thread.join();

  CloseDB();
}

TEST_F(ObsoleteFilesTest, DeleteObsoleteOptionsFile) {
  createLevel0Files(2, 50000);
  CheckFileTypeCounts(options_.wal_dir, 1, 0, 0);

  std::vector<uint64_t> optsfiles_nums;
  std::vector<bool> optsfiles_keep;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PurgeObsoleteFiles:CheckOptionsFiles:1", [&](void* arg) {
        optsfiles_nums.push_back(*reinterpret_cast<uint64_t*>(arg));
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PurgeObsoleteFiles:CheckOptionsFiles:2", [&](void* arg) {
        optsfiles_keep.push_back(*reinterpret_cast<bool*>(arg));
      });
  SyncPoint::GetInstance()->EnableProcessing();

  DBImpl* dbi = static_cast<DBImpl*>(db_);
  ASSERT_OK(dbi->DisableFileDeletions());
  for (int i = 0; i != 4; ++i) {
    if (i % 2) {
      ASSERT_OK(dbi->SetOptions(dbi->DefaultColumnFamily(),
                                {{"paranoid_file_checks", "false"}}));
    } else {
      ASSERT_OK(dbi->SetOptions(dbi->DefaultColumnFamily(),
                                {{"paranoid_file_checks", "true"}}));
    }
  }
  ASSERT_OK(dbi->EnableFileDeletions(true /* force */));
  ASSERT_EQ(optsfiles_nums.size(), optsfiles_keep.size());

  CloseDB();

  std::vector<std::string> files;
  int opts_file_count = 0;
  ASSERT_OK(env_->GetChildren(dbname_, &files));
  for (const auto& file : files) {
    uint64_t file_num;
    Slice dummy_info_log_name_prefix;
    FileType type;
    WalFileType log_type;
    if (ParseFileName(file, &file_num, dummy_info_log_name_prefix, &type,
                      &log_type) &&
        type == kOptionsFile) {
      opts_file_count++;
    }
  }
  ASSERT_EQ(2, opts_file_count);
}

} //namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr,
          "SKIPPED as DBImpl::DeleteFile is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE
