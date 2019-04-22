//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// Syncpoint prevents us building and running tests in release
#ifndef ROCKSDB_LITE

#ifndef OS_WIN
#include <unistd.h>
#endif
#include <iostream>
#include <thread>
#include <utility>
#include "db/db_impl.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/utilities/transaction_db.h"
#include "util/fault_injection_test_env.h"
#include "util/sync_point.h"
#include "util/testharness.h"

namespace rocksdb {
class CheckpointTest : public testing::Test {
 protected:
  // Sequence of option configurations to try
  enum OptionConfig {
    kDefault = 0,
  };
  int option_config_;

 public:
  std::string dbname_;
  std::string alternative_wal_dir_;
  Env* env_;
  DB* db_;
  Options last_options_;
  std::vector<ColumnFamilyHandle*> handles_;
  std::string snapshot_name_;

  CheckpointTest() : env_(Env::Default()) {
    env_->SetBackgroundThreads(1, Env::LOW);
    env_->SetBackgroundThreads(1, Env::HIGH);
    dbname_ = test::PerThreadDBPath(env_, "checkpoint_test");
    alternative_wal_dir_ = dbname_ + "/wal";
    auto options = CurrentOptions();
    auto delete_options = options;
    delete_options.wal_dir = alternative_wal_dir_;
    EXPECT_OK(DestroyDB(dbname_, delete_options));
    // Destroy it for not alternative WAL dir is used.
    EXPECT_OK(DestroyDB(dbname_, options));
    db_ = nullptr;
    snapshot_name_ = test::PerThreadDBPath(env_, "snapshot");
    std::string snapshot_tmp_name = snapshot_name_ + ".tmp";
    EXPECT_OK(DestroyDB(snapshot_name_, options));
    env_->DeleteDir(snapshot_name_);
    EXPECT_OK(DestroyDB(snapshot_tmp_name, options));
    env_->DeleteDir(snapshot_tmp_name);
    Reopen(options);
  }

  ~CheckpointTest() override {
    rocksdb::SyncPoint::GetInstance()->DisableProcessing();
    rocksdb::SyncPoint::GetInstance()->LoadDependency({});
    rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
    Close();
    Options options;
    options.db_paths.emplace_back(dbname_, 0);
    options.db_paths.emplace_back(dbname_ + "_2", 0);
    options.db_paths.emplace_back(dbname_ + "_3", 0);
    options.db_paths.emplace_back(dbname_ + "_4", 0);
    EXPECT_OK(DestroyDB(dbname_, options));
    EXPECT_OK(DestroyDB(snapshot_name_, options));
  }

  // Return the current option configuration.
  Options CurrentOptions() {
    Options options;
    options.env = env_;
    options.create_if_missing = true;
    return options;
  }

  void CreateColumnFamilies(const std::vector<std::string>& cfs,
                            const Options& options) {
    ColumnFamilyOptions cf_opts(options);
    size_t cfi = handles_.size();
    handles_.resize(cfi + cfs.size());
    for (auto cf : cfs) {
      ASSERT_OK(db_->CreateColumnFamily(cf_opts, cf, &handles_[cfi++]));
    }
  }

  void CreateAndReopenWithCF(const std::vector<std::string>& cfs,
                             const Options& options) {
    CreateColumnFamilies(cfs, options);
    std::vector<std::string> cfs_plus_default = cfs;
    cfs_plus_default.insert(cfs_plus_default.begin(), kDefaultColumnFamilyName);
    ReopenWithColumnFamilies(cfs_plus_default, options);
  }

  void ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                const std::vector<Options>& options) {
    ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
  }

  void ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                const Options& options) {
    ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
  }

  Status TryReopenWithColumnFamilies(
      const std::vector<std::string>& cfs,
      const std::vector<Options>& options) {
    Close();
    EXPECT_EQ(cfs.size(), options.size());
    std::vector<ColumnFamilyDescriptor> column_families;
    for (size_t i = 0; i < cfs.size(); ++i) {
      column_families.push_back(ColumnFamilyDescriptor(cfs[i], options[i]));
    }
    DBOptions db_opts = DBOptions(options[0]);
    return DB::Open(db_opts, dbname_, column_families, &handles_, &db_);
  }

  Status TryReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                     const Options& options) {
    Close();
    std::vector<Options> v_opts(cfs.size(), options);
    return TryReopenWithColumnFamilies(cfs, v_opts);
  }

  void Reopen(const Options& options) {
    ASSERT_OK(TryReopen(options));
  }

  void Close() {
    for (auto h : handles_) {
      delete h;
    }
    handles_.clear();
    delete db_;
    db_ = nullptr;
  }

  void DestroyAndReopen(const Options& options) {
    // Destroy using last options
    Destroy(last_options_);
    ASSERT_OK(TryReopen(options));
  }

  void Destroy(const Options& options) {
    Close();
    ASSERT_OK(DestroyDB(dbname_, options));
  }

  Status ReadOnlyReopen(const Options& options) {
    return DB::OpenForReadOnly(options, dbname_, &db_);
  }

  Status ReadOnlyReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                          const Options& options) {
    std::vector<ColumnFamilyDescriptor> column_families;
    for (const auto& cf : cfs) {
      column_families.emplace_back(cf, options);
    }
    return DB::OpenForReadOnly(options, dbname_, column_families, &handles_,
                               &db_);
  }

  Status TryReopen(const Options& options) {
    Close();
    last_options_ = options;
    return DB::Open(options, dbname_, &db_);
  }

  Status Flush(int cf = 0) {
    if (cf == 0) {
      return db_->Flush(FlushOptions());
    } else {
      return db_->Flush(FlushOptions(), handles_[cf]);
    }
  }

  Status Put(const Slice& k, const Slice& v, WriteOptions wo = WriteOptions()) {
    return db_->Put(wo, k, v);
  }

  Status Put(int cf, const Slice& k, const Slice& v,
             WriteOptions wo = WriteOptions()) {
    return db_->Put(wo, handles_[cf], k, v);
  }

  Status Delete(const std::string& k) {
    return db_->Delete(WriteOptions(), k);
  }

  Status Delete(int cf, const std::string& k) {
    return db_->Delete(WriteOptions(), handles_[cf], k);
  }

  std::string Get(const std::string& k, const Snapshot* snapshot = nullptr) {
    ReadOptions options;
    options.verify_checksums = true;
    options.snapshot = snapshot;
    std::string result;
    Status s = db_->Get(options, k, &result);
    if (s.IsNotFound()) {
      result = "NOT_FOUND";
    } else if (!s.ok()) {
      result = s.ToString();
    }
    return result;
  }

  std::string Get(int cf, const std::string& k,
                  const Snapshot* snapshot = nullptr) {
    ReadOptions options;
    options.verify_checksums = true;
    options.snapshot = snapshot;
    std::string result;
    Status s = db_->Get(options, handles_[cf], k, &result);
    if (s.IsNotFound()) {
      result = "NOT_FOUND";
    } else if (!s.ok()) {
      result = s.ToString();
    }
    return result;
  }
};

TEST_F(CheckpointTest, GetSnapshotLink) {
  for (uint64_t log_size_for_flush : {0, 1000000}) {
    Options options;
    DB* snapshotDB;
    ReadOptions roptions;
    std::string result;
    Checkpoint* checkpoint;

    options = CurrentOptions();
    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, options));

    // Create a database
    Status s;
    options.create_if_missing = true;
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    std::string key = std::string("foo");
    ASSERT_OK(Put(key, "v1"));
    // Take a snapshot
    ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
    ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_, log_size_for_flush));
    ASSERT_OK(Put(key, "v2"));
    ASSERT_EQ("v2", Get(key));
    ASSERT_OK(Flush());
    ASSERT_EQ("v2", Get(key));
    // Open snapshot and verify contents while DB is running
    options.create_if_missing = false;
    ASSERT_OK(DB::Open(options, snapshot_name_, &snapshotDB));
    ASSERT_OK(snapshotDB->Get(roptions, key, &result));
    ASSERT_EQ("v1", result);
    delete snapshotDB;
    snapshotDB = nullptr;
    delete db_;
    db_ = nullptr;

    // Destroy original DB
    ASSERT_OK(DestroyDB(dbname_, options));

    // Open snapshot and verify contents
    options.create_if_missing = false;
    dbname_ = snapshot_name_;
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    ASSERT_EQ("v1", Get(key));
    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, options));
    delete checkpoint;

    // Restore DB name
    dbname_ = test::PerThreadDBPath(env_, "db_test");
  }
}

TEST_F(CheckpointTest, CheckpointCF) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"one", "two", "three", "four", "five"}, options);
  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"CheckpointTest::CheckpointCF:2", "DBImpl::GetLiveFiles:2"},
       {"DBImpl::GetLiveFiles:1", "CheckpointTest::CheckpointCF:1"}});

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(0, "Default", "Default"));
  ASSERT_OK(Put(1, "one", "one"));
  ASSERT_OK(Put(2, "two", "two"));
  ASSERT_OK(Put(3, "three", "three"));
  ASSERT_OK(Put(4, "four", "four"));
  ASSERT_OK(Put(5, "five", "five"));

  DB* snapshotDB;
  ReadOptions roptions;
  std::string result;
  std::vector<ColumnFamilyHandle*> cphandles;

  Status s;
  // Take a snapshot
  rocksdb::port::Thread t([&]() {
    Checkpoint* checkpoint;
    ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
    ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));
    delete checkpoint;
  });
  TEST_SYNC_POINT("CheckpointTest::CheckpointCF:1");
  ASSERT_OK(Put(0, "Default", "Default1"));
  ASSERT_OK(Put(1, "one", "eleven"));
  ASSERT_OK(Put(2, "two", "twelve"));
  ASSERT_OK(Put(3, "three", "thirteen"));
  ASSERT_OK(Put(4, "four", "fourteen"));
  ASSERT_OK(Put(5, "five", "fifteen"));
  TEST_SYNC_POINT("CheckpointTest::CheckpointCF:2");
  t.join();
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(Put(1, "one", "twentyone"));
  ASSERT_OK(Put(2, "two", "twentytwo"));
  ASSERT_OK(Put(3, "three", "twentythree"));
  ASSERT_OK(Put(4, "four", "twentyfour"));
  ASSERT_OK(Put(5, "five", "twentyfive"));
  ASSERT_OK(Flush());

  // Open snapshot and verify contents while DB is running
  options.create_if_missing = false;
  std::vector<std::string> cfs;
  cfs=  {kDefaultColumnFamilyName, "one", "two", "three", "four", "five"};
  std::vector<ColumnFamilyDescriptor> column_families;
    for (size_t i = 0; i < cfs.size(); ++i) {
      column_families.push_back(ColumnFamilyDescriptor(cfs[i], options));
    }
  ASSERT_OK(DB::Open(options, snapshot_name_,
        column_families, &cphandles, &snapshotDB));
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[0], "Default", &result));
  ASSERT_EQ("Default1", result);
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[1], "one", &result));
  ASSERT_EQ("eleven", result);
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[2], "two", &result));
  for (auto h : cphandles) {
      delete h;
  }
  cphandles.clear();
  delete snapshotDB;
  snapshotDB = nullptr;
}

TEST_F(CheckpointTest, CheckpointCFNoFlush) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"one", "two", "three", "four", "five"}, options);

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(0, "Default", "Default"));
  ASSERT_OK(Put(1, "one", "one"));
  Flush();
  ASSERT_OK(Put(2, "two", "two"));

  DB* snapshotDB;
  ReadOptions roptions;
  std::string result;
  std::vector<ColumnFamilyHandle*> cphandles;

  Status s;
  // Take a snapshot
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCallFlush:start", [&](void* /*arg*/) {
        // Flush should never trigger.
        FAIL();
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_, 1000000));
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();

  delete checkpoint;
  ASSERT_OK(Put(1, "one", "two"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(2, "two", "twentytwo"));
  Close();
  EXPECT_OK(DestroyDB(dbname_, options));

  // Open snapshot and verify contents while DB is running
  options.create_if_missing = false;
  std::vector<std::string> cfs;
  cfs = {kDefaultColumnFamilyName, "one", "two", "three", "four", "five"};
  std::vector<ColumnFamilyDescriptor> column_families;
  for (size_t i = 0; i < cfs.size(); ++i) {
    column_families.push_back(ColumnFamilyDescriptor(cfs[i], options));
  }
  ASSERT_OK(DB::Open(options, snapshot_name_, column_families, &cphandles,
                     &snapshotDB));
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[0], "Default", &result));
  ASSERT_EQ("Default", result);
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[1], "one", &result));
  ASSERT_EQ("one", result);
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[2], "two", &result));
  ASSERT_EQ("two", result);
  for (auto h : cphandles) {
    delete h;
  }
  cphandles.clear();
  delete snapshotDB;
  snapshotDB = nullptr;
}

TEST_F(CheckpointTest, CurrentFileModifiedWhileCheckpointing) {
  Options options = CurrentOptions();
  options.max_manifest_file_size = 0;  // always rollover manifest for file add
  Reopen(options);

  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {// Get past the flush in the checkpoint thread before adding any keys to
       // the db so the checkpoint thread won't hit the WriteManifest
       // syncpoints.
       {"DBImpl::GetLiveFiles:1",
        "CheckpointTest::CurrentFileModifiedWhileCheckpointing:PrePut"},
       // Roll the manifest during checkpointing right after live files are
       // snapshotted.
       {"CheckpointImpl::CreateCheckpoint:SavedLiveFiles1",
        "VersionSet::LogAndApply:WriteManifest"},
       {"VersionSet::LogAndApply:WriteManifestDone",
        "CheckpointImpl::CreateCheckpoint:SavedLiveFiles2"}});
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  rocksdb::port::Thread t([&]() {
    Checkpoint* checkpoint;
    ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
    ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));
    delete checkpoint;
  });
  TEST_SYNC_POINT(
      "CheckpointTest::CurrentFileModifiedWhileCheckpointing:PrePut");
  ASSERT_OK(Put("Default", "Default1"));
  ASSERT_OK(Flush());
  t.join();

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();

  DB* snapshotDB;
  // Successful Open() implies that CURRENT pointed to the manifest in the
  // checkpoint.
  ASSERT_OK(DB::Open(options, snapshot_name_, &snapshotDB));
  delete snapshotDB;
  snapshotDB = nullptr;
}

TEST_F(CheckpointTest, CurrentFileModifiedWhileCheckpointing2PC) {
  Close();
  const std::string dbname = test::PerThreadDBPath("transaction_testdb");
  ASSERT_OK(DestroyDB(dbname, CurrentOptions()));
  env_->DeleteDir(dbname);

  Options options = CurrentOptions();
  options.allow_2pc = true;
  // allow_2pc is implicitly set with tx prepare
  // options.allow_2pc = true;
  TransactionDBOptions txn_db_options;
  TransactionDB* txdb;
  Status s = TransactionDB::Open(options, txn_db_options, dbname, &txdb);
  assert(s.ok());
  ColumnFamilyHandle* cfa;
  ColumnFamilyHandle* cfb;
  ColumnFamilyOptions cf_options;
  ASSERT_OK(txdb->CreateColumnFamily(cf_options, "CFA", &cfa));

  WriteOptions write_options;
  // Insert something into CFB so lots of log files will be kept
  // before creating the checkpoint.
  ASSERT_OK(txdb->CreateColumnFamily(cf_options, "CFB", &cfb));
  ASSERT_OK(txdb->Put(write_options, cfb, "", ""));

  ReadOptions read_options;
  std::string value;
  TransactionOptions txn_options;
  Transaction* txn = txdb->BeginTransaction(write_options, txn_options);
  s = txn->SetName("xid");
  ASSERT_OK(s);
  ASSERT_EQ(txdb->GetTransactionByName("xid"), txn);

  s = txn->Put(Slice("foo"), Slice("bar"));
  s = txn->Put(cfa, Slice("foocfa"), Slice("barcfa"));
  ASSERT_OK(s);
  // Writing prepare into middle of first WAL, then flush WALs many times
  for (int i = 1; i <= 100000; i++) {
    Transaction* tx = txdb->BeginTransaction(write_options, txn_options);
    ASSERT_OK(tx->SetName("x"));
    ASSERT_OK(tx->Put(Slice(std::to_string(i)), Slice("val")));
    ASSERT_OK(tx->Put(cfa, Slice("aaa"), Slice("111")));
    ASSERT_OK(tx->Prepare());
    ASSERT_OK(tx->Commit());
    if (i % 10000 == 0) {
      txdb->Flush(FlushOptions());
    }
    if (i == 88888) {
      ASSERT_OK(txn->Prepare());
    }
    delete tx;
  }
  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"CheckpointImpl::CreateCheckpoint:SavedLiveFiles1",
        "CheckpointTest::CurrentFileModifiedWhileCheckpointing2PC:PreCommit"},
       {"CheckpointTest::CurrentFileModifiedWhileCheckpointing2PC:PostCommit",
        "CheckpointImpl::CreateCheckpoint:SavedLiveFiles2"}});
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  rocksdb::port::Thread t([&]() {
    Checkpoint* checkpoint;
    ASSERT_OK(Checkpoint::Create(txdb, &checkpoint));
    ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));
    delete checkpoint;
  });
  TEST_SYNC_POINT(
      "CheckpointTest::CurrentFileModifiedWhileCheckpointing2PC:PreCommit");
  ASSERT_OK(txn->Commit());
  delete txn;
  TEST_SYNC_POINT(
      "CheckpointTest::CurrentFileModifiedWhileCheckpointing2PC:PostCommit");
  t.join();

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();

  // No more than two logs files should exist.
  std::vector<std::string> files;
  env_->GetChildren(snapshot_name_, &files);
  int num_log_files = 0;
  for (auto& file : files) {
    uint64_t num;
    FileType type;
    WalFileType log_type;
    if (ParseFileName(file, &num, &type, &log_type) && type == kLogFile) {
      num_log_files++;
    }
  }
  // One flush after preapare + one outstanding file before checkpoint + one log
  // file generated after checkpoint.
  ASSERT_LE(num_log_files, 3);

  TransactionDB* snapshotDB;
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, ColumnFamilyOptions()));
  column_families.push_back(
      ColumnFamilyDescriptor("CFA", ColumnFamilyOptions()));
  column_families.push_back(
      ColumnFamilyDescriptor("CFB", ColumnFamilyOptions()));
  std::vector<rocksdb::ColumnFamilyHandle*> cf_handles;
  ASSERT_OK(TransactionDB::Open(options, txn_db_options, snapshot_name_,
                                column_families, &cf_handles, &snapshotDB));
  ASSERT_OK(snapshotDB->Get(read_options, "foo", &value));
  ASSERT_EQ(value, "bar");
  ASSERT_OK(snapshotDB->Get(read_options, cf_handles[1], "foocfa", &value));
  ASSERT_EQ(value, "barcfa");

  delete cfa;
  delete cfb;
  delete cf_handles[0];
  delete cf_handles[1];
  delete cf_handles[2];
  delete snapshotDB;
  snapshotDB = nullptr;
  delete txdb;
}

TEST_F(CheckpointTest, CheckpointInvalidDirectoryName) {
  for (std::string checkpoint_dir : {"", "/", "////"}) {
    Checkpoint* checkpoint;
    ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
    ASSERT_TRUE(checkpoint->CreateCheckpoint("").IsInvalidArgument());
    delete checkpoint;
  }
}

TEST_F(CheckpointTest, CheckpointWithParallelWrites) {
  // When run with TSAN, this exposes the data race fixed in
  // https://github.com/facebook/rocksdb/pull/3603
  ASSERT_OK(Put("key1", "val1"));
  port::Thread thread([this]() { ASSERT_OK(Put("key2", "val2")); });
  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));
  delete checkpoint;
  thread.join();
}

TEST_F(CheckpointTest, CheckpointWithUnsyncedDataDropped) {
  Options options = CurrentOptions();
  std::unique_ptr<FaultInjectionTestEnv> env(new FaultInjectionTestEnv(env_));
  options.env = env.get();
  Reopen(options);
  ASSERT_OK(Put("key1", "val1"));
  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));
  delete checkpoint;
  env->DropUnsyncedFileData();

  // make sure it's openable even though whatever data that wasn't synced got
  // dropped.
  options.env = env_;
  DB* snapshot_db;
  ASSERT_OK(DB::Open(options, snapshot_name_, &snapshot_db));
  ReadOptions read_opts;
  std::string get_result;
  ASSERT_OK(snapshot_db->Get(read_opts, "key1", &get_result));
  ASSERT_EQ("val1", get_result);
  delete snapshot_db;
  delete db_;
  db_ = nullptr;
}

TEST_F(CheckpointTest, CheckpointReadOnlyDB) {
  ASSERT_OK(Put("foo", "foo_value"));
  ASSERT_OK(Flush());
  Close();
  Options options = CurrentOptions();
  ASSERT_OK(ReadOnlyReopen(options));
  Checkpoint* checkpoint = nullptr;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));
  delete checkpoint;
  checkpoint = nullptr;
  Close();
  DB* snapshot_db = nullptr;
  ASSERT_OK(DB::Open(options, snapshot_name_, &snapshot_db));
  ReadOptions read_opts;
  std::string get_result;
  ASSERT_OK(snapshot_db->Get(read_opts, "foo", &get_result));
  ASSERT_EQ("foo_value", get_result);
  delete snapshot_db;
}

TEST_F(CheckpointTest, CheckpointReadOnlyDBWithMultipleColumnFamilies) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu", "eevee"}, options);
  for (int i = 0; i != 3; ++i) {
    ASSERT_OK(Put(i, "foo", "foo_value"));
    ASSERT_OK(Flush(i));
  }
  Close();
  Status s = ReadOnlyReopenWithColumnFamilies(
      {kDefaultColumnFamilyName, "pikachu", "eevee"}, options);
  ASSERT_OK(s);
  Checkpoint* checkpoint = nullptr;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));
  delete checkpoint;
  checkpoint = nullptr;
  Close();

  std::vector<ColumnFamilyDescriptor> column_families{
      {kDefaultColumnFamilyName, options},
      {"pikachu", options},
      {"eevee", options}};
  DB* snapshot_db = nullptr;
  std::vector<ColumnFamilyHandle*> snapshot_handles;
  s = DB::Open(options, snapshot_name_, column_families, &snapshot_handles,
               &snapshot_db);
  ASSERT_OK(s);
  ReadOptions read_opts;
  for (int i = 0; i != 3; ++i) {
    std::string get_result;
    s = snapshot_db->Get(read_opts, snapshot_handles[i], "foo", &get_result);
    ASSERT_OK(s);
    ASSERT_EQ("foo_value", get_result);
  }

  for (auto snapshot_h : snapshot_handles) {
    delete snapshot_h;
  }
  snapshot_handles.clear();
  delete snapshot_db;
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr, "SKIPPED as Checkpoint is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE
