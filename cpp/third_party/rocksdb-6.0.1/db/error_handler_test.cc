//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#ifndef ROCKSDB_LITE

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/sst_file_manager.h"
#include "util/fault_injection_test_env.h"
#if !defined(ROCKSDB_LITE)
#include "util/sync_point.h"
#endif

namespace rocksdb {

class DBErrorHandlingTest : public DBTestBase {
 public:
  DBErrorHandlingTest() : DBTestBase("/db_error_handling_test") {}
};

class DBErrorHandlingEnv : public EnvWrapper {
  public:
    DBErrorHandlingEnv() : EnvWrapper(Env::Default()),
      trig_no_space(false), trig_io_error(false) {}

    void SetTrigNoSpace() {trig_no_space = true;}
    void SetTrigIoError() {trig_io_error = true;}
  private:
    bool trig_no_space;
    bool trig_io_error;
};

class ErrorHandlerListener : public EventListener {
 public:
  ErrorHandlerListener()
      : mutex_(),
        cv_(&mutex_),
        no_auto_recovery_(false),
        recovery_complete_(false),
        file_creation_started_(false),
        override_bg_error_(false),
        file_count_(0),
        fault_env_(nullptr) {}

  void OnTableFileCreationStarted(
      const TableFileCreationBriefInfo& /*ti*/) override {
    InstrumentedMutexLock l(&mutex_);
    file_creation_started_ = true;
    if (file_count_ > 0) {
      if (--file_count_ == 0) {
        fault_env_->SetFilesystemActive(false, file_creation_error_);
        file_creation_error_ = Status::OK();
      }
    }
    cv_.SignalAll();
  }

  void OnErrorRecoveryBegin(BackgroundErrorReason /*reason*/,
                            Status /*bg_error*/,
                            bool* auto_recovery) override {
    if (*auto_recovery && no_auto_recovery_) {
      *auto_recovery = false;
    }
  }

  void OnErrorRecoveryCompleted(Status /*old_bg_error*/) override {
    InstrumentedMutexLock l(&mutex_);
    recovery_complete_ = true;
    cv_.SignalAll();
  }

  bool WaitForRecovery(uint64_t /*abs_time_us*/) {
    InstrumentedMutexLock l(&mutex_);
    while (!recovery_complete_) {
      cv_.Wait(/*abs_time_us*/);
    }
    if (recovery_complete_) {
      recovery_complete_ = false;
      return true;
    }
    return false;
  }

  void WaitForTableFileCreationStarted(uint64_t /*abs_time_us*/) {
    InstrumentedMutexLock l(&mutex_);
    while (!file_creation_started_) {
      cv_.Wait(/*abs_time_us*/);
    }
    file_creation_started_ = false;
  }

  void OnBackgroundError(BackgroundErrorReason /*reason*/,
                         Status* bg_error) override {
    if (override_bg_error_) {
      *bg_error = bg_error_;
      override_bg_error_ = false;
    }
  }

  void EnableAutoRecovery(bool enable = true) { no_auto_recovery_ = !enable; }

  void OverrideBGError(Status bg_err) {
    bg_error_ = bg_err;
    override_bg_error_ = true;
  }

  void InjectFileCreationError(FaultInjectionTestEnv* env, int file_count,
                               Status s) {
    fault_env_ = env;
    file_count_ = file_count;
    file_creation_error_ = s;
  }

 private:
  InstrumentedMutex mutex_;
  InstrumentedCondVar cv_;
  bool no_auto_recovery_;
  bool recovery_complete_;
  bool file_creation_started_;
  bool override_bg_error_;
  int file_count_;
  Status file_creation_error_;
  Status bg_error_;
  FaultInjectionTestEnv* fault_env_;
};

TEST_F(DBErrorHandlingTest, FLushWriteError) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(Env::Default()));
  std::shared_ptr<ErrorHandlerListener> listener(new ErrorHandlerListener());
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.env = fault_env.get();
  options.listeners.emplace_back(listener);
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  Put(Key(0), "val");
  SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::Start", [&](void *) {
    fault_env->SetFilesystemActive(false, Status::NoSpace("Out of space"));
  });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), rocksdb::Status::Severity::kHardError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_env->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_EQ(s, Status::OK());

  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  Destroy(options);
}

TEST_F(DBErrorHandlingTest, CompactionWriteError) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(Env::Default()));
  std::shared_ptr<ErrorHandlerListener> listener(new ErrorHandlerListener());
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 2;
  options.listeners.emplace_back(listener);
  options.env = fault_env.get();
  Status s;
  DestroyAndReopen(options);

  Put(Key(0), "va;");
  Put(Key(2), "va;");
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  listener->OverrideBGError(
      Status(Status::NoSpace(), Status::Severity::kHardError)
      );
  listener->EnableAutoRecovery(false);
  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"FlushMemTableFinished", "BackgroundCallCompaction:0"}});
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void *) {
      fault_env->SetFilesystemActive(false, Status::NoSpace("Out of space"));
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Put(Key(1), "val");
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s.severity(), rocksdb::Status::Severity::kHardError);

  fault_env->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_EQ(s, Status::OK());
  Destroy(options);
}

TEST_F(DBErrorHandlingTest, CorruptionError) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(Env::Default()));
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 2;
  options.env = fault_env.get();
  Status s;
  DestroyAndReopen(options);

  Put(Key(0), "va;");
  Put(Key(2), "va;");
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"FlushMemTableFinished", "BackgroundCallCompaction:0"}});
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void *) {
      fault_env->SetFilesystemActive(false, Status::Corruption("Corruption"));
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Put(Key(1), "val");
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s.severity(), rocksdb::Status::Severity::kUnrecoverableError);

  fault_env->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_NE(s, Status::OK());
  Destroy(options);
}

TEST_F(DBErrorHandlingTest, AutoRecoverFlushError) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(Env::Default()));
  std::shared_ptr<ErrorHandlerListener> listener(new ErrorHandlerListener());
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.env = fault_env.get();
  options.listeners.emplace_back(listener);
  Status s;

  listener->EnableAutoRecovery();
  DestroyAndReopen(options);

  Put(Key(0), "val");
  SyncPoint::GetInstance()->SetCallBack("FlushJob::Start", [&](void*) {
    fault_env->SetFilesystemActive(false, Status::NoSpace("Out of space"));
  });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), rocksdb::Status::Severity::kHardError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_env->SetFilesystemActive(true);
  ASSERT_EQ(listener->WaitForRecovery(5000000), true);

  s = Put(Key(1), "val");
  ASSERT_EQ(s, Status::OK());

  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  ASSERT_EQ("val", Get(Key(1)));
  Destroy(options);
}

TEST_F(DBErrorHandlingTest, FailRecoverFlushError) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(Env::Default()));
  std::shared_ptr<ErrorHandlerListener> listener(new ErrorHandlerListener());
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.env = fault_env.get();
  options.listeners.emplace_back(listener);
  Status s;

  listener->EnableAutoRecovery();
  DestroyAndReopen(options);

  Put(Key(0), "val");
  SyncPoint::GetInstance()->SetCallBack("FlushJob::Start", [&](void*) {
    fault_env->SetFilesystemActive(false, Status::NoSpace("Out of space"));
  });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), rocksdb::Status::Severity::kHardError);
  // We should be able to shutdown the database while auto recovery is going
  // on in the background
  Close();
  DestroyDB(dbname_, options);
}

TEST_F(DBErrorHandlingTest, WALWriteError) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(Env::Default()));
  std::shared_ptr<ErrorHandlerListener> listener(new ErrorHandlerListener());
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.writable_file_max_buffer_size = 32768;
  options.env = fault_env.get();
  options.listeners.emplace_back(listener);
  Status s;
  Random rnd(301);

  listener->EnableAutoRecovery();
  DestroyAndReopen(options);

  {
    WriteBatch batch;

    for (auto i = 0; i<100; ++i) {
      batch.Put(Key(i), RandomString(&rnd, 1024));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_EQ(dbfull()->Write(wopts, &batch), Status::OK());
  };

  {
    WriteBatch batch;
    int write_error = 0;

    for (auto i = 100; i<199; ++i) {
      batch.Put(Key(i), RandomString(&rnd, 1024));
    }

    SyncPoint::GetInstance()->SetCallBack("WritableFileWriter::Append:BeforePrepareWrite", [&](void*) {
      write_error++;
      if (write_error > 2) {
        fault_env->SetFilesystemActive(false, Status::NoSpace("Out of space"));
      }
    });
    SyncPoint::GetInstance()->EnableProcessing();
    WriteOptions wopts;
    wopts.sync = true;
    s = dbfull()->Write(wopts, &batch);
    ASSERT_EQ(s, s.NoSpace());
  }
  SyncPoint::GetInstance()->DisableProcessing();
  fault_env->SetFilesystemActive(true);
  ASSERT_EQ(listener->WaitForRecovery(5000000), true);
  for (auto i=0; i<199; ++i) {
    if (i < 100) {
      ASSERT_NE(Get(Key(i)), "NOT_FOUND");
    } else {
      ASSERT_EQ(Get(Key(i)), "NOT_FOUND");
    }
  }
  Reopen(options);
  for (auto i=0; i<199; ++i) {
    if (i < 100) {
      ASSERT_NE(Get(Key(i)), "NOT_FOUND");
    } else {
      ASSERT_EQ(Get(Key(i)), "NOT_FOUND");
    }
  }
  Close();
}

TEST_F(DBErrorHandlingTest, MultiCFWALWriteError) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(Env::Default()));
  std::shared_ptr<ErrorHandlerListener> listener(new ErrorHandlerListener());
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.writable_file_max_buffer_size = 32768;
  options.env = fault_env.get();
  options.listeners.emplace_back(listener);
  Status s;
  Random rnd(301);

  listener->EnableAutoRecovery();
  CreateAndReopenWithCF({"one", "two", "three"}, options);

  {
    WriteBatch batch;

    for (auto i = 1; i < 4; ++i) {
      for (auto j = 0; j < 100; ++j) {
        batch.Put(handles_[i], Key(j), RandomString(&rnd, 1024));
      }
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_EQ(dbfull()->Write(wopts, &batch), Status::OK());
  };

  {
    WriteBatch batch;
    int write_error = 0;

    // Write to one CF
    for (auto i = 100; i < 199; ++i) {
      batch.Put(handles_[2], Key(i), RandomString(&rnd, 1024));
    }

    SyncPoint::GetInstance()->SetCallBack(
        "WritableFileWriter::Append:BeforePrepareWrite", [&](void*) {
          write_error++;
          if (write_error > 2) {
            fault_env->SetFilesystemActive(false,
                                           Status::NoSpace("Out of space"));
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    WriteOptions wopts;
    wopts.sync = true;
    s = dbfull()->Write(wopts, &batch);
    ASSERT_EQ(s, s.NoSpace());
  }
  SyncPoint::GetInstance()->DisableProcessing();
  fault_env->SetFilesystemActive(true);
  ASSERT_EQ(listener->WaitForRecovery(5000000), true);

  for (auto i = 1; i < 4; ++i) {
    // Every CF should have been flushed
    ASSERT_EQ(NumTableFilesAtLevel(0, i), 1);
  }

  for (auto i = 1; i < 4; ++i) {
    for (auto j = 0; j < 199; ++j) {
      if (j < 100) {
        ASSERT_NE(Get(i, Key(j)), "NOT_FOUND");
      } else {
        ASSERT_EQ(Get(i, Key(j)), "NOT_FOUND");
      }
    }
  }
  ReopenWithColumnFamilies({"default", "one", "two", "three"}, options);
  for (auto i = 1; i < 4; ++i) {
    for (auto j = 0; j < 199; ++j) {
      if (j < 100) {
        ASSERT_NE(Get(i, Key(j)), "NOT_FOUND");
      } else {
        ASSERT_EQ(Get(i, Key(j)), "NOT_FOUND");
      }
    }
  }
  Close();
}

TEST_F(DBErrorHandlingTest, MultiDBCompactionError) {
  FaultInjectionTestEnv* def_env = new FaultInjectionTestEnv(Env::Default());
  std::vector<std::unique_ptr<FaultInjectionTestEnv>> fault_env;
  std::vector<Options> options;
  std::vector<std::shared_ptr<ErrorHandlerListener>> listener;
  std::vector<DB*> db;
  std::shared_ptr<SstFileManager> sfm(NewSstFileManager(def_env));
  int kNumDbInstances = 3;
  Random rnd(301);

  for (auto i = 0; i < kNumDbInstances; ++i) {
    listener.emplace_back(new ErrorHandlerListener());
    options.emplace_back(GetDefaultOptions());
    fault_env.emplace_back(new FaultInjectionTestEnv(Env::Default()));
    options[i].create_if_missing = true;
    options[i].level0_file_num_compaction_trigger = 2;
    options[i].writable_file_max_buffer_size = 32768;
    options[i].env = fault_env[i].get();
    options[i].listeners.emplace_back(listener[i]);
    options[i].sst_file_manager = sfm;
    DB* dbptr;
    char buf[16];

    listener[i]->EnableAutoRecovery();
    // Setup for returning error for the 3rd SST, which would be level 1
    listener[i]->InjectFileCreationError(fault_env[i].get(), 3,
                                         Status::NoSpace("Out of space"));
    snprintf(buf, sizeof(buf), "_%d", i);
    DestroyDB(dbname_ + std::string(buf), options[i]);
    ASSERT_EQ(DB::Open(options[i], dbname_ + std::string(buf), &dbptr),
              Status::OK());
    db.emplace_back(dbptr);
  }

  for (auto i = 0; i < kNumDbInstances; ++i) {
    WriteBatch batch;

    for (auto j = 0; j <= 100; ++j) {
      batch.Put(Key(j), RandomString(&rnd, 1024));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_EQ(db[i]->Write(wopts, &batch), Status::OK());
    ASSERT_EQ(db[i]->Flush(FlushOptions()), Status::OK());
  }

  def_env->SetFilesystemActive(false, Status::NoSpace("Out of space"));
  for (auto i = 0; i < kNumDbInstances; ++i) {
    WriteBatch batch;

    // Write to one CF
    for (auto j = 100; j < 199; ++j) {
      batch.Put(Key(j), RandomString(&rnd, 1024));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_EQ(db[i]->Write(wopts, &batch), Status::OK());
    ASSERT_EQ(db[i]->Flush(FlushOptions()), Status::OK());
  }

  for (auto i = 0; i < kNumDbInstances; ++i) {
    Status s = static_cast<DBImpl*>(db[i])->TEST_WaitForCompact(true);
    ASSERT_EQ(s.severity(), Status::Severity::kSoftError);
    fault_env[i]->SetFilesystemActive(true);
  }

  def_env->SetFilesystemActive(true);
  for (auto i = 0; i < kNumDbInstances; ++i) {
    std::string prop;
    ASSERT_EQ(listener[i]->WaitForRecovery(5000000), true);
    EXPECT_TRUE(db[i]->GetProperty(
        "rocksdb.num-files-at-level" + NumberToString(0), &prop));
    EXPECT_EQ(atoi(prop.c_str()), 0);
    EXPECT_TRUE(db[i]->GetProperty(
        "rocksdb.num-files-at-level" + NumberToString(1), &prop));
    EXPECT_EQ(atoi(prop.c_str()), 1);
  }

  for (auto i = 0; i < kNumDbInstances; ++i) {
    char buf[16];
    snprintf(buf, sizeof(buf), "_%d", i);
    delete db[i];
    fault_env[i]->SetFilesystemActive(true);
    if (getenv("KEEP_DB")) {
      printf("DB is still at %s%s\n", dbname_.c_str(), buf);
    } else {
      Status s = DestroyDB(dbname_ + std::string(buf), options[i]);
    }
  }
  options.clear();
  sfm.reset();
  delete def_env;
}

TEST_F(DBErrorHandlingTest, MultiDBVariousErrors) {
  FaultInjectionTestEnv* def_env = new FaultInjectionTestEnv(Env::Default());
  std::vector<std::unique_ptr<FaultInjectionTestEnv>> fault_env;
  std::vector<Options> options;
  std::vector<std::shared_ptr<ErrorHandlerListener>> listener;
  std::vector<DB*> db;
  std::shared_ptr<SstFileManager> sfm(NewSstFileManager(def_env));
  int kNumDbInstances = 3;
  Random rnd(301);

  for (auto i = 0; i < kNumDbInstances; ++i) {
    listener.emplace_back(new ErrorHandlerListener());
    options.emplace_back(GetDefaultOptions());
    fault_env.emplace_back(new FaultInjectionTestEnv(Env::Default()));
    options[i].create_if_missing = true;
    options[i].level0_file_num_compaction_trigger = 2;
    options[i].writable_file_max_buffer_size = 32768;
    options[i].env = fault_env[i].get();
    options[i].listeners.emplace_back(listener[i]);
    options[i].sst_file_manager = sfm;
    DB* dbptr;
    char buf[16];

    listener[i]->EnableAutoRecovery();
    switch (i) {
      case 0:
        // Setup for returning error for the 3rd SST, which would be level 1
        listener[i]->InjectFileCreationError(fault_env[i].get(), 3,
                                             Status::NoSpace("Out of space"));
        break;
      case 1:
        // Setup for returning error after the 1st SST, which would result
        // in a hard error
        listener[i]->InjectFileCreationError(fault_env[i].get(), 2,
                                             Status::NoSpace("Out of space"));
        break;
      default:
        break;
    }
    snprintf(buf, sizeof(buf), "_%d", i);
    DestroyDB(dbname_ + std::string(buf), options[i]);
    ASSERT_EQ(DB::Open(options[i], dbname_ + std::string(buf), &dbptr),
              Status::OK());
    db.emplace_back(dbptr);
  }

  for (auto i = 0; i < kNumDbInstances; ++i) {
    WriteBatch batch;

    for (auto j = 0; j <= 100; ++j) {
      batch.Put(Key(j), RandomString(&rnd, 1024));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_EQ(db[i]->Write(wopts, &batch), Status::OK());
    ASSERT_EQ(db[i]->Flush(FlushOptions()), Status::OK());
  }

  def_env->SetFilesystemActive(false, Status::NoSpace("Out of space"));
  for (auto i = 0; i < kNumDbInstances; ++i) {
    WriteBatch batch;

    // Write to one CF
    for (auto j = 100; j < 199; ++j) {
      batch.Put(Key(j), RandomString(&rnd, 1024));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_EQ(db[i]->Write(wopts, &batch), Status::OK());
    if (i != 1) {
      ASSERT_EQ(db[i]->Flush(FlushOptions()), Status::OK());
    } else {
      ASSERT_EQ(db[i]->Flush(FlushOptions()), Status::NoSpace());
    }
  }

  for (auto i = 0; i < kNumDbInstances; ++i) {
    Status s = static_cast<DBImpl*>(db[i])->TEST_WaitForCompact(true);
    switch (i) {
      case 0:
        ASSERT_EQ(s.severity(), Status::Severity::kSoftError);
        break;
      case 1:
        ASSERT_EQ(s.severity(), Status::Severity::kHardError);
        break;
      case 2:
        ASSERT_EQ(s, Status::OK());
        break;
    }
    fault_env[i]->SetFilesystemActive(true);
  }

  def_env->SetFilesystemActive(true);
  for (auto i = 0; i < kNumDbInstances; ++i) {
    std::string prop;
    if (i < 2) {
      ASSERT_EQ(listener[i]->WaitForRecovery(5000000), true);
    }
    if (i == 1) {
      ASSERT_EQ(static_cast<DBImpl*>(db[i])->TEST_WaitForCompact(true),
                Status::OK());
    }
    EXPECT_TRUE(db[i]->GetProperty(
        "rocksdb.num-files-at-level" + NumberToString(0), &prop));
    EXPECT_EQ(atoi(prop.c_str()), 0);
    EXPECT_TRUE(db[i]->GetProperty(
        "rocksdb.num-files-at-level" + NumberToString(1), &prop));
    EXPECT_EQ(atoi(prop.c_str()), 1);
  }

  for (auto i = 0; i < kNumDbInstances; ++i) {
    char buf[16];
    snprintf(buf, sizeof(buf), "_%d", i);
    fault_env[i]->SetFilesystemActive(true);
    delete db[i];
    if (getenv("KEEP_DB")) {
      printf("DB is still at %s%s\n", dbname_.c_str(), buf);
    } else {
      DestroyDB(dbname_ + std::string(buf), options[i]);
    }
  }
  options.clear();
  delete def_env;
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
  fprintf(stderr, "SKIPPED as Cuckoo table is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // ROCKSDB_LITE
