//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// This test uses a custom Env to keep track of the state of a filesystem as of
// the last "sync". It then checks for data loss errors by purposely dropping
// file data (or entire files) not protected by a "sync".

#pragma once

#include <map>
#include <set>
#include <string>

#include "db/version_set.h"
#include "env/mock_env.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "util/filename.h"
#include "util/mutexlock.h"
#include "util/random.h"

namespace rocksdb {

class TestWritableFile;
class FaultInjectionTestEnv;

struct FileState {
  std::string filename_;
  ssize_t pos_;
  ssize_t pos_at_last_sync_;
  ssize_t pos_at_last_flush_;

  explicit FileState(const std::string& filename)
      : filename_(filename),
        pos_(-1),
        pos_at_last_sync_(-1),
        pos_at_last_flush_(-1) {}

  FileState() : pos_(-1), pos_at_last_sync_(-1), pos_at_last_flush_(-1) {}

  bool IsFullySynced() const { return pos_ <= 0 || pos_ == pos_at_last_sync_; }

  Status DropUnsyncedData(Env* env) const;

  Status DropRandomUnsyncedData(Env* env, Random* rand) const;
};

// A wrapper around WritableFileWriter* file
// is written to or sync'ed.
class TestWritableFile : public WritableFile {
 public:
  explicit TestWritableFile(const std::string& fname,
                            std::unique_ptr<WritableFile>&& f,
                            FaultInjectionTestEnv* env);
  virtual ~TestWritableFile();
  virtual Status Append(const Slice& data) override;
  virtual Status Truncate(uint64_t size) override {
    return target_->Truncate(size);
  }
  virtual Status Close() override;
  virtual Status Flush() override;
  virtual Status Sync() override;
  virtual bool IsSyncThreadSafe() const override { return true; }
  virtual Status PositionedAppend(const Slice& data,
                                  uint64_t offset) override {
    return target_->PositionedAppend(data, offset);
  }
  virtual bool use_direct_io() const override {
    return target_->use_direct_io();
  };

 private:
  FileState state_;
  std::unique_ptr<WritableFile> target_;
  bool writable_file_opened_;
  FaultInjectionTestEnv* env_;
};

class TestDirectory : public Directory {
 public:
  explicit TestDirectory(FaultInjectionTestEnv* env, std::string dirname,
                         Directory* dir)
      : env_(env), dirname_(dirname), dir_(dir) {}
  ~TestDirectory() {}

  virtual Status Fsync() override;

 private:
  FaultInjectionTestEnv* env_;
  std::string dirname_;
  std::unique_ptr<Directory> dir_;
};

class FaultInjectionTestEnv : public EnvWrapper {
 public:
  explicit FaultInjectionTestEnv(Env* base)
      : EnvWrapper(base), filesystem_active_(true) {}
  virtual ~FaultInjectionTestEnv() {}

  Status NewDirectory(const std::string& name,
                      std::unique_ptr<Directory>* result) override;

  Status NewWritableFile(const std::string& fname,
                         std::unique_ptr<WritableFile>* result,
                         const EnvOptions& soptions) override;

  Status ReopenWritableFile(const std::string& fname,
                            std::unique_ptr<WritableFile>* result,
                            const EnvOptions& soptions) override;

  Status NewRandomAccessFile(const std::string& fname,
                             std::unique_ptr<RandomAccessFile>* result,
                             const EnvOptions& soptions) override;

  virtual Status DeleteFile(const std::string& f) override;

  virtual Status RenameFile(const std::string& s,
                            const std::string& t) override;

  virtual Status GetFreeSpace(const std::string& path,
                              uint64_t* disk_free) override {
    if (!IsFilesystemActive() && error_ == Status::NoSpace()) {
      *disk_free = 0;
      return Status::OK();
    } else {
      return target()->GetFreeSpace(path, disk_free);
    }
  }

  void WritableFileClosed(const FileState& state);

  void WritableFileSynced(const FileState& state);

  void WritableFileAppended(const FileState& state);

  // For every file that is not fully synced, make a call to `func` with
  // FileState of the file as the parameter.
  Status DropFileData(std::function<Status(Env*, FileState)> func);

  Status DropUnsyncedFileData();

  Status DropRandomUnsyncedFileData(Random* rnd);

  Status DeleteFilesCreatedAfterLastDirSync();

  void ResetState();

  void UntrackFile(const std::string& f);

  void SyncDir(const std::string& dirname) {
    MutexLock l(&mutex_);
    dir_to_new_files_since_last_sync_.erase(dirname);
  }

  // Setting the filesystem to inactive is the test equivalent to simulating a
  // system reset. Setting to inactive will freeze our saved filesystem state so
  // that it will stop being recorded. It can then be reset back to the state at
  // the time of the reset.
  bool IsFilesystemActive() {
    MutexLock l(&mutex_);
    return filesystem_active_;
  }
  void SetFilesystemActiveNoLock(bool active,
      Status error = Status::Corruption("Not active")) {
    filesystem_active_ = active;
    if (!active) {
      error_ = error;
    }
  }
  void SetFilesystemActive(bool active,
      Status error = Status::Corruption("Not active")) {
    MutexLock l(&mutex_);
    SetFilesystemActiveNoLock(active, error);
  }
  void AssertNoOpenFile() { assert(open_files_.empty()); }
  Status GetError() { return error_; }

 private:
  port::Mutex mutex_;
  std::map<std::string, FileState> db_file_state_;
  std::set<std::string> open_files_;
  std::unordered_map<std::string, std::set<std::string>>
      dir_to_new_files_since_last_sync_;
  bool filesystem_active_;  // Record flushes, syncs, writes
  Status error_;
};

}  // namespace rocksdb
