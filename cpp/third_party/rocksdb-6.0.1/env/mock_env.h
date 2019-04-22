//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <atomic>
#include <map>
#include <string>
#include <vector>
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "port/port.h"
#include "util/mutexlock.h"

namespace rocksdb {

class MemFile;
class MockEnv : public EnvWrapper {
 public:
  explicit MockEnv(Env* base_env);

  virtual ~MockEnv();

  // Partial implementation of the Env interface.
  virtual Status NewSequentialFile(const std::string& fname,
                                   std::unique_ptr<SequentialFile>* result,
                                   const EnvOptions& soptions) override;

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     std::unique_ptr<RandomAccessFile>* result,
                                     const EnvOptions& soptions) override;

  virtual Status NewRandomRWFile(const std::string& fname,
                                 std::unique_ptr<RandomRWFile>* result,
                                 const EnvOptions& options) override;

  virtual Status ReuseWritableFile(const std::string& fname,
                                   const std::string& old_fname,
                                   std::unique_ptr<WritableFile>* result,
                                   const EnvOptions& options) override;

  virtual Status NewWritableFile(const std::string& fname,
                                 std::unique_ptr<WritableFile>* result,
                                 const EnvOptions& env_options) override;

  virtual Status NewDirectory(const std::string& name,
                              std::unique_ptr<Directory>* result) override;

  virtual Status FileExists(const std::string& fname) override;

  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) override;

  void DeleteFileInternal(const std::string& fname);

  virtual Status DeleteFile(const std::string& fname) override;

  virtual Status Truncate(const std::string& fname, size_t size) override;

  virtual Status CreateDir(const std::string& dirname) override;

  virtual Status CreateDirIfMissing(const std::string& dirname) override;

  virtual Status DeleteDir(const std::string& dirname) override;

  virtual Status GetFileSize(const std::string& fname,
                             uint64_t* file_size) override;

  virtual Status GetFileModificationTime(const std::string& fname,
                                         uint64_t* time) override;

  virtual Status RenameFile(const std::string& src,
                            const std::string& target) override;

  virtual Status LinkFile(const std::string& src,
                          const std::string& target) override;

  virtual Status NewLogger(const std::string& fname,
                           std::shared_ptr<Logger>* result) override;

  virtual Status LockFile(const std::string& fname, FileLock** flock) override;

  virtual Status UnlockFile(FileLock* flock) override;

  virtual Status GetTestDirectory(std::string* path) override;

  // Results of these can be affected by FakeSleepForMicroseconds()
  virtual Status GetCurrentTime(int64_t* unix_time) override;
  virtual uint64_t NowMicros() override;
  virtual uint64_t NowNanos() override;

  Status CorruptBuffer(const std::string& fname);

  // Doesn't really sleep, just affects output of GetCurrentTime(), NowMicros()
  // and NowNanos()
  void FakeSleepForMicroseconds(int64_t micros);

 private:
  std::string NormalizePath(const std::string path);

  // Map from filenames to MemFile objects, representing a simple file system.
  typedef std::map<std::string, MemFile*> FileSystem;
  port::Mutex mutex_;
  FileSystem file_map_;  // Protected by mutex_.

  std::atomic<int64_t> fake_sleep_micros_;
};

}  // namespace rocksdb
