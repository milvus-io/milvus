// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// An Env is an interface used by the rocksdb implementation to access
// operating system functionality like the filesystem etc.  Callers
// may wish to provide a custom Env object when opening a database to
// get fine gain control; e.g., to rate limit file system operations.
//
// All Env implementations are safe for concurrent access from
// multiple threads without any external synchronization.

#pragma once

#include "port/win/win_thread.h"
#include <rocksdb/env.h>
#include "util/threadpool_imp.h"

#include <stdint.h>
#include <windows.h>

#include <mutex>
#include <vector>
#include <string>


#undef GetCurrentTime
#undef DeleteFile
#undef GetTickCount

namespace rocksdb {
namespace port {

// Currently not designed for inheritance but rather a replacement
class WinEnvThreads {
public:

  explicit WinEnvThreads(Env* hosted_env);

  ~WinEnvThreads();

  WinEnvThreads(const WinEnvThreads&) = delete;
  WinEnvThreads& operator=(const WinEnvThreads&) = delete;

  void Schedule(void(*function)(void*), void* arg, Env::Priority pri,
    void* tag,
    void(*unschedFunction)(void* arg));

  int UnSchedule(void* arg, Env::Priority pri);

  void StartThread(void(*function)(void* arg), void* arg);

  void WaitForJoin();

  unsigned int GetThreadPoolQueueLen(Env::Priority pri) const;

  static uint64_t gettid();

  uint64_t GetThreadID() const;

  void SleepForMicroseconds(int micros);

  // Allow increasing the number of worker threads.
  void SetBackgroundThreads(int num, Env::Priority pri);
  int GetBackgroundThreads(Env::Priority pri);

  void IncBackgroundThreadsIfNeeded(int num, Env::Priority pri);

private:

  Env*                     hosted_env_;
  mutable std::mutex       mu_;
  std::vector<ThreadPoolImpl> thread_pools_;
  std::vector<WindowsThread> threads_to_join_;

};

// Designed for inheritance so can be re-used
// but certain parts replaced
class WinEnvIO {
public:
  explicit WinEnvIO(Env* hosted_env);

  virtual ~WinEnvIO();

  virtual Status DeleteFile(const std::string& fname);

  Status Truncate(const std::string& fname, size_t size);

  virtual Status GetCurrentTime(int64_t* unix_time);

  virtual Status NewSequentialFile(const std::string& fname,
    std::unique_ptr<SequentialFile>* result,
    const EnvOptions& options);

  // Helper for NewWritable and ReopenWritableFile
  virtual Status OpenWritableFile(const std::string& fname,
    std::unique_ptr<WritableFile>* result,
    const EnvOptions& options,
    bool reopen);

  virtual Status NewRandomAccessFile(const std::string& fname,
    std::unique_ptr<RandomAccessFile>* result,
    const EnvOptions& options);

  // The returned file will only be accessed by one thread at a time.
  virtual Status NewRandomRWFile(const std::string& fname,
                                 std::unique_ptr<RandomRWFile>* result,
                                 const EnvOptions& options);

  virtual Status NewMemoryMappedFileBuffer(
    const std::string& fname,
    std::unique_ptr<MemoryMappedFileBuffer>* result);

  virtual Status NewDirectory(const std::string& name,
    std::unique_ptr<Directory>* result);

  virtual Status FileExists(const std::string& fname);

  virtual Status GetChildren(const std::string& dir,
    std::vector<std::string>* result);

  virtual Status CreateDir(const std::string& name);

  virtual Status CreateDirIfMissing(const std::string& name);

  virtual Status DeleteDir(const std::string& name);

  virtual Status GetFileSize(const std::string& fname,
    uint64_t* size);

  static uint64_t FileTimeToUnixTime(const FILETIME& ftTime);

  virtual Status GetFileModificationTime(const std::string& fname,
    uint64_t* file_mtime);

  virtual Status RenameFile(const std::string& src,
    const std::string& target);

  virtual Status LinkFile(const std::string& src,
    const std::string& target);

  virtual Status NumFileLinks(const std::string& /*fname*/,
                              uint64_t* /*count*/);

  virtual Status AreFilesSame(const std::string& first,
    const std::string& second, bool* res);

  virtual Status LockFile(const std::string& lockFname,
    FileLock** lock);

  virtual Status UnlockFile(FileLock* lock);

  virtual Status GetTestDirectory(std::string* result);

  virtual Status NewLogger(const std::string& fname,
    std::shared_ptr<Logger>* result);

  virtual uint64_t NowMicros();

  virtual uint64_t NowNanos();

  virtual Status GetHostName(char* name, uint64_t len);

  virtual Status GetAbsolutePath(const std::string& db_path,
    std::string* output_path);

  virtual std::string TimeToString(uint64_t secondsSince1970);

  virtual EnvOptions OptimizeForLogWrite(const EnvOptions& env_options,
    const DBOptions& db_options) const;

  virtual EnvOptions OptimizeForManifestWrite(
    const EnvOptions& env_options) const;

  virtual EnvOptions OptimizeForManifestRead(
    const EnvOptions& env_options) const;

  size_t GetPageSize() const { return page_size_; }

  size_t GetAllocationGranularity() const { return allocation_granularity_; }

  uint64_t GetPerfCounterFrequency() const { return perf_counter_frequency_; }

  static size_t GetSectorSize(const std::string& fname);

private:
  // Returns true iff the named directory exists and is a directory.
  virtual bool DirExists(const std::string& dname);

  typedef VOID(WINAPI * FnGetSystemTimePreciseAsFileTime)(LPFILETIME);

  Env*            hosted_env_;
  size_t          page_size_;
  size_t          allocation_granularity_;
  uint64_t        perf_counter_frequency_;
  FnGetSystemTimePreciseAsFileTime GetSystemTimePreciseAsFileTime_;
};

class WinEnv : public Env {
public:
  WinEnv();

  ~WinEnv();

  Status DeleteFile(const std::string& fname) override;

  Status Truncate(const std::string& fname, size_t size) override;

  Status GetCurrentTime(int64_t* unix_time) override;

  Status NewSequentialFile(const std::string& fname,
    std::unique_ptr<SequentialFile>* result,
    const EnvOptions& options) override;

  Status NewRandomAccessFile(const std::string& fname,
    std::unique_ptr<RandomAccessFile>* result,
    const EnvOptions& options) override;

  Status NewWritableFile(const std::string& fname,
                         std::unique_ptr<WritableFile>* result,
                         const EnvOptions& options) override;

  // Create an object that writes to a new file with the specified
  // name.  Deletes any existing file with the same name and creates a
  // new file.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure stores nullptr in *result and
  // returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  Status ReopenWritableFile(const std::string& fname,
    std::unique_ptr<WritableFile>* result,
    const EnvOptions& options) override;

  // The returned file will only be accessed by one thread at a time.
  Status NewRandomRWFile(const std::string& fname,
    std::unique_ptr<RandomRWFile>* result,
    const EnvOptions& options) override;

  Status NewMemoryMappedFileBuffer(
    const std::string& fname,
    std::unique_ptr<MemoryMappedFileBuffer>* result) override;

  Status NewDirectory(const std::string& name,
    std::unique_ptr<Directory>* result) override;

  Status FileExists(const std::string& fname) override;

  Status GetChildren(const std::string& dir,
    std::vector<std::string>* result) override;

  Status CreateDir(const std::string& name) override;

  Status CreateDirIfMissing(const std::string& name) override;

  Status DeleteDir(const std::string& name) override;

  Status GetFileSize(const std::string& fname,
    uint64_t* size) override;

  Status GetFileModificationTime(const std::string& fname,
    uint64_t* file_mtime) override;

  Status RenameFile(const std::string& src,
    const std::string& target) override;

  Status LinkFile(const std::string& src,
    const std::string& target) override;

  Status NumFileLinks(const std::string& fname, uint64_t* count) override;

  Status AreFilesSame(const std::string& first,
    const std::string& second, bool* res) override;

  Status LockFile(const std::string& lockFname,
    FileLock** lock) override;

  Status UnlockFile(FileLock* lock) override;

  Status GetTestDirectory(std::string* result) override;

  Status NewLogger(const std::string& fname,
    std::shared_ptr<Logger>* result) override;

  uint64_t NowMicros() override;

  uint64_t NowNanos() override;

  Status GetHostName(char* name, uint64_t len) override;

  Status GetAbsolutePath(const std::string& db_path,
    std::string* output_path) override;

  std::string TimeToString(uint64_t secondsSince1970) override;

  Status GetThreadList(
    std::vector<ThreadStatus>* thread_list) override;

  void Schedule(void(*function)(void*), void* arg, Env::Priority pri,
    void* tag,
    void(*unschedFunction)(void* arg)) override;

  int UnSchedule(void* arg, Env::Priority pri) override;

  void StartThread(void(*function)(void* arg), void* arg) override;

  void WaitForJoin();

  unsigned int GetThreadPoolQueueLen(Env::Priority pri) const override;

  uint64_t GetThreadID() const override;

  void SleepForMicroseconds(int micros) override;

  // Allow increasing the number of worker threads.
  void SetBackgroundThreads(int num, Env::Priority pri) override;
  int GetBackgroundThreads(Env::Priority pri) override;

  void IncBackgroundThreadsIfNeeded(int num, Env::Priority pri) override;

  EnvOptions OptimizeForManifestRead(
    const EnvOptions& env_options) const override;

  EnvOptions OptimizeForLogWrite(const EnvOptions& env_options,
    const DBOptions& db_options) const override;

  EnvOptions OptimizeForManifestWrite(
    const EnvOptions& env_options) const override;


private:

  WinEnvIO      winenv_io_;
  WinEnvThreads winenv_threads_;
};

} // namespace port
} // namespace rocksdb
