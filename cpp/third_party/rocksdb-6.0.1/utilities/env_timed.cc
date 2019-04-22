// Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "monitoring/perf_context_imp.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"

namespace rocksdb {

#ifndef ROCKSDB_LITE

// An environment that measures function call times for filesystem
// operations, reporting results to variables in PerfContext.
class TimedEnv : public EnvWrapper {
 public:
  explicit TimedEnv(Env* base_env) : EnvWrapper(base_env) {}

  Status NewSequentialFile(const std::string& fname,
                           std::unique_ptr<SequentialFile>* result,
                           const EnvOptions& options) override {
    PERF_TIMER_GUARD(env_new_sequential_file_nanos);
    return EnvWrapper::NewSequentialFile(fname, result, options);
  }

  Status NewRandomAccessFile(const std::string& fname,
                             std::unique_ptr<RandomAccessFile>* result,
                             const EnvOptions& options) override {
    PERF_TIMER_GUARD(env_new_random_access_file_nanos);
    return EnvWrapper::NewRandomAccessFile(fname, result, options);
  }

  Status NewWritableFile(const std::string& fname,
                         std::unique_ptr<WritableFile>* result,
                         const EnvOptions& options) override {
    PERF_TIMER_GUARD(env_new_writable_file_nanos);
    return EnvWrapper::NewWritableFile(fname, result, options);
  }

  Status ReuseWritableFile(const std::string& fname,
                           const std::string& old_fname,
                           std::unique_ptr<WritableFile>* result,
                           const EnvOptions& options) override {
    PERF_TIMER_GUARD(env_reuse_writable_file_nanos);
    return EnvWrapper::ReuseWritableFile(fname, old_fname, result, options);
  }

  Status NewRandomRWFile(const std::string& fname,
                         std::unique_ptr<RandomRWFile>* result,
                         const EnvOptions& options) override {
    PERF_TIMER_GUARD(env_new_random_rw_file_nanos);
    return EnvWrapper::NewRandomRWFile(fname, result, options);
  }

  Status NewDirectory(const std::string& name,
                      std::unique_ptr<Directory>* result) override {
    PERF_TIMER_GUARD(env_new_directory_nanos);
    return EnvWrapper::NewDirectory(name, result);
  }

  Status FileExists(const std::string& fname) override {
    PERF_TIMER_GUARD(env_file_exists_nanos);
    return EnvWrapper::FileExists(fname);
  }

  Status GetChildren(const std::string& dir,
                     std::vector<std::string>* result) override {
    PERF_TIMER_GUARD(env_get_children_nanos);
    return EnvWrapper::GetChildren(dir, result);
  }

  Status GetChildrenFileAttributes(
      const std::string& dir, std::vector<FileAttributes>* result) override {
    PERF_TIMER_GUARD(env_get_children_file_attributes_nanos);
    return EnvWrapper::GetChildrenFileAttributes(dir, result);
  }

  Status DeleteFile(const std::string& fname) override {
    PERF_TIMER_GUARD(env_delete_file_nanos);
    return EnvWrapper::DeleteFile(fname);
  }

  Status CreateDir(const std::string& dirname) override {
    PERF_TIMER_GUARD(env_create_dir_nanos);
    return EnvWrapper::CreateDir(dirname);
  }

  Status CreateDirIfMissing(const std::string& dirname) override {
    PERF_TIMER_GUARD(env_create_dir_if_missing_nanos);
    return EnvWrapper::CreateDirIfMissing(dirname);
  }

  Status DeleteDir(const std::string& dirname) override {
    PERF_TIMER_GUARD(env_delete_dir_nanos);
    return EnvWrapper::DeleteDir(dirname);
  }

  Status GetFileSize(const std::string& fname, uint64_t* file_size) override {
    PERF_TIMER_GUARD(env_get_file_size_nanos);
    return EnvWrapper::GetFileSize(fname, file_size);
  }

  Status GetFileModificationTime(const std::string& fname,
                                 uint64_t* file_mtime) override {
    PERF_TIMER_GUARD(env_get_file_modification_time_nanos);
    return EnvWrapper::GetFileModificationTime(fname, file_mtime);
  }

  Status RenameFile(const std::string& src, const std::string& dst) override {
    PERF_TIMER_GUARD(env_rename_file_nanos);
    return EnvWrapper::RenameFile(src, dst);
  }

  Status LinkFile(const std::string& src, const std::string& dst) override {
    PERF_TIMER_GUARD(env_link_file_nanos);
    return EnvWrapper::LinkFile(src, dst);
  }

  Status LockFile(const std::string& fname, FileLock** lock) override {
    PERF_TIMER_GUARD(env_lock_file_nanos);
    return EnvWrapper::LockFile(fname, lock);
  }

  Status UnlockFile(FileLock* lock) override {
    PERF_TIMER_GUARD(env_unlock_file_nanos);
    return EnvWrapper::UnlockFile(lock);
  }

  Status NewLogger(const std::string& fname,
                   std::shared_ptr<Logger>* result) override {
    PERF_TIMER_GUARD(env_new_logger_nanos);
    return EnvWrapper::NewLogger(fname, result);
  }
};

Env* NewTimedEnv(Env* base_env) { return new TimedEnv(base_env); }

#else  // ROCKSDB_LITE

Env* NewTimedEnv(Env* /*base_env*/) { return nullptr; }

#endif  // !ROCKSDB_LITE

}  // namespace rocksdb
