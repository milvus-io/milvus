//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include <map>
#include <queue>
#include <string>
#include <thread>

#include "monitoring/instrumented_mutex.h"
#include "port/port.h"

#include "rocksdb/status.h"

namespace rocksdb {

class Env;
class Logger;
class SstFileManagerImpl;

// DeleteScheduler allows the DB to enforce a rate limit on file deletion,
// Instead of deleteing files immediately, files are marked as trash
// and deleted in a background thread that apply sleep penlty between deletes
// if they are happening in a rate faster than rate_bytes_per_sec,
//
// Rate limiting can be turned off by setting rate_bytes_per_sec = 0, In this
// case DeleteScheduler will delete files immediately.
class DeleteScheduler {
 public:
  DeleteScheduler(Env* env, int64_t rate_bytes_per_sec, Logger* info_log,
                  SstFileManagerImpl* sst_file_manager,
                  double max_trash_db_ratio, uint64_t bytes_max_delete_chunk);

  ~DeleteScheduler();

  // Return delete rate limit in bytes per second
  int64_t GetRateBytesPerSecond() { return rate_bytes_per_sec_.load(); }

  // Set delete rate limit in bytes per second
  void SetRateBytesPerSecond(int64_t bytes_per_sec) {
    rate_bytes_per_sec_.store(bytes_per_sec);
  }

  // Mark file as trash directory and schedule it's deletion. If force_bg is
  // set, it forces the file to always be deleted in the background thread,
  // except when rate limiting is disabled
  Status DeleteFile(const std::string& fname, const std::string& dir_to_sync,
      const bool force_bg = false);

  // Wait for all files being deleteing in the background to finish or for
  // destructor to be called.
  void WaitForEmptyTrash();

  // Return a map containing errors that happened in BackgroundEmptyTrash
  // file_path => error status
  std::map<std::string, Status> GetBackgroundErrors();

  uint64_t GetTotalTrashSize() { return total_trash_size_.load(); }

  // Return trash/DB size ratio where new files will be deleted immediately
  double GetMaxTrashDBRatio() {
    return max_trash_db_ratio_.load();
  }

  // Update trash/DB size ratio where new files will be deleted immediately
  void SetMaxTrashDBRatio(double r) {
    assert(r >= 0);
    max_trash_db_ratio_.store(r);
  }

  static const std::string kTrashExtension;
  static bool IsTrashFile(const std::string& file_path);

  // Check if there are any .trash filse in path, and schedule their deletion
  // Or delete immediately if sst_file_manager is nullptr
  static Status CleanupDirectory(Env* env, SstFileManagerImpl* sfm,
                                 const std::string& path);

 private:
  Status MarkAsTrash(const std::string& file_path, std::string* path_in_trash);

  Status DeleteTrashFile(const std::string& path_in_trash,
                         const std::string& dir_to_sync,
                         uint64_t* deleted_bytes, bool* is_complete);

  void BackgroundEmptyTrash();

  Env* env_;
  // total size of trash files
  std::atomic<uint64_t> total_trash_size_;
  // Maximum number of bytes that should be deleted per second
  std::atomic<int64_t> rate_bytes_per_sec_;
  // Mutex to protect queue_, pending_files_, bg_errors_, closing_
  InstrumentedMutex mu_;

  struct FileAndDir {
    FileAndDir(const std::string& f, const std::string& d) : fname(f), dir(d) {}
    std::string fname;
    std::string dir;  // empty will be skipped.
  };

  // Queue of trash files that need to be deleted
  std::queue<FileAndDir> queue_;
  // Number of trash files that are waiting to be deleted
  int32_t pending_files_;
  uint64_t bytes_max_delete_chunk_;
  // Errors that happened in BackgroundEmptyTrash (file_path => error)
  std::map<std::string, Status> bg_errors_;

  bool num_link_error_printed_ = false;
  // Set to true in ~DeleteScheduler() to force BackgroundEmptyTrash to stop
  bool closing_;
  // Condition variable signaled in these conditions
  //    - pending_files_ value change from 0 => 1
  //    - pending_files_ value change from 1 => 0
  //    - closing_ value is set to true
  InstrumentedCondVar cv_;
  // Background thread running BackgroundEmptyTrash
  std::unique_ptr<port::Thread> bg_thread_;
  // Mutex to protect threads from file name conflicts
  InstrumentedMutex file_move_mu_;
  Logger* info_log_;
  SstFileManagerImpl* sst_file_manager_;
  // If the trash size constitutes for more than this fraction of the total DB
  // size we will start deleting new files passed to DeleteScheduler
  // immediately
  std::atomic<double> max_trash_db_ratio_;
  static const uint64_t kMicrosInSecond = 1000 * 1000LL;
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
