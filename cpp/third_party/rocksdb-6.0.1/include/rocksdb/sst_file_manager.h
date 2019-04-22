//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "rocksdb/status.h"

namespace rocksdb {

class Env;
class Logger;

// SstFileManager is used to track SST files in the DB and control their
// deletion rate.
// All SstFileManager public functions are thread-safe.
// SstFileManager is not extensible.
class SstFileManager {
 public:
  virtual ~SstFileManager() {}

  // Update the maximum allowed space that should be used by RocksDB, if
  // the total size of the SST files exceeds max_allowed_space, writes to
  // RocksDB will fail.
  //
  // Setting max_allowed_space to 0 will disable this feature; maximum allowed
  // space will be infinite (Default value).
  //
  // thread-safe.
  virtual void SetMaxAllowedSpaceUsage(uint64_t max_allowed_space) = 0;

  // Set the amount of buffer room each compaction should be able to leave.
  // In other words, at its maximum disk space consumption, the compaction
  // should still leave compaction_buffer_size available on the disk so that
  // other background functions may continue, such as logging and flushing.
  virtual void SetCompactionBufferSize(uint64_t compaction_buffer_size) = 0;

  // Return true if the total size of SST files exceeded the maximum allowed
  // space usage.
  //
  // thread-safe.
  virtual bool IsMaxAllowedSpaceReached() = 0;

  // Returns true if the total size of SST files as well as estimated size
  // of ongoing compactions exceeds the maximums allowed space usage.
  virtual bool IsMaxAllowedSpaceReachedIncludingCompactions() = 0;

  // Return the total size of all tracked files.
  // thread-safe
  virtual uint64_t GetTotalSize() = 0;

  // Return a map containing all tracked files and their corresponding sizes.
  // thread-safe
  virtual std::unordered_map<std::string, uint64_t> GetTrackedFiles() = 0;

  // Return delete rate limit in bytes per second.
  // thread-safe
  virtual int64_t GetDeleteRateBytesPerSecond() = 0;

  // Update the delete rate limit in bytes per second.
  // zero means disable delete rate limiting and delete files immediately
  // thread-safe
  virtual void SetDeleteRateBytesPerSecond(int64_t delete_rate) = 0;

  // Return trash/DB size ratio where new files will be deleted immediately
  // thread-safe
  virtual double GetMaxTrashDBRatio() = 0;

  // Update trash/DB size ratio where new files will be deleted immediately
  // thread-safe
  virtual void SetMaxTrashDBRatio(double ratio) = 0;

  // Return the total size of trash files
  // thread-safe
  virtual uint64_t GetTotalTrashSize() = 0;
};

// Create a new SstFileManager that can be shared among multiple RocksDB
// instances to track SST file and control there deletion rate.
//
// @param env: Pointer to Env object, please see "rocksdb/env.h".
// @param info_log: If not nullptr, info_log will be used to log errors.
//
// == Deletion rate limiting specific arguments ==
// @param trash_dir: Deprecated, this argument have no effect
// @param rate_bytes_per_sec: How many bytes should be deleted per second, If
//    this value is set to 1024 (1 Kb / sec) and we deleted a file of size 4 Kb
//    in 1 second, we will wait for another 3 seconds before we delete other
//    files, Set to 0 to disable deletion rate limiting.
// @param delete_existing_trash: Deprecated, this argument have no effect, but
//    if user provide trash_dir we will schedule deletes for files in the dir
// @param status: If not nullptr, status will contain any errors that happened
//    during creating the missing trash_dir or deleting existing files in trash.
// @param max_trash_db_ratio: If the trash size constitutes for more than this
//    fraction of the total DB size we will start deleting new files passed to
//    DeleteScheduler immediately
// @param bytes_max_delete_chunk: if a file to delete is larger than delete
//    chunk, ftruncate the file by this size each time, rather than dropping the
//    whole file. 0 means to always delete the whole file. If the file has more
//    than one linked names, the file will be deleted as a whole. Either way,
//    `rate_bytes_per_sec` will be appreciated. NOTE that with this option,
//    files already renamed as a trash may be partial, so users should not
//    directly recover them without checking.
extern SstFileManager* NewSstFileManager(
    Env* env, std::shared_ptr<Logger> info_log = nullptr,
    std::string trash_dir = "", int64_t rate_bytes_per_sec = 0,
    bool delete_existing_trash = true, Status* status = nullptr,
    double max_trash_db_ratio = 0.25,
    uint64_t bytes_max_delete_chunk = 64 * 1024 * 1024);

}  // namespace rocksdb
