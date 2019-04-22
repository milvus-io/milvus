//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "util/delete_scheduler.h"

#include <thread>
#include <vector>

#include "port/port.h"
#include "rocksdb/env.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/sst_file_manager_impl.h"
#include "util/sync_point.h"

namespace rocksdb {

DeleteScheduler::DeleteScheduler(Env* env, int64_t rate_bytes_per_sec,
                                 Logger* info_log,
                                 SstFileManagerImpl* sst_file_manager,
                                 double max_trash_db_ratio,
                                 uint64_t bytes_max_delete_chunk)
    : env_(env),
      total_trash_size_(0),
      rate_bytes_per_sec_(rate_bytes_per_sec),
      pending_files_(0),
      bytes_max_delete_chunk_(bytes_max_delete_chunk),
      closing_(false),
      cv_(&mu_),
      info_log_(info_log),
      sst_file_manager_(sst_file_manager),
      max_trash_db_ratio_(max_trash_db_ratio) {
  assert(sst_file_manager != nullptr);
  assert(max_trash_db_ratio >= 0);
  bg_thread_.reset(
      new port::Thread(&DeleteScheduler::BackgroundEmptyTrash, this));
}

DeleteScheduler::~DeleteScheduler() {
  {
    InstrumentedMutexLock l(&mu_);
    closing_ = true;
    cv_.SignalAll();
  }
  if (bg_thread_) {
    bg_thread_->join();
  }
}

Status DeleteScheduler::DeleteFile(const std::string& file_path,
                                   const std::string& dir_to_sync,
                                   const bool force_bg) {
  Status s;
  if (rate_bytes_per_sec_.load() <= 0 || (!force_bg &&
      total_trash_size_.load() >
          sst_file_manager_->GetTotalSize() * max_trash_db_ratio_.load())) {
    // Rate limiting is disabled or trash size makes up more than
    // max_trash_db_ratio_ (default 25%) of the total DB size
    TEST_SYNC_POINT("DeleteScheduler::DeleteFile");
    s = env_->DeleteFile(file_path);
    if (s.ok()) {
      sst_file_manager_->OnDeleteFile(file_path);
    }
    return s;
  }

  // Move file to trash
  std::string trash_file;
  s = MarkAsTrash(file_path, &trash_file);

  if (!s.ok()) {
    ROCKS_LOG_ERROR(info_log_, "Failed to mark %s as trash", file_path.c_str());
    s = env_->DeleteFile(file_path);
    if (s.ok()) {
      sst_file_manager_->OnDeleteFile(file_path);
    }
    return s;
  }

  // Update the total trash size
  uint64_t trash_file_size = 0;
  env_->GetFileSize(trash_file, &trash_file_size);
  total_trash_size_.fetch_add(trash_file_size);

  // Add file to delete queue
  {
    InstrumentedMutexLock l(&mu_);
    queue_.emplace(trash_file, dir_to_sync);
    pending_files_++;
    if (pending_files_ == 1) {
      cv_.SignalAll();
    }
  }
  return s;
}

std::map<std::string, Status> DeleteScheduler::GetBackgroundErrors() {
  InstrumentedMutexLock l(&mu_);
  return bg_errors_;
}

const std::string DeleteScheduler::kTrashExtension = ".trash";
bool DeleteScheduler::IsTrashFile(const std::string& file_path) {
  return (file_path.size() >= kTrashExtension.size() &&
          file_path.rfind(kTrashExtension) ==
              file_path.size() - kTrashExtension.size());
}

Status DeleteScheduler::CleanupDirectory(Env* env, SstFileManagerImpl* sfm,
                                         const std::string& path) {
  Status s;
  // Check if there are any files marked as trash in this path
  std::vector<std::string> files_in_path;
  s = env->GetChildren(path, &files_in_path);
  if (!s.ok()) {
    return s;
  }
  for (const std::string& current_file : files_in_path) {
    if (!DeleteScheduler::IsTrashFile(current_file)) {
      // not a trash file, skip
      continue;
    }

    Status file_delete;
    std::string trash_file = path + "/" + current_file;
    if (sfm) {
      // We have an SstFileManager that will schedule the file delete
      sfm->OnAddFile(trash_file);
      file_delete = sfm->ScheduleFileDeletion(trash_file, path);
    } else {
      // Delete the file immediately
      file_delete = env->DeleteFile(trash_file);
    }

    if (s.ok() && !file_delete.ok()) {
      s = file_delete;
    }
  }

  return s;
}

Status DeleteScheduler::MarkAsTrash(const std::string& file_path,
                                    std::string* trash_file) {
  // Sanity check of the path
  size_t idx = file_path.rfind("/");
  if (idx == std::string::npos || idx == file_path.size() - 1) {
    return Status::InvalidArgument("file_path is corrupted");
  }

  Status s;
  if (DeleteScheduler::IsTrashFile(file_path)) {
    // This is already a trash file
    *trash_file = file_path;
    return s;
  }

  *trash_file = file_path + kTrashExtension;
  // TODO(tec) : Implement Env::RenameFileIfNotExist and remove
  //             file_move_mu mutex.
  int cnt = 0;
  InstrumentedMutexLock l(&file_move_mu_);
  while (true) {
    s = env_->FileExists(*trash_file);
    if (s.IsNotFound()) {
      // We found a path for our file in trash
      s = env_->RenameFile(file_path, *trash_file);
      break;
    } else if (s.ok()) {
      // Name conflict, generate new random suffix
      *trash_file = file_path + std::to_string(cnt) + kTrashExtension;
    } else {
      // Error during FileExists call, we cannot continue
      break;
    }
    cnt++;
  }
  if (s.ok()) {
    sst_file_manager_->OnMoveFile(file_path, *trash_file);
  }
  return s;
}

void DeleteScheduler::BackgroundEmptyTrash() {
  TEST_SYNC_POINT("DeleteScheduler::BackgroundEmptyTrash");

  while (true) {
    InstrumentedMutexLock l(&mu_);
    while (queue_.empty() && !closing_) {
      cv_.Wait();
    }

    if (closing_) {
      return;
    }

    // Delete all files in queue_
    uint64_t start_time = env_->NowMicros();
    uint64_t total_deleted_bytes = 0;
    int64_t current_delete_rate = rate_bytes_per_sec_.load();
    while (!queue_.empty() && !closing_) {
      if (current_delete_rate != rate_bytes_per_sec_.load()) {
        // User changed the delete rate
        current_delete_rate = rate_bytes_per_sec_.load();
        start_time = env_->NowMicros();
        total_deleted_bytes = 0;
      }

      // Get new file to delete
      const FileAndDir& fad = queue_.front();
      std::string path_in_trash = fad.fname;

      // We dont need to hold the lock while deleting the file
      mu_.Unlock();
      uint64_t deleted_bytes = 0;
      bool is_complete = true;
      // Delete file from trash and update total_penlty value
      Status s =
          DeleteTrashFile(path_in_trash, fad.dir, &deleted_bytes, &is_complete);
      total_deleted_bytes += deleted_bytes;
      mu_.Lock();
      if (is_complete) {
        queue_.pop();
      }

      if (!s.ok()) {
        bg_errors_[path_in_trash] = s;
      }

      // Apply penlty if necessary
      uint64_t total_penlty;
      if (current_delete_rate > 0) {
        // rate limiting is enabled
        total_penlty =
            ((total_deleted_bytes * kMicrosInSecond) / current_delete_rate);
        while (!closing_ && !cv_.TimedWait(start_time + total_penlty)) {}
      } else {
        // rate limiting is disabled
        total_penlty = 0;
      }
      TEST_SYNC_POINT_CALLBACK("DeleteScheduler::BackgroundEmptyTrash:Wait",
                               &total_penlty);

      if (is_complete) {
        pending_files_--;
      }
      if (pending_files_ == 0) {
        // Unblock WaitForEmptyTrash since there are no more files waiting
        // to be deleted
        cv_.SignalAll();
      }
    }
  }
}

Status DeleteScheduler::DeleteTrashFile(const std::string& path_in_trash,
                                        const std::string& dir_to_sync,
                                        uint64_t* deleted_bytes,
                                        bool* is_complete) {
  uint64_t file_size;
  Status s = env_->GetFileSize(path_in_trash, &file_size);
  *is_complete = true;
  TEST_SYNC_POINT("DeleteScheduler::DeleteTrashFile:DeleteFile");
  if (s.ok()) {
    bool need_full_delete = true;
    if (bytes_max_delete_chunk_ != 0 && file_size > bytes_max_delete_chunk_) {
      uint64_t num_hard_links = 2;
      // We don't have to worry aobut data race between linking a new
      // file after the number of file link check and ftruncte because
      // the file is now in trash and no hardlink is supposed to create
      // to trash files by RocksDB.
      Status my_status = env_->NumFileLinks(path_in_trash, &num_hard_links);
      if (my_status.ok()) {
        if (num_hard_links == 1) {
          std::unique_ptr<WritableFile> wf;
          my_status =
              env_->ReopenWritableFile(path_in_trash, &wf, EnvOptions());
          if (my_status.ok()) {
            my_status = wf->Truncate(file_size - bytes_max_delete_chunk_);
            if (my_status.ok()) {
              TEST_SYNC_POINT("DeleteScheduler::DeleteTrashFile:Fsync");
              my_status = wf->Fsync();
            }
          }
          if (my_status.ok()) {
            *deleted_bytes = bytes_max_delete_chunk_;
            need_full_delete = false;
            *is_complete = false;
          } else {
            ROCKS_LOG_WARN(info_log_,
                           "Failed to partially delete %s from trash -- %s",
                           path_in_trash.c_str(), my_status.ToString().c_str());
          }
        } else {
          ROCKS_LOG_INFO(info_log_,
                         "Cannot delete %s slowly through ftruncate from trash "
                         "as it has other links",
                         path_in_trash.c_str());
        }
      } else if (!num_link_error_printed_) {
        ROCKS_LOG_INFO(
            info_log_,
            "Cannot delete files slowly through ftruncate from trash "
            "as Env::NumFileLinks() returns error: %s",
            my_status.ToString().c_str());
        num_link_error_printed_ = true;
      }
    }

    if (need_full_delete) {
      s = env_->DeleteFile(path_in_trash);
      if (!dir_to_sync.empty()) {
        std::unique_ptr<Directory> dir_obj;
        if (s.ok()) {
          s = env_->NewDirectory(dir_to_sync, &dir_obj);
        }
        if (s.ok()) {
          s = dir_obj->Fsync();
          TEST_SYNC_POINT_CALLBACK(
              "DeleteScheduler::DeleteTrashFile::AfterSyncDir",
              reinterpret_cast<void*>(const_cast<std::string*>(&dir_to_sync)));
        }
      }
      *deleted_bytes = file_size;
      sst_file_manager_->OnDeleteFile(path_in_trash);
    }
  }
  if (!s.ok()) {
    // Error while getting file size or while deleting
    ROCKS_LOG_ERROR(info_log_, "Failed to delete %s from trash -- %s",
                    path_in_trash.c_str(), s.ToString().c_str());
    *deleted_bytes = 0;
  } else {
    total_trash_size_.fetch_sub(*deleted_bytes);
  }

  return s;
}

void DeleteScheduler::WaitForEmptyTrash() {
  InstrumentedMutexLock l(&mu_);
  while (pending_files_ > 0 && !closing_) {
    cv_.Wait();
  }
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
