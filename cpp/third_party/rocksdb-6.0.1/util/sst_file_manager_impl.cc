//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/sst_file_manager_impl.h"

#include <vector>

#include "db/db_impl.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/sst_file_manager.h"
#include "util/mutexlock.h"
#include "util/sync_point.h"

namespace rocksdb {

#ifndef ROCKSDB_LITE
SstFileManagerImpl::SstFileManagerImpl(Env* env, std::shared_ptr<Logger> logger,
                                       int64_t rate_bytes_per_sec,
                                       double max_trash_db_ratio,
                                       uint64_t bytes_max_delete_chunk)
    : env_(env),
      logger_(logger),
      total_files_size_(0),
      in_progress_files_size_(0),
      compaction_buffer_size_(0),
      cur_compactions_reserved_size_(0),
      max_allowed_space_(0),
      delete_scheduler_(env, rate_bytes_per_sec, logger.get(), this,
                        max_trash_db_ratio, bytes_max_delete_chunk),
      cv_(&mu_),
      closing_(false),
      bg_thread_(nullptr),
      reserved_disk_buffer_(0),
      free_space_trigger_(0),
      cur_instance_(nullptr) {
}

SstFileManagerImpl::~SstFileManagerImpl() {
  Close();
}

void SstFileManagerImpl::Close() {
  {
    MutexLock l(&mu_);
    if (closing_) {
      return;
    }
    closing_ = true;
    cv_.SignalAll();
  }
  if (bg_thread_) {
    bg_thread_->join();
  }
}

Status SstFileManagerImpl::OnAddFile(const std::string& file_path,
                                     bool compaction) {
  uint64_t file_size;
  Status s = env_->GetFileSize(file_path, &file_size);
  if (s.ok()) {
    MutexLock l(&mu_);
    OnAddFileImpl(file_path, file_size, compaction);
  }
  TEST_SYNC_POINT("SstFileManagerImpl::OnAddFile");
  return s;
}

Status SstFileManagerImpl::OnDeleteFile(const std::string& file_path) {
  {
    MutexLock l(&mu_);
    OnDeleteFileImpl(file_path);
  }
  TEST_SYNC_POINT("SstFileManagerImpl::OnDeleteFile");
  return Status::OK();
}

void SstFileManagerImpl::OnCompactionCompletion(Compaction* c) {
  MutexLock l(&mu_);
  uint64_t size_added_by_compaction = 0;
  for (size_t i = 0; i < c->num_input_levels(); i++) {
    for (size_t j = 0; j < c->num_input_files(i); j++) {
      FileMetaData* filemeta = c->input(i, j);
      size_added_by_compaction += filemeta->fd.GetFileSize();
    }
  }
  cur_compactions_reserved_size_ -= size_added_by_compaction;

  auto new_files = c->edit()->GetNewFiles();
  for (auto& new_file : new_files) {
    auto fn = TableFileName(c->immutable_cf_options()->cf_paths,
                            new_file.second.fd.GetNumber(),
                            new_file.second.fd.GetPathId());
    if (in_progress_files_.find(fn) != in_progress_files_.end()) {
      auto tracked_file = tracked_files_.find(fn);
      assert(tracked_file != tracked_files_.end());
      in_progress_files_size_ -= tracked_file->second;
      in_progress_files_.erase(fn);
    }
  }
}

Status SstFileManagerImpl::OnMoveFile(const std::string& old_path,
                                      const std::string& new_path,
                                      uint64_t* file_size) {
  {
    MutexLock l(&mu_);
    if (file_size != nullptr) {
      *file_size = tracked_files_[old_path];
    }
    OnAddFileImpl(new_path, tracked_files_[old_path], false);
    OnDeleteFileImpl(old_path);
  }
  TEST_SYNC_POINT("SstFileManagerImpl::OnMoveFile");
  return Status::OK();
}

void SstFileManagerImpl::SetMaxAllowedSpaceUsage(uint64_t max_allowed_space) {
  MutexLock l(&mu_);
  max_allowed_space_ = max_allowed_space;
}

void SstFileManagerImpl::SetCompactionBufferSize(
    uint64_t compaction_buffer_size) {
  MutexLock l(&mu_);
  compaction_buffer_size_ = compaction_buffer_size;
}

bool SstFileManagerImpl::IsMaxAllowedSpaceReached() {
  MutexLock l(&mu_);
  if (max_allowed_space_ <= 0) {
    return false;
  }
  return total_files_size_ >= max_allowed_space_;
}

bool SstFileManagerImpl::IsMaxAllowedSpaceReachedIncludingCompactions() {
  MutexLock l(&mu_);
  if (max_allowed_space_ <= 0) {
    return false;
  }
  return total_files_size_ + cur_compactions_reserved_size_ >=
         max_allowed_space_;
}

bool SstFileManagerImpl::EnoughRoomForCompaction(
    ColumnFamilyData* cfd, const std::vector<CompactionInputFiles>& inputs,
    Status bg_error) {
  MutexLock l(&mu_);
  uint64_t size_added_by_compaction = 0;
  // First check if we even have the space to do the compaction
  for (size_t i = 0; i < inputs.size(); i++) {
    for (size_t j = 0; j < inputs[i].size(); j++) {
      FileMetaData* filemeta = inputs[i][j];
      size_added_by_compaction += filemeta->fd.GetFileSize();
    }
  }

  // Update cur_compactions_reserved_size_ so concurrent compaction
  // don't max out space
  size_t needed_headroom =
      cur_compactions_reserved_size_ + size_added_by_compaction +
      compaction_buffer_size_;
  if (max_allowed_space_ != 0 &&
      (needed_headroom + total_files_size_ > max_allowed_space_)) {
    return false;
  }

  // Implement more aggressive checks only if this DB instance has already
  // seen a NoSpace() error. This is tin order to contain a single potentially
  // misbehaving DB instance and prevent it from slowing down compactions of
  // other DB instances
  if (CheckFreeSpace() && bg_error == Status::NoSpace()) {
    auto fn =
        TableFileName(cfd->ioptions()->cf_paths, inputs[0][0]->fd.GetNumber(),
                      inputs[0][0]->fd.GetPathId());
    uint64_t free_space = 0;
    env_->GetFreeSpace(fn, &free_space);
    // needed_headroom is based on current size reserved by compactions,
    // minus any files created by running compactions as they would count
    // against the reserved size. If user didn't specify any compaction
    // buffer, add reserved_disk_buffer_ that's calculated by default so the
    // compaction doesn't end up leaving nothing for logs and flush SSTs
    if (compaction_buffer_size_ == 0) {
      needed_headroom += reserved_disk_buffer_;
    }
    needed_headroom -= in_progress_files_size_;
    if (free_space < needed_headroom + size_added_by_compaction) {
      // We hit the condition of not enough disk space
      ROCKS_LOG_ERROR(logger_, "free space [%d bytes] is less than "
          "needed headroom [%d bytes]\n", free_space, needed_headroom);
      return false;
    }
  }

  cur_compactions_reserved_size_ += size_added_by_compaction;
  // Take a snapshot of cur_compactions_reserved_size_ for when we encounter
  // a NoSpace error.
  free_space_trigger_ = cur_compactions_reserved_size_;
  return true;
}

uint64_t SstFileManagerImpl::GetCompactionsReservedSize() {
  MutexLock l(&mu_);
  return cur_compactions_reserved_size_;
}

uint64_t SstFileManagerImpl::GetTotalSize() {
  MutexLock l(&mu_);
  return total_files_size_;
}

std::unordered_map<std::string, uint64_t>
SstFileManagerImpl::GetTrackedFiles() {
  MutexLock l(&mu_);
  return tracked_files_;
}

int64_t SstFileManagerImpl::GetDeleteRateBytesPerSecond() {
  return delete_scheduler_.GetRateBytesPerSecond();
}

void SstFileManagerImpl::SetDeleteRateBytesPerSecond(int64_t delete_rate) {
  return delete_scheduler_.SetRateBytesPerSecond(delete_rate);
}

double SstFileManagerImpl::GetMaxTrashDBRatio() {
  return delete_scheduler_.GetMaxTrashDBRatio();
}

void SstFileManagerImpl::SetMaxTrashDBRatio(double r) {
  return delete_scheduler_.SetMaxTrashDBRatio(r);
}

uint64_t SstFileManagerImpl::GetTotalTrashSize() {
  return delete_scheduler_.GetTotalTrashSize();
}

void SstFileManagerImpl::ReserveDiskBuffer(uint64_t size,
                                           const std::string& path) {
  MutexLock l(&mu_);

  reserved_disk_buffer_ += size;
  if (path_.empty()) {
    path_ = path;
  }
}

void SstFileManagerImpl::ClearError() {
  while (true) {
    MutexLock l(&mu_);

    if (closing_) {
      return;
    }

    uint64_t free_space;
    Status s = env_->GetFreeSpace(path_, &free_space);
    if (s.ok()) {
      // In case of multi-DB instances, some of them may have experienced a
      // soft error and some a hard error. In the SstFileManagerImpl, a hard
      // error will basically override previously reported soft errors. Once
      // we clear the hard error, we don't keep track of previous errors for
      // now
      if (bg_err_.severity() == Status::Severity::kHardError) {
        if (free_space < reserved_disk_buffer_) {
          ROCKS_LOG_ERROR(logger_, "free space [%d bytes] is less than "
              "required disk buffer [%d bytes]\n", free_space,
              reserved_disk_buffer_);
          ROCKS_LOG_ERROR(logger_, "Cannot clear hard error\n");
          s = Status::NoSpace();
        }
      } else if (bg_err_.severity() == Status::Severity::kSoftError) {
        if (free_space < free_space_trigger_) {
          ROCKS_LOG_WARN(logger_, "free space [%d bytes] is less than "
              "free space for compaction trigger [%d bytes]\n", free_space,
              free_space_trigger_);
          ROCKS_LOG_WARN(logger_, "Cannot clear soft error\n");
          s = Status::NoSpace();
        }
      }
    }

    // Someone could have called CancelErrorRecovery() and the list could have
    // become empty, so check again here
    if (s.ok() && !error_handler_list_.empty()) {
      auto error_handler = error_handler_list_.front();
      // Since we will release the mutex, set cur_instance_ to signal to the
      // shutdown thread, if it calls // CancelErrorRecovery() the meantime,
      // to indicate that this DB instance is busy. The DB instance is
      // guaranteed to not be deleted before RecoverFromBGError() returns,
      // since the ErrorHandler::recovery_in_prog_ flag would be true
      cur_instance_ = error_handler;
      mu_.Unlock();
      s = error_handler->RecoverFromBGError();
      mu_.Lock();
      // The DB instance might have been deleted while we were
      // waiting for the mutex, so check cur_instance_ to make sure its
      // still non-null
      if (cur_instance_) {
        // Check for error again, since the instance may have recovered but
        // immediately got another error. If that's the case, and the new
        // error is also a NoSpace() non-fatal error, leave the instance in
        // the list
        Status err = cur_instance_->GetBGError();
        if (s.ok() && err == Status::NoSpace() &&
            err.severity() < Status::Severity::kFatalError) {
          s = err;
        }
        cur_instance_ = nullptr;
      }

      if (s.ok() || s.IsShutdownInProgress() ||
          (!s.ok() && s.severity() >= Status::Severity::kFatalError)) {
        // If shutdown is in progress, abandon this handler instance
        // and continue with the others
        error_handler_list_.pop_front();
      }
    }

    if (!error_handler_list_.empty()) {
      // If there are more instances to be recovered, reschedule after 5
      // seconds
      int64_t wait_until = env_->NowMicros() + 5000000;
      cv_.TimedWait(wait_until);
    }

    // Check again for error_handler_list_ empty, as a DB instance shutdown
    // could have removed it from the queue while we were in timed wait
    if (error_handler_list_.empty()) {
      ROCKS_LOG_INFO(logger_, "Clearing error\n");
      bg_err_ = Status::OK();
      return;
    }
  }
}

void SstFileManagerImpl::StartErrorRecovery(ErrorHandler* handler,
                                            Status bg_error) {
  MutexLock l(&mu_);
  if (bg_error.severity() == Status::Severity::kSoftError) {
    if (bg_err_.ok()) {
      // Setting bg_err_ basically means we're in degraded mode
      // Assume that all pending compactions will fail similarly. The trigger
      // for clearing this condition is set to current compaction reserved
      // size, so we stop checking disk space available in
      // EnoughRoomForCompaction once this much free space is available
      bg_err_ = bg_error;
    }
  } else if (bg_error.severity() == Status::Severity::kHardError) {
    bg_err_ = bg_error;
  } else {
    assert(false);
  }

  // If this is the first instance of this error, kick of a thread to poll
  // and recover from this condition
  if (error_handler_list_.empty()) {
    error_handler_list_.push_back(handler);
    // Release lock before calling join. Its ok to do so because
    // error_handler_list_ is now non-empty, so no other invocation of this
    // function will execute this piece of code
    mu_.Unlock();
    if (bg_thread_) {
      bg_thread_->join();
    }
    // Start a new thread. The previous one would have exited.
    bg_thread_.reset(new port::Thread(&SstFileManagerImpl::ClearError, this));
    mu_.Lock();
  } else {
    // Check if this DB instance is already in the list
    for (auto iter = error_handler_list_.begin();
         iter != error_handler_list_.end(); ++iter) {
      if ((*iter) == handler) {
        return;
      }
    }
    error_handler_list_.push_back(handler);
  }
}

bool SstFileManagerImpl::CancelErrorRecovery(ErrorHandler* handler) {
  MutexLock l(&mu_);

  if (cur_instance_ == handler) {
    // This instance is currently busy attempting to recover
    // Nullify it so the recovery thread doesn't attempt to access it again
    cur_instance_ = nullptr;
    return false;
  }

  for (auto iter = error_handler_list_.begin();
       iter != error_handler_list_.end(); ++iter) {
    if ((*iter) == handler) {
      error_handler_list_.erase(iter);
      return true;
    }
  }
  return false;
}

Status SstFileManagerImpl::ScheduleFileDeletion(
    const std::string& file_path, const std::string& path_to_sync,
    const bool force_bg) {
  TEST_SYNC_POINT("SstFileManagerImpl::ScheduleFileDeletion");
  return delete_scheduler_.DeleteFile(file_path, path_to_sync,
                                      force_bg);
}

void SstFileManagerImpl::WaitForEmptyTrash() {
  delete_scheduler_.WaitForEmptyTrash();
}

void SstFileManagerImpl::OnAddFileImpl(const std::string& file_path,
                                       uint64_t file_size, bool compaction) {
  auto tracked_file = tracked_files_.find(file_path);
  if (tracked_file != tracked_files_.end()) {
    // File was added before, we will just update the size
    assert(!compaction);
    total_files_size_ -= tracked_file->second;
    total_files_size_ += file_size;
    cur_compactions_reserved_size_ -= file_size;
  } else {
    total_files_size_ += file_size;
    if (compaction) {
      // Keep track of the size of files created by in-progress compactions.
      // When calculating whether there's enough headroom for new compactions,
      // this will be subtracted from cur_compactions_reserved_size_.
      // Otherwise, compactions will be double counted.
      in_progress_files_size_ += file_size;
      in_progress_files_.insert(file_path);
    }
  }
  tracked_files_[file_path] = file_size;
}

void SstFileManagerImpl::OnDeleteFileImpl(const std::string& file_path) {
  auto tracked_file = tracked_files_.find(file_path);
  if (tracked_file == tracked_files_.end()) {
    // File is not tracked
    assert(in_progress_files_.find(file_path) == in_progress_files_.end());
    return;
  }

  total_files_size_ -= tracked_file->second;
  // Check if it belonged to an in-progress compaction
  if (in_progress_files_.find(file_path) != in_progress_files_.end()) {
    in_progress_files_size_ -= tracked_file->second;
    in_progress_files_.erase(file_path);
  }
  tracked_files_.erase(tracked_file);
}

SstFileManager* NewSstFileManager(Env* env, std::shared_ptr<Logger> info_log,
                                  std::string trash_dir,
                                  int64_t rate_bytes_per_sec,
                                  bool delete_existing_trash, Status* status,
                                  double max_trash_db_ratio,
                                  uint64_t bytes_max_delete_chunk) {
  SstFileManagerImpl* res =
      new SstFileManagerImpl(env, info_log, rate_bytes_per_sec,
                             max_trash_db_ratio, bytes_max_delete_chunk);

  // trash_dir is deprecated and not needed anymore, but if user passed it
  // we will still remove files in it.
  Status s;
  if (delete_existing_trash && trash_dir != "") {
    std::vector<std::string> files_in_trash;
    s = env->GetChildren(trash_dir, &files_in_trash);
    if (s.ok()) {
      for (const std::string& trash_file : files_in_trash) {
        if (trash_file == "." || trash_file == "..") {
          continue;
        }

        std::string path_in_trash = trash_dir + "/" + trash_file;
        res->OnAddFile(path_in_trash);
        Status file_delete =
            res->ScheduleFileDeletion(path_in_trash, trash_dir);
        if (s.ok() && !file_delete.ok()) {
          s = file_delete;
        }
      }
    }
  }

  if (status) {
    *status = s;
  }

  return res;
}

#else

SstFileManager* NewSstFileManager(Env* /*env*/,
                                  std::shared_ptr<Logger> /*info_log*/,
                                  std::string /*trash_dir*/,
                                  int64_t /*rate_bytes_per_sec*/,
                                  bool /*delete_existing_trash*/,
                                  Status* status, double /*max_trash_db_ratio*/,
                                  uint64_t /*bytes_max_delete_chunk*/) {
  if (status) {
    *status =
        Status::NotSupported("SstFileManager is not supported in ROCKSDB_LITE");
  }
  return nullptr;
}

#endif  // ROCKSDB_LITE

}  // namespace rocksdb
