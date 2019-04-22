//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "db/error_handler.h"
#include "db/db_impl.h"
#include "db/event_helpers.h"
#include "util/sst_file_manager_impl.h"

namespace rocksdb {

// Maps to help decide the severity of an error based on the
// BackgroundErrorReason, Code, SubCode and whether db_options.paranoid_checks
// is set or not. There are 3 maps, going from most specific to least specific
// (i.e from all 4 fields in a tuple to only the BackgroundErrorReason and
// paranoid_checks). The less specific map serves as a catch all in case we miss
// a specific error code or subcode.
std::map<std::tuple<BackgroundErrorReason, Status::Code, Status::SubCode, bool>,
         Status::Severity>
    ErrorSeverityMap = {
        // Errors during BG compaction
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kIOError, Status::SubCode::kNoSpace,
                         true),
         Status::Severity::kSoftError},
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kIOError, Status::SubCode::kNoSpace,
                         false),
         Status::Severity::kNoError},
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kIOError, Status::SubCode::kSpaceLimit,
                         true),
         Status::Severity::kHardError},
        // Errors during BG flush
        {std::make_tuple(BackgroundErrorReason::kFlush, Status::Code::kIOError,
                         Status::SubCode::kNoSpace, true),
         Status::Severity::kHardError},
        {std::make_tuple(BackgroundErrorReason::kFlush, Status::Code::kIOError,
                         Status::SubCode::kNoSpace, false),
         Status::Severity::kNoError},
        {std::make_tuple(BackgroundErrorReason::kFlush, Status::Code::kIOError,
                         Status::SubCode::kSpaceLimit, true),
         Status::Severity::kHardError},
        // Errors during Write
        {std::make_tuple(BackgroundErrorReason::kWriteCallback,
                         Status::Code::kIOError, Status::SubCode::kNoSpace,
                         true),
         Status::Severity::kHardError},
        {std::make_tuple(BackgroundErrorReason::kWriteCallback,
                         Status::Code::kIOError, Status::SubCode::kNoSpace,
                         false),
         Status::Severity::kHardError},
};

std::map<std::tuple<BackgroundErrorReason, Status::Code, bool>, Status::Severity>
    DefaultErrorSeverityMap = {
        // Errors during BG compaction
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kCorruption, true),
         Status::Severity::kUnrecoverableError},
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kCorruption, false),
         Status::Severity::kNoError},
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kIOError, true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kIOError, false),
         Status::Severity::kNoError},
        // Errors during BG flush
        {std::make_tuple(BackgroundErrorReason::kFlush,
                         Status::Code::kCorruption, true),
         Status::Severity::kUnrecoverableError},
        {std::make_tuple(BackgroundErrorReason::kFlush,
                         Status::Code::kCorruption, false),
         Status::Severity::kNoError},
        {std::make_tuple(BackgroundErrorReason::kFlush,
                         Status::Code::kIOError, true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kFlush,
                         Status::Code::kIOError, false),
         Status::Severity::kNoError},
        // Errors during Write
        {std::make_tuple(BackgroundErrorReason::kWriteCallback,
                         Status::Code::kCorruption, true),
         Status::Severity::kUnrecoverableError},
        {std::make_tuple(BackgroundErrorReason::kWriteCallback,
                         Status::Code::kCorruption, false),
         Status::Severity::kNoError},
        {std::make_tuple(BackgroundErrorReason::kWriteCallback,
                         Status::Code::kIOError, true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kWriteCallback,
                         Status::Code::kIOError, false),
         Status::Severity::kNoError},
};

std::map<std::tuple<BackgroundErrorReason, bool>, Status::Severity>
    DefaultReasonMap = {
        // Errors during BG compaction
        {std::make_tuple(BackgroundErrorReason::kCompaction, true),
          Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kCompaction, false),
          Status::Severity::kNoError},
        // Errors during BG flush
        {std::make_tuple(BackgroundErrorReason::kFlush, true),
          Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kFlush, false),
          Status::Severity::kNoError},
        // Errors during Write
        {std::make_tuple(BackgroundErrorReason::kWriteCallback, true),
          Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kWriteCallback, false),
          Status::Severity::kFatalError},
        // Errors during Memtable update
        {std::make_tuple(BackgroundErrorReason::kMemTable, true),
          Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kMemTable, false),
          Status::Severity::kFatalError},
};

void ErrorHandler::CancelErrorRecovery() {
#ifndef ROCKSDB_LITE
  db_mutex_->AssertHeld();

  // We'll release the lock before calling sfm, so make sure no new
  // recovery gets scheduled at that point
  auto_recovery_ = false;
  SstFileManagerImpl* sfm = reinterpret_cast<SstFileManagerImpl*>(
      db_options_.sst_file_manager.get());
  if (sfm) {
    // This may or may not cancel a pending recovery
    db_mutex_->Unlock();
    bool cancelled = sfm->CancelErrorRecovery(this);
    db_mutex_->Lock();
    if (cancelled) {
      recovery_in_prog_ = false;
    }
  }
#endif
}

// This is the main function for looking at an error during a background
// operation and deciding the severity, and error recovery strategy. The high
// level algorithm is as follows -
// 1. Classify the severity of the error based on the ErrorSeverityMap,
//    DefaultErrorSeverityMap and DefaultReasonMap defined earlier
// 2. Call a Status code specific override function to adjust the severity
//    if needed. The reason for this is our ability to recover may depend on
//    the exact options enabled in DBOptions
// 3. Determine if auto recovery is possible. A listener notification callback
//    is called, which can disable the auto recovery even if we decide its
//    feasible
// 4. For Status::NoSpace() errors, rely on SstFileManagerImpl to control
//    the actual recovery. If no sst file manager is specified in DBOptions,
//    a default one is allocated during DB::Open(), so there will always be
//    one.
// This can also get called as part of a recovery operation. In that case, we
// also track the error separately in recovery_error_ so we can tell in the
// end whether recovery succeeded or not
Status ErrorHandler::SetBGError(const Status& bg_err, BackgroundErrorReason reason) {
  db_mutex_->AssertHeld();

  if (bg_err.ok()) {
    return Status::OK();
  }

  // Check if recovery is currently in progress. If it is, we will save this
  // error so we can check it at the end to see if recovery succeeded or not
  if (recovery_in_prog_ && recovery_error_.ok()) {
    recovery_error_ = bg_err;
  }

  bool paranoid = db_options_.paranoid_checks;
  Status::Severity sev = Status::Severity::kFatalError;
  Status new_bg_err;
  bool found = false;

  {
    auto entry = ErrorSeverityMap.find(std::make_tuple(reason, bg_err.code(),
          bg_err.subcode(), paranoid));
    if (entry != ErrorSeverityMap.end()) {
      sev = entry->second;
      found = true;
    }
  }

  if (!found) {
    auto entry = DefaultErrorSeverityMap.find(std::make_tuple(reason,
          bg_err.code(), paranoid));
    if (entry != DefaultErrorSeverityMap.end()) {
      sev = entry->second;
      found = true;
    }
  }

  if (!found) {
    auto entry = DefaultReasonMap.find(std::make_tuple(reason, paranoid));
    if (entry != DefaultReasonMap.end()) {
      sev = entry->second;
    }
  }

  new_bg_err = Status(bg_err, sev);

  bool auto_recovery = auto_recovery_;
  if (new_bg_err.severity() >= Status::Severity::kFatalError && auto_recovery) {
    auto_recovery = false;
    ;
  }

  // Allow some error specific overrides
  if (new_bg_err == Status::NoSpace()) {
    new_bg_err = OverrideNoSpaceError(new_bg_err, &auto_recovery);
  }

  if (!new_bg_err.ok()) {
    Status s = new_bg_err;
    EventHelpers::NotifyOnBackgroundError(db_options_.listeners, reason, &s,
                                          db_mutex_, &auto_recovery);
    if (!s.ok() && (s.severity() > bg_error_.severity())) {
      bg_error_ = s;
    } else {
      // This error is less severe than previously encountered error. Don't
      // take any further action
      return bg_error_;
    }
  }

  if (auto_recovery) {
    recovery_in_prog_ = true;

    // Kick-off error specific recovery
    if (bg_error_ == Status::NoSpace()) {
      RecoverFromNoSpace();
    }
  }
  return bg_error_;
}

Status ErrorHandler::OverrideNoSpaceError(Status bg_error,
                                          bool* auto_recovery) {
#ifndef ROCKSDB_LITE
  if (bg_error.severity() >= Status::Severity::kFatalError) {
    return bg_error;
  }

  if (db_options_.sst_file_manager.get() == nullptr) {
    // We rely on SFM to poll for enough disk space and recover
    *auto_recovery = false;
    return bg_error;
  }

  if (db_options_.allow_2pc &&
      (bg_error.severity() <= Status::Severity::kSoftError)) {
    // Don't know how to recover, as the contents of the current WAL file may
    // be inconsistent, and it may be needed for 2PC. If 2PC is not enabled,
    // we can just flush the memtable and discard the log
    *auto_recovery = false;
    return Status(bg_error, Status::Severity::kFatalError);
  }

  {
    uint64_t free_space;
    if (db_options_.env->GetFreeSpace(db_options_.db_paths[0].path,
                                      &free_space) == Status::NotSupported()) {
      *auto_recovery = false;
    }
  }

  return bg_error;
#else
  (void)auto_recovery;
  return Status(bg_error, Status::Severity::kFatalError);
#endif
}

void ErrorHandler::RecoverFromNoSpace() {
#ifndef ROCKSDB_LITE
  SstFileManagerImpl* sfm =
      reinterpret_cast<SstFileManagerImpl*>(db_options_.sst_file_manager.get());

  // Inform SFM of the error, so it can kick-off the recovery
  if (sfm) {
    sfm->StartErrorRecovery(this, bg_error_);
  }
#endif
}

Status ErrorHandler::ClearBGError() {
#ifndef ROCKSDB_LITE
  db_mutex_->AssertHeld();

  // Signal that recovery succeeded
  if (recovery_error_.ok()) {
    Status old_bg_error = bg_error_;
    bg_error_ = Status::OK();
    recovery_in_prog_ = false;
    EventHelpers::NotifyOnErrorRecoveryCompleted(db_options_.listeners,
                                                 old_bg_error, db_mutex_);
  }
  return recovery_error_;
#else
  return bg_error_;
#endif
}

Status ErrorHandler::RecoverFromBGError(bool is_manual) {
#ifndef ROCKSDB_LITE
  InstrumentedMutexLock l(db_mutex_);
  if (is_manual) {
    // If its a manual recovery and there's a background recovery in progress
    // return busy status
    if (recovery_in_prog_) {
      return Status::Busy();
    }
    recovery_in_prog_ = true;
  }

  if (bg_error_.severity() == Status::Severity::kSoftError) {
    // Simply clear the background error and return
    recovery_error_ = Status::OK();
    return ClearBGError();
  }

  // Reset recovery_error_. We will use this to record any errors that happen
  // during the recovery process. While recovering, the only operations that
  // can generate background errors should be the flush operations
  recovery_error_ = Status::OK();
  Status s = db_->ResumeImpl();
  // For manual recover, shutdown, and fatal error  cases, set
  // recovery_in_prog_ to false. For automatic background recovery, leave it
  // as is regardless of success or failure as it will be retried
  if (is_manual || s.IsShutdownInProgress() ||
      bg_error_.severity() >= Status::Severity::kFatalError) {
    recovery_in_prog_ = false;
  }
  return s;
#else
  (void)is_manual;
  return bg_error_;
#endif
}
}
