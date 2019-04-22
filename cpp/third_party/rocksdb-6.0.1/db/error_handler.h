//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include "monitoring/instrumented_mutex.h"
#include "options/db_options.h"
#include "rocksdb/listener.h"
#include "rocksdb/status.h"

namespace rocksdb {

class DBImpl;

class ErrorHandler {
  public:
   ErrorHandler(DBImpl* db, const ImmutableDBOptions& db_options,
                InstrumentedMutex* db_mutex)
       : db_(db),
         db_options_(db_options),
         bg_error_(Status::OK()),
         recovery_error_(Status::OK()),
         db_mutex_(db_mutex),
         auto_recovery_(false),
         recovery_in_prog_(false) {}
   ~ErrorHandler() {}

   void EnableAutoRecovery() { auto_recovery_ = true; }

   Status::Severity GetErrorSeverity(BackgroundErrorReason reason,
                                     Status::Code code,
                                     Status::SubCode subcode);

   Status SetBGError(const Status& bg_err, BackgroundErrorReason reason);

   Status GetBGError() { return bg_error_; }

   Status GetRecoveryError() { return recovery_error_; }

   Status ClearBGError();

   bool IsDBStopped() {
     return !bg_error_.ok() &&
            bg_error_.severity() >= Status::Severity::kHardError;
    }

    bool IsBGWorkStopped() {
      return !bg_error_.ok() &&
             (bg_error_.severity() >= Status::Severity::kHardError ||
              !auto_recovery_);
    }

    bool IsRecoveryInProgress() { return recovery_in_prog_; }

    Status RecoverFromBGError(bool is_manual = false);
    void CancelErrorRecovery();

   private:
    DBImpl* db_;
    const ImmutableDBOptions& db_options_;
    Status bg_error_;
    // A separate Status variable used to record any errors during the
    // recovery process from hard errors
    Status recovery_error_;
    InstrumentedMutex* db_mutex_;
    // A flag indicating whether automatic recovery from errors is enabled
    bool auto_recovery_;
    bool recovery_in_prog_;

    Status OverrideNoSpaceError(Status bg_error, bool* auto_recovery);
    void RecoverFromNoSpace();
};

}
