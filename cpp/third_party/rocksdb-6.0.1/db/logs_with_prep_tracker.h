//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once

#include <stdint.h>
#include <cassert>
#include <cstdlib>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace rocksdb {

// This class is used to track the log files with outstanding prepare entries.
class LogsWithPrepTracker {
 public:
  // Called when a transaction prepared in `log` has been committed or aborted.
  void MarkLogAsHavingPrepSectionFlushed(uint64_t log);
  // Called when a transaction is prepared in `log`.
  void MarkLogAsContainingPrepSection(uint64_t log);
  // Return the earliest log file with outstanding prepare entries.
  uint64_t FindMinLogContainingOutstandingPrep();
  size_t TEST_PreparedSectionCompletedSize() {
    return prepared_section_completed_.size();
  }
  size_t TEST_LogsWithPrepSize() { return logs_with_prep_.size(); }

 private:
  // REQUIRES: logs_with_prep_mutex_ held
  //
  // sorted list of log numbers still containing prepared data.
  // this is used by FindObsoleteFiles to determine which
  // flushed logs we must keep around because they still
  // contain prepared data which has not been committed or rolled back
  struct LogCnt {
    uint64_t log;  // the log number
    uint64_t cnt;  // number of prepared sections in the log
  };
  std::vector<LogCnt> logs_with_prep_;
  std::mutex logs_with_prep_mutex_;

  // REQUIRES: prepared_section_completed_mutex_ held
  //
  // to be used in conjunction with logs_with_prep_.
  // once a transaction with data in log L is committed or rolled back
  // rather than updating logs_with_prep_ directly we keep track of that
  // in prepared_section_completed_ which maps LOG -> instance_count. This helps
  // avoiding contention between a commit thread and the prepare threads.
  //
  // when trying to determine the minimum log still active we first
  // consult logs_with_prep_. while that root value maps to
  // an equal value in prepared_section_completed_ we erase the log from
  // both logs_with_prep_ and prepared_section_completed_.
  std::unordered_map<uint64_t, uint64_t> prepared_section_completed_;
  std::mutex prepared_section_completed_mutex_;

};
}  // namespace rocksdb
