//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "db/logs_with_prep_tracker.h"

#include "port/likely.h"

namespace rocksdb {
void LogsWithPrepTracker::MarkLogAsHavingPrepSectionFlushed(uint64_t log) {
  assert(log != 0);
  std::lock_guard<std::mutex> lock(prepared_section_completed_mutex_);
  auto it = prepared_section_completed_.find(log);
  if (UNLIKELY(it == prepared_section_completed_.end())) {
    prepared_section_completed_[log] = 1;
  } else {
    it->second += 1;
  }
}

void LogsWithPrepTracker::MarkLogAsContainingPrepSection(uint64_t log) {
  assert(log != 0);
  std::lock_guard<std::mutex> lock(logs_with_prep_mutex_);

  auto rit = logs_with_prep_.rbegin();
  bool updated = false;
  // Most probably the last log is the one that is being marked for
  // having a prepare section; so search from the end.
  for (; rit != logs_with_prep_.rend() && rit->log >= log; ++rit) {
    if (rit->log == log) {
      rit->cnt++;
      updated = true;
      break;
    }
  }
  if (!updated) {
    // We are either at the start, or at a position with rit->log < log
    logs_with_prep_.insert(rit.base(), {log, 1});
  }
}

uint64_t LogsWithPrepTracker::FindMinLogContainingOutstandingPrep() {
  std::lock_guard<std::mutex> lock(logs_with_prep_mutex_);
  auto it = logs_with_prep_.begin();
  // start with the smallest log
  for (; it != logs_with_prep_.end();) {
    auto min_log = it->log;
    {
      std::lock_guard<std::mutex> lock2(prepared_section_completed_mutex_);
      auto completed_it = prepared_section_completed_.find(min_log);
      if (completed_it == prepared_section_completed_.end() ||
          completed_it->second < it->cnt) {
        return min_log;
      }
      assert(completed_it != prepared_section_completed_.end() &&
             completed_it->second == it->cnt);
      prepared_section_completed_.erase(completed_it);
    }
    // erase from beginning in vector is not efficient but this function is not
    // on the fast path.
    it = logs_with_prep_.erase(it);
  }
  // no such log found
  return 0;
}
}  // namespace rocksdb
