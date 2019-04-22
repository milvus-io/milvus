// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"
#include "db/in_memory_stats_history.h"

namespace rocksdb {

InMemoryStatsHistoryIterator::~InMemoryStatsHistoryIterator() {}

bool InMemoryStatsHistoryIterator::Valid() const { return valid_; }

Status InMemoryStatsHistoryIterator::status() const { return status_; }

void InMemoryStatsHistoryIterator::Next() {
  // increment start_time by 1 to avoid infinite loop
  AdvanceIteratorByTime(GetStatsTime() + 1, end_time_);
}

uint64_t InMemoryStatsHistoryIterator::GetStatsTime() const { return time_; }

const std::map<std::string, uint64_t>&
InMemoryStatsHistoryIterator::GetStatsMap() const {
  return stats_map_;
}

// advance the iterator to the next time between [start_time, end_time)
// if success, update time_ and stats_map_ with new_time and stats_map
void InMemoryStatsHistoryIterator::AdvanceIteratorByTime(uint64_t start_time,
                                                         uint64_t end_time) {
  // try to find next entry in stats_history_ map
  if (db_impl_ != nullptr) {
    valid_ =
        db_impl_->FindStatsByTime(start_time, end_time, &time_, &stats_map_);
  } else {
    valid_ = false;
  }
}

}  // namespace rocksdb
