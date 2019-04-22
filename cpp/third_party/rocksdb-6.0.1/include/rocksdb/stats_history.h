// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <map>
#include <string>

// #include "db/db_impl.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"

namespace rocksdb {

class DBImpl;

class StatsHistoryIterator {
 public:
  StatsHistoryIterator() {}
  virtual ~StatsHistoryIterator() {}

  virtual bool Valid() const = 0;

  // Moves to the next stats history record.  After this call, Valid() is
  // true iff the iterator was not positioned at the last entry in the source.
  // REQUIRES: Valid()
  virtual void Next() = 0;

  // Return the time stamp (in microseconds) when stats history is recorded.
  // REQUIRES: Valid()
  virtual uint64_t GetStatsTime() const = 0;

  // Return the current stats history as an std::map which specifies the
  // mapping from stats name to stats value . The underlying storage
  // for the returned map is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual const std::map<std::string, uint64_t>& GetStatsMap() const = 0;

  // If an error has occurred, return it.  Else return an ok status.
  virtual Status status() const = 0;
};

}  // namespace rocksdb
