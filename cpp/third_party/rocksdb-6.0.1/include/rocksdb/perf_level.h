// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <stdint.h>
#include <string>

namespace rocksdb {

// How much perf stats to collect. Affects perf_context and iostats_context.
enum PerfLevel : unsigned char {
  kUninitialized = 0,             // unknown setting
  kDisable = 1,                   // disable perf stats
  kEnableCount = 2,               // enable only count stats
  kEnableTimeExceptForMutex = 3,  // Other than count stats, also enable time
                                  // stats except for mutexes
  // Other than time, also measure CPU time counters. Still don't measure
  // time (neither wall time nor CPU time) for mutexes.
  kEnableTimeAndCPUTimeExceptForMutex = 4,
  kEnableTime = 5,  // enable count and time stats
  kOutOfBounds = 6  // N.B. Must always be the last value!
};

// set the perf stats level for current thread
void SetPerfLevel(PerfLevel level);

// get current perf stats level for current thread
PerfLevel GetPerfLevel();

}  // namespace rocksdb
