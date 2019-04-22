// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <stdint.h>
#include <string>

#include "rocksdb/perf_level.h"

// A thread local context for gathering io-stats efficiently and transparently.
// Use SetPerfLevel(PerfLevel::kEnableTime) to enable time stats.

namespace rocksdb {

struct IOStatsContext {
  // reset all io-stats counter to zero
  void Reset();

  std::string ToString(bool exclude_zero_counters = false) const;

  // the thread pool id
  uint64_t thread_pool_id;

  // number of bytes that has been written.
  uint64_t bytes_written;
  // number of bytes that has been read.
  uint64_t bytes_read;

  // time spent in open() and fopen().
  uint64_t open_nanos;
  // time spent in fallocate().
  uint64_t allocate_nanos;
  // time spent in write() and pwrite().
  uint64_t write_nanos;
  // time spent in read() and pread()
  uint64_t read_nanos;
  // time spent in sync_file_range().
  uint64_t range_sync_nanos;
  // time spent in fsync
  uint64_t fsync_nanos;
  // time spent in preparing write (fallocate etc).
  uint64_t prepare_write_nanos;
  // time spent in Logger::Logv().
  uint64_t logger_nanos;
  // CPU time spent in write() and pwrite()
  uint64_t cpu_write_nanos;
  // CPU time spent in read() and pread()
  uint64_t cpu_read_nanos;
};

// Get Thread-local IOStatsContext object pointer
IOStatsContext* get_iostats_context();

}  // namespace rocksdb
