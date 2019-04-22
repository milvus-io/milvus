// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <stdint.h>
#include <memory>
#include <string>
#include "rocksdb/cache.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"

namespace rocksdb {

class SimCache;

// For instrumentation purpose, use NewSimCache instead of NewLRUCache API
// NewSimCache is a wrapper function returning a SimCache instance that can
// have additional interface provided in Simcache class besides Cache interface
// to predict block cache hit rate without actually allocating the memory. It
// can help users tune their current block cache size, and determine how
// efficient they are using the memory.
//
// Since GetSimCapacity() returns the capacity for simulutation, it differs from
// actual memory usage, which can be estimated as:
// sim_capacity * entry_size / (entry_size + block_size),
// where 76 <= entry_size <= 104,
// BlockBasedTableOptions.block_size = 4096 by default but is configurable,
// Therefore, generally the actual memory overhead of SimCache is Less than
// sim_capacity * 2%
extern std::shared_ptr<SimCache> NewSimCache(std::shared_ptr<Cache> cache,
                                             size_t sim_capacity,
                                             int num_shard_bits);

class SimCache : public Cache {
 public:
  SimCache() {}

  ~SimCache() override {}

  const char* Name() const override { return "SimCache"; }

  // returns the maximum configured capacity of the simcache for simulation
  virtual size_t GetSimCapacity() const = 0;

  // simcache doesn't provide internal handler reference to user, so always
  // PinnedUsage = 0 and the behavior will be not exactly consistent the
  // with real cache.
  // returns the memory size for the entries residing in the simcache.
  virtual size_t GetSimUsage() const = 0;

  // sets the maximum configured capacity of the simcache. When the new
  // capacity is less than the old capacity and the existing usage is
  // greater than new capacity, the implementation will purge old entries
  // to fit new capapicty.
  virtual void SetSimCapacity(size_t capacity) = 0;

  // returns the lookup times of simcache
  virtual uint64_t get_miss_counter() const = 0;
  // returns the hit times of simcache
  virtual uint64_t get_hit_counter() const = 0;
  // reset the lookup and hit counters
  virtual void reset_counter() = 0;
  // String representation of the statistics of the simcache
  virtual std::string ToString() const = 0;

  // Start storing logs of the cache activity (Add/Lookup) into
  // a file located at activity_log_file, max_logging_size option can be used to
  // stop logging to the file automatically after reaching a specific size in
  // bytes, a values of 0 disable this feature
  virtual Status StartActivityLogging(const std::string& activity_log_file,
                                      Env* env, uint64_t max_logging_size = 0) = 0;

  // Stop cache activity logging if any
  virtual void StopActivityLogging() = 0;

  // Status of cache logging happening in background
  virtual Status GetActivityLoggingStatus() = 0;

 private:
  SimCache(const SimCache&);
  SimCache& operator=(const SimCache&);
};

}  // namespace rocksdb
