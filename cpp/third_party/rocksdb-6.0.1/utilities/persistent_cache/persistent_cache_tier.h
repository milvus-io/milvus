//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once

#ifndef ROCKSDB_LITE

#include <limits>
#include <list>
#include <map>
#include <string>
#include <vector>

#include "monitoring/histogram.h"
#include "rocksdb/env.h"
#include "rocksdb/persistent_cache.h"
#include "rocksdb/status.h"

// Persistent Cache
//
// Persistent cache is tiered key-value cache that can use persistent medium. It
// is a generic design and can leverage any storage medium -- disk/SSD/NVM/RAM.
// The code has been kept generic but significant benchmark/design/development
// time has been spent to make sure the cache performs appropriately for
// respective storage medium.
// The file defines
// PersistentCacheTier    : Implementation that handles individual cache tier
// PersistentTieresCache  : Implementation that handles all tiers as a logical
//                          unit
//
// PersistentTieredCache architecture:
// +--------------------------+ PersistentCacheTier that handles multiple tiers
// | +----------------+       |
// | | RAM            | PersistentCacheTier that handles RAM (VolatileCacheImpl)
// | +----------------+       |
// |   | next                 |
// |   v                      |
// | +----------------+       |
// | | NVM            | PersistentCacheTier implementation that handles NVM
// | +----------------+ (BlockCacheImpl)
// |   | next                 |
// |   V                      |
// | +----------------+       |
// | | LE-SSD         | PersistentCacheTier implementation that handles LE-SSD
// | +----------------+ (BlockCacheImpl)
// |   |                      |
// |   V                      |
// |  null                    |
// +--------------------------+
//               |
//               V
//              null
namespace rocksdb {

// Persistent Cache Config
//
// This struct captures all the options that are used to configure persistent
// cache. Some of the terminologies used in naming the options are
//
// dispatch size :
// This is the size in which IO is dispatched to the device
//
// write buffer size :
// This is the size of an individual write buffer size. Write buffers are
// grouped to form buffered file.
//
// cache size :
// This is the logical maximum for the cache size
//
// qdepth :
// This is the max number of IOs that can issues to the device in parallel
//
// pepeling :
// The writer code path follows pipelined architecture, which means the
// operations are handed off from one stage to another
//
// pipelining backlog size :
// With the pipelined architecture, there can always be backlogging of ops in
// pipeline queues. This is the maximum backlog size after which ops are dropped
// from queue
struct PersistentCacheConfig {
  explicit PersistentCacheConfig(
      Env* const _env, const std::string& _path, const uint64_t _cache_size,
      const std::shared_ptr<Logger>& _log,
      const uint32_t _write_buffer_size = 1 * 1024 * 1024 /*1MB*/) {
    env = _env;
    path = _path;
    log = _log;
    cache_size = _cache_size;
    writer_dispatch_size = write_buffer_size = _write_buffer_size;
  }

  //
  // Validate the settings. Our intentions are to catch erroneous settings ahead
  // of time instead going violating invariants or causing dead locks.
  //
  Status ValidateSettings() const {
    // (1) check pre-conditions for variables
    if (!env || path.empty()) {
      return Status::InvalidArgument("empty or null args");
    }

    // (2) assert size related invariants
    // - cache size cannot be less than cache file size
    // - individual write buffer size cannot be greater than cache file size
    // - total write buffer size cannot be less than 2X cache file size
    if (cache_size < cache_file_size || write_buffer_size >= cache_file_size ||
        write_buffer_size * write_buffer_count() < 2 * cache_file_size) {
      return Status::InvalidArgument("invalid cache size");
    }

    // (2) check writer settings
    // - Queue depth cannot be 0
    // - writer_dispatch_size cannot be greater than writer_buffer_size
    // - dispatch size and buffer size need to be aligned
    if (!writer_qdepth || writer_dispatch_size > write_buffer_size ||
        write_buffer_size % writer_dispatch_size) {
      return Status::InvalidArgument("invalid writer settings");
    }

    return Status::OK();
  }

  //
  // Env abstraction to use for systmer level operations
  //
  Env* env;

  //
  // Path for the block cache where blocks are persisted
  //
  std::string path;

  //
  // Log handle for logging messages
  //
  std::shared_ptr<Logger> log;

  //
  // Enable direct IO for reading
  //
  bool enable_direct_reads = true;

  //
  // Enable direct IO for writing
  //
  bool enable_direct_writes = false;

  //
  // Logical cache size
  //
  uint64_t cache_size = std::numeric_limits<uint64_t>::max();

  // cache-file-size
  //
  // Cache consists of multiples of small files. This parameter defines the
  // size of an individual cache file
  //
  // default: 1M
  uint32_t cache_file_size = 100ULL * 1024 * 1024;

  // writer-qdepth
  //
  // The writers can issues IO to the devices in parallel. This parameter
  // controls the max number if IOs that can issues in parallel to the block
  // device
  //
  // default :1
  uint32_t writer_qdepth = 1;

  // pipeline-writes
  //
  // The write optionally follow pipelined architecture. This helps
  // avoid regression in the eviction code path of the primary tier. This
  // parameter defines if pipelining is enabled or disabled
  //
  // default: true
  bool pipeline_writes = true;

  // max-write-pipeline-backlog-size
  //
  // Max pipeline buffer size. This is the maximum backlog we can accumulate
  // while waiting for writes. After the limit, new ops will be dropped.
  //
  // Default: 1GiB
  uint64_t max_write_pipeline_backlog_size = 1ULL * 1024 * 1024 * 1024;

  // write-buffer-size
  //
  // This is the size in which buffer slabs are allocated.
  //
  // Default: 1M
  uint32_t write_buffer_size = 1ULL * 1024 * 1024;

  // write-buffer-count
  //
  // This is the total number of buffer slabs. This is calculated as a factor of
  // file size in order to avoid dead lock.
  size_t write_buffer_count() const {
    assert(write_buffer_size);
    return static_cast<size_t>((writer_qdepth + 1.2) * cache_file_size /
                               write_buffer_size);
  }

  // writer-dispatch-size
  //
  // The writer thread will dispatch the IO at the specified IO size
  //
  // default: 1M
  uint64_t writer_dispatch_size = 1ULL * 1024 * 1024;

  // is_compressed
  //
  // This option determines if the cache will run in compressed mode or
  // uncompressed mode
  bool is_compressed = true;

  PersistentCacheConfig MakePersistentCacheConfig(
      const std::string& path, const uint64_t size,
      const std::shared_ptr<Logger>& log);

  std::string ToString() const;
};

// Persistent Cache Tier
//
// This a logical abstraction that defines a tier of the persistent cache. Tiers
// can be stacked over one another. PersistentCahe provides the basic definition
// for accessing/storing in the cache. PersistentCacheTier extends the interface
// to enable management and stacking of tiers.
class PersistentCacheTier : public PersistentCache {
 public:
  typedef std::shared_ptr<PersistentCacheTier> Tier;

  virtual ~PersistentCacheTier() {}

  // Open the persistent cache tier
  virtual Status Open();

  // Close the persistent cache tier
  virtual Status Close();

  // Reserve space up to 'size' bytes
  virtual bool Reserve(const size_t size);

  // Erase a key from the cache
  virtual bool Erase(const Slice& key);

  // Print stats to string recursively
  virtual std::string PrintStats();

  virtual PersistentCache::StatsType Stats() override;

  // Insert to page cache
  virtual Status Insert(const Slice& page_key, const char* data,
                        const size_t size) override = 0;

  // Lookup page cache by page identifier
  virtual Status Lookup(const Slice& page_key, std::unique_ptr<char[]>* data,
                        size_t* size) override = 0;

  // Does it store compressed data ?
  virtual bool IsCompressed() override = 0;

  virtual std::string GetPrintableOptions() const override = 0;

  // Return a reference to next tier
  virtual Tier& next_tier() { return next_tier_; }

  // Set the value for next tier
  virtual void set_next_tier(const Tier& tier) {
    assert(!next_tier_);
    next_tier_ = tier;
  }

  virtual void TEST_Flush() {
    if (next_tier_) {
      next_tier_->TEST_Flush();
    }
  }

 private:
  Tier next_tier_;  // next tier
};

// PersistentTieredCache
//
// Abstraction that helps you construct a tiers of persistent caches as a
// unified cache. The tier(s) of cache will act a single tier for management
// ease and support PersistentCache methods for accessing data.
class PersistentTieredCache : public PersistentCacheTier {
 public:
  virtual ~PersistentTieredCache();

  Status Open() override;
  Status Close() override;
  bool Erase(const Slice& key) override;
  std::string PrintStats() override;
  PersistentCache::StatsType Stats() override;
  Status Insert(const Slice& page_key, const char* data,
                const size_t size) override;
  Status Lookup(const Slice& page_key, std::unique_ptr<char[]>* data,
                size_t* size) override;
  bool IsCompressed() override;

  std::string GetPrintableOptions() const override {
    return "PersistentTieredCache";
  }

  void AddTier(const Tier& tier);

  Tier& next_tier() override {
    auto it = tiers_.end();
    return (*it)->next_tier();
  }

  void set_next_tier(const Tier& tier) override {
    auto it = tiers_.end();
    (*it)->set_next_tier(tier);
  }

  void TEST_Flush() override {
    assert(!tiers_.empty());
    tiers_.front()->TEST_Flush();
    PersistentCacheTier::TEST_Flush();
  }

 protected:
  std::list<Tier> tiers_;  // list of tiers top-down
};

}  // namespace rocksdb

#endif
