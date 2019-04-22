//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#ifndef ROCKSDB_LITE

#ifndef  OS_WIN
#include <unistd.h>
#endif // ! OS_WIN

#include <atomic>
#include <list>
#include <memory>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>

#include "rocksdb/cache.h"
#include "rocksdb/comparator.h"
#include "rocksdb/persistent_cache.h"

#include "utilities/persistent_cache/block_cache_tier_file.h"
#include "utilities/persistent_cache/block_cache_tier_metadata.h"
#include "utilities/persistent_cache/persistent_cache_util.h"

#include "memtable/skiplist.h"
#include "monitoring/histogram.h"
#include "port/port.h"
#include "util/arena.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/mutexlock.h"

namespace rocksdb {

//
// Block cache tier implementation
//
class BlockCacheTier : public PersistentCacheTier {
 public:
  explicit BlockCacheTier(const PersistentCacheConfig& opt)
      : opt_(opt),
        insert_ops_(static_cast<size_t>(opt_.max_write_pipeline_backlog_size)),
        buffer_allocator_(opt.write_buffer_size, opt.write_buffer_count()),
        writer_(this, opt_.writer_qdepth, static_cast<size_t>(opt_.writer_dispatch_size)) {
    Info(opt_.log, "Initializing allocator. size=%d B count=%d",
         opt_.write_buffer_size, opt_.write_buffer_count());
  }

  virtual ~BlockCacheTier() {
    // Close is re-entrant so we can call close even if it is already closed
    Close();
    assert(!insert_th_.joinable());
  }

  Status Insert(const Slice& key, const char* data, const size_t size) override;
  Status Lookup(const Slice& key, std::unique_ptr<char[]>* data,
                size_t* size) override;
  Status Open() override;
  Status Close() override;
  bool Erase(const Slice& key) override;
  bool Reserve(const size_t size) override;

  bool IsCompressed() override { return opt_.is_compressed; }

  std::string GetPrintableOptions() const override { return opt_.ToString(); }

  PersistentCache::StatsType Stats() override;

  void TEST_Flush() override {
    while (insert_ops_.Size()) {
      /* sleep override */
      Env::Default()->SleepForMicroseconds(1000000);
    }
  }

 private:
  // Percentage of cache to be evicted when the cache is full
  static const size_t kEvictPct = 10;
  // Max attempts to insert key, value to cache in pipelined mode
  static const size_t kMaxRetry = 3;

  // Pipelined operation
  struct InsertOp {
    explicit InsertOp(const bool signal) : signal_(signal) {}
    explicit InsertOp(std::string&& key, const std::string& data)
        : key_(std::move(key)), data_(data) {}
    ~InsertOp() {}

    InsertOp() = delete;
    InsertOp(InsertOp&& /*rhs*/) = default;
    InsertOp& operator=(InsertOp&& rhs) = default;

    // used for estimating size by bounded queue
    size_t Size() { return data_.size() + key_.size(); }

    std::string key_;
    std::string data_;
    const bool signal_ = false;  // signal to request processing thread to exit
  };

  // entry point for insert thread
  void InsertMain();
  // insert implementation
  Status InsertImpl(const Slice& key, const Slice& data);
  // Create a new cache file
  Status NewCacheFile();
  // Get cache directory path
  std::string GetCachePath() const { return opt_.path + "/cache"; }
  // Cleanup folder
  Status CleanupCacheFolder(const std::string& folder);

  // Statistics
  struct Statistics {
    HistogramImpl bytes_pipelined_;
    HistogramImpl bytes_written_;
    HistogramImpl bytes_read_;
    HistogramImpl read_hit_latency_;
    HistogramImpl read_miss_latency_;
    HistogramImpl write_latency_;
    std::atomic<uint64_t> cache_hits_{0};
    std::atomic<uint64_t> cache_misses_{0};
    std::atomic<uint64_t> cache_errors_{0};
    std::atomic<uint64_t> insert_dropped_{0};

    double CacheHitPct() const {
      const auto lookups = cache_hits_ + cache_misses_;
      return lookups ? 100 * cache_hits_ / static_cast<double>(lookups) : 0.0;
    }

    double CacheMissPct() const {
      const auto lookups = cache_hits_ + cache_misses_;
      return lookups ? 100 * cache_misses_ / static_cast<double>(lookups) : 0.0;
    }
  };

  port::RWMutex lock_;                          // Synchronization
  const PersistentCacheConfig opt_;             // BlockCache options
  BoundedQueue<InsertOp> insert_ops_;           // Ops waiting for insert
  rocksdb::port::Thread insert_th_;                       // Insert thread
  uint32_t writer_cache_id_ = 0;                // Current cache file identifier
  WriteableCacheFile* cache_file_ = nullptr;    // Current cache file reference
  CacheWriteBufferAllocator buffer_allocator_;  // Buffer provider
  ThreadedWriter writer_;                       // Writer threads
  BlockCacheTierMetadata metadata_;             // Cache meta data manager
  std::atomic<uint64_t> size_{0};               // Size of the cache
  Statistics stats_;                                 // Statistics
};

}  // namespace rocksdb

#endif
