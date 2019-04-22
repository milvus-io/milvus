//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once

#ifndef ROCKSDB_LITE

#include <atomic>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

#include "rocksdb/cache.h"
#include "utilities/persistent_cache/hash_table.h"
#include "utilities/persistent_cache/hash_table_evictable.h"
#include "utilities/persistent_cache/persistent_cache_tier.h"

// VolatileCacheTier
//
// This file provides persistent cache tier implementation for caching
// key/values in RAM.
//
//        key/values
//           |
//           V
// +-------------------+
// | VolatileCacheTier | Store in an evictable hash table
// +-------------------+
//           |
//           V
//       on eviction
//   pushed to next tier
//
// The implementation is designed to be concurrent. The evictable hash table
// implementation is not concurrent at this point though.
//
// The eviction algorithm is LRU
namespace rocksdb {

class VolatileCacheTier : public PersistentCacheTier {
 public:
  explicit VolatileCacheTier(
      const bool is_compressed = true,
      const size_t max_size = std::numeric_limits<size_t>::max())
      : is_compressed_(is_compressed), max_size_(max_size) {}

  virtual ~VolatileCacheTier();

  // insert to cache
  Status Insert(const Slice& page_key, const char* data,
                const size_t size) override;
  // lookup key in cache
  Status Lookup(const Slice& page_key, std::unique_ptr<char[]>* data,
                size_t* size) override;

  // is compressed cache ?
  bool IsCompressed() override { return is_compressed_; }

  // erase key from cache
  bool Erase(const Slice& key) override;

  std::string GetPrintableOptions() const override {
    return "VolatileCacheTier";
  }

  // Expose stats as map
  PersistentCache::StatsType Stats() override;

 private:
  //
  // Cache data abstraction
  //
  struct CacheData : LRUElement<CacheData> {
    explicit CacheData(CacheData&& rhs) ROCKSDB_NOEXCEPT
        : key(std::move(rhs.key)),
          value(std::move(rhs.value)) {}

    explicit CacheData(const std::string& _key, const std::string& _value = "")
        : key(_key), value(_value) {}

    virtual ~CacheData() {}

    const std::string key;
    const std::string value;
  };

  static void DeleteCacheData(CacheData* data);

  //
  // Index and LRU definition
  //
  struct CacheDataHash {
    uint64_t operator()(const CacheData* obj) const {
      assert(obj);
      return std::hash<std::string>()(obj->key);
    }
  };

  struct CacheDataEqual {
    bool operator()(const CacheData* lhs, const CacheData* rhs) const {
      assert(lhs);
      assert(rhs);
      return lhs->key == rhs->key;
    }
  };

  struct Statistics {
    std::atomic<uint64_t> cache_misses_{0};
    std::atomic<uint64_t> cache_hits_{0};
    std::atomic<uint64_t> cache_inserts_{0};
    std::atomic<uint64_t> cache_evicts_{0};

    double CacheHitPct() const {
      auto lookups = cache_hits_ + cache_misses_;
      return lookups ? 100 * cache_hits_ / static_cast<double>(lookups) : 0.0;
    }

    double CacheMissPct() const {
      auto lookups = cache_hits_ + cache_misses_;
      return lookups ? 100 * cache_misses_ / static_cast<double>(lookups) : 0.0;
    }
  };

  typedef EvictableHashTable<CacheData, CacheDataHash, CacheDataEqual>
      IndexType;

  // Evict LRU tail
  bool Evict();

  const bool is_compressed_ = true;    // does it store compressed data
  IndexType index_;                    // in-memory cache
  std::atomic<uint64_t> max_size_{0};  // Maximum size of the cache
  std::atomic<uint64_t> size_{0};      // Size of the cache
  Statistics stats_;
};

}  // namespace rocksdb

#endif
