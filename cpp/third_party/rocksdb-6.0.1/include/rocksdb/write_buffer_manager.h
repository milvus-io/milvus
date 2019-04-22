//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBufferManager is for managing memory allocation for one or more
// MemTables.

#pragma once

#include <atomic>
#include <cstddef>
#include "rocksdb/cache.h"

namespace rocksdb {

class WriteBufferManager {
 public:
  // _buffer_size = 0 indicates no limit. Memory won't be capped.
  // memory_usage() won't be valid and ShouldFlush() will always return true.
  // if `cache` is provided, we'll put dummy entries in the cache and cost
  // the memory allocated to the cache. It can be used even if _buffer_size = 0.
  explicit WriteBufferManager(size_t _buffer_size,
                              std::shared_ptr<Cache> cache = {});
  ~WriteBufferManager();

  bool enabled() const { return buffer_size_ != 0; }

  bool cost_to_cache() const { return cache_rep_ != nullptr; }

  // Only valid if enabled()
  size_t memory_usage() const {
    return memory_used_.load(std::memory_order_relaxed);
  }
  size_t mutable_memtable_memory_usage() const {
    return memory_active_.load(std::memory_order_relaxed);
  }
  size_t buffer_size() const { return buffer_size_; }

  // Should only be called from write thread
  bool ShouldFlush() const {
    if (enabled()) {
      if (mutable_memtable_memory_usage() > mutable_limit_) {
        return true;
      }
      if (memory_usage() >= buffer_size_ &&
          mutable_memtable_memory_usage() >= buffer_size_ / 2) {
        // If the memory exceeds the buffer size, we trigger more aggressive
        // flush. But if already more than half memory is being flushed,
        // triggering more flush may not help. We will hold it instead.
        return true;
      }
    }
    return false;
  }

  void ReserveMem(size_t mem) {
    if (cache_rep_ != nullptr) {
      ReserveMemWithCache(mem);
    } else if (enabled()) {
      memory_used_.fetch_add(mem, std::memory_order_relaxed);
    }
    if (enabled()) {
      memory_active_.fetch_add(mem, std::memory_order_relaxed);
    }
  }
  // We are in the process of freeing `mem` bytes, so it is not considered
  // when checking the soft limit.
  void ScheduleFreeMem(size_t mem) {
    if (enabled()) {
      memory_active_.fetch_sub(mem, std::memory_order_relaxed);
    }
  }
  void FreeMem(size_t mem) {
    if (cache_rep_ != nullptr) {
      FreeMemWithCache(mem);
    } else if (enabled()) {
      memory_used_.fetch_sub(mem, std::memory_order_relaxed);
    }
  }

 private:
  const size_t buffer_size_;
  const size_t mutable_limit_;
  std::atomic<size_t> memory_used_;
  // Memory that hasn't been scheduled to free.
  std::atomic<size_t> memory_active_;
  struct CacheRep;
  std::unique_ptr<CacheRep> cache_rep_;

  void ReserveMemWithCache(size_t mem);
  void FreeMemWithCache(size_t mem);

  // No copying allowed
  WriteBufferManager(const WriteBufferManager&) = delete;
  WriteBufferManager& operator=(const WriteBufferManager&) = delete;
};
}  // namespace rocksdb
