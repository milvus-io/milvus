//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/write_buffer_manager.h"
#include <mutex>
#include "util/coding.h"

namespace rocksdb {
#ifndef ROCKSDB_LITE
namespace {
const size_t kSizeDummyEntry = 1024 * 1024;
// The key will be longer than keys for blocks in SST files so they won't
// conflict.
const size_t kCacheKeyPrefix = kMaxVarint64Length * 4 + 1;
}  // namespace

struct WriteBufferManager::CacheRep {
  std::shared_ptr<Cache> cache_;
  std::mutex cache_mutex_;
  std::atomic<size_t> cache_allocated_size_;
  // The non-prefix part will be updated according to the ID to use.
  char cache_key_[kCacheKeyPrefix + kMaxVarint64Length];
  uint64_t next_cache_key_id_ = 0;
  std::vector<Cache::Handle*> dummy_handles_;

  explicit CacheRep(std::shared_ptr<Cache> cache)
      : cache_(cache), cache_allocated_size_(0) {
    memset(cache_key_, 0, kCacheKeyPrefix);
    size_t pointer_size = sizeof(const void*);
    assert(pointer_size <= kCacheKeyPrefix);
    memcpy(cache_key_, static_cast<const void*>(this), pointer_size);
  }

  Slice GetNextCacheKey() {
    memset(cache_key_ + kCacheKeyPrefix, 0, kMaxVarint64Length);
    char* end =
        EncodeVarint64(cache_key_ + kCacheKeyPrefix, next_cache_key_id_++);
    return Slice(cache_key_, static_cast<size_t>(end - cache_key_));
  }
};
#else
struct WriteBufferManager::CacheRep {};
#endif  // ROCKSDB_LITE

WriteBufferManager::WriteBufferManager(size_t _buffer_size,
                                       std::shared_ptr<Cache> cache)
    : buffer_size_(_buffer_size),
      mutable_limit_(buffer_size_ * 7 / 8),
      memory_used_(0),
      memory_active_(0),
      cache_rep_(nullptr) {
#ifndef ROCKSDB_LITE
  if (cache) {
    // Construct the cache key using the pointer to this.
    cache_rep_.reset(new CacheRep(cache));
  }
#else
  (void)cache;
#endif  // ROCKSDB_LITE
}

WriteBufferManager::~WriteBufferManager() {
#ifndef ROCKSDB_LITE
  if (cache_rep_) {
    for (auto* handle : cache_rep_->dummy_handles_) {
      cache_rep_->cache_->Release(handle, true);
    }
  }
#endif  // ROCKSDB_LITE
}

// Should only be called from write thread
void WriteBufferManager::ReserveMemWithCache(size_t mem) {
#ifndef ROCKSDB_LITE
  assert(cache_rep_ != nullptr);
  // Use a mutex to protect various data structures. Can be optimized to a
  // lock-free solution if it ends up with a performance bottleneck.
  std::lock_guard<std::mutex> lock(cache_rep_->cache_mutex_);

  size_t new_mem_used = memory_used_.load(std::memory_order_relaxed) + mem;
  memory_used_.store(new_mem_used, std::memory_order_relaxed);
  while (new_mem_used > cache_rep_->cache_allocated_size_) {
    // Expand size by at least 1MB.
    // Add a dummy record to the cache
    Cache::Handle* handle;
    cache_rep_->cache_->Insert(cache_rep_->GetNextCacheKey(), nullptr,
                               kSizeDummyEntry, nullptr, &handle);
    cache_rep_->dummy_handles_.push_back(handle);
    cache_rep_->cache_allocated_size_ += kSizeDummyEntry;
  }
#else
  (void)mem;
#endif  // ROCKSDB_LITE
}

void WriteBufferManager::FreeMemWithCache(size_t mem) {
#ifndef ROCKSDB_LITE
  assert(cache_rep_ != nullptr);
  // Use a mutex to protect various data structures. Can be optimized to a
  // lock-free solution if it ends up with a performance bottleneck.
  std::lock_guard<std::mutex> lock(cache_rep_->cache_mutex_);
  size_t new_mem_used = memory_used_.load(std::memory_order_relaxed) - mem;
  memory_used_.store(new_mem_used, std::memory_order_relaxed);
  // Gradually shrink memory costed in the block cache if the actual
  // usage is less than 3/4 of what we reserve from the block cache.
  // We do this because:
  // 1. we don't pay the cost of the block cache immediately a memtable is
  //    freed, as block cache insert is expensive;
  // 2. eventually, if we walk away from a temporary memtable size increase,
  //    we make sure shrink the memory costed in block cache over time.
  // In this way, we only shrink costed memory showly even there is enough
  // margin.
  if (new_mem_used < cache_rep_->cache_allocated_size_ / 4 * 3 &&
      cache_rep_->cache_allocated_size_ - kSizeDummyEntry > new_mem_used) {
    assert(!cache_rep_->dummy_handles_.empty());
    cache_rep_->cache_->Release(cache_rep_->dummy_handles_.back(), true);
    cache_rep_->dummy_handles_.pop_back();
    cache_rep_->cache_allocated_size_ -= kSizeDummyEntry;
  }
#else
  (void)mem;
#endif  // ROCKSDB_LITE
}
}  // namespace rocksdb
