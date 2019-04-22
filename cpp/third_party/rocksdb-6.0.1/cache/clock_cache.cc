//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "cache/clock_cache.h"

#ifndef SUPPORT_CLOCK_CACHE

namespace rocksdb {

std::shared_ptr<Cache> NewClockCache(size_t /*capacity*/, int /*num_shard_bits*/,
                                     bool /*strict_capacity_limit*/) {
  // Clock cache not supported.
  return nullptr;
}

}  // namespace rocksdb

#else

#include <assert.h>
#include <atomic>
#include <deque>

// "tbb/concurrent_hash_map.h" requires RTTI if exception is enabled.
// Disable it so users can chooose to disable RTTI.
#ifndef ROCKSDB_USE_RTTI
#define TBB_USE_EXCEPTIONS 0
#endif
#include "tbb/concurrent_hash_map.h"

#include "cache/sharded_cache.h"
#include "port/port.h"
#include "util/autovector.h"
#include "util/mutexlock.h"

namespace rocksdb {

namespace {

// An implementation of the Cache interface based on CLOCK algorithm, with
// better concurrent performance than LRUCache. The idea of CLOCK algorithm
// is to maintain all cache entries in a circular list, and an iterator
// (the "head") pointing to the last examined entry. Eviction starts from the
// current head. Each entry is given a second chance before eviction, if it
// has been access since last examine. In contrast to LRU, no modification
// to the internal data-structure (except for flipping the usage bit) needs
// to be done upon lookup. This gives us oppertunity to implement a cache
// with better concurrency.
//
// Each cache entry is represented by a cache handle, and all the handles
// are arranged in a circular list, as describe above. Upon erase of an entry,
// we never remove the handle. Instead, the handle is put into a recycle bin
// to be re-use. This is to avoid memory dealocation, which is hard to deal
// with in concurrent environment.
//
// The cache also maintains a concurrent hash map for lookup. Any concurrent
// hash map implementation should do the work. We currently use
// tbb::concurrent_hash_map because it supports concurrent erase.
//
// Each cache handle has the following flags and counters, which are squeeze
// in an atomic interger, to make sure the handle always be in a consistent
// state:
//
//   * In-cache bit: whether the entry is reference by the cache itself. If
//     an entry is in cache, its key would also be available in the hash map.
//   * Usage bit: whether the entry has been access by user since last
//     examine for eviction. Can be reset by eviction.
//   * Reference count: reference count by user.
//
// An entry can be reference only when it's in cache. An entry can be evicted
// only when it is in cache, has no usage since last examine, and reference
// count is zero.
//
// The follow figure shows a possible layout of the cache. Boxes represents
// cache handles and numbers in each box being in-cache bit, usage bit and
// reference count respectively.
//
//    hash map:
//      +-------+--------+
//      |  key  | handle |
//      +-------+--------+
//      | "foo" |    5   |-------------------------------------+
//      +-------+--------+                                     |
//      | "bar" |    2   |--+                                  |
//      +-------+--------+  |                                  |
//                          |                                  |
//                     head |                                  |
//                       |  |                                  |
//    circular list:     |  |                                  |
//         +-------+   +-------+   +-------+   +-------+   +-------+   +-------
//         |(0,0,0)|---|(1,1,0)|---|(0,0,0)|---|(0,1,3)|---|(1,0,0)|---|  ...
//         +-------+   +-------+   +-------+   +-------+   +-------+   +-------
//             |                       |
//             +-------+   +-----------+
//                     |   |
//                   +---+---+
//    recycle bin:   | 1 | 3 |
//                   +---+---+
//
// Suppose we try to insert "baz" into the cache at this point and the cache is
// full. The cache will first look for entries to evict, starting from where
// head points to (the second entry). It resets usage bit of the second entry,
// skips the third and fourth entry since they are not in cache, and finally
// evict the fifth entry ("foo"). It looks at recycle bin for available handle,
// grabs handle 3, and insert the key into the handle. The following figure
// shows the resulting layout.
//
//    hash map:
//      +-------+--------+
//      |  key  | handle |
//      +-------+--------+
//      | "baz" |    3   |-------------+
//      +-------+--------+             |
//      | "bar" |    2   |--+          |
//      +-------+--------+  |          |
//                          |          |
//                          |          |                                 head
//                          |          |                                   |
//    circular list:        |          |                                   |
//         +-------+   +-------+   +-------+   +-------+   +-------+   +-------
//         |(0,0,0)|---|(1,0,0)|---|(1,0,0)|---|(0,1,3)|---|(0,0,0)|---|  ...
//         +-------+   +-------+   +-------+   +-------+   +-------+   +-------
//             |                                               |
//             +-------+   +-----------------------------------+
//                     |   |
//                   +---+---+
//    recycle bin:   | 1 | 5 |
//                   +---+---+
//
// A global mutex guards the circular list, the head, and the recycle bin.
// We additionally require that modifying the hash map needs to hold the mutex.
// As such, Modifying the cache (such as Insert() and Erase()) require to
// hold the mutex. Lookup() only access the hash map and the flags associated
// with each handle, and don't require explicit locking. Release() has to
// acquire the mutex only when it releases the last reference to the entry and
// the entry has been erased from cache explicitly. A future improvement could
// be to remove the mutex completely.
//
// Benchmark:
// We run readrandom db_bench on a test DB of size 13GB, with size of each
// level:
//
//    Level    Files   Size(MB)
//    -------------------------
//      L0        1       0.01
//      L1       18      17.32
//      L2      230     182.94
//      L3     1186    1833.63
//      L4     4602    8140.30
//
// We test with both 32 and 16 read threads, with 2GB cache size (the whole DB
// doesn't fits in) and 64GB cache size (the whole DB can fit in cache), and
// whether to put index and filter blocks in block cache. The benchmark runs
// with
// with RocksDB 4.10. We got the following result:
//
// Threads Cache     Cache               ClockCache               LRUCache
//         Size  Index/Filter Throughput(MB/s)   Hit Throughput(MB/s)    Hit
//     32   2GB       yes               466.7  85.9%           433.7   86.5%
//     32   2GB       no                529.9  72.7%           532.7   73.9%
//     32  64GB       yes               649.9  99.9%           507.9   99.9%
//     32  64GB       no                740.4  99.9%           662.8   99.9%
//     16   2GB       yes               278.4  85.9%           283.4   86.5%
//     16   2GB       no                318.6  72.7%           335.8   73.9%
//     16  64GB       yes               391.9  99.9%           353.3   99.9%
//     16  64GB       no                433.8  99.8%           419.4   99.8%

// Cache entry meta data.
struct CacheHandle {
  Slice key;
  uint32_t hash;
  void* value;
  size_t charge;
  void (*deleter)(const Slice&, void* value);

  // Flags and counters associated with the cache handle:
  //   lowest bit: n-cache bit
  //   second lowest bit: usage bit
  //   the rest bits: reference count
  // The handle is unused when flags equals to 0. The thread decreases the count
  // to 0 is responsible to put the handle back to recycle_ and cleanup memory.
  std::atomic<uint32_t> flags;

  CacheHandle() = default;

  CacheHandle(const CacheHandle& a) { *this = a; }

  CacheHandle(const Slice& k, void* v,
              void (*del)(const Slice& key, void* value))
      : key(k), value(v), deleter(del) {}

  CacheHandle& operator=(const CacheHandle& a) {
    // Only copy members needed for deletion.
    key = a.key;
    value = a.value;
    deleter = a.deleter;
    return *this;
  }
};

// Key of hash map. We store hash value with the key for convenience.
struct CacheKey {
  Slice key;
  uint32_t hash_value;

  CacheKey() = default;

  CacheKey(const Slice& k, uint32_t h) {
    key = k;
    hash_value = h;
  }

  static bool equal(const CacheKey& a, const CacheKey& b) {
    return a.hash_value == b.hash_value && a.key == b.key;
  }

  static size_t hash(const CacheKey& a) {
    return static_cast<size_t>(a.hash_value);
  }
};

struct CleanupContext {
  // List of values to be deleted, along with the key and deleter.
  autovector<CacheHandle> to_delete_value;

  // List of keys to be deleted.
  autovector<const char*> to_delete_key;
};

// A cache shard which maintains its own CLOCK cache.
class ClockCacheShard : public CacheShard {
 public:
  // Hash map type.
  typedef tbb::concurrent_hash_map<CacheKey, CacheHandle*, CacheKey> HashTable;

  ClockCacheShard();
  ~ClockCacheShard() override;

  // Interfaces
  void SetCapacity(size_t capacity) override;
  void SetStrictCapacityLimit(bool strict_capacity_limit) override;
  Status Insert(const Slice& key, uint32_t hash, void* value, size_t charge,
                void (*deleter)(const Slice& key, void* value),
                Cache::Handle** handle, Cache::Priority priority) override;
  Cache::Handle* Lookup(const Slice& key, uint32_t hash) override;
  // If the entry in in cache, increase reference count and return true.
  // Return false otherwise.
  //
  // Not necessary to hold mutex_ before being called.
  bool Ref(Cache::Handle* handle) override;
  bool Release(Cache::Handle* handle, bool force_erase = false) override;
  void Erase(const Slice& key, uint32_t hash) override;
  bool EraseAndConfirm(const Slice& key, uint32_t hash,
                       CleanupContext* context);
  size_t GetUsage() const override;
  size_t GetPinnedUsage() const override;
  void EraseUnRefEntries() override;
  void ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                              bool thread_safe) override;

 private:
  static const uint32_t kInCacheBit = 1;
  static const uint32_t kUsageBit = 2;
  static const uint32_t kRefsOffset = 2;
  static const uint32_t kOneRef = 1 << kRefsOffset;

  // Helper functions to extract cache handle flags and counters.
  static bool InCache(uint32_t flags) { return flags & kInCacheBit; }
  static bool HasUsage(uint32_t flags) { return flags & kUsageBit; }
  static uint32_t CountRefs(uint32_t flags) { return flags >> kRefsOffset; }

  // Decrease reference count of the entry. If this decreases the count to 0,
  // recycle the entry. If set_usage is true, also set the usage bit.
  //
  // returns true if a value is erased.
  //
  // Not necessary to hold mutex_ before being called.
  bool Unref(CacheHandle* handle, bool set_usage, CleanupContext* context);

  // Unset in-cache bit of the entry. Recycle the handle if necessary.
  //
  // returns true if a value is erased.
  //
  // Has to hold mutex_ before being called.
  bool UnsetInCache(CacheHandle* handle, CleanupContext* context);

  // Put the handle back to recycle_ list, and put the value associated with
  // it into to-be-deleted list. It doesn't cleanup the key as it might be
  // reused by another handle.
  //
  // Has to hold mutex_ before being called.
  void RecycleHandle(CacheHandle* handle, CleanupContext* context);

  // Delete keys and values in to-be-deleted list. Call the method without
  // holding mutex, as destructors can be expensive.
  void Cleanup(const CleanupContext& context);

  // Examine the handle for eviction. If the handle is in cache, usage bit is
  // not set, and referece count is 0, evict it from cache. Otherwise unset
  // the usage bit.
  //
  // Has to hold mutex_ before being called.
  bool TryEvict(CacheHandle* value, CleanupContext* context);

  // Scan through the circular list, evict entries until we get enough capacity
  // for new cache entry of specific size. Return true if success, false
  // otherwise.
  //
  // Has to hold mutex_ before being called.
  bool EvictFromCache(size_t charge, CleanupContext* context);

  CacheHandle* Insert(const Slice& key, uint32_t hash, void* value,
                      size_t change,
                      void (*deleter)(const Slice& key, void* value),
                      bool hold_reference, CleanupContext* context);

  // Guards list_, head_, and recycle_. In addition, updating table_ also has
  // to hold the mutex, to avoid the cache being in inconsistent state.
  mutable port::Mutex mutex_;

  // The circular list of cache handles. Initially the list is empty. Once a
  // handle is needed by insertion, and no more handles are available in
  // recycle bin, one more handle is appended to the end.
  //
  // We use std::deque for the circular list because we want to make sure
  // pointers to handles are valid through out the life-cycle of the cache
  // (in contrast to std::vector), and be able to grow the list (in contrast
  // to statically allocated arrays).
  std::deque<CacheHandle> list_;

  // Pointer to the next handle in the circular list to be examine for
  // eviction.
  size_t head_;

  // Recycle bin of cache handles.
  autovector<CacheHandle*> recycle_;

  // Maximum cache size.
  std::atomic<size_t> capacity_;

  // Current total size of the cache.
  std::atomic<size_t> usage_;

  // Total un-released cache size.
  std::atomic<size_t> pinned_usage_;

  // Whether allow insert into cache if cache is full.
  std::atomic<bool> strict_capacity_limit_;

  // Hash table (tbb::concurrent_hash_map) for lookup.
  HashTable table_;
};

ClockCacheShard::ClockCacheShard()
    : head_(0), usage_(0), pinned_usage_(0), strict_capacity_limit_(false) {}

ClockCacheShard::~ClockCacheShard() {
  for (auto& handle : list_) {
    uint32_t flags = handle.flags.load(std::memory_order_relaxed);
    if (InCache(flags) || CountRefs(flags) > 0) {
      if (handle.deleter != nullptr) {
        (*handle.deleter)(handle.key, handle.value);
      }
      delete[] handle.key.data();
    }
  }
}

size_t ClockCacheShard::GetUsage() const {
  return usage_.load(std::memory_order_relaxed);
}

size_t ClockCacheShard::GetPinnedUsage() const {
  return pinned_usage_.load(std::memory_order_relaxed);
}

void ClockCacheShard::ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                             bool thread_safe) {
  if (thread_safe) {
    mutex_.Lock();
  }
  for (auto& handle : list_) {
    // Use relaxed semantics instead of acquire semantics since we are either
    // holding mutex, or don't have thread safe requirement.
    uint32_t flags = handle.flags.load(std::memory_order_relaxed);
    if (InCache(flags)) {
      callback(handle.value, handle.charge);
    }
  }
  if (thread_safe) {
    mutex_.Unlock();
  }
}

void ClockCacheShard::RecycleHandle(CacheHandle* handle,
                                    CleanupContext* context) {
  mutex_.AssertHeld();
  assert(!InCache(handle->flags) && CountRefs(handle->flags) == 0);
  context->to_delete_key.push_back(handle->key.data());
  context->to_delete_value.emplace_back(*handle);
  handle->key.clear();
  handle->value = nullptr;
  handle->deleter = nullptr;
  recycle_.push_back(handle);
  usage_.fetch_sub(handle->charge, std::memory_order_relaxed);
}

void ClockCacheShard::Cleanup(const CleanupContext& context) {
  for (const CacheHandle& handle : context.to_delete_value) {
    if (handle.deleter) {
      (*handle.deleter)(handle.key, handle.value);
    }
  }
  for (const char* key : context.to_delete_key) {
    delete[] key;
  }
}

bool ClockCacheShard::Ref(Cache::Handle* h) {
  auto handle = reinterpret_cast<CacheHandle*>(h);
  // CAS loop to increase reference count.
  uint32_t flags = handle->flags.load(std::memory_order_relaxed);
  while (InCache(flags)) {
    // Use acquire semantics on success, as further operations on the cache
    // entry has to be order after reference count is increased.
    if (handle->flags.compare_exchange_weak(flags, flags + kOneRef,
                                            std::memory_order_acquire,
                                            std::memory_order_relaxed)) {
      if (CountRefs(flags) == 0) {
        // No reference count before the operation.
        pinned_usage_.fetch_add(handle->charge, std::memory_order_relaxed);
      }
      return true;
    }
  }
  return false;
}

bool ClockCacheShard::Unref(CacheHandle* handle, bool set_usage,
                            CleanupContext* context) {
  if (set_usage) {
    handle->flags.fetch_or(kUsageBit, std::memory_order_relaxed);
  }
  // Use acquire-release semantics as previous operations on the cache entry
  // has to be order before reference count is decreased, and potential cleanup
  // of the entry has to be order after.
  uint32_t flags = handle->flags.fetch_sub(kOneRef, std::memory_order_acq_rel);
  assert(CountRefs(flags) > 0);
  if (CountRefs(flags) == 1) {
    // this is the last reference.
    pinned_usage_.fetch_sub(handle->charge, std::memory_order_relaxed);
    // Cleanup if it is the last reference.
    if (!InCache(flags)) {
      MutexLock l(&mutex_);
      RecycleHandle(handle, context);
    }
  }
  return context->to_delete_value.size();
}

bool ClockCacheShard::UnsetInCache(CacheHandle* handle,
                                   CleanupContext* context) {
  mutex_.AssertHeld();
  // Use acquire-release semantics as previous operations on the cache entry
  // has to be order before reference count is decreased, and potential cleanup
  // of the entry has to be order after.
  uint32_t flags =
      handle->flags.fetch_and(~kInCacheBit, std::memory_order_acq_rel);
  // Cleanup if it is the last reference.
  if (InCache(flags) && CountRefs(flags) == 0) {
    RecycleHandle(handle, context);
  }
  return context->to_delete_value.size();
}

bool ClockCacheShard::TryEvict(CacheHandle* handle, CleanupContext* context) {
  mutex_.AssertHeld();
  uint32_t flags = kInCacheBit;
  if (handle->flags.compare_exchange_strong(flags, 0, std::memory_order_acquire,
                                            std::memory_order_relaxed)) {
    bool erased __attribute__((__unused__)) =
        table_.erase(CacheKey(handle->key, handle->hash));
    assert(erased);
    RecycleHandle(handle, context);
    return true;
  }
  handle->flags.fetch_and(~kUsageBit, std::memory_order_relaxed);
  return false;
}

bool ClockCacheShard::EvictFromCache(size_t charge, CleanupContext* context) {
  size_t usage = usage_.load(std::memory_order_relaxed);
  size_t capacity = capacity_.load(std::memory_order_relaxed);
  if (usage == 0) {
    return charge <= capacity;
  }
  size_t new_head = head_;
  bool second_iteration = false;
  while (usage + charge > capacity) {
    assert(new_head < list_.size());
    if (TryEvict(&list_[new_head], context)) {
      usage = usage_.load(std::memory_order_relaxed);
    }
    new_head = (new_head + 1 >= list_.size()) ? 0 : new_head + 1;
    if (new_head == head_) {
      if (second_iteration) {
        return false;
      } else {
        second_iteration = true;
      }
    }
  }
  head_ = new_head;
  return true;
}

void ClockCacheShard::SetCapacity(size_t capacity) {
  CleanupContext context;
  {
    MutexLock l(&mutex_);
    capacity_.store(capacity, std::memory_order_relaxed);
    EvictFromCache(0, &context);
  }
  Cleanup(context);
}

void ClockCacheShard::SetStrictCapacityLimit(bool strict_capacity_limit) {
  strict_capacity_limit_.store(strict_capacity_limit,
                               std::memory_order_relaxed);
}

CacheHandle* ClockCacheShard::Insert(
    const Slice& key, uint32_t hash, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value), bool hold_reference,
    CleanupContext* context) {
  MutexLock l(&mutex_);
  bool success = EvictFromCache(charge, context);
  bool strict = strict_capacity_limit_.load(std::memory_order_relaxed);
  if (!success && (strict || !hold_reference)) {
    context->to_delete_key.push_back(key.data());
    if (!hold_reference) {
      context->to_delete_value.emplace_back(key, value, deleter);
    }
    return nullptr;
  }
  // Grab available handle from recycle bin. If recycle bin is empty, create
  // and append new handle to end of circular list.
  CacheHandle* handle = nullptr;
  if (!recycle_.empty()) {
    handle = recycle_.back();
    recycle_.pop_back();
  } else {
    list_.emplace_back();
    handle = &list_.back();
  }
  // Fill handle.
  handle->key = key;
  handle->hash = hash;
  handle->value = value;
  handle->charge = charge;
  handle->deleter = deleter;
  uint32_t flags = hold_reference ? kInCacheBit + kOneRef : kInCacheBit;
  handle->flags.store(flags, std::memory_order_relaxed);
  HashTable::accessor accessor;
  if (table_.find(accessor, CacheKey(key, hash))) {
    CacheHandle* existing_handle = accessor->second;
    table_.erase(accessor);
    UnsetInCache(existing_handle, context);
  }
  table_.insert(HashTable::value_type(CacheKey(key, hash), handle));
  if (hold_reference) {
    pinned_usage_.fetch_add(charge, std::memory_order_relaxed);
  }
  usage_.fetch_add(charge, std::memory_order_relaxed);
  return handle;
}

Status ClockCacheShard::Insert(const Slice& key, uint32_t hash, void* value,
                               size_t charge,
                               void (*deleter)(const Slice& key, void* value),
                               Cache::Handle** out_handle,
                               Cache::Priority /*priority*/) {
  CleanupContext context;
  HashTable::accessor accessor;
  char* key_data = new char[key.size()];
  memcpy(key_data, key.data(), key.size());
  Slice key_copy(key_data, key.size());
  CacheHandle* handle = Insert(key_copy, hash, value, charge, deleter,
                               out_handle != nullptr, &context);
  Status s;
  if (out_handle != nullptr) {
    if (handle == nullptr) {
      s = Status::Incomplete("Insert failed due to LRU cache being full.");
    } else {
      *out_handle = reinterpret_cast<Cache::Handle*>(handle);
    }
  }
  Cleanup(context);
  return s;
}

Cache::Handle* ClockCacheShard::Lookup(const Slice& key, uint32_t hash) {
  HashTable::const_accessor accessor;
  if (!table_.find(accessor, CacheKey(key, hash))) {
    return nullptr;
  }
  CacheHandle* handle = accessor->second;
  accessor.release();
  // Ref() could fail if another thread sneak in and evict/erase the cache
  // entry before we are able to hold reference.
  if (!Ref(reinterpret_cast<Cache::Handle*>(handle))) {
    return nullptr;
  }
  // Double check the key since the handle may now representing another key
  // if other threads sneak in, evict/erase the entry and re-used the handle
  // for another cache entry.
  if (hash != handle->hash || key != handle->key) {
    CleanupContext context;
    Unref(handle, false, &context);
    // It is possible Unref() delete the entry, so we need to cleanup.
    Cleanup(context);
    return nullptr;
  }
  return reinterpret_cast<Cache::Handle*>(handle);
}

bool ClockCacheShard::Release(Cache::Handle* h, bool force_erase) {
  CleanupContext context;
  CacheHandle* handle = reinterpret_cast<CacheHandle*>(h);
  bool erased = Unref(handle, true, &context);
  if (force_erase && !erased) {
    erased = EraseAndConfirm(handle->key, handle->hash, &context);
  }
  Cleanup(context);
  return erased;
}

void ClockCacheShard::Erase(const Slice& key, uint32_t hash) {
  CleanupContext context;
  EraseAndConfirm(key, hash, &context);
  Cleanup(context);
}

bool ClockCacheShard::EraseAndConfirm(const Slice& key, uint32_t hash,
                                      CleanupContext* context) {
  MutexLock l(&mutex_);
  HashTable::accessor accessor;
  bool erased = false;
  if (table_.find(accessor, CacheKey(key, hash))) {
    CacheHandle* handle = accessor->second;
    table_.erase(accessor);
    erased = UnsetInCache(handle, context);
  }
  return erased;
}

void ClockCacheShard::EraseUnRefEntries() {
  CleanupContext context;
  {
    MutexLock l(&mutex_);
    table_.clear();
    for (auto& handle : list_) {
      UnsetInCache(&handle, &context);
    }
  }
  Cleanup(context);
}

class ClockCache : public ShardedCache {
 public:
  ClockCache(size_t capacity, int num_shard_bits, bool strict_capacity_limit)
      : ShardedCache(capacity, num_shard_bits, strict_capacity_limit) {
    int num_shards = 1 << num_shard_bits;
    shards_ = new ClockCacheShard[num_shards];
    SetCapacity(capacity);
    SetStrictCapacityLimit(strict_capacity_limit);
  }

  ~ClockCache() override { delete[] shards_; }

  const char* Name() const override { return "ClockCache"; }

  CacheShard* GetShard(int shard) override {
    return reinterpret_cast<CacheShard*>(&shards_[shard]);
  }

  const CacheShard* GetShard(int shard) const override {
    return reinterpret_cast<CacheShard*>(&shards_[shard]);
  }

  void* Value(Handle* handle) override {
    return reinterpret_cast<const CacheHandle*>(handle)->value;
  }

  size_t GetCharge(Handle* handle) const override {
    return reinterpret_cast<const CacheHandle*>(handle)->charge;
  }

  uint32_t GetHash(Handle* handle) const override {
    return reinterpret_cast<const CacheHandle*>(handle)->hash;
  }

  void DisownData() override { shards_ = nullptr; }

 private:
  ClockCacheShard* shards_;
};

}  // end anonymous namespace

std::shared_ptr<Cache> NewClockCache(size_t capacity, int num_shard_bits,
                                     bool strict_capacity_limit) {
  if (num_shard_bits < 0) {
    num_shard_bits = GetDefaultCacheShardBits(capacity);
  }
  return std::make_shared<ClockCache>(capacity, num_shard_bits,
                                      strict_capacity_limit);
}

}  // namespace rocksdb

#endif  // SUPPORT_CLOCK_CACHE
