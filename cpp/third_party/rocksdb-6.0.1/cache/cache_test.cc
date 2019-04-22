//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/cache.h"

#include <forward_list>
#include <functional>
#include <iostream>
#include <string>
#include <vector>
#include "cache/clock_cache.h"
#include "cache/lru_cache.h"
#include "util/coding.h"
#include "util/string_util.h"
#include "util/testharness.h"

namespace rocksdb {

// Conversions between numeric keys/values and the types expected by Cache.
static std::string EncodeKey(int k) {
  std::string result;
  PutFixed32(&result, k);
  return result;
}
static int DecodeKey(const Slice& k) {
  assert(k.size() == 4);
  return DecodeFixed32(k.data());
}
static void* EncodeValue(uintptr_t v) { return reinterpret_cast<void*>(v); }
static int DecodeValue(void* v) {
  return static_cast<int>(reinterpret_cast<uintptr_t>(v));
}

const std::string kLRU = "lru";
const std::string kClock = "clock";

void dumbDeleter(const Slice& /*key*/, void* /*value*/) {}

void eraseDeleter(const Slice& /*key*/, void* value) {
  Cache* cache = reinterpret_cast<Cache*>(value);
  cache->Erase("foo");
}

class CacheTest : public testing::TestWithParam<std::string> {
 public:
  static CacheTest* current_;

  static void Deleter(const Slice& key, void* v) {
    current_->deleted_keys_.push_back(DecodeKey(key));
    current_->deleted_values_.push_back(DecodeValue(v));
  }

  static const int kCacheSize = 1000;
  static const int kNumShardBits = 4;

  static const int kCacheSize2 = 100;
  static const int kNumShardBits2 = 2;

  std::vector<int> deleted_keys_;
  std::vector<int> deleted_values_;
  std::shared_ptr<Cache> cache_;
  std::shared_ptr<Cache> cache2_;

  CacheTest()
      : cache_(NewCache(kCacheSize, kNumShardBits, false)),
        cache2_(NewCache(kCacheSize2, kNumShardBits2, false)) {
    current_ = this;
  }

  ~CacheTest() override {}

  std::shared_ptr<Cache> NewCache(size_t capacity) {
    auto type = GetParam();
    if (type == kLRU) {
      return NewLRUCache(capacity);
    }
    if (type == kClock) {
      return NewClockCache(capacity);
    }
    return nullptr;
  }

  std::shared_ptr<Cache> NewCache(size_t capacity, int num_shard_bits,
                                  bool strict_capacity_limit) {
    auto type = GetParam();
    if (type == kLRU) {
      return NewLRUCache(capacity, num_shard_bits, strict_capacity_limit);
    }
    if (type == kClock) {
      return NewClockCache(capacity, num_shard_bits, strict_capacity_limit);
    }
    return nullptr;
  }

  int Lookup(shared_ptr<Cache> cache, int key) {
    Cache::Handle* handle = cache->Lookup(EncodeKey(key));
    const int r = (handle == nullptr) ? -1 : DecodeValue(cache->Value(handle));
    if (handle != nullptr) {
      cache->Release(handle);
    }
    return r;
  }

  void Insert(shared_ptr<Cache> cache, int key, int value, int charge = 1) {
    cache->Insert(EncodeKey(key), EncodeValue(value), charge,
                  &CacheTest::Deleter);
  }

  void Erase(shared_ptr<Cache> cache, int key) {
    cache->Erase(EncodeKey(key));
  }


  int Lookup(int key) {
    return Lookup(cache_, key);
  }

  void Insert(int key, int value, int charge = 1) {
    Insert(cache_, key, value, charge);
  }

  void Erase(int key) {
    Erase(cache_, key);
  }

  int Lookup2(int key) {
    return Lookup(cache2_, key);
  }

  void Insert2(int key, int value, int charge = 1) {
    Insert(cache2_, key, value, charge);
  }

  void Erase2(int key) {
    Erase(cache2_, key);
  }
};
CacheTest* CacheTest::current_;

TEST_P(CacheTest, UsageTest) {
  // cache is std::shared_ptr and will be automatically cleaned up.
  const uint64_t kCapacity = 100000;
  auto cache = NewCache(kCapacity, 8, false);

  size_t usage = 0;
  char value[10] = "abcdef";
  // make sure everything will be cached
  for (int i = 1; i < 100; ++i) {
    std::string key(i, 'a');
    auto kv_size = key.size() + 5;
    cache->Insert(key, reinterpret_cast<void*>(value), kv_size, dumbDeleter);
    usage += kv_size;
    ASSERT_EQ(usage, cache->GetUsage());
  }

  // make sure the cache will be overloaded
  for (uint64_t i = 1; i < kCapacity; ++i) {
    auto key = ToString(i);
    cache->Insert(key, reinterpret_cast<void*>(value), key.size() + 5,
                  dumbDeleter);
  }

  // the usage should be close to the capacity
  ASSERT_GT(kCapacity, cache->GetUsage());
  ASSERT_LT(kCapacity * 0.95, cache->GetUsage());
}

TEST_P(CacheTest, PinnedUsageTest) {
  // cache is std::shared_ptr and will be automatically cleaned up.
  const uint64_t kCapacity = 100000;
  auto cache = NewCache(kCapacity, 8, false);

  size_t pinned_usage = 0;
  char value[10] = "abcdef";

  std::forward_list<Cache::Handle*> unreleased_handles;

  // Add entries. Unpin some of them after insertion. Then, pin some of them
  // again. Check GetPinnedUsage().
  for (int i = 1; i < 100; ++i) {
    std::string key(i, 'a');
    auto kv_size = key.size() + 5;
    Cache::Handle* handle;
    cache->Insert(key, reinterpret_cast<void*>(value), kv_size, dumbDeleter,
                  &handle);
    pinned_usage += kv_size;
    ASSERT_EQ(pinned_usage, cache->GetPinnedUsage());
    if (i % 2 == 0) {
      cache->Release(handle);
      pinned_usage -= kv_size;
      ASSERT_EQ(pinned_usage, cache->GetPinnedUsage());
    } else {
      unreleased_handles.push_front(handle);
    }
    if (i % 3 == 0) {
      unreleased_handles.push_front(cache->Lookup(key));
      // If i % 2 == 0, then the entry was unpinned before Lookup, so pinned
      // usage increased
      if (i % 2 == 0) {
        pinned_usage += kv_size;
      }
      ASSERT_EQ(pinned_usage, cache->GetPinnedUsage());
    }
  }

  // check that overloading the cache does not change the pinned usage
  for (uint64_t i = 1; i < 2 * kCapacity; ++i) {
    auto key = ToString(i);
    cache->Insert(key, reinterpret_cast<void*>(value), key.size() + 5,
                  dumbDeleter);
  }
  ASSERT_EQ(pinned_usage, cache->GetPinnedUsage());

  // release handles for pinned entries to prevent memory leaks
  for (auto handle : unreleased_handles) {
    cache->Release(handle);
  }
}

TEST_P(CacheTest, HitAndMiss) {
  ASSERT_EQ(-1, Lookup(100));

  Insert(100, 101);
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(-1,  Lookup(200));
  ASSERT_EQ(-1,  Lookup(300));

  Insert(200, 201);
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(-1,  Lookup(300));

  Insert(100, 102);
  ASSERT_EQ(102, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(-1,  Lookup(300));

  ASSERT_EQ(1U, deleted_keys_.size());
  ASSERT_EQ(100, deleted_keys_[0]);
  ASSERT_EQ(101, deleted_values_[0]);
}

TEST_P(CacheTest, InsertSameKey) {
  Insert(1, 1);
  Insert(1, 2);
  ASSERT_EQ(2, Lookup(1));
}

TEST_P(CacheTest, Erase) {
  Erase(200);
  ASSERT_EQ(0U, deleted_keys_.size());

  Insert(100, 101);
  Insert(200, 201);
  Erase(100);
  ASSERT_EQ(-1,  Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(1U, deleted_keys_.size());
  ASSERT_EQ(100, deleted_keys_[0]);
  ASSERT_EQ(101, deleted_values_[0]);

  Erase(100);
  ASSERT_EQ(-1,  Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(1U, deleted_keys_.size());
}

TEST_P(CacheTest, EntriesArePinned) {
  Insert(100, 101);
  Cache::Handle* h1 = cache_->Lookup(EncodeKey(100));
  ASSERT_EQ(101, DecodeValue(cache_->Value(h1)));
  ASSERT_EQ(1U, cache_->GetUsage());

  Insert(100, 102);
  Cache::Handle* h2 = cache_->Lookup(EncodeKey(100));
  ASSERT_EQ(102, DecodeValue(cache_->Value(h2)));
  ASSERT_EQ(0U, deleted_keys_.size());
  ASSERT_EQ(2U, cache_->GetUsage());

  cache_->Release(h1);
  ASSERT_EQ(1U, deleted_keys_.size());
  ASSERT_EQ(100, deleted_keys_[0]);
  ASSERT_EQ(101, deleted_values_[0]);
  ASSERT_EQ(1U, cache_->GetUsage());

  Erase(100);
  ASSERT_EQ(-1, Lookup(100));
  ASSERT_EQ(1U, deleted_keys_.size());
  ASSERT_EQ(1U, cache_->GetUsage());

  cache_->Release(h2);
  ASSERT_EQ(2U, deleted_keys_.size());
  ASSERT_EQ(100, deleted_keys_[1]);
  ASSERT_EQ(102, deleted_values_[1]);
  ASSERT_EQ(0U, cache_->GetUsage());
}

TEST_P(CacheTest, EvictionPolicy) {
  Insert(100, 101);
  Insert(200, 201);

  // Frequently used entry must be kept around
  for (int i = 0; i < kCacheSize + 100; i++) {
    Insert(1000+i, 2000+i);
    ASSERT_EQ(101, Lookup(100));
  }
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(-1, Lookup(200));
}

TEST_P(CacheTest, ExternalRefPinsEntries) {
  Insert(100, 101);
  Cache::Handle* h = cache_->Lookup(EncodeKey(100));
  ASSERT_TRUE(cache_->Ref(h));
  ASSERT_EQ(101, DecodeValue(cache_->Value(h)));
  ASSERT_EQ(1U, cache_->GetUsage());

  for (int i = 0; i < 3; ++i) {
    if (i > 0) {
      // First release (i == 1) corresponds to Ref(), second release (i == 2)
      // corresponds to Lookup(). Then, since all external refs are released,
      // the below insertions should push out the cache entry.
      cache_->Release(h);
    }
    // double cache size because the usage bit in block cache prevents 100 from
    // being evicted in the first kCacheSize iterations
    for (int j = 0; j < 2 * kCacheSize + 100; j++) {
      Insert(1000 + j, 2000 + j);
    }
    if (i < 2) {
      ASSERT_EQ(101, Lookup(100));
    }
  }
  ASSERT_EQ(-1, Lookup(100));
}

TEST_P(CacheTest, EvictionPolicyRef) {
  Insert(100, 101);
  Insert(101, 102);
  Insert(102, 103);
  Insert(103, 104);
  Insert(200, 101);
  Insert(201, 102);
  Insert(202, 103);
  Insert(203, 104);
  Cache::Handle* h201 = cache_->Lookup(EncodeKey(200));
  Cache::Handle* h202 = cache_->Lookup(EncodeKey(201));
  Cache::Handle* h203 = cache_->Lookup(EncodeKey(202));
  Cache::Handle* h204 = cache_->Lookup(EncodeKey(203));
  Insert(300, 101);
  Insert(301, 102);
  Insert(302, 103);
  Insert(303, 104);

  // Insert entries much more than Cache capacity
  for (int i = 0; i < kCacheSize + 100; i++) {
    Insert(1000 + i, 2000 + i);
  }

  // Check whether the entries inserted in the beginning
  // are evicted. Ones without extra ref are evicted and
  // those with are not.
  ASSERT_EQ(-1, Lookup(100));
  ASSERT_EQ(-1, Lookup(101));
  ASSERT_EQ(-1, Lookup(102));
  ASSERT_EQ(-1, Lookup(103));

  ASSERT_EQ(-1, Lookup(300));
  ASSERT_EQ(-1, Lookup(301));
  ASSERT_EQ(-1, Lookup(302));
  ASSERT_EQ(-1, Lookup(303));

  ASSERT_EQ(101, Lookup(200));
  ASSERT_EQ(102, Lookup(201));
  ASSERT_EQ(103, Lookup(202));
  ASSERT_EQ(104, Lookup(203));

  // Cleaning up all the handles
  cache_->Release(h201);
  cache_->Release(h202);
  cache_->Release(h203);
  cache_->Release(h204);
}

TEST_P(CacheTest, EvictEmptyCache) {
  // Insert item large than capacity to trigger eviction on empty cache.
  auto cache = NewCache(1, 0, false);
  ASSERT_OK(cache->Insert("foo", nullptr, 10, dumbDeleter));
}

TEST_P(CacheTest, EraseFromDeleter) {
  // Have deleter which will erase item from cache, which will re-enter
  // the cache at that point.
  std::shared_ptr<Cache> cache = NewCache(10, 0, false);
  ASSERT_OK(cache->Insert("foo", nullptr, 1, dumbDeleter));
  ASSERT_OK(cache->Insert("bar", cache.get(), 1, eraseDeleter));
  cache->Erase("bar");
  ASSERT_EQ(nullptr, cache->Lookup("foo"));
  ASSERT_EQ(nullptr, cache->Lookup("bar"));
}

TEST_P(CacheTest, ErasedHandleState) {
  // insert a key and get two handles
  Insert(100, 1000);
  Cache::Handle* h1 = cache_->Lookup(EncodeKey(100));
  Cache::Handle* h2 = cache_->Lookup(EncodeKey(100));
  ASSERT_EQ(h1, h2);
  ASSERT_EQ(DecodeValue(cache_->Value(h1)), 1000);
  ASSERT_EQ(DecodeValue(cache_->Value(h2)), 1000);

  // delete the key from the cache
  Erase(100);
  // can no longer find in the cache
  ASSERT_EQ(-1, Lookup(100));

  // release one handle
  cache_->Release(h1);
  // still can't find in cache
  ASSERT_EQ(-1, Lookup(100));

  cache_->Release(h2);
}

TEST_P(CacheTest, HeavyEntries) {
  // Add a bunch of light and heavy entries and then count the combined
  // size of items still in the cache, which must be approximately the
  // same as the total capacity.
  const int kLight = 1;
  const int kHeavy = 10;
  int added = 0;
  int index = 0;
  while (added < 2*kCacheSize) {
    const int weight = (index & 1) ? kLight : kHeavy;
    Insert(index, 1000+index, weight);
    added += weight;
    index++;
  }

  int cached_weight = 0;
  for (int i = 0; i < index; i++) {
    const int weight = (i & 1 ? kLight : kHeavy);
    int r = Lookup(i);
    if (r >= 0) {
      cached_weight += weight;
      ASSERT_EQ(1000+i, r);
    }
  }
  ASSERT_LE(cached_weight, kCacheSize + kCacheSize/10);
}

TEST_P(CacheTest, NewId) {
  uint64_t a = cache_->NewId();
  uint64_t b = cache_->NewId();
  ASSERT_NE(a, b);
}


class Value {
 public:
  explicit Value(size_t v) : v_(v) { }

  size_t v_;
};

namespace {
void deleter(const Slice& /*key*/, void* value) {
  delete static_cast<Value *>(value);
}
}  // namespace

TEST_P(CacheTest, ReleaseAndErase) {
  std::shared_ptr<Cache> cache = NewCache(5, 0, false);
  Cache::Handle* handle;
  Status s = cache->Insert(EncodeKey(100), EncodeValue(100), 1,
                           &CacheTest::Deleter, &handle);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(5U, cache->GetCapacity());
  ASSERT_EQ(1U, cache->GetUsage());
  ASSERT_EQ(0U, deleted_keys_.size());
  auto erased = cache->Release(handle, true);
  ASSERT_TRUE(erased);
  // This tests that deleter has been called
  ASSERT_EQ(1U, deleted_keys_.size());
}

TEST_P(CacheTest, ReleaseWithoutErase) {
  std::shared_ptr<Cache> cache = NewCache(5, 0, false);
  Cache::Handle* handle;
  Status s = cache->Insert(EncodeKey(100), EncodeValue(100), 1,
                           &CacheTest::Deleter, &handle);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(5U, cache->GetCapacity());
  ASSERT_EQ(1U, cache->GetUsage());
  ASSERT_EQ(0U, deleted_keys_.size());
  auto erased = cache->Release(handle);
  ASSERT_FALSE(erased);
  // This tests that deleter is not called. When cache has free capacity it is
  // not expected to immediately erase the released items.
  ASSERT_EQ(0U, deleted_keys_.size());
}

TEST_P(CacheTest, SetCapacity) {
  // test1: increase capacity
  // lets create a cache with capacity 5,
  // then, insert 5 elements, then increase capacity
  // to 10, returned capacity should be 10, usage=5
  std::shared_ptr<Cache> cache = NewCache(5, 0, false);
  std::vector<Cache::Handle*> handles(10);
  // Insert 5 entries, but not releasing.
  for (size_t i = 0; i < 5; i++) {
    std::string key = ToString(i+1);
    Status s = cache->Insert(key, new Value(i + 1), 1, &deleter, &handles[i]);
    ASSERT_TRUE(s.ok());
  }
  ASSERT_EQ(5U, cache->GetCapacity());
  ASSERT_EQ(5U, cache->GetUsage());
  cache->SetCapacity(10);
  ASSERT_EQ(10U, cache->GetCapacity());
  ASSERT_EQ(5U, cache->GetUsage());

  // test2: decrease capacity
  // insert 5 more elements to cache, then release 5,
  // then decrease capacity to 7, final capacity should be 7
  // and usage should be 7
  for (size_t i = 5; i < 10; i++) {
    std::string key = ToString(i+1);
    Status s = cache->Insert(key, new Value(i + 1), 1, &deleter, &handles[i]);
    ASSERT_TRUE(s.ok());
  }
  ASSERT_EQ(10U, cache->GetCapacity());
  ASSERT_EQ(10U, cache->GetUsage());
  for (size_t i = 0; i < 5; i++) {
    cache->Release(handles[i]);
  }
  ASSERT_EQ(10U, cache->GetCapacity());
  ASSERT_EQ(10U, cache->GetUsage());
  cache->SetCapacity(7);
  ASSERT_EQ(7, cache->GetCapacity());
  ASSERT_EQ(7, cache->GetUsage());

  // release remaining 5 to keep valgrind happy
  for (size_t i = 5; i < 10; i++) {
    cache->Release(handles[i]);
  }
}

TEST_P(CacheTest, SetStrictCapacityLimit) {
  // test1: set the flag to false. Insert more keys than capacity. See if they
  // all go through.
  std::shared_ptr<Cache> cache = NewLRUCache(5, 0, false);
  std::vector<Cache::Handle*> handles(10);
  Status s;
  for (size_t i = 0; i < 10; i++) {
    std::string key = ToString(i + 1);
    s = cache->Insert(key, new Value(i + 1), 1, &deleter, &handles[i]);
    ASSERT_OK(s);
    ASSERT_NE(nullptr, handles[i]);
  }

  // test2: set the flag to true. Insert and check if it fails.
  std::string extra_key = "extra";
  Value* extra_value = new Value(0);
  cache->SetStrictCapacityLimit(true);
  Cache::Handle* handle;
  s = cache->Insert(extra_key, extra_value, 1, &deleter, &handle);
  ASSERT_TRUE(s.IsIncomplete());
  ASSERT_EQ(nullptr, handle);

  for (size_t i = 0; i < 10; i++) {
    cache->Release(handles[i]);
  }

  // test3: init with flag being true.
  std::shared_ptr<Cache> cache2 = NewLRUCache(5, 0, true);
  for (size_t i = 0; i < 5; i++) {
    std::string key = ToString(i + 1);
    s = cache2->Insert(key, new Value(i + 1), 1, &deleter, &handles[i]);
    ASSERT_OK(s);
    ASSERT_NE(nullptr, handles[i]);
  }
  s = cache2->Insert(extra_key, extra_value, 1, &deleter, &handle);
  ASSERT_TRUE(s.IsIncomplete());
  ASSERT_EQ(nullptr, handle);
  // test insert without handle
  s = cache2->Insert(extra_key, extra_value, 1, &deleter);
  // AS if the key have been inserted into cache but get evicted immediately.
  ASSERT_OK(s);
  ASSERT_EQ(5, cache->GetUsage());
  ASSERT_EQ(nullptr, cache2->Lookup(extra_key));

  for (size_t i = 0; i < 5; i++) {
    cache2->Release(handles[i]);
  }
}

TEST_P(CacheTest, OverCapacity) {
  size_t n = 10;

  // a LRUCache with n entries and one shard only
  std::shared_ptr<Cache> cache = NewCache(n, 0, false);

  std::vector<Cache::Handle*> handles(n+1);

  // Insert n+1 entries, but not releasing.
  for (size_t i = 0; i < n + 1; i++) {
    std::string key = ToString(i+1);
    Status s = cache->Insert(key, new Value(i + 1), 1, &deleter, &handles[i]);
    ASSERT_TRUE(s.ok());
  }

  // Guess what's in the cache now?
  for (size_t i = 0; i < n + 1; i++) {
    std::string key = ToString(i+1);
    auto h = cache->Lookup(key);
    ASSERT_TRUE(h != nullptr);
    if (h) cache->Release(h);
  }

  // the cache is over capacity since nothing could be evicted
  ASSERT_EQ(n + 1U, cache->GetUsage());
  for (size_t i = 0; i < n + 1; i++) {
    cache->Release(handles[i]);
  }
  // Make sure eviction is triggered.
  cache->SetCapacity(n);

  // cache is under capacity now since elements were released
  ASSERT_EQ(n, cache->GetUsage());

  // element 0 is evicted and the rest is there
  // This is consistent with the LRU policy since the element 0
  // was released first
  for (size_t i = 0; i < n + 1; i++) {
    std::string key = ToString(i+1);
    auto h = cache->Lookup(key);
    if (h) {
      ASSERT_NE(i, 0U);
      cache->Release(h);
    } else {
      ASSERT_EQ(i, 0U);
    }
  }
}

namespace {
std::vector<std::pair<int, int>> callback_state;
void callback(void* entry, size_t charge) {
  callback_state.push_back({DecodeValue(entry), static_cast<int>(charge)});
}
};

TEST_P(CacheTest, ApplyToAllCacheEntiresTest) {
  std::vector<std::pair<int, int>> inserted;
  callback_state.clear();

  for (int i = 0; i < 10; ++i) {
    Insert(i, i * 2, i + 1);
    inserted.push_back({i * 2, i + 1});
  }
  cache_->ApplyToAllCacheEntries(callback, true);

  std::sort(inserted.begin(), inserted.end());
  std::sort(callback_state.begin(), callback_state.end());
  ASSERT_TRUE(inserted == callback_state);
}

TEST_P(CacheTest, DefaultShardBits) {
  // test1: set the flag to false. Insert more keys than capacity. See if they
  // all go through.
  std::shared_ptr<Cache> cache = NewCache(16 * 1024L * 1024L);
  ShardedCache* sc = dynamic_cast<ShardedCache*>(cache.get());
  ASSERT_EQ(5, sc->GetNumShardBits());

  cache = NewLRUCache(511 * 1024L, -1, true);
  sc = dynamic_cast<ShardedCache*>(cache.get());
  ASSERT_EQ(0, sc->GetNumShardBits());

  cache = NewLRUCache(1024L * 1024L * 1024L, -1, true);
  sc = dynamic_cast<ShardedCache*>(cache.get());
  ASSERT_EQ(6, sc->GetNumShardBits());
}

#ifdef SUPPORT_CLOCK_CACHE
shared_ptr<Cache> (*new_clock_cache_func)(size_t, int, bool) = NewClockCache;
INSTANTIATE_TEST_CASE_P(CacheTestInstance, CacheTest,
                        testing::Values(kLRU, kClock));
#else
INSTANTIATE_TEST_CASE_P(CacheTestInstance, CacheTest, testing::Values(kLRU));
#endif  // SUPPORT_CLOCK_CACHE

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
