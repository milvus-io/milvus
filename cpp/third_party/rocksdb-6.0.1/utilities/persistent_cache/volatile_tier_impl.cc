//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE

#include "utilities/persistent_cache/volatile_tier_impl.h"

#include <string>

namespace rocksdb {

void VolatileCacheTier::DeleteCacheData(VolatileCacheTier::CacheData* data) {
  assert(data);
  delete data;
}

VolatileCacheTier::~VolatileCacheTier() { index_.Clear(&DeleteCacheData); }

PersistentCache::StatsType VolatileCacheTier::Stats() {
  std::map<std::string, double> stat;
  stat.insert({"persistent_cache.volatile_cache.hits",
               static_cast<double>(stats_.cache_hits_)});
  stat.insert({"persistent_cache.volatile_cache.misses",
               static_cast<double>(stats_.cache_misses_)});
  stat.insert({"persistent_cache.volatile_cache.inserts",
               static_cast<double>(stats_.cache_inserts_)});
  stat.insert({"persistent_cache.volatile_cache.evicts",
               static_cast<double>(stats_.cache_evicts_)});
  stat.insert({"persistent_cache.volatile_cache.hit_pct",
               static_cast<double>(stats_.CacheHitPct())});
  stat.insert({"persistent_cache.volatile_cache.miss_pct",
               static_cast<double>(stats_.CacheMissPct())});

  auto out = PersistentCacheTier::Stats();
  out.push_back(stat);
  return out;
}

Status VolatileCacheTier::Insert(const Slice& page_key, const char* data,
                                 const size_t size) {
  // precondition
  assert(data);
  assert(size);

  // increment the size
  size_ += size;

  // check if we have overshot the limit, if so evict some space
  while (size_ > max_size_) {
    if (!Evict()) {
      // unable to evict data, we give up so we don't spike read
      // latency
      assert(size_ >= size);
      size_ -= size;
      return Status::TryAgain("Unable to evict any data");
    }
  }

  assert(size_ >= size);

  // insert order: LRU, followed by index
  std::string key(page_key.data(), page_key.size());
  std::string value(data, size);
  std::unique_ptr<CacheData> cache_data(
      new CacheData(std::move(key), std::move(value)));
  bool ok = index_.Insert(cache_data.get());
  if (!ok) {
    // decrement the size that we incremented ahead of time
    assert(size_ >= size);
    size_ -= size;
    // failed to insert to cache, block already in cache
    return Status::TryAgain("key already exists in volatile cache");
  }

  cache_data.release();
  stats_.cache_inserts_++;
  return Status::OK();
}

Status VolatileCacheTier::Lookup(const Slice& page_key,
                                 std::unique_ptr<char[]>* result,
                                 size_t* size) {
  CacheData key(std::move(page_key.ToString()));
  CacheData* kv;
  bool ok = index_.Find(&key, &kv);
  if (ok) {
    // set return data
    result->reset(new char[kv->value.size()]);
    memcpy(result->get(), kv->value.c_str(), kv->value.size());
    *size = kv->value.size();
    // drop the reference on cache data
    kv->refs_--;
    // update stats
    stats_.cache_hits_++;
    return Status::OK();
  }

  stats_.cache_misses_++;

  if (next_tier()) {
    return next_tier()->Lookup(page_key, result, size);
  }

  return Status::NotFound("key not found in volatile cache");
}

bool VolatileCacheTier::Erase(const Slice& /*key*/) {
  assert(!"not supported");
  return true;
}

bool VolatileCacheTier::Evict() {
  CacheData* edata = index_.Evict();
  if (!edata) {
    // not able to evict any object
    return false;
  }

  stats_.cache_evicts_++;

  // push the evicted object to the next level
  if (next_tier()) {
    next_tier()->Insert(Slice(edata->key), edata->value.c_str(),
                        edata->value.size());
  }

  // adjust size and destroy data
  size_ -= edata->value.size();
  delete edata;

  return true;
}

}  // namespace rocksdb

#endif
