//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#ifndef ROCKSDB_LITE

#include "utilities/persistent_cache/block_cache_tier_metadata.h"

#include <functional>

namespace rocksdb {

bool BlockCacheTierMetadata::Insert(BlockCacheFile* file) {
  return cache_file_index_.Insert(file);
}

BlockCacheFile* BlockCacheTierMetadata::Lookup(const uint32_t cache_id) {
  BlockCacheFile* ret = nullptr;
  BlockCacheFile lookup_key(cache_id);
  bool ok = cache_file_index_.Find(&lookup_key, &ret);
  if (ok) {
    assert(ret->refs_);
    return ret;
  }
  return nullptr;
}

BlockCacheFile* BlockCacheTierMetadata::Evict() {
  using std::placeholders::_1;
  auto fn = std::bind(&BlockCacheTierMetadata::RemoveAllKeys, this, _1);
  return cache_file_index_.Evict(fn);
}

void BlockCacheTierMetadata::Clear() {
  cache_file_index_.Clear([](BlockCacheFile* arg){ delete arg; });
  block_index_.Clear([](BlockInfo* arg){ delete arg; });
}

BlockInfo* BlockCacheTierMetadata::Insert(const Slice& key, const LBA& lba) {
  std::unique_ptr<BlockInfo> binfo(new BlockInfo(key, lba));
  if (!block_index_.Insert(binfo.get())) {
    return nullptr;
  }
  return binfo.release();
}

bool BlockCacheTierMetadata::Lookup(const Slice& key, LBA* lba) {
  BlockInfo lookup_key(key);
  BlockInfo* block;
  port::RWMutex* rlock = nullptr;
  if (!block_index_.Find(&lookup_key, &block, &rlock)) {
    return false;
  }

  ReadUnlock _(rlock);
  assert(block->key_ == key.ToString());
  if (lba) {
    *lba = block->lba_;
  }
  return true;
}

BlockInfo* BlockCacheTierMetadata::Remove(const Slice& key) {
  BlockInfo lookup_key(key);
  BlockInfo* binfo = nullptr;
  bool ok __attribute__((__unused__));
  ok = block_index_.Erase(&lookup_key, &binfo);
  assert(ok);
  return binfo;
}

void BlockCacheTierMetadata::RemoveAllKeys(BlockCacheFile* f) {
  for (BlockInfo* binfo : f->block_infos()) {
    BlockInfo* tmp = nullptr;
    bool status = block_index_.Erase(binfo, &tmp);
    (void)status;
    assert(status);
    assert(tmp == binfo);
    delete binfo;
  }
  f->block_infos().clear();
}

}  // namespace rocksdb

#endif
