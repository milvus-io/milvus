// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//

#include "util/compression_context_cache.h"

#include "util/compression.h"
#include "util/core_local.h"

#include <atomic>

namespace rocksdb {
namespace compression_cache {

void* const SentinelValue = nullptr;
// Cache ZSTD uncompression contexts for reads
// if needed we can add ZSTD compression context caching
// which is currently is not done since BlockBasedTableBuilder
// simply creates one compression context per new SST file.
struct ZSTDCachedData {
  // We choose to cache the below structure instead of a ptr
  // because we want to avoid a) native types leak b) make
  // cache use transparent for the user
  ZSTDUncompressCachedData uncomp_cached_data_;
  std::atomic<void*> zstd_uncomp_sentinel_;

  char
      padding[(CACHE_LINE_SIZE -
               (sizeof(ZSTDUncompressCachedData) + sizeof(std::atomic<void*>)) %
                   CACHE_LINE_SIZE)];  // unused padding field

  ZSTDCachedData() : zstd_uncomp_sentinel_(&uncomp_cached_data_) {}
  ZSTDCachedData(const ZSTDCachedData&) = delete;
  ZSTDCachedData& operator=(const ZSTDCachedData&) = delete;

  ZSTDUncompressCachedData GetUncompressData(int64_t idx) {
    ZSTDUncompressCachedData result;
    void* expected = &uncomp_cached_data_;
    if (zstd_uncomp_sentinel_.compare_exchange_strong(expected,
                                                      SentinelValue)) {
      uncomp_cached_data_.CreateIfNeeded();
      result.InitFromCache(uncomp_cached_data_, idx);
    } else {
      // Creates one time use data
      result.CreateIfNeeded();
    }
    return result;
  }
  // Return the entry back into circulation
  // This is executed only when we successfully obtained
  // in the first place
  void ReturnUncompressData() {
    if (zstd_uncomp_sentinel_.exchange(&uncomp_cached_data_) != SentinelValue) {
      // Means we are returning while not having it acquired.
      assert(false);
    }
  }
};
static_assert(sizeof(ZSTDCachedData) % CACHE_LINE_SIZE == 0,
              "Expected CACHE_LINE_SIZE alignment");
}  // namespace compression_cache

using namespace compression_cache;

class CompressionContextCache::Rep {
 public:
  Rep() {}
  ZSTDUncompressCachedData GetZSTDUncompressData() {
    auto p = per_core_uncompr_.AccessElementAndIndex();
    int64_t idx = static_cast<int64_t>(p.second);
    return p.first->GetUncompressData(idx);
  }
  void ReturnZSTDUncompressData(int64_t idx) {
    assert(idx >= 0);
    auto* cn = per_core_uncompr_.AccessAtCore(static_cast<size_t>(idx));
    cn->ReturnUncompressData();
  }

 private:
  CoreLocalArray<ZSTDCachedData> per_core_uncompr_;
};

CompressionContextCache::CompressionContextCache() : rep_(new Rep()) {}

CompressionContextCache* CompressionContextCache::Instance() {
  static CompressionContextCache instance;
  return &instance;
}

void CompressionContextCache::InitSingleton() { Instance(); }

ZSTDUncompressCachedData
CompressionContextCache::GetCachedZSTDUncompressData() {
  return rep_->GetZSTDUncompressData();
}

void CompressionContextCache::ReturnCachedZSTDUncompressData(int64_t idx) {
  rep_->ReturnZSTDUncompressData(idx);
}

CompressionContextCache::~CompressionContextCache() { delete rep_; }

}  // namespace rocksdb
